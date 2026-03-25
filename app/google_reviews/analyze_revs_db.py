from datetime import datetime, timezone, timedelta
from typing import List, Optional, Dict, Any
import asyncio

from pydantic.types import T
from app.models.pydantic_models import ReviewFilter, Review
from app.google_reviews.get_reviews import get_reviews
from app.google_reviews.review_analyzer import perform_review_analysis
from app.google_reviews.save_analyzed_reviews import (
    save_review_analysis, 
    mark_review_analysis_failed
)
from app.shared_services.logger_setup import setup_logger

logger = setup_logger()


async def _process_review_concurrent(review: Review, min_score: Optional[float], max_score: Optional[float], 
                                     reanalyze: bool, delay_between_reviews: float) -> Dict[str, Any]:
    try:
        # Apply score filters
        if min_score is not None and review.score < min_score:
            return {'success': False, 'review_id': review.review_id, 'skipped': True}
        if max_score is not None and review.score > max_score:
            return {'success': False, 'review_id': review.review_id, 'skipped': True}
        
        # Skip already analyzed reviews unless reanalyze is True
        if not reanalyze and review.analyzed:
            return {'success': False, 'review_id': review.review_id, 'skipped': True}
        
        # Rate limiting delay
        if delay_between_reviews > 0:
            await asyncio.sleep(delay_between_reviews)
        
        # Perform analysis
        analysis_results = await perform_review_analysis(review.content)
        
        # Check for critical errors
        has_critical_errors = False
        error_details = []
        
        if analysis_results.get('error'):
            has_critical_errors = True
            error_details.append(f"Top-level error: {analysis_results.get('error')}")
        
        node_history = analysis_results.get('node_history', [])
        for node in node_history:
            if node.get('error'):
                has_critical_errors = True
                error_details.append(f"Node {node.get('node_name')} error: {node['error'].get('error_message', str(node['error']))}")
        
        if has_critical_errors:
            error_msg = f"Analysis failed for review {review.review_id}. Critical errors: {'; '.join(error_details)}"
            error_data = {
                "error_message": error_msg,
                "error_details": error_details,
                "failed_at": datetime.now(timezone.utc).isoformat(),
                "analysis_results": analysis_results
            }
            mark_review_analysis_failed(review.review_id, review.app_id, error_data)
            return {'success': False, 'review_id': review.review_id, 'error': error_msg, 'failed': True}
        
        # Save the analysis
        if save_review_analysis(review_id=review.review_id, analysis_data=analysis_results, app_id=review.app_id):
            return {'success': True, 'review_id': review.review_id}
        else:
            return {'success': False, 'review_id': review.review_id, 'error': 'Failed to save analysis'}
            
    except Exception as e:
        error_type = type(e).__name__
        error_msg = str(e)
        
        # Check for critical errors
        is_critical_error = any(keyword in error_msg.lower() for keyword in [
            'token', 'quota', 'rate limit', 'rate', 'limit exceeded', 
            '429', 'insufficient quota', 'billing', 'credit',
            'insufficient tokens', 'out of tokens', 'token limit',
            'account', 'subscription', 'payment required'
        ])
        
        error_data = {
            "error_message": f"{'Critical error' if is_critical_error else 'Analysis exception'}: {error_type} - {error_msg}",
            "error_details": [f"Exception type: {error_type}", f"Error message: {error_msg}"],
            "failed_at": datetime.now(timezone.utc).isoformat(),
            "is_token_error": is_critical_error,
            "is_critical": is_critical_error,
            "analysis_results": None
        }
        
        mark_review_analysis_failed(review.review_id, review.app_id, error_data)
        
        if is_critical_error:
            raise  # Re-raise critical errors to stop processing
        
        return {
            'success': False, 
            'review_id': review.review_id, 
            'error': f"{error_type}: {error_msg}",
            'is_critical': is_critical_error
        }


async def analyze_reviews(
    batch_size: int = 10,
    max_reviews: Optional[int] = None,
    app_id: Optional[str] = None,
    min_date: Optional[datetime] = None,
    max_date: Optional[datetime] = None,
    date_list: Optional[List[datetime]] = None,
    min_score: Optional[float] = None,
    max_score: Optional[float] = None,
    analyzed: bool = False,
    reanalyze: bool = False,
    stale_analysis: bool = False,
    delay_between_reviews: float = 60.0,
    concurrent: bool = False,
    max_concurrent: int = 5
) -> int:
    """
    Get reviews from database based on filters, analyze them, and save the results.
    
    Returns:
        int: Number of reviews successfully analyzed
    """
    total_analyzed = 0
    total_failed = 0
    reviews_remaining = max_reviews if max_reviews else float('inf')
    
    # Log what we're analyzing
    logger.info(f"Starting review analysis:")
    logger.info(f"  - Processing mode: {'CONCURRENT (asyncio)' if concurrent else 'SEQUENTIAL'}")
    if concurrent:
        logger.info(f"  - max_concurrent: {max_concurrent}")
    logger.info(f"  - batch_size: {batch_size}")
    logger.info(f"  - app_id: {app_id if app_id else 'ALL APPS'}")
    logger.info(f"  - min_date: {min_date if min_date else 'None (all dates)'}")
    logger.info(f"  - max_date: {max_date if max_date else 'None (all dates)'}")
    if date_list:
        logger.info(f"  - date_list: {len(date_list)} specific dates")
    else:
        logger.info("  - date_list: None")
    if stale_analysis and not reanalyze:
        reanalyze = True
        logger.info("  - stale_analysis=True implies reanalyze=True")
    logger.info(f"  - analyzed filter: {analyzed} (False = only unanalyzed)")
    logger.info(f"  - reanalyze: {reanalyze}")
    logger.info(f"  - stale_analysis: {stale_analysis}")
    logger.info(f"  - max_reviews: {max_reviews if max_reviews else 'Unlimited'}")
    
    try:
        while reviews_remaining > 0:
            current_batch_size = min(batch_size, reviews_remaining)
            
            # Create filter for reviews
            # If analyzed=False (default), only get unanalyzed reviews
            # If reanalyze=True, get already analyzed reviews too
            # If reanalyze=True, we need to include analyzed reviews in the filter
            filters = ReviewFilter(
                app_id=app_id,  # Use the app_id parameter - can be None to analyze all apps
                limit=current_batch_size,
                order_by="review_created_at",
                order_direction="desc",
                from_date=min_date,  # Optional - if None, no date filter
                to_date=max_date,    # Optional - if None, no date filter
                date_list=date_list,  # Optional - if None, no specific date filter
                username=None,
                review_id=None,
                analyzed=(None if stale_analysis else (analyzed or reanalyze)),
                stale_analysis=True if stale_analysis else None,
            )
            
            # Get reviews based on filters
            reviews = await get_reviews(filters)
            
            if not reviews:
                logger.info("No more reviews found matching the criteria")
                break
                
            logger.info(f"Found {len(reviews)} reviews to process")
            
            # Filter reviews based on score and analyzed status before processing
            reviews_to_process = []
            for review in reviews:
                # Apply score filters if specified
                if min_score is not None and review.score < min_score:
                    logger.debug(f"Skipping review {review.review_id} - score {review.score} < min_score {min_score}")
                    continue
                if max_score is not None and review.score > max_score:
                    logger.debug(f"Skipping review {review.review_id} - score {review.score} > max_score {max_score}")
                    continue
                    
                # Skip already analyzed reviews unless reanalyze is True
                if not reanalyze and review.analyzed:
                    logger.debug(f"Skipping review {review.review_id} - already analyzed and reanalyze=False")
                    continue
                
                reviews_to_process.append(review)
            
            if not reviews_to_process:
                logger.info("No reviews to process after filtering")
                if len(reviews) < current_batch_size:
                    break
                continue
            
            # Process reviews based on the selected mode
            if concurrent:
                # CONCURRENT MODE: Process reviews concurrently using asyncio with concurrency limit
                semaphore = asyncio.Semaphore(max_concurrent)
                
                async def process_with_semaphore(review: Review):
                    """Wrapper to limit concurrency using semaphore"""
                    async with semaphore:
                        return await _process_review_concurrent(review, min_score, max_score, reanalyze, delay_between_reviews)
                
                tasks = [
                    process_with_semaphore(review)
                    for review in reviews_to_process
                ]
                
                results = await asyncio.gather(*tasks, return_exceptions=True)
                
                for result in results:
                    if isinstance(result, Exception):
                        # Check if it's a critical error
                        error_msg = str(result)
                        is_critical = any(keyword in error_msg.lower() for keyword in [
                            'token', 'quota', 'rate limit', 'rate', 'limit exceeded', 
                            '429', 'insufficient quota', 'billing', 'credit',
                            'insufficient tokens', 'out of tokens', 'token limit',
                            'account', 'subscription', 'payment required'
                        ])
                        
                        if is_critical:
                            logger.error(f"CRITICAL error in concurrent processing: {error_msg}")
                            raise result
                        else:
                            total_failed += 1
                            logger.error(f"Error in concurrent processing: {error_msg}")
                    elif isinstance(result, dict):
                        if result.get('success'):
                            total_analyzed += 1
                            logger.info(f"Successfully analyzed and saved review {result.get('review_id')}")
                        elif result.get('skipped'):
                            logger.debug(f"Skipped review {result.get('review_id')}")
                        else:
                            total_failed += 1
                            error = result.get('error', 'Unknown error')
                            logger.error(f"Failed to process review {result.get('review_id')}: {error}")
                            
                            # If critical error, stop processing
                            if result.get('is_critical'):
                                logger.error(f"CRITICAL error detected. Stopping process.")
                                raise Exception(f"Critical error: {error}")
                        
                        # Update reviews_remaining for successful/skipped/failed reviews
                        if not result.get('skipped'):
                            reviews_remaining -= 1
                            if reviews_remaining <= 0:
                                break
            
            else:
                # SEQUENTIAL MODE: Process reviews one by one (original behavior)
                for review in reviews_to_process:
                    try:
                        # Log the review content for debugging
                        logger.info(f"Starting analysis for review {review.review_id} (app_id: {review.app_id}) - content: {review.content[:100]}...")
                        
                        # Rate limiting: Add delay before processing to avoid hitting API limits
                        if delay_between_reviews > 0:
                            await asyncio.sleep(delay_between_reviews)
                        
                        # Perform the analysis with error handling
                        try:
                            analysis_results = await perform_review_analysis(review.content)
                        except Exception as analysis_error:
                            # Handle LLM/token errors and other analysis failures
                            error_type = type(analysis_error).__name__
                            error_msg = str(analysis_error)
                            
                            # Check for critical LLM/token/billing errors that should STOP the process
                            is_critical_error = any(keyword in error_msg.lower() for keyword in [
                                'token', 'quota', 'rate limit', 'rate', 'limit exceeded', 
                                '429', 'insufficient quota', 'billing', 'credit',
                                'insufficient tokens', 'out of tokens', 'token limit',
                                'account', 'subscription', 'payment required'
                            ])
                            
                            if is_critical_error:
                                # Critical error - stop the entire process
                                logger.error(f"CRITICAL LLM/Billing error for review {review.review_id}: {error_type} - {error_msg}")
                                logger.error("STOPPING analysis process due to critical error (insufficient tokens/quota/billing issue)")
                                
                                # Mark this review as failed
                                error_data = {
                                    "error_message": f"Critical error (process stopped): {error_type} - {error_msg}",
                                    "error_details": [f"Exception type: {error_type}", f"Error message: {error_msg}"],
                                    "failed_at": datetime.now(timezone.utc).isoformat(),
                                    "is_token_error": True,
                                    "is_critical": True,
                                    "analysis_results": None
                                }
                                
                                mark_review_analysis_failed(review.review_id, review.app_id, error_data)
                                
                                # Raise the error to stop the outer loop
                                raise analysis_error
                            else:
                                # Non-critical error - mark review as failed and continue
                                logger.error(f"Analysis error for review {review.review_id}: {error_type} - {error_msg}")
                                
                                error_data = {
                                    "error_message": f"Analysis exception: {error_type} - {error_msg}",
                                    "error_details": [f"Exception type: {error_type}", f"Error message: {error_msg}"],
                                    "failed_at": datetime.now(timezone.utc).isoformat(),
                                    "is_token_error": False,
                                    "is_critical": False,
                                    "analysis_results": None
                                }
                                
                                if mark_review_analysis_failed(review.review_id, review.app_id, error_data):
                                    total_failed += 1
                                    logger.info(f"Marked review {review.review_id} as failed (non-critical), continuing...")
                                else:
                                    logger.error(f"Failed to mark review {review.review_id} as failed")
                                
                                # Continue to next review for non-critical errors
                                continue
                        
                        # Add detailed logging
                        logger.info(f"Analysis results for review {review.review_id}:")
                        logger.info(f"Review content: {review.content[:100]}...")
                        logger.info(f"Analysis content: {analysis_results.get('content', 'No content')[:100]}...")
                        logger.info(f"Full analysis results: {analysis_results}")
                        
                        # Check for critical errors in the analysis
                        has_critical_errors = False
                        error_details = []

                        # Check for top-level error
                        if analysis_results.get('error'):
                            has_critical_errors = True
                            error_details.append(f"Top-level error: {analysis_results.get('error')}")

                        # Check node history for processing errors
                        node_history = analysis_results.get('node_history', [])
                        for node in node_history:
                            if node.get('error'):
                                has_critical_errors = True
                                error_details.append(f"Node {node.get('node_name')} error: {node['error'].get('error_message', str(node['error']))}")

                        if has_critical_errors:
                            error_msg = f"Analysis failed for review {review.review_id}. Critical errors: {'; '.join(error_details)}"
                            logger.error(error_msg)
                            
                            # Mark the review as failed but also as analyzed
                            error_data = {
                                "error_message": error_msg,
                                "error_details": error_details,
                                "failed_at": datetime.now(timezone.utc).isoformat(),
                                "analysis_results": analysis_results
                            }
                            
                            if mark_review_analysis_failed(review.review_id, review.app_id, error_data):
                                total_failed += 1
                                logger.info(f"Successfully marked review {review.review_id} as failed (total failed: {total_failed})")
                            else:
                                logger.error(f"Failed to mark review {review.review_id} as failed in database")
                            
                            # Continue to next review - don't stop the entire process
                            continue
                            
                        # Save the analysis results if no critical errors
                        if save_review_analysis(
                            review_id=review.review_id,
                            analysis_data=analysis_results,
                            app_id=review.app_id
                        ):
                            total_analyzed += 1
                            logger.info(f"Successfully analyzed and saved review {review.review_id} for app {review.app_id}")
                        else:
                            logger.error(f"Failed to save analysis for review {review.review_id}")
                            
                    except Exception as e:
                        logger.error(f"Error processing review {review.review_id}: {e}")
                        # If it's a critical error, re-raise to stop processing
                        error_msg = str(e)
                        is_critical = any(keyword in error_msg.lower() for keyword in [
                            'token', 'quota', 'rate limit', 'rate', 'limit exceeded', 
                            '429', 'insufficient quota', 'billing', 'credit',
                            'insufficient tokens', 'out of tokens', 'token limit',
                            'account', 'subscription', 'payment required'
                        ])
                        if is_critical:
                            raise
                        continue
                    
                    reviews_remaining -= 1
                    if reviews_remaining <= 0:
                        break
            
            logger.info(f"Completed batch. Total analyzed: {total_analyzed}, Total failed: {total_failed}")
            
            # If we got fewer reviews than requested, we're done
            if len(reviews) < current_batch_size:
                logger.info("No more reviews found matching criteria")
                break
        
        logger.info(f"Analysis complete. Total reviews analyzed: {total_analyzed}, Total failed: {total_failed}")
        return total_analyzed
        
    except Exception as e:
        logger.error(f"Critical error in analyze_reviews outer loop: {e}", exc_info=True)
        logger.info(f"Process stopped due to error. Total analyzed before error: {total_analyzed}, Total failed: {total_failed}")
        return total_analyzed

async def test_review_analysis(
                           app_id: str ,
                            start_date: datetime , 
                           end_date: datetime ,
                           batch_size: int = 5,
                           max_reviews_per_day: int = 200000):
    logger.info("Starting review analysis test")
    total_reviews = 0
    
    current_date = start_date
    while current_date <= end_date:
        logger.info(f"\nProcessing reviews for date: {current_date.strftime('%Y-%m-%d')}")
        
        result = await analyze_reviews(
            max_reviews=max_reviews_per_day,
            batch_size=batch_size,
            app_id=app_id,
            date_list=[current_date],
            analyzed=False
        )
        
        logger.info(f"Analyzed {result} reviews for {current_date.strftime('%Y-%m-%d')}")
        total_reviews += result
        current_date += timedelta(days=1)
    
    logger.info(f"\nAnalysis Complete: Total reviews analyzed across all dates: {total_reviews}")
    return total_reviews

if __name__ == "__main__":
    import asyncio

    # Limit analysis to a single app (KCB) just like in reviews_scraper.main()
    # KCB Google Play app id used in reviews_scraper.py:
    #   com.kcb.mobilebanking.android.mbp
    KCB_APP_ID = "com.kcb.mobilebanking.android.mbp"

    # Run end-to-end analysis (canonicalization pipeline) for KCB only.
    # You can tune batch_size / max_concurrent / date filters as needed.
    asyncio.run(
        analyze_reviews(
            app_id=KCB_APP_ID,
            concurrent=True,
            max_concurrent=50,
            batch_size=100,
            analyzed=False,   # only pick unanalyzed reviews
            reanalyze=False,  # set True if you want to re-run on already analyzed
        )
    )









