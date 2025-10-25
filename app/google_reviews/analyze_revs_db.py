# Get reviews from the database
# Analyze them
# Get in batches
from datetime import datetime, timezone, timedelta
from typing import List, Optional
from app.models.pydantic_models import ReviewFilter, Review
from app.google_reviews.get_reviews import get_reviews
from app.google_reviews.review_analyzer import perform_review_analysis
from app.google_reviews.save_analyzed_reviews import save_review_analysis, mark_review_analysis_failed
from app.shared_services.logger_setup import setup_logger

logger = setup_logger()

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
    reanalyze: bool = False
) -> int:
    """
    Get reviews from database based on filters, analyze them, and save the results.
    Only saves reviews that have critical errors in their analysis (top-level or node processing errors).
    Component-specific errors (like positive feedback errors) don't prevent saving.
    
    Args:
        batch_size: Number of reviews to process in each batch
        max_reviews: Maximum number of reviews to analyze in total
        app_id: Optional app ID to filter reviews for a specific app (if None, analyze reviews from all apps)
        min_date: Only analyze reviews after this date
        max_date: Only analyze reviews before this date
        date_list: Only analyze reviews on these dates
        min_score: Only analyze reviews with score >= this value (1-5)
        max_score: Only analyze reviews with score <= this value (1-5)
        analyzed: If True, include already analyzed reviews
        reanalyze: If True, reanalyze reviews even if they were analyzed before
        
    Returns:
        int: Number of reviews successfully analyzed
    """
    total_analyzed = 0
    reviews_remaining = max_reviews if max_reviews else float('inf')
    
    try:
        while reviews_remaining > 0:
            current_batch_size = min(batch_size, reviews_remaining)
            
            # Create filter for reviews
            filters = ReviewFilter(
                app_id='com.kcb.mobilebanking.android.mbp',
                limit=current_batch_size,
                order_by="review_created_at",
                order_direction="desc",
                from_date=min_date,
                to_date=max_date,
                date_list=date_list,
                username=None,  # Make these optional in the model
                review_id=None,
                analyzed=analyzed
            )
            
            # Get reviews based on filters
            reviews = await get_reviews(filters)
            
            if not reviews:
                logger.info("No more reviews found matching the criteria")
                break
                
            logger.info(f"Found {len(reviews)} reviews to process")
            
            # Process each review in the batch
            for review in reviews:
                try:
                    # Apply score filters if specified
                    if min_score is not None and review.score < min_score:
                        continue
                    if max_score is not None and review.score > max_score:
                        continue
                        
                    # Skip already analyzed reviews unless reanalyze is True
                    if not reanalyze and review.analyzed and not analyzed:
                        continue
                    
                    # Log the review content for debugging
                    logger.info(f"Starting internal review analysis for content: {review.content[:100]}...")
                    
                    # Perform the analysis
                    analysis_results = await perform_review_analysis(review.content)
                    
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
                            logger.info(f"Successfully marked review {review.review_id} as failed")
                        else:
                            logger.error(f"Failed to mark review {review.review_id} as failed in database")
                        
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
                    continue
                
                reviews_remaining -= 1
                if reviews_remaining <= 0:
                    break
            
            logger.info(f"Completed batch. Total reviews analyzed so far: {total_analyzed}")
            
            # If we got fewer reviews than requested, we're done
            if len(reviews) < current_batch_size:
                break
        
        logger.info(f"Analysis complete. Total reviews analyzed: {total_analyzed}")
        return total_analyzed
        
    except Exception as e:
        logger.error(f"Error in analyze_reviews: {e}")
        return total_analyzed

async def test_review_analysis(
                           app_id: str ,
                            start_date: datetime , 
                           end_date: datetime ,
                           batch_size: int = 5,
                           max_reviews_per_day: int = 200000):
    """
    Test function to analyze reviews day by day between start_date and end_date.
    
    Args:
        start_date: Start date for analysis (inclusive)
        end_date: End date for analysis (inclusive)
        batch_size: Number of reviews to process in each batch
        max_reviews_per_day: Maximum number of reviews to analyze per day
    """
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
        
        # Move to next day
        current_date += timedelta(days=1)
    
    logger.info(f"\nAnalysis Complete: Total reviews analyzed across all dates: {total_reviews}")
    return total_reviews

if __name__ == "__main__":
    import asyncio
    
    # Example usage:
    # Process reviews from March 1st to March 5th, 2025
    app_id = 'ke.co.equitygroup.equitymobile'
    start_date = datetime(2025, 5, 1)
    end_date = datetime(2025, 5, 15)
    
    asyncio.run(test_review_analysis(start_date=start_date, end_date=end_date, app_id=app_id))









