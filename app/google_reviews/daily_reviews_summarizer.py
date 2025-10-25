"""
Daily Reviews Summarizer
This module takes data in type of DailySummaryRequest and returns a DailySummary.
It handles both the analysis and database operations for review summarization.
"""

from uuid import uuid4
import logging
from datetime import date, datetime, timedelta, timezone
from typing import List, Dict, Optional, Tuple, Any, Union
from app.models.summary_models import (
    DailySummaryRequest, 
    DailySummaryState, 
    DailySummary, 
    ProcessingError,
    SentimentDistribution,
    BusinessImpact,
    AppReviewAnalysis,
    DailySummaryError
)
from app.models.pydantic_models import ReviewFilter, Review
from app.google_reviews.get_reviews import get_reviews
from app.graph.daily_summary_graph import build_graph
from app.agents.daily_summary.daily_summary_agent_MVP import daily_summary_node
from app.google_reviews.save_daily_summary import save_daily_summary, mark_daily_summary_failed
from app.google_reviews.save_analyzed_reviews import save_review_analysis, mark_review_analysis_failed
from itertools import groupby
from operator import attrgetter

logger = logging.getLogger(__name__)

def convert_review_to_analysis(review: Review) -> AppReviewAnalysis:
    """
    Convert a Review object to an AppReviewAnalysis object.
    
    Args:
        review: Review object from the database
        
    Returns:
        AppReviewAnalysis: Analysis object ready for processing
    """
    return AppReviewAnalysis(
        review_id=review.review_id,
        app_id=review.app_id,
        review_created_at=review.review_created_at,
        content=review.content,
        score=review.score,
        sentiment=None,  # These will be populated during analysis
        aspects=None,
        action_items=None,
        opportunities=None,
        roadmap=None,
        positive_feedback=None,
        response_recommendation=None,
        sentiment_attempts=0,
        aspects_attempts=0,
        action_items_attempts=0,
        opportunities_attempts=0,
        roadmap_attempts=0,
        positive_feedback_attempts=0,
        response_recommendation_attempts=0
    )

async def process_review_batches(
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
    test_mode: bool = False,
    save_individual_reviews: bool = True
) -> Union[List[Tuple[date, DailySummary]], int]:
    """
    Process batches of reviews and generate daily summaries.
    
    Args:
        batch_size: Number of reviews to process in each batch
        max_reviews: Maximum number of reviews to analyze in total
        app_id: Optional app ID to filter reviews
        min_date: Only analyze reviews after this date
        max_date: Only analyze reviews before this date
        date_list: Only analyze reviews on these dates
        min_score: Only analyze reviews with score >= this value (1-5)
        max_score: Only analyze reviews with score <= this value (1-5)
        analyzed: If True, include already analyzed reviews
        reanalyze: If True, reanalyze reviews even if analyzed before
        test_mode: If True, only performs basic analysis without running the full graph
        save_individual_reviews: If True, saves analysis results for individual reviews
        
    Returns:
        Union[List[Tuple[date, DailySummary]], int]: 
            In normal mode: List of tuples containing (date, summary) pairs
            In test mode: Number of successfully processed reviews
    """
    reviews_remaining = max_reviews if max_reviews else float('inf')
    daily_summaries = []
    current_offset = 0
    total_processed = 0
    
    try:
        while reviews_remaining > 0:
            current_batch_size = min(batch_size, reviews_remaining)
            
            # Create filter for reviews
            filters = ReviewFilter(
                app_id=app_id,
                limit=current_batch_size,
                offset=current_offset,
                order_by="review_created_at",
                order_direction="desc",
                from_date=min_date,
                to_date=max_date,
                date_list=date_list,
                username=None,
                review_id=None,
                analyzed=analyzed
            )
            
            # Get reviews based on filters
            reviews = await get_reviews(filters)
            
            if not reviews:
                logger.info("No more reviews found matching the criteria")
                break
                
            logger.info(f"Found {len(reviews)} reviews to process")
            
            # Convert Review objects to AppReviewAnalysis objects
            analysis_reviews = [convert_review_to_analysis(review) for review in reviews]
            
            # Process this batch of reviews
            try:
                # Group and process reviews by date
                batch_results = await process_reviews_by_date(
                    analysis_reviews, 
                    test_mode=test_mode,
                    save_individual_reviews=save_individual_reviews
                )
                
                if test_mode:
                    total_processed += len(reviews)
                else:
                    # Add results to our list
                    for day, summary in batch_results.items():
                        daily_summaries.append((day, summary))
                        logger.info(f"Added summary for date {day}")
                
            except Exception as e:
                logger.error(f"Error processing batch: {str(e)}")
                # Continue with next batch
                
            # Update counters
            reviews_remaining -= len(reviews)
            current_offset += len(reviews)
            
            logger.info(f"Processed batch. Reviews remaining: {reviews_remaining}")
            
            # If we got fewer reviews than requested, we're done
            if len(reviews) < current_batch_size:
                break
        
        if test_mode:
            logger.info(f"Test mode complete. Total reviews processed: {total_processed}")
            return total_processed
        else:
            # Sort results by date
            daily_summaries.sort(key=lambda x: x[0])
            logger.info(f"Completed processing all batches. Total days processed: {len(daily_summaries)}")
            return daily_summaries
        
    except Exception as e:
        logger.error(f"Error in batch processing: {str(e)}")
        return total_processed if test_mode else daily_summaries

async def process_reviews_by_date(
    reviews: List[AppReviewAnalysis], 
    test_mode: bool = False,
    save_individual_reviews: bool = True
) -> Dict[date, DailySummary]:
    """
    Group reviews by date and process each day's reviews through the analysis graph.
    
    Args:
        reviews: List of AppReviewAnalysis objects to process
        test_mode: If True, only performs basic analysis without running the full graph
        save_individual_reviews: If True, saves analysis results for individual reviews
        
    Returns:
        Dict[date, DailySummary]: Dictionary mapping dates to their analysis results
    """
    # Sort reviews by date for grouping
    sorted_reviews = sorted(reviews, key=lambda x: x.review_created_at.date())
    
    # Group reviews by date
    grouped_results = {}
    for day, day_reviews in groupby(sorted_reviews, key=lambda x: x.review_created_at.date()):
        day_reviews_list = list(day_reviews)
        logger.info(f"Processing {len(day_reviews_list)} reviews for date: {day}")
        
        # Create a DailySummaryRequest for this day's reviews
        request = DailySummaryRequest(
            summary_date=day,
            app_id=day_reviews_list[0].app_id,  # All reviews in a group should be for the same app
            review_analysis=day_reviews_list
        )
        
        try:
            # Process the day's reviews through the graph
            result = await perform_daily_analysis(request, test_mode)
            
            # Save individual review analysis results if requested
            if save_individual_reviews:
                for review in day_reviews_list:
                    try:
                        # Check for critical errors in the analysis
                        has_critical_errors = False
                        error_details = []

                        if hasattr(review, 'error') and review.error:
                            has_critical_errors = True
                            error_details.append(f"Review analysis error: {review.error}")

                        if has_critical_errors:
                            error_msg = f"Analysis failed for review {review.review_id}. Errors: {'; '.join(error_details)}"
                            logger.error(error_msg)
                            
                            # Mark the review as failed
                            error_data = {
                                "error_message": error_msg,
                                "error_details": error_details,
                                "failed_at": datetime.now(timezone.utc).isoformat(),
                                "analysis_results": review.model_dump()
                            }
                            
                            if mark_review_analysis_failed(review.review_id, review.app_id, error_data):
                                logger.info(f"Marked review {review.review_id} as failed")
                            else:
                                logger.error(f"Failed to mark review {review.review_id} as failed")
                            
                            continue
                        
                        # Save successful analysis
                        if save_review_analysis(
                            review_id=review.review_id,
                            analysis_data=review.model_dump(),
                            app_id=review.app_id
                        ):
                            logger.info(f"Saved analysis for review {review.review_id}")
                        else:
                            logger.error(f"Failed to save analysis for review {review.review_id}")
                            
                    except Exception as e:
                        logger.error(f"Error saving review analysis for {review.review_id}: {str(e)}")
            
            # Save the daily summary
            if save_daily_summary(result):
                logger.info(f"Successfully saved daily summary for date {day}")
            else:
                logger.error(f"Failed to save daily summary for date {day}")
            
            grouped_results[day] = result
            logger.info(f"Successfully processed reviews for date {day}")
            
        except Exception as e:
            error_msg = f"Error processing daily summary for date {day}: {str(e)}"
            logger.error(error_msg)
            
            # Create Error object
            error = ProcessingError(
                agent="daily_summary_processor",
                error_message=error_msg
            )
            
            # Save the error state
            error_details = {
                "agent": "daily_summarizer",
                "error_message": error_msg,
                "failed_at": datetime.now().isoformat()
            }
            if mark_daily_summary_failed(request.app_id, day, error_details):
                logger.info(f"Successfully marked daily summary as failed for date {day}")
            else:
                logger.error(f"Failed to mark daily summary as failed for date {day}")
            
            # Create a DailySummary with error
            error_summary = DailySummary(
                summary_date=day,
                app_id=request.app_id,
                sentiment_distribution=SentimentDistribution(),
                issue_groups=[],
                feature_areas={},
                actions=[],
                business_impact=BusinessImpact(
                    severity="low",
                    affected_areas=[],
                    metrics_impact={
                        "user_retention": False,
                        "app_rating": False,
                        "user_acquisition": False
                    },
                    recommendation="",
                    confidence=0.0
                ),
                error=DailySummaryError(
                    agent="daily_summary_processor",
                    error_message=error_msg
                )
            )
            
            grouped_results[day] = error_summary
    
    return grouped_results

async def perform_daily_analysis(daily_summary_request: DailySummaryRequest, test_mode: bool = False) -> DailySummary:
    """
    Performs a comprehensive analysis of a given daily summary request using a predefined graph.
    In test mode, only performs daily summary by calling the daily summary node directly.

    Args:
        daily_summary_request (DailySummaryRequest): The daily summary request to be analyzed.
        test_mode (bool): If True, only performs sentiment analysis without running the full graph.

    Returns:
        DailySummary: The analysis results for the day.
    """
    logger.info(f"Starting daily summary analysis for app_id: {daily_summary_request.app_id} on date: {daily_summary_request.summary_date}")

    # Create the DailySummary object with initialized fields
    daily_summary = DailySummary(
        summary_date=daily_summary_request.summary_date,
        app_id=daily_summary_request.app_id,
        sentiment_distribution=SentimentDistribution(),  # Initialize with default values
        issue_groups=[],  # Will be populated by the graph
        feature_areas={},  # Will be populated by the graph
        actions=[],  # Will be populated by the graph
        business_impact=BusinessImpact(  # Initialize with required fields
            severity="low",
            affected_areas=[],
            metrics_impact={
                "user_retention": False,
                "app_rating": False,
                "user_acquisition": False
            },
            recommendation="",
            confidence=0.0
        )
    )

    # Create the initial state
    initial_state = DailySummaryState(
        daily_summary_request=daily_summary_request,
        daily_summary=daily_summary,
        node_history=[],
        current_step="daily_summary",
        error=None
    )

    logger.debug(f"Initialized state for date: {daily_summary.summary_date}")

    try:
        if test_mode:
            # In test mode, call the daily summary node directly
            result = await daily_summary_node(initial_state.model_dump())
            final_state = DailySummaryState(**result)
        else:
            # Full analysis using the graph
            graph = build_graph()
            results = await graph.abatch([initial_state.model_dump()])
            final_state = DailySummaryState(**results[0])
            
        # Log completion status
        logger.info(f"Completed analysis for date: {final_state.daily_summary.summary_date}")
        
        # Log node history for debugging
        for node in final_state.node_history:
            logger.debug(f"Node {node.get('node_name')} completed with status: {node.get('status', 'unknown')}")
        
        return final_state.daily_summary

    except Exception as e:
        error_msg = f"Error processing daily summary: {str(e)}"
        logger.error(error_msg)
        
        daily_summary.error = DailySummaryError(
            agent="daily_summary_processor",
            error_message=error_msg
        )
        
        return daily_summary

async def test_review_analysis():
    """
    Test function to analyze a small batch of reviews with different scenarios.
    """
    logger.info("Starting review analysis test")
    
    total_analyzed = 0
    
    # Test Case 1: Basic batch processing
    logger.info("\nTest Case 1: Basic batch processing (unanalyzed reviews)")
    result1 = await process_review_batches(
        max_reviews=20,
        batch_size=5,
        analyzed=False,
        test_mode=True
    )
    logger.info(f"Test Case 1 Results: Analyzed {result1} reviews")
    total_analyzed += result1
    
    # Test Case 2: Specific app reviews
    logger.info("\nTest Case 2: Specific app reviews")
    result2 = await process_review_batches(
        app_id="com.example.app",
        max_reviews=3,
        min_score=4,
        analyzed=False,
        test_mode=True
    )
    logger.info(f"Test Case 2 Results: Analyzed {result2} reviews")
    total_analyzed += result2
    
    # Test Case 3: Recent reviews
    logger.info("\nTest Case 3: Recent reviews (last 7 days)")
    result3 = await process_review_batches(
        max_reviews=3,
        min_date=datetime.now(timezone.utc) - timedelta(days=7),
        max_date=datetime.now(timezone.utc),
        analyzed=False,
        test_mode=True
    )
    logger.info(f"Test Case 3 Results: Analyzed {result3} reviews")
    total_analyzed += result3
    
    # Test Case 4: Reanalyze existing reviews
    logger.info("\nTest Case 4: Reanalyze existing reviews")
    result4 = await process_review_batches(
        max_reviews=2,
        analyzed=True,
        reanalyze=True,
        test_mode=True
    )
    logger.info(f"Test Case 4 Results: Reanalyzed {result4} reviews")
    total_analyzed += result4
    
    logger.info(f"\nTest Complete: Total reviews analyzed across all tests: {total_analyzed}")
    return total_analyzed

if __name__ == "__main__":
    import asyncio
    asyncio.run(test_review_analysis())
