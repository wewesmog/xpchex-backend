# This is a MVP for the daily reviews summarizer.
# Takes the date and app_id
# Gets the reviews for the date from View
# Sends the reviews to the LLM using the daily_analysis graph

import asyncio
from app.shared_services.db import get_postgres_connection
from app.shared_services.logger_setup import setup_logger
from datetime import date
from typing import List
from pydantic import BaseModel

from app.models.summary_models import DailySummary, DailySummaryRequest, DailySummaryState, DailySummaryError, SentimentDistribution, BusinessImpact
from app.graph.daily_summary_graph import build_graph
from app.google_reviews.save_daily_summary import save_daily_summary, mark_daily_summary_failed 

logger = setup_logger()

# Model for the daily reviews view


def get_reviews_for_date(date: date, app_id: str) -> DailySummaryRequest:
    conn = get_postgres_connection("daily_reviews_view")
    try:
        with conn.cursor() as cur:
            query = """
            SELECT * FROM daily_reviews_view
            WHERE review_date = %s AND app_id = %s
            """
            logger.info(f"Executing query: {str(cur.mogrify(query, (date, app_id)))[:500]}...")
            cur.execute(query, (date, app_id))

            # Get column names from cursor description
            columns = [desc[0] for desc in cur.description]

            rows = cur.fetchall()

            # Convert list of tuples to list of dictionaries
            reviews_list = [dict(zip(columns, row)) for row in rows]

            logger.info(f"Fetched {len(rows)} reviews for date {date} and app_id {app_id}")
            return DailySummaryRequest(review_date=date, app_id=app_id, reviews=reviews_list)
    except Exception as e:
        logger.error(f"Error fetching reviews: {str(e)[:500]}...")
        raise e
    finally:
        conn.close()

async def perform_daily_analysis(daily_summary_request: DailySummaryRequest,) -> DailySummary:
    logger.info(f"Starting daily summary analysis for app_id: {daily_summary_request.app_id} on date: {daily_summary_request.review_date} with {len(daily_summary_request.reviews)} reviews" )
    pass


    # Create the DailySummary object with initialized fields
    daily_summary = DailySummary(
        summary_date=daily_summary_request.review_date,
        app_id=daily_summary_request.app_id,
        daily_summary_statement="",  # Will be populated by the graph
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
        daily_summary_request=daily_summary_request.model_dump(),
        daily_summary=daily_summary,
        node_history=[],
        current_step="daily_summary",
        error=None
    )

    logger.debug(f"Initialized state for date: {daily_summary.summary_date}")

    try:
             # Full analysis using the graph
        graph = build_graph()
        results = await graph.abatch([initial_state.model_dump()])
        final_state = DailySummaryState(**results[0])
            
        # Log completion status
        logger.info(f"Completed analysis for date: {final_state.daily_summary.summary_date}")
        
        # Save the final state to file
        with open(f"final_state_{daily_summary.summary_date}_{daily_summary.app_id}.json", "w") as f:
            f.write(final_state.model_dump_json(indent=2))
        logger.info(f"Final state saved to file: final_state_{daily_summary.summary_date}_{daily_summary.app_id}.json")
        
        # Save the final state to the database
        success = save_daily_summary(final_state.daily_summary)
        if success:
            logger.info(f"Daily summary saved to database successfully for app_id={final_state.daily_summary.app_id}, date={final_state.daily_summary.summary_date}")
        else:
            logger.error(f"Failed to save daily summary to database for app_id={final_state.daily_summary.app_id}, date={final_state.daily_summary.summary_date}")
        
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
        
        # Save the failed summary to database
        success = mark_daily_summary_failed(
            app_id=daily_summary.app_id,
            summary_date=daily_summary.summary_date,
            error_details={
                "agent": "daily_summary_processor",
                "error_message": error_msg
            }
        )
        if success:
            logger.info(f"Failed daily summary saved to database for app_id={daily_summary.app_id}, date={daily_summary.summary_date}")
        else:
            logger.error(f"Failed to save failed daily summary to database for app_id={daily_summary.app_id}, date={daily_summary.summary_date}")
        
        return daily_summary
    






if __name__ == "__main__":
    reviews = get_reviews_for_date(date(2025, 3, 12), "com.kcb.mobilebanking.android.mbp")
    print(reviews.model_dump_json(indent=2))
    asyncio.run(perform_daily_analysis(reviews))