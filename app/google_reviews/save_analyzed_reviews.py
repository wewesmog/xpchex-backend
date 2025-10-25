#save the analyzed reviews to the database
# Save to the table 
from datetime import datetime, timezone
from typing import Dict, Any, Tuple, Optional
import json
from ..shared_services.db import get_postgres_connection
from ..shared_services.logger_setup import setup_logger
from ..shared_services.utils import DateTimeEncoder

logger = setup_logger()

def get_review_app_id(review_id: str, conn) -> Optional[str]:
    """
    Get the app_id for a review from the database.
    
    Args:
        review_id: The ID of the review
        conn: Database connection to use
        
    Returns:
        str or None: The app_id if found, None otherwise
    """
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT app_id 
                FROM processed_app_reviews 
                WHERE review_id = %s
                LIMIT 1
            """, (review_id,))
            result = cur.fetchone()
            return result[0] if result else None
    except Exception as e:
        logger.error(f"Error getting app_id for review {review_id}: {e}")
        return None

def mark_review_analysis_failed(review_id: str, app_id: str, error_details: Dict[str, Any]) -> bool:
    """
    Mark a review as failed in analysis and save the error details.
    Also marks the review as analyzed to prevent infinite loops.
    
    Args:
        review_id: The ID of the review that failed analysis
        app_id: The ID of the app the review belongs to
        error_details: Dictionary containing error information
        
    Returns:
        bool: True if update was successful, False otherwise
    """
    conn = get_postgres_connection("processed_app_reviews")
    try:
        with conn.cursor() as cur:
            # Update processed_app_reviews to mark as failed AND analyzed
            cur.execute("""
                UPDATE processed_app_reviews
                SET 
                    last_analyzed_on = CURRENT_TIMESTAMP,
                    analyzed = true,
                    analysis_failed = true,
                    analysis_error = %s
                WHERE app_id = %s AND review_id = %s
            """, (json.dumps(error_details, cls=DateTimeEncoder), app_id, review_id))
            
            conn.commit()
            logger.info(f"Marked review as failed for app_id={app_id}, review_id={review_id}")
            return True
            
    except Exception as e:
        conn.rollback()
        logger.error(f"Error marking review as failed: {e}")
        return False
    finally:
        conn.close()

def save_review_analysis(review_id: str, analysis_data: Dict[str, Any], app_id: str) -> bool:
    """
    Save review analysis to ai_review_analysis table and update processed_app_reviews.
    
    Args:
        review_id: The ID of the review being analyzed
        analysis_data: Dictionary containing the analysis results
        app_id: The ID of the app the review belongs to (from the review object)
        
    Returns:
        bool: True if save was successful, False otherwise
    """
    conn = get_postgres_connection("ai_review_analysis")  # Using ai_review_analysis as the main table since we're writing to it
    try:
        with conn.cursor() as cur:
            # Log the analysis data for debugging
            logger.debug(f"Saving analysis data for review {review_id}:")
            logger.debug(f"Content: {analysis_data.get('content', 'No content')[:100]}...")
            logger.debug(f"Review ID: {analysis_data.get('review_id', 'No review_id')}")
            
            # Get the original review content
            cur.execute("""
                SELECT content 
                FROM processed_app_reviews 
                WHERE app_id = %s AND review_id = %s
            """, (app_id, review_id))
            
            result = cur.fetchone()
            if not result:
                logger.error(f"Review not found: app_id={app_id}, review_id={review_id}")
                return False
                
            original_content = result[0]
            
            # Ensure the analysis data has the correct review_id and content
            analysis_data['review_id'] = review_id
            analysis_data['content'] = original_content
            
            # First insert into ai_review_analysis
            cur.execute("""
                INSERT INTO ai_review_analysis 
                    (review_id, analysis_date, analysis)
                VALUES 
                    (%s, CURRENT_TIMESTAMP, %s)
                RETURNING analysis_id
            """, (review_id, json.dumps(analysis_data, cls=DateTimeEncoder)))
            
            # Get the generated analysis_id
            analysis_id = cur.fetchone()[0]
            
            # Update processed_app_reviews using both app_id and review_id
            cur.execute("""
                UPDATE processed_app_reviews
                SET 
                    last_analyzed_on = CURRENT_TIMESTAMP,
                    analyzed = true,
                    analysis_failed = false,
                    analysis_error = NULL,
                    latest_analysis = %s,
                    latest_analysis_id = %s
                WHERE app_id = %s AND review_id = %s
            """, (json.dumps(analysis_data, cls=DateTimeEncoder), analysis_id, app_id, review_id))
            
            conn.commit()
            logger.info(f"Successfully saved analysis for app_id={app_id}, review_id={review_id} with analysis_id={analysis_id}")
            return True
            
    except Exception as e:
        conn.rollback()
        logger.error(f"Error saving review analysis: {e}")
        return False
    finally:
        conn.close() 