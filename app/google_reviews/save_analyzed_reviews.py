#save the analyzed reviews to the database
# Save to the table 
from datetime import datetime, timezone
from typing import Dict, Any, Tuple, Optional, List
import hashlib
import json
from psycopg2.extras import execute_values
from ..shared_services.db import pooled_connection
from ..shared_services.logger_setup import setup_logger
from ..shared_services.utils import DateTimeEncoder

logger = setup_logger()


def analysis_input_fingerprint(content: Optional[str], reply_content: Optional[str]) -> str:
    """MD5 of review body + reply body (UTF-8). Must match SQL in get_reviews stale_analysis filter."""
    payload = (content or "") + "\x1e" + (reply_content or "")
    return hashlib.md5(payload.encode("utf-8")).hexdigest()


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

def mark_reviews_analysis_failed_bulk(failed_reviews: List[Dict[str, Any]]) -> int:
    """
    Bulk mark multiple reviews as failed in analysis.
    
    Args:
        failed_reviews: List of dicts with keys: review_id, app_id, error_details
        
    Returns:
        int: Number of reviews successfully marked as failed
    """
    if not failed_reviews:
        return 0
    
    try:
        with pooled_connection() as conn:
            with conn.cursor() as cur:
                # Prepare data for bulk update
                update_data = [
                    (
                        json.dumps(review['error_details'], cls=DateTimeEncoder),
                        review['app_id'],
                        review['review_id']
                    )
                    for review in failed_reviews
                ]
                
                # Bulk update using execute_values
                execute_values(
                    cur,
                    """
                    UPDATE processed_app_reviews
                    SET 
                        last_analyzed_on = CURRENT_TIMESTAMP,
                        analyzed = true,
                        analysis_failed = true,
                        analysis_error = %s
                    WHERE app_id = %s AND review_id = %s
                    """,
                    update_data,
                    page_size=100
                )
                
                conn.commit()
                count = len(failed_reviews)
                logger.info(f"Bulk marked {count} reviews as failed")
                return count
    except Exception as e:
        logger.error(f"Error bulk marking reviews as failed: {e}")
        return 0


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
    try:
        with pooled_connection() as conn:
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
        logger.error(f"Error marking review as failed: {e}")
        return False

def save_reviews_analysis_bulk(analyses: List[Dict[str, Any]]) -> int:
    """
    Bulk save multiple review analyses to database.
    
    Args:
        analyses: List of dicts with keys: review_id, app_id, analysis_data, original_content
        
    Returns:
        int: Number of reviews successfully saved
    """
    if not analyses:
        return 0
    
    try:
        with pooled_connection() as conn:
            with conn.cursor() as cur:
                # First, get all review contents in one query using VALUES
                if analyses:
                    # Build VALUES clause for tuple matching
                    values_clause = ','.join(['(%s,%s)'] * len(analyses))
                    params = []
                    for a in analyses:
                        params.extend([a['app_id'], a['review_id']])
                    
                    cur.execute(f"""
                        SELECT par.app_id, par.review_id, par.content, par.reply_content
                        FROM processed_app_reviews par
                        INNER JOIN (VALUES {values_clause}) AS v(app_id, review_id)
                        ON par.app_id = v.app_id AND par.review_id = v.review_id
                    """, params)
                    
                    content_map = {(row[0], row[1]): (row[2], row[3]) for row in cur.fetchall()}
                else:
                    content_map = {}
                
                # Prepare analysis data with content
                analysis_records = []
                update_records = []
                
                for analysis in analyses:
                    app_id = analysis['app_id']
                    review_id = analysis['review_id']
                    analysis_data = analysis['analysis_data']
                    
                    # Get original content
                    row_pair = content_map.get((app_id, review_id))
                    if not row_pair:
                        logger.warning(f"Review not found: app_id={app_id}, review_id={review_id}")
                        continue
                    original_content, reply_content = row_pair
                    
                    # Ensure analysis data has correct fields
                    analysis_data['review_id'] = review_id
                    analysis_data['content'] = original_content
                    analysis_data['_analysis_input_fingerprint'] = analysis_input_fingerprint(
                        original_content, reply_content
                    )
                    
                    analysis_json = json.dumps(analysis_data, cls=DateTimeEncoder)
                    
                    # Prepare for ai_review_analysis insert
                    analysis_records.append((review_id, analysis_json))
                    
                    # Prepare for processed_app_reviews update (will update after getting analysis_ids)
                    update_records.append({
                        'app_id': app_id,
                        'review_id': review_id,
                        'analysis_json': analysis_json
                    })
                
                if not analysis_records:
                    return 0
                
                # Bulk insert into ai_review_analysis and get analysis_ids
                execute_values(
                    cur,
                    """
                    INSERT INTO ai_review_analysis (review_id, analysis_date, analysis)
                    VALUES %s
                    RETURNING analysis_id, review_id
                    """,
                    analysis_records,
                    page_size=100
                )
                
                # Map review_id to analysis_id
                analysis_id_map = {row[1]: row[0] for row in cur.fetchall()}
                
                # Bulk update processed_app_reviews
                update_data = [
                    (
                        update_records[i]['analysis_json'],
                        analysis_id_map.get(update_records[i]['review_id']),
                        update_records[i]['app_id'],
                        update_records[i]['review_id']
                    )
                    for i in range(len(update_records))
                    if update_records[i]['review_id'] in analysis_id_map
                ]
                
                if update_data:
                    execute_values(
                        cur,
                        """
                        UPDATE processed_app_reviews
                        SET 
                            last_analyzed_on = CURRENT_TIMESTAMP,
                            analyzed = true,
                            analysis_failed = false,
                            analysis_error = NULL,
                            latest_analysis = %s,
                            latest_analysis_id = %s
                        WHERE app_id = %s AND review_id = %s
                        """,
                        update_data,
                        page_size=100
                    )
                
                conn.commit()
                count = len(update_data)
                logger.info(f"Bulk saved {count} review analyses")
                return count
                
    except Exception as e:
        logger.error(f"Error bulk saving review analyses: {e}")
        return 0


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
    try:
        with pooled_connection() as conn:
            with conn.cursor() as cur:
                # Log the analysis data for debugging
                logger.debug(f"Saving analysis data for review {review_id}:")
                logger.debug(f"Content: {analysis_data.get('content', 'No content')[:100]}...")
                logger.debug(f"Review ID: {analysis_data.get('review_id', 'No review_id')}")
                
                # Get the original review content
                cur.execute("""
                    SELECT content, reply_content
                    FROM processed_app_reviews 
                    WHERE app_id = %s AND review_id = %s
                """, (app_id, review_id))
                
                result = cur.fetchone()
                if not result:
                    logger.error(f"Review not found: app_id={app_id}, review_id={review_id}")
                    return False
                    
                original_content, reply_content = result[0], result[1]
                
                # Ensure the analysis data has the correct review_id and content
                analysis_data['review_id'] = review_id
                analysis_data['content'] = original_content
                analysis_data['_analysis_input_fingerprint'] = analysis_input_fingerprint(
                    original_content, reply_content
                )
                
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
        logger.error(f"Error saving review analysis: {e}")
        return False 