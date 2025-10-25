# google_reviews/canonization.py

import os
import sys
from app.models.canonization_models import ExistingStatement, CanonizationRequest
from typing import List, Tuple, Dict, Optional
from datetime import date, datetime, timedelta
from app.agents.canonize_statement import canonize_statement_node
from app.models.canonization_models import CanonizationLLMResponse
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, 
                   format='%(message)s')  # Simplified format
logger = logging.getLogger(__name__)

# Suppress other loggers
logging.getLogger('QueryStateLogger').setLevel(logging.WARNING)
logging.getLogger('urllib3').setLevel(logging.WARNING)
logging.getLogger('asyncio').setLevel(logging.WARNING)
logging.getLogger('httpx').setLevel(logging.WARNING)  # OpenRouter uses httpx
logging.getLogger('openai').setLevel(logging.WARNING)  # Suppress OpenAI client logs

# Add the backend directory to the Python path
backend_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.append(backend_dir)

from app.shared_services.db import get_postgres_connection

def get_issue_statements_by_review(review_id: str) -> List[Tuple[str, str]]: # Corrected type hint
    """
    Get all relevant text descriptions (issues, actions, positive mentions) for a given review.
    """
    conn = None
    cursor = None
    issue_statements = []
    try:
        conn = get_postgres_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT
                'issue' as section_type,
                issue_data->>'description' AS free_text_description
            FROM
                processed_app_reviews,
                jsonb_array_elements(latest_analysis->'issues'->'issues') AS issue_data
            WHERE
                issue_data->>'description' IS NOT NULL AND review_id = %s

            UNION ALL

            SELECT
                'issue_action' as section_type, -- Consistent column name for union
                action_data->>'description' AS free_text_description
            FROM
                processed_app_reviews,
                jsonb_array_elements(latest_analysis->'issues'->'issues') AS issue_data,
                jsonb_array_elements(issue_data->'actions') AS action_data
            WHERE
                action_data->>'description' IS NOT NULL AND review_id = %s

            UNION ALL

            SELECT
                'positive' as section_type, -- Consistent column name for union
                positive_data->>'description' AS free_text_description
            FROM
                processed_app_reviews,
                jsonb_array_elements(latest_analysis->'positive_feedback'->'positive_mentions') AS positive_data
            WHERE
                positive_data->>'description' IS NOT NULL AND review_id = %s
        """, (review_id, review_id, review_id)) # Pass review_id for each %s
        issue_statements = cursor.fetchall()

        logger.info(f"Found {len(issue_statements)} statements for review {review_id}:")
        for section_type, description in issue_statements: # Unpack tuple for clearer logging
            logger.info(f"  - Section: '{section_type}', Description: '{description}'")

    except Exception as e:
        logger.error(f"Error fetching issue statements for review {review_id}: {e}")
        # Optionally re-raise the exception or handle it more gracefully
        raise # Re-raise for callers to handle
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
    return issue_statements

def check_statement_exists(statement: str) -> Tuple[bool, Optional[str]]:
    """
    Check if a statement already exists in canonical_statements
    Returns: (exists, canonical_id)
    """
    conn = get_postgres_connection()
    cursor = conn.cursor()
    try:
        logger.info(f"Checking if statement exists: '{statement}'")
        cursor.execute("""
            SELECT canonical_id 
            FROM canonical_statements 
            WHERE statement = %s
            LIMIT 1
        """, (statement,))
        result = cursor.fetchone()
        exists = bool(result)
        canonical_id = result[0] if result else None
        logger.info(f"Statement exists: {exists}, canonical_id: {canonical_id}")
        return (exists, canonical_id)
    finally:
        cursor.close()
        conn.close()

def get_canonical_id_and_statements_pairs() -> List[Tuple[str, str]]:
    """
    Get all canonical_id and statements pairs
    Returns: List of tuples containing (canonical_id, statement)
    """
    conn = get_postgres_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM get_canonical_id_and_statements_pairs()")
    canonical_id_and_statements_pairs = cursor.fetchall()
    conn.close()
    return canonical_id_and_statements_pairs


async def process_reviews_canonization(
    start_date: datetime = datetime(2025, 3, 1),
    end_date: datetime = datetime(2025, 3, 5)
) -> int:
    """
    Process reviews day by day between start_date and end_date for canonization.
    Updates processed_app_reviews.CANONIZATION_STATUS based on overall success of all statements in a review.
    """
    total_reviews = 0
    
    current_date = start_date
    while current_date <= end_date:
        daily_reviews = 0
        logger.info(f"\nProcessing date: {current_date.strftime('%Y-%m-%d')}")
        
        # Get reviews that need canonization for current date
        conn = get_postgres_connection()
        cursor = conn.cursor()
        
        try:
            # Get reviews for the day where canonization hasn't been done
            cursor.execute("""
                SELECT review_id 
                FROM processed_app_reviews 
                WHERE DATE(review_created_at) = %s 
                AND (CANONIZATION_STATUS IS NULL or CANONIZATION_STATUS = 'failed' or CANONIZATION_STATUS = 'success')
            """, (current_date,))
            
            reviews = cursor.fetchall()
            if not reviews:
                logger.info(f"No reviews found for {current_date.strftime('%Y-%m-%d')}")
                current_date += timedelta(days=1)
                continue
                
            logger.info(f"Found {len(reviews)} reviews to process")
            
            for (review_id,) in reviews:
                try:
                    # Get statements for this review using the new function
                    statements = get_issue_statements_by_review(review_id)
                    if not statements:
                        logger.warning(f"No statements found for review {review_id}")
                        continue
                    
                    total_statements = len(statements)
                    failed_statements = 0
                    
                    # Get all existing canonical pairs once
                    canonical_pairs = get_canonical_id_and_statements_pairs()
                    
                    for statement, section, _ in statements:
                        try:
                            # First check if statement already exists
                            exists, canonical_id = check_statement_exists(statement)
                            if exists:
                                logger.info(f"Statement '{statement}' already exists with canonical_id: {canonical_id}")
                                # Create state for existing statement
                                state = {
                                    "canonization_status": "already_exists",
                                    "statement": statement,
                                    "review_id": review_id,
                                    "review_section": section,
                                    "canonical_id": canonical_id
                                }
                                canonize_statement_node(state)
                                continue

                            logger.info(f"Statement '{statement}' does not exist, proceeding with canonization")
                            
                            # Create request object for new statement
                            canonization_request = CanonizationRequest(
                                review_id=review_id,
                                review_section=section,
                                statement=statement,
                                # Pass all, the agent will reduce to top-N before prompting
                                existing_pairs=[ExistingStatement(statement=stmt, canonical_id=c_id) 
                                              for c_id, stmt in canonical_pairs]
                            )
                            
                            # Create initial state
                            state = {
                                "canonization_request": canonization_request,
                                "canonization_response": None,
                                "canonization_status": "pending",
                                "current_step": "canonization_requested",
                                "canonization_attempt": 0
                            }
                            
                            # Send to LLM
                            canonization_result = canonize_statement_node(state)
                            
                            # Check if canonization was successful
                            if (not canonization_result or 
                                not canonization_result.canonization_response or 
                                not canonization_result.canonization_response.canonical_id or 
                                canonization_result.canonization_status != 'completed'):
                                failed_statements += 1
                                logger.error(f"Failed to canonize statement for review {review_id}: {statement}")
                                
                        except Exception as e:
                            failed_statements += 1
                            logger.error(f"Error processing statement in review {review_id}: {e}")
                    
                    # Update review status based on all statements
                    review_status = 'success'
                    if failed_statements == total_statements:
                        review_status = 'failed'
                    elif failed_statements > 0:
                        review_status = 'partial_fail'
                    
                    # Update the review status
                    cursor.execute("""
                        UPDATE processed_app_reviews 
                        SET CANONIZATION_STATUS = %s
                        WHERE review_id = %s
                    """, (review_status, review_id))
                    
                    conn.commit()
                    daily_reviews += 1
                    total_reviews += 1
                    
                except Exception as e:
                    conn.rollback()
                    logger.error(f"Error processing review {review_id}: {e}")
            
            logger.info(f"Completed {daily_reviews} reviews for {current_date.strftime('%Y-%m-%d')}")
            
        except Exception as e:
            logger.error(f"Error processing date {current_date}: {e}")
        finally:
            cursor.close()
            conn.close()
        
        # Move to next day
        current_date += timedelta(days=1)
    
    logger.info(f"\nCanonization Complete: Total reviews canonicalized across all dates: {total_reviews}")
    return total_reviews

if __name__ == "__main__":
    import asyncio
    
    # Process reviews for February 2025
    start_date = datetime(2025, 2, 28)
    end_date = datetime(2025, 7, 28)
    
    print("\n" + "="*50)
    print(f"Starting canonization process")
    print(f"Date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    print("="*50 + "\n")
    
    asyncio.run(process_reviews_canonization(start_date, end_date))
    
    print("\n" + "="*50)
    print("Canonicalization process completed")
    print("="*50)

    
    
        

