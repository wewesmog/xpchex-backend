from datetime import datetime
from typing import Optional, List, Tuple
import json
from app.models.pydantic_models import Review, ReviewFilter
from app.shared_services.db import get_postgres_connection
from ..shared_services.logger_setup import setup_logger


logger = setup_logger()


def format_review_data(row_dict: dict) -> str:
    """
    Format review data for logging, handling Unicode characters safely.
    
    Args:
        row_dict: Dictionary containing review data
        
    Returns:
        str: Safely formatted review data string
    """
    # Create a clean copy of the dict for logging
    log_dict = row_dict.copy()
    
    # Handle potentially problematic Unicode content
    if 'content' in log_dict:
        # Limit content length and escape Unicode
        content = log_dict['content']
        if content:  # Check if content is not None
            if len(content) > 100:
                content = content[:97] + "..."
            log_dict['content'] = content.encode('unicode_escape').decode('ascii')
        else:
            log_dict['content'] = "No content"
    
    return str(log_dict)


async def get_reviews(filters: ReviewFilter) -> List[Review]:
    """
    Get reviews with flexible filtering options.
    
    Args:
        filters: ReviewFilter object containing:
            - app_id: Optional filter by app_id
            - username: Optional filter by username
            - review_id: Optional filter by review_id
            - limit: Maximum number of results (default 50)
            - offset: Number of results to skip (for pagination)
            - order_by: Field to sort by (review_created_at)
            - order_direction: Sort direction (asc or desc)
            - from_date: Optional filter for dates after this
            - to_date: Optional filter for dates before this
    """
    conditions = []
    params = []
    
    # Build WHERE clause dynamically
    if filters.app_id:
        conditions.append("app_id = %s")
        params.append(filters.app_id)

    if filters.username:
        conditions.append("username = %s")
        params.append(filters.username)
    
    if filters.review_id:
        conditions.append("review_id = %s")
        params.append(filters.review_id)
    
    if filters.from_date:
        conditions.append("review_created_at >= %s")
        params.append(filters.from_date)
    
    if filters.to_date:
        conditions.append("review_created_at <= %s")
        params.append(filters.to_date)
    
    # Filter to input a list of dates
    if filters.date_list:
        date_strings = [d.strftime('%Y-%m-%d') for d in filters.date_list]
        conditions.append(f"date(review_created_at) in ({','.join(['%s'] * len(date_strings))})")
        params.extend(date_strings)
    
    # Add analyzed flag condition
    conditions.append("analyzed = %s")
    params.append(filters.analyzed)
    
    # Add content not null condition
    conditions.append("content IS NOT NULL AND content != ''")
    
    # Construct the WHERE clause
    where_clause = " AND ".join(conditions) if conditions else "TRUE"
    
    # Construct the full query
    query = f"""
        SELECT 
            id,
            app_id,
            review_id,
            username,
            user_image,
            content,
            score,
            thumbs_up_count,
            review_created_at,
            reply_content,
            reply_created_at,
            app_version,
            analyzed,
            latest_analysis        
        FROM processed_app_reviews 
        WHERE {where_clause}
        ORDER BY {filters.order_by} {filters.order_direction}
        LIMIT %s OFFSET %s
    """
    
    # Add limit and offset to params
    params.extend([filters.limit, filters.offset])
    
    conn = get_postgres_connection()
    try:
        with conn.cursor() as cur:
            logger.info(f"Executing query: {cur.mogrify(query, tuple(params))}")
            cur.execute(query, tuple(params))
            rows = cur.fetchall()
            columns = [desc[0] for desc in cur.description]
            result = []
            
            for row in rows:
                # Create a dictionary with explicit column mapping
                row_dict = {}
                for i, column in enumerate(columns):
                    row_dict[column] = row[i]
                
                # Log safely formatted data
                logger.info(f"Row data: {format_review_data(row_dict)}")
                
                # Skip rows with no content
                if not row_dict.get('content'):
                    logger.warning(f"Skipping review {row_dict.get('review_id')} due to missing content")
                    continue
                
                result.append(Review(**row_dict))
            
            return result
    except Exception as e:
        logger.error(f"Error fetching reviews: {str(e)}")
        raise e
    finally:
        conn.close()
