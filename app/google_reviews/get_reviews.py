from datetime import datetime
import os
from typing import Optional, List, Tuple
import json
from app.models.pydantic_models import Review, ReviewFilter
from app.shared_services.db import pooled_connection
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


SLA_DAYS = int(os.getenv("REPLY_SLA_DAYS", "3"))


def _build_where_clause_and_params(filters: ReviewFilter):
    """Build WHERE clause and params for list/count. Same logic for both."""
    conditions = []
    params = []
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
    if filters.date_list:
        date_strings = [d.strftime('%Y-%m-%d') for d in filters.date_list]
        conditions.append(f"date(review_created_at) in ({','.join(['%s'] * len(date_strings))})")
        params.extend(date_strings)
    if filters.min_score is not None:
        conditions.append("(score::numeric) >= %s")
        params.append(float(filters.min_score))
    if filters.max_score is not None:
        conditions.append("(score::numeric) <= %s")
        params.append(float(filters.max_score))
    if filters.sentiment:
        conditions.append("(latest_analysis->>'sentiment') = %s")
        params.append(filters.sentiment)
    if filters.replied is False:
        conditions.append("reply_created_at IS NULL")
    elif filters.replied is True:
        conditions.append("reply_created_at IS NOT NULL")
    if filters.stale_analysis:
        conditions.append("analyzed = true")
        conditions.append("latest_analysis IS NOT NULL")
        # Only re-run when fingerprint exists but review/reply text no longer matches (legacy rows without key are untouched).
        conditions.append("latest_analysis ? '_analysis_input_fingerprint'")
        conditions.append(
            "latest_analysis->>'_analysis_input_fingerprint' <> md5("
            "coalesce(content, '') || E'\\x1e' || coalesce(reply_content, '')"
            ")"
        )
    elif filters.analyzed is not None:
        conditions.append("analyzed = %s")
        params.append(filters.analyzed)
    # Past SLA inbox: unanswered and older than SLA_DAYS since review date
    if filters.inbox_id and filters.inbox_id.strip().lower() == "past_sla":
        conditions.append(
            "reply_created_at IS NULL "
            "AND review_created_at < (NOW() - (%s || ' days')::interval)"
        )
        params.append(SLA_DAYS)
    conditions.append("content IS NOT NULL AND content != ''")
    where_clause = " AND ".join(conditions) if conditions else "TRUE"
    return where_clause, params


async def get_reviews(filters: ReviewFilter) -> List[Review]:
    """
    Get reviews with flexible filtering options.
    """
    where_clause, params = _build_where_clause_and_params(filters)
    # Copy so we don't mutate the list used for count
    list_params = list(params)
    list_params.extend([filters.limit, filters.offset])
    
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
        ORDER BY (reply_created_at IS NULL) DESC, {filters.order_by} {filters.order_direction}
        LIMIT %s OFFSET %s
    """
    
    with pooled_connection() as conn:
        with conn.cursor() as cur:
            logger.info(f"Executing query: {cur.mogrify(query, tuple(list_params))}")
            cur.execute(query, tuple(list_params))
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



async def get_reviews_count(filters: ReviewFilter) -> int:
    """
    Total count for the same filters as get_reviews.
    Uses the same WHERE as list: app_id, from_date, to_date, min_score, max_score,
    sentiment, analyzed (when set), and content IS NOT NULL. No LIMIT/OFFSET.
    """
    where_clause, params = _build_where_clause_and_params(filters)
    query = f"""
        SELECT COUNT(*) FROM processed_app_reviews WHERE {where_clause}
    """
    with pooled_connection() as conn:
        with conn.cursor() as cur:
            logger.info(f"Executing count query: {cur.mogrify(query, tuple(params))}")
            cur.execute(query, tuple(params))
            rows = cur.fetchall()
            count = rows[0][0] if rows else 0
            # Ensure int for JSON (e.g. Decimal from PostgreSQL)
            return int(count)
