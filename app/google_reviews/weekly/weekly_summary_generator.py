from datetime import date
from typing import Dict, Any
from app.shared_services.db import get_postgres_connection
from app.shared_services.llm import call_llm_api
from app.prompts.weekly_summary import (
    get_overall_summary_prompt,
    get_positive_summary_prompt,
    get_issues_summary_prompt
)
from app.shared_services.logger_setup import setup_logger
import json

logger = setup_logger()

def _extract_text(response: object) -> str:
    """Return plain text from various response shapes.
    Supports string, objects with .content, or dicts with 'content'.
    """
    if isinstance(response, str):
        return response
    # Pydantic/SDK objects with .content
    content = getattr(response, 'content', None)
    if isinstance(content, str):
        return content
    # Dict-like
    try:
        content = response.get('content')  # type: ignore[attr-defined]
        if isinstance(content, str):
            return content
    except Exception:
        pass
    return str(response)

def generate_overall_summary(weekly_data: Dict[str, Any]) -> str:
    """Generate overall weekly summary statement"""
    
    positive_mentions = weekly_data.get('positive_mentions', []) or []
    issues = weekly_data.get('issues', []) or []

    # Build a minimal, robust context that relies only on available arrays
    context = {
        'week_start': weekly_data.get('week_start'),
        'week_end': weekly_data.get('week_end'),
        'app_id': weekly_data.get('app_id'),
        'positive_mentions': sorted(positive_mentions, key=lambda x: x.get('count', 0), reverse=True)[:5],
        'issues': sorted(issues, key=lambda x: x.get('count', 0), reverse=True)[:5],
        'total_positive': sum(item.get('count', 0) for item in positive_mentions),
        'total_issues': sum(item.get('count', 0) for item in issues),
    }
    
    prompt = get_overall_summary_prompt(context)
    
    messages = [
        {"role": "system", "content": "You are a weekly summary generator. Create concise, high-level summaries that give an executive overview."},
        {"role": "user", "content": prompt}
    ]
    
    try:
        response = call_llm_api(messages=messages, temperature=0.7)
        return _extract_text(response)
    except Exception as e:
        logger.error(f"Error generating overall summary: {e}")
        return f"Error generating overall summary: {str(e)}"

def generate_positive_summary(weekly_data: Dict[str, Any]) -> str:
    """Generate positive feedback summary statement"""
    
    positive_mentions = sorted((weekly_data.get('positive_mentions') or []), key=lambda x: x.get('count', 0), reverse=True)
    
    context = {
        'week_start': weekly_data.get('week_start'),
        'week_end': weekly_data.get('week_end'),
        'app_id': weekly_data.get('app_id'),
        'positive_mentions': positive_mentions,
        'positive_count': len(positive_mentions),
    }
    
    prompt = get_positive_summary_prompt(context)
    
    messages = [
        {"role": "system", "content": "You are a positive feedback analyst. Focus on strengths, user satisfaction, and what's working well."},
        {"role": "user", "content": prompt}
    ]
    
    try:
        response = call_llm_api(messages=messages, temperature=0.7)
        return _extract_text(response)
    except Exception as e:
        logger.error(f"Error generating positive summary: {e}")
        return f"Error generating positive summary: {str(e)}"

def generate_issues_summary(weekly_data: Dict[str, Any]) -> str:
    """Generate issues summary statement"""
    
    issues = sorted((weekly_data.get('issues') or []), key=lambda x: x.get('count', 0), reverse=True)
    
    context = {
        'week_start': weekly_data.get('week_start'),
        'week_end': weekly_data.get('week_end'),
        'app_id': weekly_data.get('app_id'),
        'issues': issues,
        'issue_count': len(issues)
    }
    
    prompt = get_issues_summary_prompt(context)
    
    messages = [
        {"role": "system", "content": "You are an issue analyst. Focus on problems, user pain points, and actionable improvements."},
        {"role": "user", "content": prompt}
    ]
    
    try:
        response = call_llm_api(messages=messages, temperature=0.7)
        return _extract_text(response)
    except Exception as e:
        logger.error(f"Error generating issues summary: {e}")
        return f"Error generating issues summary: {str(e)}"

def process_weekly_aggregations(app_id: str, week_start: date) -> bool:
    """Process unprocessed weekly aggregations with multiple summaries"""
    conn = get_postgres_connection("weekly_aggregations")
    try:
        with conn.cursor() as cur:
            # Get unprocessed aggregation
            cur.execute("""
                SELECT aggregated_data 
                FROM weekly_aggregations 
                WHERE app_id = %s AND week_start = %s AND processed = false
            """, (app_id, week_start))
            
            result = cur.fetchone()
            if not result:
                logger.info(f"No unprocessed aggregation found for {app_id} week {week_start}")
                return False
            
            weekly_data = result[0]
            
            # Generate three separate summaries (overall, positives, issues)
            overall_summary = generate_overall_summary(weekly_data)
            positive_summary = generate_positive_summary(weekly_data)
            issues_summary = generate_issues_summary(weekly_data)
            
            # Update with all summaries
            cur.execute("""
                UPDATE weekly_aggregations 
                SET 
                    overall_summary_statement = %s,
                    positive_summary_statement = %s,
                    negative_summary_statement = NULL,
                    issues_summary_statement = %s,
                    processed = true, 
                    updated_at = CURRENT_TIMESTAMP
                WHERE app_id = %s AND week_start = %s
            """, (overall_summary, positive_summary, issues_summary, app_id, week_start))
            
            conn.commit()
            logger.info(f"Generated weekly summaries for {app_id} week {week_start}")
            return True
            
    except Exception as e:
        logger.error(f"Error processing weekly aggregation: {e}")
        conn.rollback()
        return False
    finally:
        conn.close()