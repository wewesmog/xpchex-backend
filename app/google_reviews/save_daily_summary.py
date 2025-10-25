"""
Save daily summaries to the database
"""
from datetime import date
import json
import logging
from typing import Optional
from app.models.summary_models import DailySummary
from app.shared_services.db import get_postgres_connection
from app.shared_services.logger_setup import setup_logger
from app.shared_services.utils import DateTimeEncoder

logger = setup_logger()

def save_daily_summary(daily_summary: DailySummary) -> bool:
    """
    Save a daily summary to the daily_summaries table.
    
    Args:
        daily_summary: The DailySummary object to save
        
    Returns:
        bool: True if save was successful, False otherwise
    """
    conn = get_postgres_connection("daily_summaries")
    try:
        with conn.cursor() as cur:
            # Insert or update the daily summary
            cur.execute("""
                INSERT INTO daily_summaries 
                    (date, app_id, daily_summary_statement, sentiment_distribution, issue_groups, 
                     feature_areas, actions, business_impact, error)
                VALUES 
                    (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (app_id, date) DO UPDATE SET
                    daily_summary_statement = EXCLUDED.daily_summary_statement,
                    sentiment_distribution = EXCLUDED.sentiment_distribution,
                    issue_groups = EXCLUDED.issue_groups,
                    feature_areas = EXCLUDED.feature_areas,
                    actions = EXCLUDED.actions,
                    business_impact = EXCLUDED.business_impact,
                    error = EXCLUDED.error,
                    updated_at = CURRENT_TIMESTAMP
            """, (
                daily_summary.summary_date,
                daily_summary.app_id,
                daily_summary.daily_summary_statement,
                json.dumps(daily_summary.sentiment_distribution.dict(), cls=DateTimeEncoder),
                json.dumps([group.dict() for group in daily_summary.issue_groups], cls=DateTimeEncoder),
                json.dumps({k: v.dict() for k, v in daily_summary.feature_areas.items()}, cls=DateTimeEncoder),
                json.dumps([action.dict() for action in daily_summary.actions], cls=DateTimeEncoder),
                json.dumps(daily_summary.business_impact.dict(), cls=DateTimeEncoder),
                json.dumps(daily_summary.error.dict(), cls=DateTimeEncoder) if daily_summary.error else None
            ))
            
            conn.commit()
            logger.info(f"Successfully saved daily summary for app_id={daily_summary.app_id}, date={daily_summary.summary_date}")
            return True
            
    except Exception as e:
        conn.rollback()
        logger.error(f"Error saving daily summary: {e}")
        return False
    finally:
        conn.close()

def mark_daily_summary_failed(app_id: str, summary_date: date, error_details: dict) -> bool:
    """
    Mark a daily summary as failed and save the error details.
    
    Args:
        app_id: The ID of the app
        summary_date: The date of the summary
        error_details: Dictionary containing error information
        
    Returns:
        bool: True if update was successful, False otherwise
    """
    conn = get_postgres_connection("daily_summaries")
    try:
        with conn.cursor() as cur:
            # Insert or update with error information
            cur.execute("""
                INSERT INTO daily_summaries 
                    (date, app_id, daily_summary_statement, sentiment_distribution, issue_groups, 
                     feature_areas, actions, business_impact, error)
                VALUES 
                    (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (app_id, date) DO UPDATE SET
                    error = EXCLUDED.error,
                    updated_at = CURRENT_TIMESTAMP
            """, (
                summary_date,
                app_id,
                "",  # Empty daily summary statement
                json.dumps({}, cls=DateTimeEncoder),  # Empty sentiment distribution
                json.dumps([], cls=DateTimeEncoder),  # Empty issue groups
                json.dumps({}, cls=DateTimeEncoder),  # Empty feature areas
                json.dumps([], cls=DateTimeEncoder),  # Empty actions
                json.dumps({    # Default business impact
                    "severity": "unknown",
                    "affected_areas": [],
                    "metrics_impact": {
                        "user_retention": False,
                        "app_rating": False,
                        "user_acquisition": False
                    },
                    "recommendation": "",
                    "confidence": 0.0
                }, cls=DateTimeEncoder),
                json.dumps(error_details, cls=DateTimeEncoder)
            ))
            
            conn.commit()
            logger.info(f"Marked daily summary as failed for app_id={app_id}, date={summary_date}")
            return True
            
    except Exception as e:
        conn.rollback()
        logger.error(f"Error marking daily summary as failed: {e}")
        return False
    finally:
        conn.close() 