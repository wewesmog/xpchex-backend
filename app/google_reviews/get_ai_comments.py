# import db connection
from psycopg2.extras import RealDictCursor

from app.shared_services.db import pooled_connection
from ..shared_services.logger_setup import setup_logger
from app.google_reviews.calculate_metrics import calculate_metric


logger = setup_logger()


def get_periods():
    """Get all active periods from period_types, ordered by period_id. Returns full row dicts (period_id, period_type, refresh_cycle, period_format, etc.)."""
    with pooled_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(
                """
                SELECT * FROM period_types
                WHERE is_active = TRUE and period_format != 'daily' and period_type = 'last_90_days'
                ORDER BY period_id
                """
            )
            periods = cursor.fetchall()
            logger.info(f"Loaded {len(periods)} active period types")
            return [dict(row) for row in periods]

def get_metrics():
    """Get all active metrics from metrics, ordered by metric_id. Returns full row dicts (metric_id, metric_code, display_name, description, unit, is_active, etc.)."""
    with pooled_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(
                """
                SELECT * FROM metrics
                WHERE is_active = TRUE 
                ORDER BY metric_id
                """
            )
            metrics = cursor.fetchall()
            logger.info(f"Loaded {len(metrics)} active metrics")
            return [dict(row) for row in metrics]

def get_existing_ai_comments(app_id, metric_id, period_id, metric_name=None, period_name=None):
    """Get count of existing ai comments for this app/metric/period. Names are for logs only."""
    with pooled_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(
                """
                SELECT COUNT(*) AS total_comments, MAX(updated_on) AS latest_update
                FROM metric_summaries
                WHERE app_id = %s AND metric_id = %s AND period_id = %s
                  AND period_value IS NOT NULL AND ai_comment IS NOT NULL
                """,
                (str(app_id), metric_id, period_id),
            )
            row = cursor.fetchone()
            total = (row["total_comments"] or 0) if row else 0
            m_name = metric_name or metric_id
            p_name = period_name or period_id
            if total == 0:
                logger.info(f"No existing ai comments for app {app_id}, metric {m_name}, period {p_name}")
            else:
                logger.info(f"Loaded {total} existing ai comments for app {app_id}, metric {m_name}, period {p_name}")
            return {"total_comments": total, "latest_update": row.get("latest_update") if row else None}

def prepare_ai_request(number, metric_name, description, period):
    """Prepare prompt to AI (number, metric name, metric desc, period)"""
    ai_request = {
        "number": number,
        "metric_name": metric_name,
        "description": description or "",
        "period": period,
    }
    logger.info(f"AI request: {ai_request}")
    return ai_request

def generate_ai_comments(app_id, metric_id, period_id, metric_name=None, period_name=None, description=None):
    """Generate ai comments for an app. metric_name/period_name are used in logs."""
    existing_ai_comments = get_existing_ai_comments(
        app_id=app_id, metric_id=metric_id, period_id=period_id,
        metric_name=metric_name, period_name=period_name
    )
    total = existing_ai_comments.get("total_comments", 0) or 0
    m_name = metric_name or metric_id
    p_name = period_name or period_id
    if total == 0:
        logger.info(f"No existing ai comments for app {app_id}, metric {m_name}, period {p_name} — generating initial ai comments")
        number = calculate_metric(app_id, metric_id, period_id)
        print(f"Calculated metric {m_name} for app {app_id}, period {p_name}")
        # prepare prompt to AI (number, metric name, metric desc, period)
        prepare_ai_request(number, m_name, description, p_name)
       
    else:
        logger.info(f"Existing ai comments for app {app_id}, metric {m_name}, period {p_name} ({total}) — generating new ai comments")

 

def main_function(app_id):
    """Main function to generate ai comments for an app. app_id can be int or str (DB column is VARCHAR)."""
    # Load period and metric data
    periods = get_periods()
    metrics = get_metrics()
    # loop through periods and metrics and generate ai comments
    for period in periods:
        for metric in metrics:
            generate_ai_comments(
                app_id ,
                metric_id=metric['metric_id'],
                period_id=period['period_id'],
                metric_name=metric.get('display_name'),
                period_name=period.get('period_type'),
                description=metric.get('description'),
            )
if __name__ == "__main__":
    main_function(app_id="com.kcb.mobilebanking.android.mbp")
   