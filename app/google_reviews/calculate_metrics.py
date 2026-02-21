"""
Calculate metric values from DB for a given app_id, metric_id, period_id.
One metric at a time; mirrors frontend/router logic (Sentiments, Issues, Delights, Recommendations).
Uses shared date_ranges for uniform date logic across routers and scripts.
"""
from datetime import datetime
from decimal import Decimal

from psycopg2.extras import RealDictCursor

from app.shared_services.db import pooled_connection
from app.shared_services.date_ranges import TimeRange, get_date_range
from app.shared_services.logger_setup import setup_logger

logger = setup_logger()

# Backward compat: DB may still have last_90_days -> treat as last_3_months
_PERIOD_TYPE_ALIAS = {"last_90_days": TimeRange.LAST_3_MONTHS}


def get_date_range_for_period(period_id: int) -> tuple[datetime, datetime]:
    """Look up period_type from period_types by period_id, then return (start_date, end_date) using shared date_ranges."""
    with pooled_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(
                "SELECT period_type FROM period_types WHERE period_id = %s AND is_active = TRUE",
                (period_id,),
            )
            row = cursor.fetchone()
    if not row:
        raise ValueError(f"Unknown or inactive period_id: {period_id}")
    period_type = (row["period_type"] or "").strip().lower()
    time_range = _PERIOD_TYPE_ALIAS.get(period_type) or TimeRange(period_type)
    return get_date_range(time_range)


# ---- Sentiments (processed_app_reviews, latest_analysis->sentiment) ----
def _calculate_sentiment_metric(
    app_id: str, metric_code: str, start_date: datetime, end_date: datetime
) -> float:
    """One query per metric; table: processed_app_reviews."""
    app_id = str(app_id)
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")
    logger.info(f"Calculating sentiment metric {metric_code} for app {app_id} from {start_str} to {end_str}")
    if metric_code == "total_reviews":
        q = """
        SELECT COUNT(*) AS val FROM processed_app_reviews
        WHERE app_id = %s AND latest_analysis IS NOT NULL
          AND DATE(review_created_at) BETWEEN %s AND %s
        """
        params = (app_id, start_str, end_str)
    elif metric_code == "positive_reviews":
        q = """
        SELECT COUNT(*) AS val FROM processed_app_reviews
        WHERE app_id = %s AND latest_analysis IS NOT NULL
          AND latest_analysis->'sentiment'->'overall'->>'classification' = 'positive'
          AND DATE(review_created_at) BETWEEN %s AND %s
        """
        params = (app_id, start_str, end_str)
    elif metric_code == "negative_reviews":
        q = """
        SELECT COUNT(*) AS val FROM processed_app_reviews
        WHERE app_id = %s AND latest_analysis IS NOT NULL
          AND latest_analysis->'sentiment'->'overall'->>'classification' = 'negative'
          AND DATE(review_created_at) BETWEEN %s AND %s
        """
        params = (app_id, start_str, end_str)
    elif metric_code == "sentiment_nps":
        # NPS = % positive - % negative among reviews with clear sentiment (positive or negative only).
        # Denominator = pos + neg so the score reflects sentiment split, not diluted by neutrals.
        q = """
        SELECT
          COUNT(*) FILTER (WHERE latest_analysis->'sentiment'->'overall'->>'classification' = 'positive') AS pos,
          COUNT(*) FILTER (WHERE latest_analysis->'sentiment'->'overall'->>'classification' = 'negative') AS neg
        FROM processed_app_reviews
        WHERE app_id = %s AND latest_analysis IS NOT NULL
          AND DATE(review_created_at) BETWEEN %s AND %s
        """
        params = (app_id, start_str, end_str)
        with pooled_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(q, params)
                row = cursor.fetchone()
        if not row:
            return 0.0
        pos = int(row["pos"] or 0)
        neg = int(row["neg"] or 0)
        sentiment_total = pos + neg
        if sentiment_total == 0:
            return 0.0
        return round(((pos - neg) / sentiment_total) * 100, 0)
    else:
        raise ValueError(f"Unknown sentiment metric_code: {metric_code}")

    with pooled_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(q, params)
            row = cursor.fetchone()
    return float(row["val"] or 0)


# ---- Issues (vw_flattened_issues, severity) ----
# Filter by review_created_at in time range; app scope via join to processed_app_reviews.
def _calculate_issues_metric(
    app_id: str, metric_code: str, start_date: datetime, end_date: datetime
) -> float:
    """Count issues where review_created_at is in date range. App filter via join to processed_app_reviews."""
    app_id = str(app_id)
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")
    logger.info(f"Calculating issues metric {metric_code} for app {app_id} from {start_str} to {end_str}")
    join_app = """
        FROM vw_flattened_issues v
        JOIN processed_app_reviews p ON p.review_id = v.review_id AND p.app_id = %s
        WHERE DATE(p.review_created_at) BETWEEN %s AND %s
    """
    if metric_code == "total_issues":
        q = "SELECT COUNT(*) AS val " + join_app
    elif metric_code == "critical_issues":
        q = "SELECT COUNT(*) AS val " + join_app.replace(
            "BETWEEN %s AND %s", "BETWEEN %s AND %s\n        AND v.severity = 'critical'"
        )
    elif metric_code == "high_issues":
        q = "SELECT COUNT(*) AS val " + join_app.replace(
            "BETWEEN %s AND %s", "BETWEEN %s AND %s\n        AND v.severity = 'high'"
        )
    elif metric_code == "medium_issues":
        q = "SELECT COUNT(*) AS val " + join_app.replace(
            "BETWEEN %s AND %s", "BETWEEN %s AND %s\n        AND v.severity = 'medium'"
        )
    else:
        raise ValueError(f"Unknown issues metric_code: {metric_code}")

    params = (app_id, start_str, end_str)
    with pooled_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(q, params)
            row = cursor.fetchone()
    return float(row["val"] or 0)


# ---- Delights (processed_app_reviews, positive_feedback->positive_mentions, impact_score) ----
def _calculate_delights_metric(
    app_id: str, metric_code: str, start_date: datetime, end_date: datetime
) -> float:
    """One query per metric; table: processed_app_reviews, jsonb positive_mentions."""
    app_id = str(app_id)
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")
    logger.info(f"Calculating delights metric {metric_code} for app {app_id} from {start_str} to {end_str}")
    # Count rows from expanded positive_mentions in date range
    if metric_code == "total_positives":
        q = """
        SELECT COUNT(*) AS val
        FROM processed_app_reviews pr,
             jsonb_array_elements(pr.latest_analysis->'positive_feedback'->'positive_mentions') AS pm
        WHERE pr.app_id = %s AND pr.latest_analysis IS NOT NULL
          AND pr.latest_analysis->'positive_feedback'->'positive_mentions' IS NOT NULL
          AND jsonb_typeof(pr.latest_analysis->'positive_feedback'->'positive_mentions') = 'array'
          AND DATE(pr.review_created_at) BETWEEN %s AND %s
        """
        params = (app_id, start_str, end_str)
    elif metric_code == "high_impact":
        q = """
        SELECT COUNT(*) AS val
        FROM processed_app_reviews pr,
             jsonb_array_elements(pr.latest_analysis->'positive_feedback'->'positive_mentions') AS pm
        WHERE pr.app_id = %s AND (pm->>'impact_score')::numeric > 70
          AND DATE(pr.review_created_at) BETWEEN %s AND %s
        """
        params = (app_id, start_str, end_str)
    elif metric_code == "medium_impact":
        q = """
        SELECT COUNT(*) AS val
        FROM processed_app_reviews pr,
             jsonb_array_elements(pr.latest_analysis->'positive_feedback'->'positive_mentions') AS pm
        WHERE pr.app_id = %s AND (pm->>'impact_score')::numeric > 40 AND (pm->>'impact_score')::numeric <= 70
          AND DATE(pr.review_created_at) BETWEEN %s AND %s
        """
        params = (app_id, start_str, end_str)
    elif metric_code == "low_impact":
        q = """
        SELECT COUNT(*) AS val
        FROM processed_app_reviews pr,
             jsonb_array_elements(pr.latest_analysis->'positive_feedback'->'positive_mentions') AS pm
        WHERE pr.app_id = %s AND (pm->>'impact_score')::numeric <= 40
          AND DATE(pr.review_created_at) BETWEEN %s AND %s
        """
        params = (app_id, start_str, end_str)
    else:
        raise ValueError(f"Unknown delights metric_code: {metric_code}")

    with pooled_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(q, params)
            row = cursor.fetchone()
    return float(row["val"] or 0)


# ---- Recommendations (vw_flattened_actions: total_actions, quick_wins, must_do, good_to_have) ----
# Filter by review_created_at in time range; app scope via join to processed_app_reviews.
def _calculate_recommendations_metric(
    app_id: str, metric_code: str, start_date: datetime, end_date: datetime
) -> float:
    """Count distinct actions where review_created_at is in date range. App filter via join to processed_app_reviews."""
    app_id = str(app_id)
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")
    logger.info(f"Calculating recommendations metric {metric_code} for app {app_id} from {start_str} to {end_str}")
    join_app = """
        FROM vw_flattened_actions v
        JOIN processed_app_reviews p ON p.review_id = v.review_id AND p.app_id = %s
        WHERE DATE(p.review_created_at) BETWEEN %s AND %s
    """
    params = (app_id, start_str, end_str)

    if metric_code == "total_actions":
        q = "SELECT COUNT(DISTINCT v.action_desc) AS val " + join_app
    elif metric_code == "quick_wins":
        q = "SELECT COUNT(DISTINCT v.action_desc) AS val " + join_app.replace(
            "BETWEEN %s AND %s", "BETWEEN %s AND %s\n        AND v.estimated_effort = 'low' AND v.suggested_timeline = 'short-term'"
        )
    elif metric_code == "must_do":
        q = "SELECT COUNT(DISTINCT v.action_desc) AS val " + join_app.replace(
            "BETWEEN %s AND %s", "BETWEEN %s AND %s\n        AND v.estimated_effort = 'high'"
        )
    elif metric_code == "good_to_have":
        q = "SELECT COUNT(DISTINCT v.action_desc) AS val " + join_app.replace(
            "BETWEEN %s AND %s", "BETWEEN %s AND %s\n        AND v.estimated_effort = 'low' AND v.suggested_timeline != 'short-term'"
        )
    else:
        raise ValueError(f"Unknown recommendations metric_code: {metric_code}")

    with pooled_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(q, params)
            row = cursor.fetchone()
    return float(row["val"] or 0)


def get_metric_by_id(metric_id: int) -> dict:
    """Fetch metric row by metric_id (metric_code, metric_category)."""
    with pooled_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(
                "SELECT metric_id, metric_code, metric_category, display_name FROM metrics WHERE metric_id = %s AND is_active = TRUE",
                (metric_id,),
            )
            row = cursor.fetchone()
    if not row:
        raise ValueError(f"Unknown or inactive metric_id: {metric_id}")
    return dict(row)


def calculate_metric(app_id, metric_id: int, period_id: int):
    """
    Compute the numeric value for one metric for the given app and period.
    Returns a number (int or float). Uses DB only; one metric at a time.
    """
    app_id = str(app_id)
    metric = get_metric_by_id(metric_id)
    metric_code = (metric.get("metric_code") or "").strip().lower()
    metric_category = (metric.get("metric_category") or "").strip()

    start_date, end_date = get_date_range_for_period(period_id)

    if metric_category == "Sentiments":
        value = _calculate_sentiment_metric(app_id, metric_code, start_date, end_date)
    elif metric_category == "Issues":
        value = _calculate_issues_metric(app_id, metric_code, start_date, end_date)
    elif metric_category == "Delights":
        value = _calculate_delights_metric(app_id, metric_code, start_date, end_date)
    elif metric_category == "Recommendations":
        value = _calculate_recommendations_metric(app_id, metric_code, start_date, end_date)
    else:
        raise ValueError(f"Unknown metric_category: {metric_category}")

    if isinstance(value, (Decimal,)):
        value = float(value)
    logger.info(f"Calculated {metric.get('display_name')} ({metric_code}) = {value} for app {app_id}, period_id {period_id}")
    return value
