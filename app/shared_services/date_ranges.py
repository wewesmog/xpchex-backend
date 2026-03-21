"""
Canonical date range logic for time_range (period_type).
Used by all routers (sentiments, issues, positives, actions), commentary jobs, and calculate_metrics.

Exports:
  TimeRange, get_date_range — calendar window for a preset
  Granularity, get_granularity_for_range, get_alltime_granularity — chart aggregation level
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta
from enum import Enum
from typing import Literal, Optional

from dateutil.relativedelta import relativedelta

from app.shared_services.db import pooled_connection

logger = logging.getLogger(__name__)


# Bounded "all time" = last N years through end of month before current (see ALL_TIME branch).
ALL_TIME_LOOKBACK_YEARS = 3


class TimeRange(str, Enum):
    LAST_7_DAYS = "last_7_days"
    LAST_30_DAYS = "last_30_days"
    LAST_3_MONTHS = "last_3_months"
    LAST_6_MONTHS = "last_6_months"
    LAST_12_MONTHS = "last_12_months"
    THIS_YEAR = "this_year"
    ALL_TIME = "all_time"


class Granularity(str, Enum):
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"
    YEARLY = "yearly"


_ALLTIME_VIEW_BY_SOURCE: dict[Literal["issues", "actions"], str] = {
    "issues": "vw_flattened_issues",
    "actions": "vw_flattened_actions",
}


def get_date_range(time_range: TimeRange) -> tuple[datetime, datetime]:
    """
    Single canonical (start_date, end_date) for a time range.
    Same logic everywhere: routers and calculate_metrics use this.
    """
    now = datetime.now()
    first_day_current_month = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

    if time_range == TimeRange.LAST_7_DAYS:
        end_date = now.replace(hour=23, minute=59, second=59, microsecond=999999) - timedelta(days=1)
        start_date = end_date - timedelta(days=6)
    elif time_range == TimeRange.LAST_30_DAYS:
        days_since_sunday = (now.weekday() + 1) % 7
        last_sunday = now - timedelta(days=days_since_sunday)
        end_date = last_sunday.replace(hour=23, minute=59, second=59, microsecond=999999)
        start_date = end_date - timedelta(days=29)
    elif time_range == TimeRange.LAST_3_MONTHS:
        end_date = first_day_current_month - timedelta(seconds=1)
        start_date = (first_day_current_month - relativedelta(months=3)).replace(day=1)
    elif time_range == TimeRange.LAST_6_MONTHS:
        end_date = first_day_current_month - timedelta(seconds=1)
        start_date = (first_day_current_month - relativedelta(months=6)).replace(day=1)
    elif time_range == TimeRange.LAST_12_MONTHS:
        end_date = first_day_current_month - timedelta(seconds=1)
        start_date = (first_day_current_month - relativedelta(months=12)).replace(day=1)
    elif time_range == TimeRange.THIS_YEAR:
        start_date = datetime(now.year, 1, 1)
        end_date = first_day_current_month - timedelta(seconds=1)
    elif time_range == TimeRange.ALL_TIME:
        end_date = first_day_current_month - timedelta(seconds=1)
        start_date = end_date - relativedelta(years=ALL_TIME_LOOKBACK_YEARS)
    else:
        end_date = first_day_current_month - timedelta(seconds=1)
        start_date = end_date - relativedelta(years=ALL_TIME_LOOKBACK_YEARS)

    return start_date, end_date


def get_alltime_granularity(
    app_id: str,
    source: Literal["issues", "actions"],
) -> Granularity:
    """
    For ALL_TIME only: yearly buckets if the app has more than one year of data in the
    flattened view; otherwise monthly. `source` picks vw_flattened_issues vs vw_flattened_actions.
    """
    view = _ALLTIME_VIEW_BY_SOURCE[source]
    try:
        query = f"""
        SELECT MIN(p.review_created_at) FROM {view} v
        JOIN processed_app_reviews p ON p.review_id = v.review_id AND p.app_id = %s
        """
        with pooled_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (str(app_id),))
                row = cur.fetchone()
            if row and row[0] is not None:
                min_date = row[0]
                current_date = datetime.now()
                if hasattr(min_date, "tzinfo") and min_date.tzinfo is not None:
                    min_date = min_date.replace(tzinfo=None)
                years_diff = (current_date - min_date).days / 365.25
                if years_diff > 1:
                    return Granularity.YEARLY
                return Granularity.MONTHLY
            return Granularity.MONTHLY
    except Exception as e:
        logger.warning(
            "Error determining all-time granularity (%s, app_id=%s): %s. Defaulting to monthly.",
            source,
            app_id,
            e,
        )
        return Granularity.MONTHLY


def get_granularity_for_range(
    time_range: TimeRange,
    app_id: Optional[str] = None,
    *,
    all_time_source: Optional[Literal["issues", "actions"]] = None,
) -> Granularity:
    """
    Auto-determine chart aggregation for a time preset (aligned with get_date_range).

    For TimeRange.ALL_TIME, pass `all_time_source` so the min-review query uses the correct
    flattened view (issues vs actions). If ALL_TIME is requested without app_id or
    all_time_source, returns MONTHLY (safe default).

    Commentary / batch jobs can import this alongside get_date_range for consistent metadata.
    """
    if time_range == TimeRange.LAST_7_DAYS:
        return Granularity.DAILY
    if time_range == TimeRange.LAST_30_DAYS:
        return Granularity.WEEKLY
    if time_range in (
        TimeRange.LAST_3_MONTHS,
        TimeRange.LAST_6_MONTHS,
        TimeRange.LAST_12_MONTHS,
        TimeRange.THIS_YEAR,
    ):
        return Granularity.MONTHLY
    if time_range == TimeRange.ALL_TIME:
        if not app_id or not all_time_source:
            logger.warning(
                "ALL_TIME granularity without app_id/all_time_source; defaulting to monthly."
            )
            return Granularity.MONTHLY
        return get_alltime_granularity(app_id, all_time_source)
    return Granularity.MONTHLY
