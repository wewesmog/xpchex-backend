"""
Canonical date range logic for time_range (period_type).
Used by all routers (sentiments, issues, positives, actions) and by calculate_metrics.
"""
from datetime import datetime, timedelta
from enum import Enum
from dateutil.relativedelta import relativedelta


class TimeRange(str, Enum):
    LAST_7_DAYS = "last_7_days"
    LAST_30_DAYS = "last_30_days"
    LAST_3_MONTHS = "last_3_months"
    LAST_6_MONTHS = "last_6_months"
    LAST_12_MONTHS = "last_12_months"
    THIS_YEAR = "this_year"
    ALL_TIME = "all_time"


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
        start_date = end_date - relativedelta(years=5)
    else:
        end_date = first_day_current_month - timedelta(seconds=1)
        start_date = end_date - relativedelta(years=5)

    return start_date, end_date
