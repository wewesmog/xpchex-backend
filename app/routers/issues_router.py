from fastapi import APIRouter, HTTPException, Query, status
from datetime import datetime, timedelta
import logging
from typing import Optional, List
from enum import Enum
from dateutil.relativedelta import relativedelta
import ast

from app.shared_services.db import get_postgres_connection
import pandas as pd

logger = logging.getLogger(__name__)

class TimeRange(str, Enum):
    LAST_7_DAYS = "last_7_days"
    LAST_30_DAYS = "last_30_days" 
    LAST_90_DAYS = "last_90_days"
    LAST_6_MONTHS = "last_6_months"
    LAST_12_MONTHS = "last_12_months"
    THIS_YEAR = "this_year"
    ALL_TIME = "all_time"

class Granularity(str, Enum):
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"
    YEARLY = "yearly"

router = APIRouter(
    prefix="/issues",
    tags=["issues"]
)

@router.get("/issues_analytics", status_code=status.HTTP_200_OK)
async def get_issues_analytics(
    app_id: str = Query(..., description="App ID"),
    time_range: TimeRange = Query(default=TimeRange.THIS_YEAR),
    severity: Optional[str] = Query(default=None),
    category: Optional[str] = Query(default=None)
):
    """
    Get issues analytics with automatic granularity assignment based on time range.
    
    Automatic granularity rules:
    - Last 7 days: Daily aggregation
    - Last 30-90 days: Weekly aggregation  
    - Last 6-12 months: Monthly aggregation
    - This year: Monthly aggregation
    - All time: Dynamic (yearly if >1 year of data, monthly otherwise)
    
    Granularity is automatically determined and cannot be overridden.
    """
    try:
        # Auto-determine granularity based on time range
        granularity = _get_granularity_for_range(time_range)
        
        # Calculate date range
        start_date, end_date = _calculate_date_range(time_range)
        
        # Get aggregated data based on granularity
        if granularity == Granularity.DAILY:
            data = await _get_aggregated_issues_data(start_date, end_date, granularity, severity, category)
        elif granularity == Granularity.WEEKLY:
            data = await _get_aggregated_issues_data(start_date, end_date, granularity, severity, category)
        elif granularity == Granularity.MONTHLY:
            data = await _get_aggregated_issues_data(start_date, end_date, granularity, severity, category)
        elif granularity == Granularity.YEARLY:
            data = await _get_aggregated_issues_data(start_date, end_date, granularity, severity, category)
        else:
            data = await _get_aggregated_issues_data(start_date, end_date, granularity, severity, category)
            

        return {
            "status": "success",
            "time_range": time_range,
            "granularity": granularity,
            "date_range": {
                "start": start_date.isoformat(),
                "end": end_date.isoformat()
            },
            "data": data
        }
        
    except Exception as e:
        logger.error(f"Error getting issues analytics: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, 
            detail=f"Error getting issues analytics: {str(e)}"
        )

@router.get("/list", status_code=status.HTTP_200_OK)
async def list_issues(
    app_id: str = Query(..., description="App ID"),
    time_range: TimeRange = Query(default=TimeRange.THIS_YEAR),
    order_by: str = Query(default='count'),
    severity: Optional[str] = Query(default=None),
    category: Optional[str] = Query(default=None),
    limit: int = Query(default=5, ge=1, le=100),
    offset: int = Query(default=0, ge=0)
):
    """List individual issues with filtering by time range"""
    try:
        # Calculate date range based on time_range parameter
        start_date, end_date = _calculate_date_range(time_range)
        
        # Get issues data filtered by date range
        issues = await _get_issues_list(
            start_date=start_date, 
            end_date=end_date, 
            order_by=order_by,
            severity=severity, 
            category=category, 
            limit=limit, 
            offset=offset

        )
        
        # Get total count for pagination
        total_count = await _get_issues_list_count(
            start_date=start_date,
            end_date=end_date,
            severity=severity,
            category=category
        )
        
        return {
            "status": "success",
            "time_range": time_range,
            "order_by" : order_by,
            "date_range": {
                "start": start_date.isoformat(),
                "end": end_date.isoformat()
            },
            "pagination": {
                "total": total_count,
                "limit": limit,
                "offset": offset,
                "has_more": (offset + limit) < total_count
            },
            "data": issues
        }
        
    except Exception as e:
        logger.error(f"Error listing issues: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error listing issues: {str(e)}"
        )
# Helper functions
def _get_granularity_for_range(time_range: TimeRange) -> Granularity:
    """Auto-determine granularity based on time range"""
    if time_range in [TimeRange.LAST_7_DAYS]:
        return Granularity.DAILY
    elif time_range in [TimeRange.THIS_YEAR, TimeRange.LAST_90_DAYS]:
        return Granularity.WEEKLY
    elif time_range in [TimeRange.LAST_6_MONTHS, TimeRange.LAST_12_MONTHS]:
        return Granularity.MONTHLY
    elif time_range in [TimeRange.THIS_YEAR]:
        return Granularity.MONTHLY
    elif time_range == TimeRange.ALL_TIME:
        # For all time, dynamically determine based on data span
        return _get_alltime_granularity()
    else:
        return Granularity.MONTHLY

def _calculate_date_range(time_range: TimeRange) -> tuple[datetime, datetime]:
    """Calculate start and end dates based on time range"""
    now = datetime.now()
    
    if time_range == TimeRange.LAST_7_DAYS:
        # Daily granularity: up to yesterday end
        end_date = now.replace(hour=23, minute=59, second=59, microsecond=999999) - timedelta(days=1)
        start_date = end_date - timedelta(days=6)  # 7 days total including end_date
    elif time_range == TimeRange.THIS_YEAR:
        # Weekly granularity: up to last complete week (Sunday)
        days_since_sunday = (now.weekday() + 1) % 7  # Monday=0, so Sunday=6 -> (6+1)%7=0
        last_sunday = now - timedelta(days=days_since_sunday)
        end_date = last_sunday.replace(hour=23, minute=59, second=59, microsecond=999999)
        start_date = end_date - timedelta(days=29)  # 30 days total
    elif time_range == TimeRange.LAST_90_DAYS:
        # Weekly granularity: up to last complete week (Sunday)
        days_since_sunday = (now.weekday() + 1) % 7
        last_sunday = now - timedelta(days=days_since_sunday)
        end_date = last_sunday.replace(hour=23, minute=59, second=59, microsecond=999999)
        start_date = end_date - timedelta(days=89)  # 90 days total
    elif time_range == TimeRange.LAST_6_MONTHS:
        # Monthly granularity: up to last complete month
        first_day_current_month = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        end_date = first_day_current_month - timedelta(seconds=1)  # End of last month
        start_date = (first_day_current_month - relativedelta(months=6)).replace(day=1)  # Start of 6 months ago
    elif time_range == TimeRange.LAST_12_MONTHS:
        # Monthly granularity: up to last complete month
        first_day_current_month = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        end_date = first_day_current_month - timedelta(seconds=1)  # End of last month
        start_date = (first_day_current_month - relativedelta(months=12)).replace(day=1)  # Start of 12 months ago
    elif time_range == TimeRange.THIS_YEAR:
        # Monthly granularity: from Jan 1 to end of last complete month
        start_date = datetime(now.year, 1, 1)
        first_day_current_month = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        end_date = first_day_current_month - timedelta(seconds=1)  # End of last month
    elif time_range == TimeRange.ALL_TIME:
        # Yearly granularity: up to end of last complete month
        first_day_current_month = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        end_date = first_day_current_month - timedelta(seconds=1)  # End of last month
        start_date = end_date - relativedelta(years=5)
    else:
        # Default: up to end of last complete month
        first_day_current_month = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        end_date = first_day_current_month - timedelta(seconds=1)
        start_date = end_date - relativedelta(years=5)
    
    return start_date, end_date

def _get_alltime_granularity() -> Granularity:
    """
    Dynamically determine granularity for all-time data based on data span.
    If the app has been collecting data for more than 1 year, use yearly aggregation.
    Otherwise, use monthly aggregation.
    """
    try:
        # Get the minimum date from the database synchronously
        query = """
        SELECT MIN(REVIEW_CREATED_AT) FROM vw_flattened_issues
        """
        with get_postgres_connection() as conn:
            result = pd.read_sql(query, conn)
            if not result.empty and result.iloc[0, 0] is not None:
                min_date = result.iloc[0, 0]
                current_date = datetime.now()
                
                # Calculate the difference in years
                years_diff = (current_date - min_date).days / 365.25
                
                if years_diff > 1:
                    return Granularity.YEARLY
                else:
                    return Granularity.MONTHLY
            else:
                # If no data found, default to monthly
                return Granularity.MONTHLY
    except Exception as e:
        logger.warning(f"Error determining all-time granularity: {str(e)}. Defaulting to monthly.")
        return Granularity.MONTHLY

# Automatic granularity assignment based on time range:
# - Last 7 days: Daily aggregation
# - Last 30-90 days: Weekly aggregation  
# - Last 6-12 months: Monthly aggregation
# - This year: Monthly aggregation
# - All time: Dynamic (yearly if >1 year of data, monthly otherwise)

    



async def _get_aggregated_issues_data(
    start_date: datetime,
    end_date: datetime,
    aggregation_level: str,
    severity: Optional[str] = None,
    category: Optional[str] = None
):
    """
    Get aggregated issues data for a given date range and aggregation level.
    """
    
    # Map aggregation levels to SQL DATE_TRUNC arguments
    aggregation_map = {
        'daily': 'day',
        'weekly': 'week',
        'monthly': 'month',
        'yearly': 'year'
    }
    
    if aggregation_level not in aggregation_map:
        raise ValueError("Invalid aggregation level. Must be 'daily', 'weekly', 'monthly', or 'yearly'.")

    trunc_level = aggregation_map[aggregation_level]

    base_query = f"""
    WITH RankedData AS (
        SELECT
            "issue_type",
            "severity",
            "category",
            DATE_TRUNC('{trunc_level}', REVIEW_CREATED_AT) AS issue_period,
            COUNT(*) as issue_count,
            ROW_NUMBER() OVER (
                PARTITION BY DATE_TRUNC('{trunc_level}', REVIEW_CREATED_AT), "desc", "issue_type"
                ORDER BY COUNT(*) DESC, "issue_type"
            ) AS rn
        FROM
            vw_flattened_issues
        WHERE
            DATE(REVIEW_CREATED_AT) BETWEEN %s AND %s
            -- Dynamic filters will be added here
        GROUP BY
            "desc", "issue_type", "severity", "category", "snippet", "key_words", DATE_TRUNC('{trunc_level}', REVIEW_CREATED_AT)
    )
    SELECT
        issue_period,
        SUM(issue_count) AS total_issues,
        SUM(CASE WHEN severity = 'critical' THEN issue_count ELSE 0 END) AS critical_count,
        SUM(CASE WHEN severity = 'high' THEN issue_count ELSE 0 END) AS high_count,
        SUM(CASE WHEN severity = 'medium' THEN issue_count ELSE 0 END) AS medium_count,
        SUM(CASE WHEN severity = 'low' THEN issue_count ELSE 0 END) AS low_count,
        SUM(CASE WHEN "issue_type" = 'Bug' THEN issue_count ELSE 0 END) AS bug_count
    FROM
        RankedData
    GROUP BY
        issue_period
    ORDER BY
        issue_period;
    """

    where_parts = []
    params = [start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')]

    if severity:
        # Handle comma-separated severity values
        severity_list = [s.strip() for s in severity.split(',')]
        placeholders = ', '.join(['%s'] * len(severity_list))
        where_parts.append(f"severity IN ({placeholders})")
        params.extend(severity_list)
    if category:
        where_parts.append("category = %s")
        params.append(category)

    final_query = base_query.replace(
        "-- Dynamic filters will be added here",
        " AND " + " AND ".join(where_parts) if where_parts else ""
    )

    try:
        with get_postgres_connection() as conn:
            logger.info(f"Executing aggregation query with params: {params}")
            logger.info(f"Final query: {final_query}")
            data = pd.read_sql(final_query, conn, params=tuple(params))
            if not data.empty:
                logger.info(f"Issues data found: {len(data)} rows")
                logger.info(f"Data columns: {list(data.columns)}")
                
                # Convert DataFrame to JSON-safe dictionary format
                try:
                    # Convert DataFrame to the format expected by frontend charts
                    # Ensure all data types are JSON-serializable
                    result = {}
                    for col in data.columns:
                        column_data = {}
                        for i in range(len(data)):
                            value = data[col].iloc[i]
                            
                            # Convert pandas/numpy types to native Python types
                            if pd.isna(value):
                                column_data[str(i)] = None
                            elif col == 'issue_period':
                                # Handle datetime/timestamp columns
                                if hasattr(value, 'strftime'):
                                    column_data[str(i)] = value.strftime('%Y-%m-%d')
                                else:
                                    column_data[str(i)] = str(value)
                            else:
                                # Handle numeric columns - convert numpy types to Python types
                                try:
                                    if isinstance(value, (int, float)):
                                        column_data[str(i)] = float(value)
                                    elif hasattr(value, 'item'):  # numpy types have .item() method
                                        column_data[str(i)] = value.item()
                                    else:
                                        column_data[str(i)] = str(value)
                                except (ValueError, TypeError):
                                    column_data[str(i)] = str(value)
                        
                        result[col] = column_data
                    
                    return result
                    
                except Exception as conversion_error:
                    logger.error(f"Error converting data to JSON format: {conversion_error}")
                    # Fallback: return empty result
                    return {}
            else:
                logger.warning("No aggregated issues data found - this might indicate a query issue")
                return {}
    except Exception as e:
        logger.error(f"Error getting issues data: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error getting issues data: {str(e)}"
        )

# Example usage within a FastAPI endpoint
# @app.get("/issues/daily")
# async def get_daily_issues(start_date: datetime, end_date: datetime, ...):
#     return await _get_issues_data(start_date, end_date, 'daily', ...)

# @app.get("/issues/monthly")
# async def get_monthly_issues(start_date: datetime, end_date: datetime, ...):
#     return await _get_issues_data(start_date, end_date, 'monthly', ...)

async def _get_issues_list(
    start_date: datetime,
    end_date: datetime,
    order_by: str = 'count',
    severity: Optional[str] = None,
    category: Optional[str] = None,
    issue_type: Optional[str] = None,
    sort_by: Optional[str] = None,
    order: Optional[str] = 'DESC',
    limit: Optional[int] = None,
    offset: Optional[int] = None
):
    """
    Get a filtered and aggregated list of issues.
    """
    
    # 1. Input Validation for literal values
    valid_sort_columns = ['count', '"desc"', 'issue_type', 'severity', 'category']
    valid_order_directions = ['ASC', 'DESC']

    if sort_by and sort_by not in valid_sort_columns:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid sort_by column. Must be one of: {', '.join(valid_sort_columns)}"
        )
    if order and order.upper() not in valid_order_directions:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid order direction. Must be 'ASC' or 'DESC'."
        )
    
    # 2. SQL Query with placeholders
    base_query = """
    WITH RankedData AS (
        SELECT
            "desc",
            "issue_type",
            "severity",
            "category",
            "snippet",
            "key_words",
            ROW_NUMBER() OVER (
                PARTITION BY "desc"
                ORDER BY REVIEW_CREATED_AT DESC
            ) AS rn
        FROM
            vw_flattened_issues
        WHERE
            DATE(REVIEW_CREATED_AT) BETWEEN %s AND %s
            -- Dynamic filters will be added here
        GROUP BY
            "desc", "issue_type", "severity", "category", "snippet", "key_words", REVIEW_CREATED_AT
    )
    SELECT
        COUNT(*) AS count,
        "desc",
        MAX(CASE WHEN rn = 1 THEN "issue_type" END) AS issue_type,
        STRING_AGG(snippet, ', ') AS snippets,
        STRING_AGG(key_words, ', ') AS keywords,
        MAX(CASE WHEN rn = 1 THEN "severity" END) AS severity,
        MAX(CASE WHEN rn = 1 THEN "category" END) AS category
    FROM
        RankedData
    GROUP BY
        "desc"
    ORDER BY
        -- Dynamic order by will be added here
    """
    
    # 3. Build WHERE clause and parameter list
    where_parts = []
    params = [start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')]

    if severity:
        # Handle comma-separated severity values
        severity_list = [s.strip() for s in severity.split(',')]
        placeholders = ', '.join(['%s'] * len(severity_list))
        where_parts.append(f"severity IN ({placeholders})")
        params.extend(severity_list)
    if category:
        where_parts.append("category = %s")
        params.append(category)
    if issue_type:
        where_parts.append("issue_type = %s")
        params.append(issue_type)
        
    final_query = base_query.replace(
        "-- Dynamic filters will be added here",
        " AND " + " AND ".join(where_parts) if where_parts else ""
    )
    
    # 4. Inject literal values into ORDER BY clause
    order_by_clause = f"{order_by} {order}" if not sort_by else f'"{sort_by}" {order}'
    final_query = final_query.replace("-- Dynamic order by will be added here", order_by_clause)
    
    # 5. Add pagination if specified
    if limit is not None:
        final_query += f" LIMIT {limit}"
        if offset is not None:
            final_query += f" OFFSET {offset}"
    
    # 6. Execute query and return data
    try:
        with get_postgres_connection() as conn:
            data = pd.read_sql(final_query, conn, params=tuple(params))
            if not data.empty:
                logger.info(f"List data: {len(data)}")
                
                # Convert the data to records and parse JSON fields
                records = data.to_dict('records')
                
                # Simple string replacement to remove inner brackets
                for record in records:
                    if 'snippets' in record and record['snippets']:
                        # Convert to string, replace all inner brackets, then parse back
                        snippets_str = str(record['snippets'])
                        # Remove all inner brackets by replacing multiple patterns
                        snippets_str = snippets_str.replace('[[', '[').replace(']]', ']').replace('], [', ', ').replace('], [', ', ')
                        record['snippets'] = ast.literal_eval(snippets_str)
                    
                    if 'keywords' in record and record['keywords']:
                        # Convert to string, replace all inner brackets, then parse back
                        keywords_str = str(record['keywords'])
                        # Remove all inner brackets by replacing multiple patterns
                        keywords_str = keywords_str.replace('[[', '[').replace(']]', ']').replace('], [', ', ').replace('], [', ', ')
                        record['keywords'] = ast.literal_eval(keywords_str)
                
                return records
            else:
                logger.info("No list data found")
                return []
    except Exception as e:
        logger.error(f"Error getting list data: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error getting list data: {str(e)}"
        )


async def _get_minimum_date(app_id: str):
    """Get minimum date for a given app_id"""
    query = f"""
    SELECT MIN(REVIEW_CREATED_AT) FROM vw_flattened_issues WHERE app_id = %s
    """
    return pd.read_sql(query, get_postgres_connection(), params=(app_id,))

async def _get_issues_list_count(
    start_date: datetime,
    end_date: datetime,
    severity: Optional[str] = None,
    category: Optional[str] = None,
    issue_type: Optional[str] = None,
):
    """
    Get the count of distinct issues with optional filters.
    """
    
    # SQL Query to get the count of distinct issues
    base_query = """
    SELECT
        COUNT(*) AS count
    FROM (
        WITH RankedData AS (
            SELECT
                "desc",
                "issue_type",
                "severity",
                "category",
                "snippet",
                "key_words",
                ROW_NUMBER() OVER (
                    PARTITION BY "desc"
                    ORDER BY REVIEW_CREATED_AT DESC
                ) AS rn
            FROM
                vw_flattened_issues
            WHERE
                DATE(REVIEW_CREATED_AT) BETWEEN %s AND %s
                -- Dynamic filters will be added here
            GROUP BY
                "desc", "issue_type", "severity", "category", "snippet", "key_words", REVIEW_CREATED_AT
        )
        SELECT
            "desc"
        FROM
            RankedData
        GROUP BY
            "desc"
    ) AS final_issues;
    """
    
    # Build WHERE clause and parameter list
    where_parts = []
    params = [start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')]

    if severity:
        # Handle comma-separated severity values
        severity_list = [s.strip() for s in severity.split(',')]
        placeholders = ', '.join(['%s'] * len(severity_list))
        where_parts.append(f"severity IN ({placeholders})")
        params.extend(severity_list)
    if category:
        where_parts.append("category = %s")
        params.append(category)
    if issue_type:
        where_parts.append("issue_type = %s")
        params.append(issue_type)
        
    final_query = base_query.replace(
        "-- Dynamic filters will be added here",
        " AND " + " AND ".join(where_parts) if where_parts else ""
    )
    
    # Execute query and return data
    try:
        with get_postgres_connection() as conn:
            data = pd.read_sql(final_query, conn, params=tuple(params))
            if not data.empty:
                count = int(data['count'].iloc[0])
                logger.info(f"Filtered issue count: {count}")
                return count
            else:
                logger.info("No issues found with filters, count is 0")
                return 0
    except Exception as e:
        logger.error(f"Error getting filtered issue count: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error getting filtered issue count: {str(e)}"
        )
