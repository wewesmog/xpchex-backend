from fastapi import APIRouter, HTTPException, Query, status
from datetime import datetime, timedelta
import logging
from typing import Optional, List
from dateutil.relativedelta import relativedelta
import ast

from app.shared_services.db import pooled_connection
from app.shared_services.date_ranges import (
    TimeRange,
    get_date_range,
    Granularity,
    get_granularity_for_range,
)
import pandas as pd

logger = logging.getLogger(__name__)

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
    - Last 30 days–3 months: Weekly aggregation  
    - Last 6-12 months: Monthly aggregation
    - This year: Monthly aggregation
    - All time: Dynamic (yearly if >1 year of data, monthly otherwise)
    
    Granularity is automatically determined and cannot be overridden.
    """
    try:
        # Auto-determine granularity based on time range (app_id needed for all-time)
        granularity = get_granularity_for_range(
            time_range, app_id, all_time_source="issues"
        )
        
        # Calculate date range
        start_date, end_date = get_date_range(time_range)
        
        # Get aggregated data based on granularity (scoped by app_id)
        if granularity == Granularity.DAILY:
            data = await _get_aggregated_issues_data(app_id, start_date, end_date, granularity, severity, category)
        elif granularity == Granularity.WEEKLY:
            data = await _get_aggregated_issues_data(app_id, start_date, end_date, granularity, severity, category)
        elif granularity == Granularity.MONTHLY:
            data = await _get_aggregated_issues_data(app_id, start_date, end_date, granularity, severity, category)
        elif granularity == Granularity.YEARLY:
            data = await _get_aggregated_issues_data(app_id, start_date, end_date, granularity, severity, category)
        else:
            data = await _get_aggregated_issues_data(app_id, start_date, end_date, granularity, severity, category)
            

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
        start_date, end_date = get_date_range(time_range)
        
        # Get issues data filtered by date range and app_id
        issues = await _get_issues_list(
            app_id=app_id,
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
            app_id=app_id,
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
async def _get_aggregated_issues_data(
    app_id: str,
    start_date: datetime,
    end_date: datetime,
    aggregation_level: str,
    severity: Optional[str] = None,
    category: Optional[str] = None
):
    """
    Get aggregated issues data for a given app_id, date range and aggregation level.
    Scoped by app via join to processed_app_reviews (vw_flattened_issues may not have app_id).
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
    app_id = str(app_id)

    base_query = f"""
    WITH RankedData AS (
        SELECT
            v."issue_type",
            v."severity",
            v."category",
            DATE_TRUNC('{trunc_level}', p.review_created_at) AS issue_period,
            COUNT(*) as issue_count,
            ROW_NUMBER() OVER (
                PARTITION BY DATE_TRUNC('{trunc_level}', p.review_created_at), v."desc", v."issue_type"
                ORDER BY COUNT(*) DESC, v."issue_type"
            ) AS rn
        FROM
            vw_flattened_issues v
            JOIN processed_app_reviews p ON p.review_id = v.review_id AND p.app_id = %s
        WHERE
            DATE(p.review_created_at) BETWEEN %s AND %s
            AND v."desc" IS NOT NULL
            AND NULLIF(TRIM(v."desc"), '') IS NOT NULL
            -- Dynamic filters will be added here
        GROUP BY
            v."desc", v."issue_type", v."severity", v."category", v."snippet", v."key_words", DATE_TRUNC('{trunc_level}', p.review_created_at)
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
    params = [app_id, start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')]

    if severity:
        # Handle comma-separated severity values
        severity_list = [s.strip() for s in severity.split(',')]
        placeholders = ', '.join(['%s'] * len(severity_list))
        where_parts.append(f"v.severity IN ({placeholders})")
        params.extend(severity_list)
    if category:
        where_parts.append("v.category = %s")
        params.append(category)

    final_query = base_query.replace(
        "-- Dynamic filters will be added here",
        " AND " + " AND ".join(where_parts) if where_parts else ""
    )

    try:
        with pooled_connection() as conn:
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
    app_id: str,
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
    Get a filtered and aggregated list of issues for this app. Scoped by app_id via join.
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
    
    app_id = str(app_id)
    # 2. SQL Query with placeholders (scoped by app via join to processed_app_reviews)
    base_query = """
    WITH RankedData AS (
        SELECT
            v."desc",
            v."issue_type",
            v."severity",
            v."category",
            v."snippet",
            v."key_words",
            ROW_NUMBER() OVER (
                PARTITION BY v."desc"
                ORDER BY p.review_created_at DESC
            ) AS rn
        FROM
            vw_flattened_issues v
            JOIN processed_app_reviews p ON p.review_id = v.review_id AND p.app_id = %s
        WHERE
            DATE(p.review_created_at) BETWEEN %s AND %s
            AND v."desc" IS NOT NULL
            AND NULLIF(TRIM(v."desc"), '') IS NOT NULL
            -- Dynamic filters will be added here
        GROUP BY
            v."desc", v."issue_type", v."severity", v."category", v."snippet", v."key_words", p.review_created_at
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
    params = [app_id, start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')]

    if severity:
        severity_list = [s.strip() for s in severity.split(',')]
        placeholders = ', '.join(['%s'] * len(severity_list))
        where_parts.append(f"v.severity IN ({placeholders})")
        params.extend(severity_list)
    if category:
        where_parts.append("v.category = %s")
        params.append(category)
    if issue_type:
        where_parts.append("v.issue_type = %s")
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
        with pooled_connection() as conn:
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
    """Get minimum date for a given app_id. Scoped via join (vw_flattened_issues may not have app_id)."""
    query = """
    SELECT MIN(v.REVIEW_CREATED_AT) FROM vw_flattened_issues v
    JOIN processed_app_reviews p ON p.review_id = v.review_id AND p.app_id = %s
    """
    with pooled_connection() as conn:
        return pd.read_sql(query, conn, params=(str(app_id),))

async def _get_issues_list_count(
    app_id: str,
    start_date: datetime,
    end_date: datetime,
    severity: Optional[str] = None,
    category: Optional[str] = None,
    issue_type: Optional[str] = None,
):
    """
    Get the count of distinct issues with optional filters. Scoped by app_id via join.
    """
    
    app_id = str(app_id)
    # SQL Query to get the count of distinct issues (scoped by app)
    base_query = """
    SELECT
        COUNT(*) AS count
    FROM (
        WITH RankedData AS (
            SELECT
                v."desc",
                v."issue_type",
                v."severity",
                v."category",
                v."snippet",
                v."key_words",
                ROW_NUMBER() OVER (
                    PARTITION BY v."desc"
                    ORDER BY p.review_created_at DESC
                ) AS rn
            FROM
                vw_flattened_issues v
                JOIN processed_app_reviews p ON p.review_id = v.review_id AND p.app_id = %s
            WHERE
                DATE(p.review_created_at) BETWEEN %s AND %s
                AND v."desc" IS NOT NULL
                AND NULLIF(TRIM(v."desc"), '') IS NOT NULL
                -- Dynamic filters will be added here
            GROUP BY
                v."desc", v."issue_type", v."severity", v."category", v."snippet", v."key_words", p.review_created_at
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
    params = [app_id, start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')]

    if severity:
        severity_list = [s.strip() for s in severity.split(',')]
        placeholders = ', '.join(['%s'] * len(severity_list))
        where_parts.append(f"v.severity IN ({placeholders})")
        params.extend(severity_list)
    if category:
        where_parts.append("v.category = %s")
        params.append(category)
    if issue_type:
        where_parts.append("v.issue_type = %s")
        params.append(issue_type)
        
    final_query = base_query.replace(
        "-- Dynamic filters will be added here",
        " AND " + " AND ".join(where_parts) if where_parts else ""
    )
    
    # Execute query and return data
    try:
        with pooled_connection() as conn:
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
