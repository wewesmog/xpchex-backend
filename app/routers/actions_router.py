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
    prefix="/actions",
    tags=["actions"]
)

@router.get("/actions_analytics", status_code=status.HTTP_200_OK)
async def get_actions_analytics(
    app_id: str = Query(..., description="App ID"),
    time_range: TimeRange = Query(default=TimeRange.LAST_30_DAYS),
    estimated_effort: Optional[str] = Query(default=None, description="Filter by effort level: low, medium, high"),
    suggested_timeline: Optional[str] = Query(default=None, description="Filter by timeline: short-term, medium-term, long-term")
):
    """
    Get actions analytics with automatic granularity assignment based on time range.
    
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
            time_range, app_id, all_time_source="actions"
        )
        
        # Calculate date range
        start_date, end_date = get_date_range(time_range)
        
        # Get aggregated data based on granularity (scoped by app_id)
        if granularity == Granularity.DAILY:
            actions_data = await _get_aggregated_actions_data(app_id, start_date, end_date, granularity, estimated_effort, suggested_timeline)
        elif granularity == Granularity.WEEKLY:
            actions_data = await _get_aggregated_actions_data(app_id, start_date, end_date, granularity, estimated_effort, suggested_timeline)
        elif granularity == Granularity.MONTHLY:
            actions_data = await _get_aggregated_actions_data(app_id, start_date, end_date, granularity, estimated_effort, suggested_timeline)
        elif granularity == Granularity.YEARLY:
            actions_data = await _get_aggregated_actions_data(app_id, start_date, end_date, granularity, estimated_effort, suggested_timeline)
        else:
            actions_data = await _get_aggregated_actions_data(app_id, start_date, end_date, granularity, estimated_effort, suggested_timeline)
            

        return {
            "status": "success",
            "time_range": time_range,
            "granularity": granularity,
            "date_range": {
                "start": start_date.isoformat(),
                "end": end_date.isoformat()
            },
            "data": actions_data
        }
        
    except Exception as e:
        logger.error(f"Error getting actions analytics: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, 
            detail=f"Error getting actions analytics: {str(e)}"
        )

@router.get("/list_actions", status_code=status.HTTP_200_OK)
async def list_actions(
    app_id: str = Query(..., description="App ID"),
    time_range: TimeRange = Query(default=TimeRange.LAST_30_DAYS),
    order_by: str = Query(default='number_of_actions'),
    estimated_effort: Optional[str] = Query(default=None, description="Filter by effort level: low, medium, high"),
    suggested_timeline: Optional[str] = Query(default=None, description="Filter by timeline: short-term, medium-term, long-term"),
    action_type: Optional[str] = Query(default=None, description="Filter by action type: investigation, improvement, fix, etc."),
    category: Optional[str] = Query(default=None, description="Filter by category: General, Authentication, Performance, etc."),
    limit: int = Query(default=5, ge=1, le=100),
    offset: int = Query(default=0, ge=0)
):
    """List individual actions with filtering by time range"""
    try:
        # Calculate date range based on time_range parameter
        start_date, end_date = get_date_range(time_range)
        
        # Get actions data filtered by date range and app_id
        actions = await _get_actions_list(
            app_id=app_id,
            start_date=start_date, 
            end_date=end_date, 
            order_by=order_by,
            estimated_effort=estimated_effort,
            suggested_timeline=suggested_timeline,
            action_type=action_type,
            category=category,
            limit=limit, 
            offset=offset
        )
        
        # Get total count for pagination
        total_count = await _get_actions_list_count(
            app_id=app_id,
            start_date=start_date,
            end_date=end_date,
            estimated_effort=estimated_effort,
            suggested_timeline=suggested_timeline,
            action_type=action_type,
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
            "data": actions
        }
        
    except Exception as e:
        logger.error(f"Error listing actions: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error listing actions: {str(e)}"
        )
async def _get_aggregated_actions_data(
    app_id: str,
    start_date: datetime,
    end_date: datetime,
    aggregation_level: str,
    estimated_effort: Optional[str] = None,
    suggested_timeline: Optional[str] = None
):
    """
    Get aggregated actions data for app_id, date range and aggregation level.
    Uses vw_flattened_actions (has review_id); scope by app via join to processed_app_reviews.
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

    # Filter by review_created_at in range; period from p.review_created_at. Scope by app via join.
    # action_volume: one row per (action_period, descr) with action_count = frequency (vw_flattened_actions has no number_of_actions).
    base_query = f"""
    WITH action_data AS (
        SELECT
            v.action_type,
            v.estimated_effort,
            v.suggested_timeline,
            v.category,
            v."desc" AS descr,
            DATE_TRUNC('{trunc_level}', p.review_created_at) AS action_period
        FROM
            vw_flattened_actions v
            JOIN processed_app_reviews p ON p.review_id = v.review_id AND p.app_id = %s
        WHERE
            DATE(p.review_created_at) BETWEEN %s AND %s
            -- Dynamic filters will be added here
    ),
    action_volume AS (
        SELECT
            action_period,
            descr,
            COUNT(*) AS action_count
        FROM action_data
        GROUP BY action_period, descr
    ),
    action_type_counts AS (
        SELECT
            action_period,
            action_type,
            count(*) AS action_type_count
        FROM
            action_data
        GROUP BY
            action_period, action_type
    ),
    estimated_effort_counts AS (
        SELECT
            action_period,
            estimated_effort,
            count(*) AS estimated_effort_count
        FROM
            action_data
        GROUP BY
            action_period, estimated_effort
    ),
    suggested_timeline_counts AS (
        SELECT
            action_period,
            suggested_timeline,
            count(*) AS suggested_timeline_count
        FROM
            action_data
        GROUP BY
            action_period, suggested_timeline
    ),
    category_counts AS (
        SELECT
            action_period,
            category,
            count(*) AS category_count
        FROM
            action_data
        GROUP BY
            action_period, category
    ),
    quartile_boundaries AS (
        SELECT
            action_period,
            PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY action_count) AS q1,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY action_count) AS q2,
            PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY action_count) AS q3
        FROM
            action_volume
        GROUP BY
            action_period
    ),
    quartile_summary AS (
        SELECT
            av.action_period,
            CASE 
                WHEN av.action_count <= qb.q1 THEN 1
                WHEN av.action_count <= qb.q2 THEN 2
                WHEN av.action_count <= qb.q3 THEN 3
                ELSE 4
            END AS quartile,
            COUNT(*) AS quartile_count
        FROM
            action_volume av
        JOIN
            quartile_boundaries qb ON av.action_period = qb.action_period
        GROUP BY
            av.action_period,
            CASE 
                WHEN av.action_count <= qb.q1 THEN 1
                WHEN av.action_count <= qb.q2 THEN 2
                WHEN av.action_count <= qb.q3 THEN 3
                ELSE 4
            END
    )
    SELECT
        action_period,
        count(descr) AS total_actions,
        sum(
            CASE
                WHEN estimated_effort = 'low' AND suggested_timeline = 'short-term' THEN 1
                ELSE 0
            END
        ) AS quick_wins,
     
        -- Quartile boundaries
        (SELECT q1 FROM quartile_boundaries WHERE action_period = ad.action_period) AS q1_boundary,
        (SELECT q2 FROM quartile_boundaries WHERE action_period = ad.action_period) AS q2_boundary,
        (SELECT q3 FROM quartile_boundaries WHERE action_period = ad.action_period) AS q3_boundary,
        -- Quartile counts
        (
            SELECT
                jsonb_agg(
                    jsonb_build_object(
                        'quartile', quartile,
                        'count', quartile_count
                    )
                    ORDER BY quartile
                )
            FROM
                quartile_summary
            WHERE
                action_period = ad.action_period
        ) AS quartile_breakdown,
        (
            SELECT
                jsonb_agg(
                    jsonb_build_object(
                        'category', category,
                        'count', category_count
                    )
                    ORDER BY category_count DESC
                )
            FROM
                category_counts
            WHERE
                action_period = ad.action_period
        ) AS category_breakdown,
        (
            SELECT
                jsonb_agg(
                    jsonb_build_object(
                        'action_type', action_type,
                        'count', action_type_count
                    )
                    ORDER BY action_type_count DESC
                )
            FROM
                action_type_counts
            WHERE
                action_period = ad.action_period
        ) AS action_type_breakdown,
        (
            SELECT
                jsonb_agg(
                    jsonb_build_object(
                        'estimated_effort', estimated_effort,
                        'count', estimated_effort_count
                    )
                    ORDER BY estimated_effort_count DESC
                )
            FROM
                estimated_effort_counts
            WHERE
                action_period = ad.action_period
        ) AS estimated_effort_breakdown,
        (
            SELECT
                jsonb_agg(
                    jsonb_build_object(
                        'suggested_timeline', suggested_timeline,
                        'count', suggested_timeline_count
                    )
                    ORDER BY suggested_timeline_count DESC
                )
            FROM
                suggested_timeline_counts
            WHERE
                action_period = ad.action_period
        ) AS suggested_timeline_breakdown
    FROM
        action_data ad
    GROUP BY
        action_period
    ORDER BY
        action_period;
    """

    app_id = str(app_id)
    where_parts = []
    params = [app_id, start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')]

    if estimated_effort:
        effort_list = [s.strip() for s in estimated_effort.split(',')]
        placeholders = ', '.join(['%s'] * len(effort_list))
        where_parts.append(f"v.estimated_effort IN ({placeholders})")
        params.extend(effort_list)
    if suggested_timeline:
        timeline_list = [s.strip() for s in suggested_timeline.split(',')]
        placeholders = ', '.join(['%s'] * len(timeline_list))
        where_parts.append(f"v.suggested_timeline IN ({placeholders})")
        params.extend(timeline_list)

    final_query = base_query.replace(
        "-- Dynamic filters will be added here",
        " AND " + " AND ".join(where_parts) if where_parts else ""
    )

    try:
        with pooled_connection() as conn:
            logger.info(f"Executing aggregation query with params: {params}")
            logger.info(f"Final query: {final_query}")
            
            debug_query = """
            SELECT COUNT(*) as total_count,
                   MIN(p.review_created_at) as min_date,
                   MAX(p.review_created_at) as max_date
            FROM vw_flattened_actions v
            JOIN processed_app_reviews p ON p.review_id = v.review_id AND p.app_id = %s
            WHERE DATE(p.review_created_at) BETWEEN %s AND %s
            """
            debug_result = pd.read_sql(debug_query, conn, params=tuple(params[:3]))
            logger.info(f"Debug - Data in date range: {debug_result.to_dict('records')}")
            
            data = pd.read_sql(final_query, conn, params=tuple(params))
            if not data.empty:
                logger.info(f"Actions data found: {len(data)} rows")
                logger.info(f"Data columns: {list(data.columns)}")
                
                # Convert DataFrame to JSON-safe format
                try:
                    # Convert DataFrame to records and handle datetime formatting
                    records = data.to_dict('records')
                    
                    # Format datetime columns
                    for record in records:
                        if 'action_period' in record and record['action_period'] is not None:
                            if hasattr(record['action_period'], 'strftime'):
                                record['action_period'] = record['action_period'].strftime('%Y-%m-%d')
                            else:
                                record['action_period'] = str(record['action_period'])
                    
                    return records
                    
                except Exception as conversion_error:
                    logger.error(f"Error converting data to JSON format: {conversion_error}")
                    # Fallback: return empty result
                    return {}
            else:
                logger.warning("No aggregated actions data found - this might indicate a query issue")
                return {}
    except Exception as e:
        logger.error(f"Error getting actions data: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error getting actions data: {str(e)}"
        )

# Example usage within a FastAPI endpoint
# @app.get("/issues/daily")
# async def get_daily_issues(start_date: datetime, end_date: datetime, ...):
#     return await _get_issues_data(start_date, end_date, 'daily', ...)

# @app.get("/issues/monthly")
# async def get_monthly_issues(start_date: datetime, end_date: datetime, ...):
#     return await _get_issues_data(start_date, end_date, 'monthly', ...)

async def _get_actions_list(
    app_id: str,
    start_date: datetime,
    end_date: datetime,
    order_by: str = 'count',
    action_type: Optional[str] = None,
    estimated_effort: Optional[str] = None,
    suggested_timeline: Optional[str] = None,
    category: Optional[str] = None,
    sort_by: Optional[str] = None,
    order: Optional[str] = 'DESC',
    limit: Optional[int] = None,
    offset: Optional[int] = None
):
    """
    Get a filtered and aggregated list of actions for this app. Scoped by app_id via join.
    """
    
    # 1. Input Validation for literal values
    valid_sort_columns = ['count', 'desc', 'action_type', 'estimated_effort', 'suggested_timeline', 'category']
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
    # Filter by review_created_at in range; first/latest = MIN/MAX(p.review_created_at) per action for table display.
    base_query = """
    WITH filtered AS (
        SELECT
            v."desc" AS descr,
            v.action_type,
            v.estimated_effort,
            v.suggested_timeline,
            v.category,
            p.review_created_at
        FROM
            vw_flattened_actions v
            JOIN processed_app_reviews p ON p.review_id = v.review_id AND p.app_id = %s
        WHERE
            DATE(p.review_created_at) BETWEEN %s AND %s
            -- Dynamic filters will be added here
    )
    SELECT
        descr,
        COUNT(*) AS number_of_actions,
        MIN(review_created_at) AS first_date_recommended,
        MAX(review_created_at) AS latest_date_recommended,
        MAX(action_type) AS action_type,
        MAX(estimated_effort) AS estimated_effort,
        MAX(suggested_timeline) AS suggested_timeline,
        MAX(category) AS category
    FROM filtered
    GROUP BY descr
    ORDER BY
        number_of_actions DESC,
        latest_date_recommended DESC
    """
    
    # 3. Build WHERE clause and parameter list
    where_parts = []
    params = [app_id, start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')]

    if action_type:
        action_type_list = [s.strip() for s in action_type.split(',')]
        placeholders = ', '.join(['%s'] * len(action_type_list))
        where_parts.append(f"v.action_type IN ({placeholders})")
        params.extend(action_type_list)
    if estimated_effort:
        estimated_effort_list = [s.strip() for s in estimated_effort.split(',')]
        placeholders = ', '.join(['%s'] * len(estimated_effort_list))
        where_parts.append(f"v.estimated_effort IN ({placeholders})")
        params.extend(estimated_effort_list)
    if suggested_timeline:
        suggested_timeline_list = [s.strip() for s in suggested_timeline.split(',')]
        placeholders = ', '.join(['%s'] * len(suggested_timeline_list))
        where_parts.append(f"v.suggested_timeline IN ({placeholders})")
        params.extend(suggested_timeline_list)
    if category:
        category_list = [s.strip() for s in category.split(',')]
        placeholders = ', '.join(['%s'] * len(category_list))
        where_parts.append(f"v.category IN ({placeholders})")
        params.extend(category_list)
        
    final_query = base_query.replace(
        "-- Dynamic filters will be added here",
        " AND " + " AND ".join(where_parts) if where_parts else ""
    )
    
    # 4. Add pagination if specified
    if limit is not None:
        final_query += f" LIMIT {limit}"
        if offset is not None:
            final_query += f" OFFSET {offset}"
    
    # 5. Execute query and return data
    try:
        with pooled_connection() as conn:
            logger.info(f"Executing actions list query with params: {params}")
            logger.info(f"Final query: {final_query}")
            data = pd.read_sql(final_query, conn, params=tuple(params))
            if not data.empty:
                logger.info(f"List data: {len(data)} rows")
                logger.info(f"Data columns: {list(data.columns)}")
                logger.info(f"Sample data: {data.head(2).to_dict('records')}")
                
                # Convert the data to records and parse JSON fields
                records = data.to_dict('records')
                
                return records
            else:
                logger.info("No actions data found")
                return []
    except Exception as e:
        logger.error(f"Error getting actions data: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error getting actions data: {str(e)}"
        )


async def _get_minimum_date(app_id: str):
    """Get minimum date for app_id. Uses MIN(p.review_created_at); scope via join to processed_app_reviews."""
    query = """
    SELECT MIN(p.review_created_at) FROM vw_flattened_actions v
    JOIN processed_app_reviews p ON p.review_id = v.review_id AND p.app_id = %s
    """
    with pooled_connection() as conn:
        return pd.read_sql(query, conn, params=(str(app_id),))

async def _get_actions_list_count(
    app_id: str,
    start_date: datetime,
    end_date: datetime,
    action_type: Optional[str] = None,
    estimated_effort: Optional[str] = None,
    suggested_timeline: Optional[str] = None,
    category: Optional[str] = None,
):
    """
    Get the count of distinct actions with optional filters. Uses vw_flattened_actions; scope by app via join.
    """
    
    app_id = str(app_id)
    # Filter by review_created_at in range; scope by app via join.
    base_query = """
    SELECT COUNT(DISTINCT v."desc") AS count
    FROM vw_flattened_actions v
    JOIN processed_app_reviews p ON p.review_id = v.review_id AND p.app_id = %s
    WHERE DATE(p.review_created_at) BETWEEN %s AND %s
    -- Dynamic filters will be added here
    """
    
    # Build WHERE clause and parameter list
    where_parts = []
    params = [app_id, start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')]

    if action_type:
        action_type_list = [s.strip() for s in action_type.split(',')]
        placeholders = ', '.join(['%s'] * len(action_type_list))
        where_parts.append(f"v.action_type IN ({placeholders})")
        params.extend(action_type_list)
    if estimated_effort:
        estimated_effort_list = [s.strip() for s in estimated_effort.split(',')]
        placeholders = ', '.join(['%s'] * len(estimated_effort_list))
        where_parts.append(f"v.estimated_effort IN ({placeholders})")
        params.extend(estimated_effort_list)
    if suggested_timeline:
        suggested_timeline_list = [s.strip() for s in suggested_timeline.split(',')]
        placeholders = ', '.join(['%s'] * len(suggested_timeline_list))
        where_parts.append(f"v.suggested_timeline IN ({placeholders})")
        params.extend(suggested_timeline_list)
    if category:
        category_list = [s.strip() for s in category.split(',')]
        placeholders = ', '.join(['%s'] * len(category_list))
        where_parts.append(f"v.category IN ({placeholders})")
        params.extend(category_list)
        
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
                logger.info(f"Filtered action count: {count}")
                return count
            else:
                logger.info("No actions found with filters, count is 0")
                return 0
    except Exception as e:
        logger.error(f"Error getting filtered action count: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error getting filtered action count: {str(e)}"
        )
