# positives router

from fastapi import APIRouter, HTTPException, Query, status
from datetime import datetime, timedelta
import logging
from typing import Optional, List
from enum import Enum
from dateutil.relativedelta import relativedelta
import ast

from app.shared_services.db import pooled_connection
from app.shared_services.date_ranges import TimeRange, get_date_range
import pandas as pd

logger = logging.getLogger(__name__)

class Granularity(str, Enum):
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"
    YEARLY = "yearly"

router = APIRouter(
    prefix="/positives",
    tags=["positives"]
)

@router.get("/positives_analytics", status_code=status.HTTP_200_OK)
async def get_positives_analytics(
    app_id: str = Query(..., description="App ID"),
    time_range: TimeRange = Query(default=TimeRange.LAST_6_MONTHS),
    severity: Optional[str] = Query(default=None),
    category: Optional[str] = Query(default=None)
):
    """
    Get positives analytics with automatic granularity assignment based on time range.
    
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
        granularity = _get_granularity_for_range(time_range, app_id)
        
        # Calculate date range
        start_date, end_date = get_date_range(time_range)
        
        # Get aggregated data based on granularity (scoped by app_id)
        if granularity == Granularity.DAILY:
            data = await _get_aggregated_positives_data(app_id, start_date, end_date, granularity, severity, category)
        elif granularity == Granularity.WEEKLY:
            data = await _get_aggregated_positives_data(app_id, start_date, end_date, granularity, severity, category)
        elif granularity == Granularity.MONTHLY:
            data = await _get_aggregated_positives_data(app_id, start_date, end_date, granularity, severity, category)
        elif granularity == Granularity.YEARLY:
            data = await _get_aggregated_positives_data(app_id, start_date, end_date, granularity, severity, category)
        else:
            data = await _get_aggregated_positives_data(app_id, start_date, end_date, granularity, severity, category)
            

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
        logger.error(f"Error getting positives analytics: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, 
            detail=f"Error getting positives analytics: {str(e)}"
        )

@router.get("/list", status_code=status.HTTP_200_OK)
async def list_positives(
    app_id: str = Query(..., description="App ID"),
    time_range: TimeRange = Query(default=TimeRange.LAST_30_DAYS),
    order_by: str = Query(default='total_reviews'),
    impact_level: Optional[str] = Query(default=None),
    category: Optional[str] = Query(default=None),
    limit: int = Query(default=5, ge=1, le=100),
    offset: int = Query(default=0, ge=0)
):
    """List individual positives with filtering by time range"""
    try:
        # Calculate date range based on time_range parameter
        start_date, end_date = get_date_range(time_range)
        
        # Get positives data filtered by date range and app_id
        positives = await _get_positives_list(
            app_id=app_id,
            start_date=start_date, 
            end_date=end_date, 
            order_by=order_by,
            impact_level=impact_level, 
            category=category, 
            limit=limit, 
            offset=offset
        )
        
        # Get total count for pagination
        total_count = await _get_positives_list_count(
            app_id=app_id,
            start_date=start_date,
            end_date=end_date,
            impact_level=impact_level,
            category=category
        )
        
        return {
            "status": "success",
            "time_range": time_range,
            "order_by": order_by,
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
            "data": positives
        }
        
    except Exception as e:
        logger.error(f"Error listing positives: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error listing positives: {str(e)}"
        )
# Helper functions
def _get_granularity_for_range(time_range: TimeRange, app_id: Optional[str] = None) -> Granularity:
    """Auto-determine granularity based on time range (aligned with shared date_ranges). app_id required for ALL_TIME."""
    if time_range == TimeRange.LAST_7_DAYS:
        return Granularity.DAILY
    elif time_range == TimeRange.LAST_30_DAYS:
        return Granularity.WEEKLY
    elif time_range in [TimeRange.LAST_3_MONTHS, TimeRange.LAST_6_MONTHS, TimeRange.LAST_12_MONTHS, TimeRange.THIS_YEAR]:
        return Granularity.MONTHLY
    elif time_range == TimeRange.ALL_TIME:
        return _get_alltime_granularity(app_id)
    else:
        return Granularity.MONTHLY

def _get_alltime_granularity(app_id: Optional[str] = None) -> Granularity:
    """
    Dynamically determine granularity for all-time data based on data span for this app.
    Uses processed_app_reviews (has app_id) for positives.
    """
    try:
        if not app_id:
            return Granularity.MONTHLY
        query = """
        SELECT MIN(review_created_at) FROM processed_app_reviews WHERE app_id = %s
        """
        with pooled_connection() as conn:
            result = pd.read_sql(query, conn, params=(str(app_id),))
            if not result.empty and result.iloc[0, 0] is not None:
                min_date = result.iloc[0, 0]
                current_date = datetime.now()
                years_diff = (current_date - min_date).days / 365.25
                if years_diff > 1:
                    return Granularity.YEARLY
                return Granularity.MONTHLY
            return Granularity.MONTHLY
    except Exception as e:
        logger.warning(f"Error determining all-time granularity: {str(e)}. Defaulting to monthly.")
        return Granularity.MONTHLY

# Automatic granularity assignment based on time range:
# - Last 7 days: Daily aggregation
# - Last 30 days–3 months: Weekly aggregation  
# - Last 6-12 months: Monthly aggregation
# - This year: Monthly aggregation
# - All time: Dynamic (yearly if >1 year of data, monthly otherwise)

    




async def _get_aggregated_positives_data(
    app_id: str,
    start_date: datetime,
    end_date: datetime,
    aggregation_level: str,
    impact_level: Optional[str] = None,
    category: Optional[str] = None
):
    """
    Get aggregated positives data for a given app_id, date range and aggregation level.
    Scoped by app_id (processed_app_reviews has app_id).
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
    WITH POSITIVES AS (
        SELECT
            pr.review_id,
            pr.review_created_at,
            positive_mentions->>'description' AS description,
            positive_mentions->>'impact_score' AS impact_score
        FROM
            processed_app_reviews pr,
            jsonb_array_elements(pr.latest_analysis->'positive_feedback'->'positive_mentions') AS positive_mentions
        WHERE
            pr.app_id = %s AND DATE(pr.review_created_at) BETWEEN %s AND %s
            -- Dynamic filters will be added here
    ),
    CANONICAL_STATEMENTS AS (
        SELECT
            A.canonical_id,
            A.statement,
            B.category,
            B.subcategory,
            B.display_label,
            REPLACE(description, 'Auto-generated canonical ID for:', '') as desc
        FROM
            canonical_statements A
        LEFT OUTER JOIN
            statement_taxonomy B
        ON (A.canonical_id = B.canonical_id)
    )
    SELECT
        DATE_TRUNC('{trunc_level}', A.review_created_at) AS period,
        COUNT(*) AS total_positives,
        SUM(CASE WHEN A.impact_score::numeric > 70 THEN 1 ELSE 0 END) AS high_impact_count,
        SUM(CASE WHEN A.impact_score::numeric > 40 AND A.impact_score::numeric <= 70 THEN 1 ELSE 0 END) AS mid_impact_count,
        SUM(CASE WHEN A.impact_score::numeric <= 40 THEN 1 ELSE 0 END) AS low_impact_count
    FROM 
        POSITIVES A
    LEFT OUTER JOIN
        CANONICAL_STATEMENTS B
    ON (A.description = B.statement)
    GROUP BY
        period
    ORDER BY
        period;
    """

    where_parts = []
    params = [app_id, start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')]

    if category:
        category_list = [s.strip() for s in category.split(',')]
        placeholders = ', '.join(['%s'] * len(category_list))
        where_parts.append(f"category IN ({placeholders})")
        params.extend(category_list)
    if impact_level:
        where_parts.append("impact_level = %s")
        params.append(impact_level)

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
                logger.info(f"Positives data found: {len(data)} rows")
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
                            elif col == 'period':
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
                logger.warning("No aggregated positives data found - this might indicate a query issue")
                return {}
    except Exception as e:
        logger.error(f"Error getting positives data: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error getting positives data: {str(e)}"
        )

# Example usage within a FastAPI endpoint
# @app.get("/issues/daily")
# async def get_daily_issues(start_date: datetime, end_date: datetime, ...):
#     return await _get_issues_data(start_date, end_date, 'daily', ...)

# @app.get("/issues/monthly")
# async def get_monthly_issues(start_date: datetime, end_date: datetime, ...):
#     return await _get_issues_data(start_date, end_date, 'monthly', ...)

async def _get_positives_list(
    app_id: str,
    start_date: datetime,
    end_date: datetime,
    order_by: str = 'total_reviews',
    impact_level: Optional[str] = None,
    category: Optional[str] = None,
    limit: Optional[int] = None,
    offset: Optional[int] = None
):
    """
    Get a filtered and aggregated list of positives for this app. Scoped by app_id.
    """
    
    # 1. Input Validation for literal values
    valid_sort_columns = ['total_reviews', 'desc', 'impact_level', 'category', 'impact_area']
    
    if order_by and order_by not in valid_sort_columns:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid order_by column. Must be one of: {', '.join(valid_sort_columns)}"
        )
    
    app_id = str(app_id)
    # 2. SQL Query (scoped by app_id)
    base_query = """
    WITH POSITIVES AS (
        SELECT
            pr.review_id,
            pr.review_created_at,
            positive_mentions->>'description' AS description,
            positive_mentions->>'impact_score' AS impact_score,
            positive_mentions->>'quote' AS quote,
            positive_mentions->>'metrics' AS metrics,
            positive_mentions->>'keywords' AS keywords,
            positive_mentions->>'impact_area' AS impact_area,
            positive_mentions->>'user_segments' AS user_segments
        FROM
            processed_app_reviews pr,
            jsonb_array_elements(pr.latest_analysis->'positive_feedback'->'positive_mentions') AS positive_mentions
        WHERE
            pr.app_id = %s AND DATE(pr.review_created_at) BETWEEN %s AND %s
            -- Dynamic filters will be added here
    ),
    CANONICAL_STATEMENTSS AS (
        SELECT
            A.canonical_id,
            A.statement,
            B.category,
            B.subcategory,
            B.display_label,
            REPLACE(description, 'Auto-generated canonical ID for:', '') as desc
        FROM
            canonical_statements A
        LEFT OUTER JOIN
            statement_taxonomy B
        ON (A.canonical_id = B.canonical_id)
    )
    SELECT
        B.desc,
        -- Subquery to select the most common category
        (
            SELECT category
            FROM CANONICAL_STATEMENTSS b_sub
            WHERE b_sub.desc = B.desc
            GROUP BY category
            ORDER BY COUNT(*) DESC, RANDOM()
            LIMIT 1
        ) AS category,
        STRING_AGG(DISTINCT A.quote, ', ') AS quote,
        -- Get unique keywords after unnesting
        STRING_AGG(DISTINCT unnested_keywords, ', ') AS keywords,
        -- Subquery to select the most common impact area
        (
            SELECT impact_area
            FROM POSITIVES a_sub
            LEFT OUTER JOIN CANONICAL_STATEMENTSS b_sub
            ON (a_sub.description = b_sub.statement)
            WHERE b_sub.desc = B.desc
            GROUP BY impact_area
            ORDER BY COUNT(*) DESC, RANDOM()
            LIMIT 1
        ) AS impact_area,
        CASE
            WHEN AVG(A.impact_score::numeric) > 70 THEN 'High'
            WHEN AVG(A.impact_score::numeric) > 40 THEN 'Medium'
            ELSE 'Low'
        END AS impact_level,
        COUNT(*) AS total_reviews
    FROM
        POSITIVES A
    CROSS JOIN LATERAL
        jsonb_array_elements_text(A.keywords::jsonb) AS unnested_keywords
    LEFT OUTER JOIN
        CANONICAL_STATEMENTSS B
    ON (A.description = B.statement)
    WHERE
        B.desc IS NOT NULL
        -- Additional dynamic filters will be added here
    GROUP BY
        B.desc
    ORDER BY
        -- Dynamic order by will be added here
    """
    
    # 3. Build WHERE clause and parameter list
    where_parts = []
    params = [app_id, start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')]

    if impact_level:
        # Handle comma-separated impact_level values
        impact_level_list = [s.strip() for s in impact_level.split(',')]
        # We need to add this as a HAVING clause since impact_level is calculated
        having_clause = f"HAVING CASE WHEN AVG(A.impact_score::numeric) > 70 THEN 'High' WHEN AVG(A.impact_score::numeric) > 40 THEN 'Medium' ELSE 'Low' END IN ({', '.join(['%s'] * len(impact_level_list))})"
        params.extend(impact_level_list)
    else:
        having_clause = ""
        
    if category:
        where_parts.append("B.category = %s")
        params.append(category)

    # Replace placeholders in query
    final_query = base_query.replace(
        "-- Additional dynamic filters will be added here",
        " AND " + " AND ".join(where_parts) if where_parts else ""
    )
    
    # Add HAVING clause if needed
    if having_clause:
        final_query = final_query.replace(
            "ORDER BY",
            having_clause + "\n    ORDER BY"
        )
    
    # 4. Add ORDER BY clause
    order_direction = "DESC"  # Default to descending
    order_by_clause = f"{order_by} {order_direction}"
    final_query = final_query.replace("-- Dynamic order by will be added here", order_by_clause)
    
    # 5. Add pagination if specified
    if limit is not None:
        final_query += f" LIMIT {limit}"
        if offset is not None:
            final_query += f" OFFSET {offset}"
    
    # 6. Execute query and return data
    try:
        with pooled_connection() as conn:
            logger.info(f"Executing positives list query with params: {params}")
            data = pd.read_sql(final_query, conn, params=tuple(params))
            if not data.empty:
                logger.info(f"Positives list data: {len(data)} rows")
                
                # Convert the data to records
                records = data.to_dict('records')
                
                # Clean up data types and handle potential JSON parsing
                for record in records:
                    # Convert numpy types to Python types
                    for key, value in record.items():
                        if pd.isna(value):
                            record[key] = None
                        elif hasattr(value, 'item'):  # numpy types
                            record[key] = value.item()
                        elif isinstance(value, (int, float)):
                            record[key] = float(value) if isinstance(value, float) else int(value)
                    
                    # Process comma-separated strings for quotes and keywords
                    if 'quote' in record and record['quote']:
                        # Convert comma-separated string to array
                        quotes_str = str(record['quote'])
                        if quotes_str and quotes_str != 'None':
                            # Split by comma and clean up
                            quotes_list = [q.strip() for q in quotes_str.split(',') if q.strip()]
                            record['quote'] = quotes_list
                        else:
                            record['quote'] = []
                    
                    if 'keywords' in record and record['keywords']:
                        # Convert comma-separated string to array
                        keywords_str = str(record['keywords'])
                        if keywords_str and keywords_str != 'None':
                            # Split by comma and clean up
                            keywords_list = [k.strip() for k in keywords_str.split(',') if k.strip()]
                            record['keywords'] = keywords_list
                        else:
                            record['keywords'] = []
                
                return records
            else:
                logger.info("No positives list data found")
                return []
    except Exception as e:
        logger.error(f"Error getting positives list data: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error getting positives list data: {str(e)}"
        )


async def _get_minimum_date(app_id: str):
    """Get minimum date for a given app_id. Positives use processed_app_reviews (has app_id)."""
    query = """
    SELECT MIN(review_created_at) FROM processed_app_reviews WHERE app_id = %s
    """
    with pooled_connection() as conn:
        return pd.read_sql(query, conn, params=(str(app_id),))

async def _get_positives_list_count(
    app_id: str,
    start_date: datetime,
    end_date: datetime,
    impact_level: Optional[str] = None,
    category: Optional[str] = None
):
    """
    Get the count of distinct positives with optional filters. Scoped by app_id.
    """
    
    app_id = str(app_id)
    # SQL Query to get the count of distinct positives (scoped by app_id)
    base_query = """
    SELECT
        COUNT(*) AS count
    FROM (
        WITH POSITIVES AS (
            SELECT
                pr.review_id,
                pr.review_created_at,
                positive_mentions->>'description' AS description,
                positive_mentions->>'impact_score' AS impact_score,
                positive_mentions->>'quote' AS quote,
                positive_mentions->>'metrics' AS metrics,
                positive_mentions->>'keywords' AS keywords,
                positive_mentions->>'impact_area' AS impact_area,
                positive_mentions->>'user_segments' AS user_segments
            FROM
                processed_app_reviews pr,
                jsonb_array_elements(pr.latest_analysis->'positive_feedback'->'positive_mentions') AS positive_mentions
            WHERE
                pr.app_id = %s AND DATE(pr.review_created_at) BETWEEN %s AND %s
                -- Dynamic filters will be added here
        ),
        CANONICAL_STATEMENTSS AS (
            SELECT
                A.canonical_id,
                A.statement,
                B.category,
                B.subcategory,
                B.display_label,
                REPLACE(description, 'Auto-generated canonical ID for:', '') as desc
            FROM
                canonical_statements A
            LEFT OUTER JOIN
                statement_taxonomy B
            ON (A.canonical_id = B.canonical_id)
        )
        SELECT
            B.desc
        FROM
            POSITIVES A
        LEFT OUTER JOIN
            CANONICAL_STATEMENTSS B
        ON (A.description = B.statement)
        WHERE
            B.desc IS NOT NULL
            -- Additional dynamic filters will be added here
        GROUP BY
            B.desc
    ) AS final_positives;
    """
    
    # Build WHERE clause and parameter list
    where_parts = []
    params = [app_id, start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')]

    if category:
        where_parts.append("B.category = %s")
        params.append(category)
        
    final_query = base_query.replace(
        "-- Additional dynamic filters will be added here",
        " AND " + " AND ".join(where_parts) if where_parts else ""
    )
    
    # Handle impact_level filtering with HAVING clause
    if impact_level:
        impact_level_list = [s.strip() for s in impact_level.split(',')]
        having_clause = f"HAVING CASE WHEN AVG(A.impact_score::numeric) > 70 THEN 'High' WHEN AVG(A.impact_score::numeric) > 40 THEN 'Medium' ELSE 'Low' END IN ({', '.join(['%s'] * len(impact_level_list))})"
        params.extend(impact_level_list)
        final_query = final_query.replace(
            "GROUP BY",
            having_clause + "\n        GROUP BY"
        )
    
    # Execute query and return data
    try:
        with pooled_connection() as conn:
            data = pd.read_sql(final_query, conn, params=tuple(params))
            if not data.empty:
                count = int(data['count'].iloc[0])
                logger.info(f"Filtered positives count: {count}")
                return count
            else:
                logger.info("No positives found with filters, count is 0")
                return 0
    except Exception as e:
        logger.error(f"Error getting filtered positives count: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error getting filtered positives count: {str(e)}"
        )