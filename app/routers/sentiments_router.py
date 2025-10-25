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
    prefix="/sentiments",
    tags=["sentiments"]
)

@router.get("/sentiments_analytics", status_code=status.HTTP_200_OK)
async def get_sentiments_analytics(
    app_id: str = Query(..., description="App ID"),
    time_range: TimeRange = Query(default=TimeRange.THIS_YEAR),
    sentiment: Optional[str] = Query(default=None, description="Filter by sentiment: positive, negative, neutral"),
    rating: Optional[str] = Query(default=None, description="Filter by rating: 1, 2, 3, 4, 5")
):
    """
    Get sentiments analytics with automatic granularity assignment based on time range.
    
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
        logger.info(f"Date range for {time_range}: {start_date} to {end_date}")
        
        # Get aggregated data based on granularity
        if granularity == Granularity.DAILY:
            sentiments_data = await _get_aggregated_sentiments_data(start_date, end_date, granularity, sentiment, rating)
        elif granularity == Granularity.WEEKLY:
            sentiments_data = await _get_aggregated_sentiments_data(start_date, end_date, granularity, sentiment, rating)
        elif granularity == Granularity.MONTHLY:
            sentiments_data = await _get_aggregated_sentiments_data(start_date, end_date, granularity, sentiment, rating)
        elif granularity == Granularity.YEARLY:
            sentiments_data = await _get_aggregated_sentiments_data(start_date, end_date, granularity, sentiment, rating)
        else:
            sentiments_data = await _get_aggregated_sentiments_data(start_date, end_date, granularity, sentiment, rating)
            

        return {
            "status": "success",
            "time_range": time_range,
            "granularity": granularity,
            "date_range": {
                "start": start_date.isoformat(),
                "end": end_date.isoformat()
            },
            "data": sentiments_data
        }
        
    except Exception as e:
        logger.error(f"Error getting sentiments analytics: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, 
            detail=f"Error getting sentiments analytics: {str(e)}"
        )
@router.get("/list_segments", status_code=status.HTTP_200_OK)
async def list_segments(
    app_id: str = Query(..., description="App ID"),
    time_range: TimeRange = Query(default=TimeRange.THIS_YEAR),
):
    """List segments for a given date range"""
    try:
        start_date, end_date = _calculate_date_range(time_range)
        segments = await _get_segments_data(app_id, start_date, end_date)
        return {
            "status": "success",
            "time_range": time_range,
            "date_range": {
                "start": start_date.isoformat(),
                "end": end_date.isoformat()
            },
            "data": segments
        }
    except Exception as e:
        logger.error(f"Error getting segments: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error getting segments: {str(e)}"
        )

@router.get("/list_all_segments", status_code=status.HTTP_200_OK)
async def list_all_segments(
    app_id: str = Query(..., description="App ID"),
    time_range: TimeRange = Query(default=TimeRange.THIS_YEAR),
):
    """List ALL segments for word cloud analysis (no limit)"""
    try:
        start_date, end_date = _calculate_date_range(time_range)
        segments = await _get_all_segments_data(app_id, start_date, end_date)
        return {
            "status": "success",
            "time_range": time_range,
            "date_range": {
                "start": start_date.isoformat(),
                "end": end_date.isoformat()
            },
            "data": segments
        }
    except Exception as e:
        logger.error(f"Error getting all segments: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error getting all segments: {str(e)}"
        )

@router.get("/list_emotions", status_code=status.HTTP_200_OK)
async def list_emotions(
    app_id: str = Query(..., description="App ID"),
    time_range: TimeRange = Query(default=TimeRange.THIS_YEAR),
):
    """List emotions for a given date range"""
    try:
        start_date, end_date = _calculate_date_range(time_range)
        emotions = await _get_emotions_data(app_id, start_date, end_date)
        return {
            "status": "success",
            "time_range": time_range,
            "date_range": {
                "start": start_date.isoformat(),
                "end": end_date.isoformat()
            },
            "data": emotions
        }
    except Exception as e:
        logger.error(f"Error getting emotions: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error getting emotions: {str(e)}"
        )


@router.get("/list_reviews", status_code=status.HTTP_200_OK)
async def list_reviews(
    app_id: str = Query(..., description="App ID"),
    time_range: TimeRange = Query(default=TimeRange.THIS_YEAR),
    order_by: str = Query(default='thumbs_up_count'),
    sentiment: Optional[str] = Query(default=None, description="Filter by sentiment: positive, negative, neutral"),
    rating: Optional[str] = Query(default=None, description="Filter by rating: 1, 2, 3, 4, 5"),
    limit: int = Query(default=5, ge=1, le=100),
    offset: int = Query(default=0, ge=0)
):
    """List individual reviews with filtering by time range"""
    try:
        # Calculate date range based on time_range parameter
        start_date, end_date = _calculate_date_range(time_range)
        
        # Get reviews data filtered by date range
        reviews = await _get_reviews_list(
            app_id=app_id,
            start_date=start_date, 
            end_date=end_date, 
            order_by=order_by,
            sentiment=sentiment,
            rating=rating,
            limit=limit, 
            offset=offset
        )
        
        # Get total count for pagination
        total_count = await _get_reviews_list_count(
            app_id=app_id,
            start_date=start_date,
            end_date=end_date,
            sentiment=sentiment,
            rating=rating
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
            "data": reviews
        }
        
    except Exception as e:
        logger.error(f"Error listing reviews: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error listing reviews: {str(e)}"
        )

# Helper functions
def _get_granularity_for_range(time_range: TimeRange) -> Granularity:
    """Auto-determine granularity based on time range"""
    if time_range in [TimeRange.LAST_7_DAYS]:
        return Granularity.DAILY
    elif time_range in [TimeRange.LAST_30_DAYS, TimeRange.LAST_90_DAYS]:
        return Granularity.WEEKLY
    elif time_range in [TimeRange.LAST_6_MONTHS, TimeRange.LAST_12_MONTHS, TimeRange.THIS_YEAR]:
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
    elif time_range == TimeRange.LAST_30_DAYS:
        # Weekly granularity: up to last complete week (Sunday)
        days_since_sunday = (now.weekday() + 1) % 7
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
        # Monthly granularity: include current month
        end_date = now  # Include current month
        start_date = (now - relativedelta(months=6)).replace(day=1, hour=0, minute=0, second=0, microsecond=0)  # Start of 6 months ago
    elif time_range == TimeRange.LAST_12_MONTHS:
        # Monthly granularity: include current month
        end_date = now  # Include current month
        start_date = (now - relativedelta(months=12)).replace(day=1, hour=0, minute=0, second=0, microsecond=0)  # Start of 12 months ago
    elif time_range == TimeRange.THIS_YEAR:
        # Monthly granularity: from Jan 1 to end of last complete month
        start_date = datetime(now.year, 1, 1)
        first_day_current_month = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        end_date = first_day_current_month - timedelta(seconds=1)  # End of last month
    elif time_range == TimeRange.ALL_TIME:
        # Define ALL_TIME as from Jan 1 of the previous year up to end of last complete month
        start_date = datetime(now.year - 1, 1, 1)
        first_day_current_month = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        end_date = first_day_current_month - timedelta(seconds=1)
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
        SELECT MIN(review_created_at) FROM processed_app_reviews
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

# Segments
async def _get_segments_data(
    app_id: str,
    start_date: datetime,
    end_date: datetime
):
    """
    Get segments data for a given date range.
    """
    try:
        base_query = """    
SELECT
    p.review_id,
    p.review_created_at,
    p.username,
    p.user_image,
    (t ->> 'text') AS text,
    (t -> 'sentiment' ->> 'label') AS segment_sentiment_label,
    (t -> 'sentiment' ->> 'score')::numeric AS segment_sentiment_score,
    p.review_id
FROM
    processed_app_reviews AS p
CROSS JOIN LATERAL
    jsonb_array_elements(p.latest_analysis -> 'sentiment' -> 'segments') AS t
WHERE
    p.latest_analysis IS NOT NULL AND p.app_id = %s AND DATE(p.review_created_at) BETWEEN %s AND %s
ORDER BY RANDOM()
LIMIT 5
"""
        params = [app_id, start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')]
        with get_postgres_connection() as conn:
            data = pd.read_sql(base_query, conn, params=tuple(params))
            records = data.to_dict('records')
            return records
    except Exception as e:
        logger.error(f"Error getting segments data: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error getting segments data: {str(e)}"
        )

async def _get_all_segments_data(
    app_id: str,
    start_date: datetime,
    end_date: datetime
):
    """
    Get ALL segments data for word cloud analysis (no limit).
    """
    try:
        base_query = """    
SELECT
    p.review_id,
    p.review_created_at,
    p.username,
    p.user_image,
    (t ->> 'text') AS text,
    (t -> 'sentiment' ->> 'label') AS segment_sentiment_label,
    (t -> 'sentiment' ->> 'score')::numeric AS segment_sentiment_score,
    p.review_id
FROM
    processed_app_reviews AS p
CROSS JOIN LATERAL
    jsonb_array_elements(p.latest_analysis -> 'sentiment' -> 'segments') AS t
WHERE
    p.latest_analysis IS NOT NULL AND p.app_id = %s AND DATE(p.review_created_at) BETWEEN %s AND %s
"""
        params = [app_id, start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')]
        with get_postgres_connection() as conn:
            data = pd.read_sql(base_query, conn, params=tuple(params))
            records = data.to_dict('records')
            return records
    except Exception as e:
        logger.error(f"Error getting all segments data: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error getting all segments data: {str(e)}"
        )

async def _get_emotions_data(
    app_id: str,
    start_date: datetime,
    end_date: datetime
):
    """
    Get emotions data for a given date range.
    """
    try:
        base_query = """
SELECT
    p.review_id,
    p.review_created_at,
    -- Overall Classification for context
    p.latest_analysis -> 'sentiment' -> 'overall' ->> 'classification' AS overall_sentiment,
    -- Unnested Emotion Data
    t.key AS emotion,
    (t.value)::numeric AS emotion_score
FROM
    processed_app_reviews AS p
CROSS JOIN LATERAL
    jsonb_each_text(p.latest_analysis -> 'sentiment' -> 'emotions' -> 'emotion_scores') AS t(key, value)
WHERE
    p.latest_analysis IS NOT NULL 
    AND p.app_id = %s 
    AND DATE(p.review_created_at) BETWEEN %s AND %s
"""
        params = [app_id, start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')]
        with get_postgres_connection() as conn:
            data = pd.read_sql(base_query, conn, params=tuple(params))
            return data.to_dict('records')
    except Exception as e:
        logger.error(f"Error getting emotions data: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error getting emotions data: {str(e)}"
        )

async def _get_aggregated_sentiments_data(
    start_date: datetime,
    end_date: datetime,
    aggregation_level: str,
    sentiment: Optional[str] = None,
    rating: Optional[str] = None
):
    """
    Get aggregated sentiments data for a given date range and aggregation level.
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
    WITH review_data AS (
        SELECT
            latest_analysis->'sentiment'->'overall'->>'classification' as sentiment,
            score as rating,
            thumbs_up_count as thumbs_up_count,
            0 as thumbs_down_count ,
            review_created_at,
            latest_analysis,
            DATE_TRUNC('{trunc_level}', review_created_at) AS sentiment_period
        FROM
            processed_app_reviews
        WHERE
            DATE(review_created_at) BETWEEN %s AND %s
            -- Dynamic filters will be added here
    ),
    sentiment_counts AS (
        SELECT
            sentiment_period,
            sentiment,
            count(*) AS sentiment_count
        FROM
            review_data
        GROUP BY
            sentiment_period, sentiment
    ),
    rating_counts AS (
        SELECT
            sentiment_period,
            rating,
            count(*) AS rating_count
        FROM
            review_data
        GROUP BY
            sentiment_period, rating
    ),
    emotion_counts AS (
        SELECT
            sentiment_period,
            emotion_key,
            sum(emotion_count) AS emotion_count
        FROM (
            SELECT
                sentiment_period,
                jsonb_object_keys(latest_analysis->'emotions'->'emotion_scores') AS emotion_key,
                1 AS emotion_count
            FROM
                review_data
            WHERE
                latest_analysis IS NOT NULL 
                AND latest_analysis->'emotions'->'emotion_scores' IS NOT NULL
                AND jsonb_typeof(latest_analysis->'emotions'->'emotion_scores') = 'object'
        ) emotion_expanded
        GROUP BY
            sentiment_period, emotion_key
    ),
    nps_calculation AS (
        SELECT
            sentiment_period,
            count(*) AS total_reviews,
            sum(CASE WHEN rating >= 4 THEN 1 ELSE 0 END) AS promoters,
            sum(CASE WHEN rating <= 2 THEN 1 ELSE 0 END) AS detractors,
            -- NPS based on sentiment data
            sum(CASE WHEN sentiment = 'positive' THEN 1 ELSE 0 END) AS sentiment_promoters,
            sum(CASE WHEN sentiment = 'negative' THEN 1 ELSE 0 END) AS sentiment_detractors,
            sum(CASE WHEN sentiment = 'neutral' THEN 1 ELSE 0 END) AS sentiment_neutrals
        FROM
            review_data
        WHERE
            rating IS NOT NULL
        GROUP BY
            sentiment_period
    )
    SELECT
        sentiment_period,
        count(*) AS total_reviews,
        sum(thumbs_up_count) AS total_thumbs_up,
        sum(thumbs_down_count) AS total_thumbs_down,
        avg(rating) AS average_rating,
        -- NPS calculation (rating-based)
        (SELECT promoters FROM nps_calculation WHERE sentiment_period = rd.sentiment_period) AS promoters,
        (SELECT detractors FROM nps_calculation WHERE sentiment_period = rd.sentiment_period) AS detractors,
        (SELECT total_reviews FROM nps_calculation WHERE sentiment_period = rd.sentiment_period) AS nps_total,
        -- NPS calculation (sentiment-based)
        (SELECT sentiment_promoters FROM nps_calculation WHERE sentiment_period = rd.sentiment_period) AS sentiment_promoters,
        (SELECT sentiment_detractors FROM nps_calculation WHERE sentiment_period = rd.sentiment_period) AS sentiment_detractors,
        (SELECT sentiment_neutrals FROM nps_calculation WHERE sentiment_period = rd.sentiment_period) AS sentiment_neutrals,
        -- Sentiment breakdown
        (
            SELECT
                jsonb_agg(
                    jsonb_build_object(
                        'sentiment', sentiment,
                        'count', sentiment_count
                    )
                    ORDER BY sentiment_count DESC
                )
            FROM
                sentiment_counts
            WHERE
                sentiment_period = rd.sentiment_period
        ) AS sentiment_breakdown,
        -- Rating breakdown
        (
            SELECT
                jsonb_agg(
                    jsonb_build_object(
                        'rating', rating,
                        'count', rating_count
                    )
                    ORDER BY rating
                )
            FROM
                rating_counts
            WHERE
                sentiment_period = rd.sentiment_period
        ) AS rating_breakdown,
        -- Emotion breakdown
        (
            SELECT
                jsonb_agg(
                    jsonb_build_object(
                        'emotion', emotion_key,
                        'count', emotion_count
                    )
                    ORDER BY emotion_count DESC
                )
            FROM
                emotion_counts
            WHERE
                sentiment_period = rd.sentiment_period
        ) AS emotion_breakdown
    FROM
        review_data rd
    GROUP BY
        sentiment_period
    ORDER BY
        sentiment_period;
    """

    where_parts = []
    params = [start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')]

    if sentiment:
        # Handle comma-separated sentiment values
        sentiment_list = [s.strip() for s in sentiment.split(',')]
        placeholders = ', '.join(['%s'] * len(sentiment_list))
        where_parts.append(f"latest_analysis->'sentiment'->'overall'->>'classification' IN ({placeholders})")
        params.extend(sentiment_list)
    if rating:
        rating_list = [s.strip() for s in rating.split(',')]
        placeholders = ', '.join(['%s'] * len(rating_list))
        where_parts.append(f"score IN ({placeholders})")
        params.extend(rating_list)

    final_query = base_query.replace(
        "-- Dynamic filters will be added here",
        " AND " + " AND ".join(where_parts) if where_parts else ""
    )

    try:
        with get_postgres_connection() as conn:
            logger.info(f"Executing aggregation query with params: {params}")
            logger.info(f"Final query: {final_query}")
            
            # Debug: Check if there's any data in the date range
            debug_query = """
            SELECT COUNT(*) as total_count, 
                   MIN(review_created_at) as min_date, 
                   MAX(review_created_at) as max_date
            FROM processed_app_reviews 
            WHERE DATE(review_created_at) BETWEEN %s AND %s
            """
            debug_result = pd.read_sql(debug_query, conn, params=tuple(params[:2]))
            logger.info(f"Debug - Data in date range: {debug_result.to_dict('records')}")
            
            # Debug: Check what periods we're getting
            period_debug_query = f"""
            SELECT 
                DATE_TRUNC('{trunc_level}', review_created_at) AS sentiment_period,
                COUNT(*) as period_count
            FROM processed_app_reviews 
            WHERE DATE(review_created_at) BETWEEN %s AND %s
            GROUP BY DATE_TRUNC('{trunc_level}', review_created_at)
            ORDER BY sentiment_period
            """
            period_debug_result = pd.read_sql(period_debug_query, conn, params=tuple(params[:2]))
            logger.info(f"Debug - Periods found: {period_debug_result.to_dict('records')}")
            
            # Debug: Check sample data structure
            sample_query = """
            SELECT 
                review_id,
                review_created_at,
                latest_analysis->'sentiment'->'overall'->>'classification' as sentiment,
                score as rating,
                thumbs_up_count,
                latest_analysis->>'recommended_response' as recommended_response_direct,
                latest_analysis->'response_recommendation'->>'suggested_response' as recommended_response_1,
                latest_analysis->'recommended_response'->>'text' as recommended_response_2,
                latest_analysis->'recommended_response' as recommended_response_3,
                latest_analysis->'response_recommendation' as response_recommendation_full,
                latest_analysis as full_analysis
            FROM processed_app_reviews 
            WHERE DATE(review_created_at) BETWEEN %s AND %s
            LIMIT 3
            """
            sample_result = pd.read_sql(sample_query, conn, params=tuple(params[:2]))
            logger.info(f"Debug - Sample data: {sample_result.to_dict('records')}")
            
            data = pd.read_sql(final_query, conn, params=tuple(params))
            if not data.empty:
                logger.info(f"Sentiments data found: {len(data)} rows")
                logger.info(f"Data columns: {list(data.columns)}")
                
                # Convert DataFrame to JSON-safe format
                try:
                    # Convert DataFrame to records and handle datetime formatting
                    records = data.to_dict('records')
                    
                    # Format datetime columns
                    for record in records:
                        if 'sentiment_period' in record and record['sentiment_period'] is not None:
                            if hasattr(record['sentiment_period'], 'strftime'):
                                record['sentiment_period'] = record['sentiment_period'].strftime('%Y-%m-%d')
                            else:
                                record['sentiment_period'] = str(record['sentiment_period'])
                        
                        # Calculate NPS score (rating-based)
                        if record.get('nps_total', 0) > 0:
                            promoters = record.get('promoters', 0)
                            detractors = record.get('detractors', 0)
                            total = record.get('nps_total', 0)
                            record['nps_score'] = round(((promoters - detractors) / total) * 100, 1)
                        else:
                            record['nps_score'] = 0
                        
                        # Calculate NPS score (sentiment-based)
                        sentiment_promoters = record.get('sentiment_promoters', 0)
                        sentiment_detractors = record.get('sentiment_detractors', 0)
                        sentiment_neutrals = record.get('sentiment_neutrals', 0)
                        sentiment_total = sentiment_promoters + sentiment_detractors + sentiment_neutrals
                        
                        if sentiment_total > 0:
                            record['sentiment_nps_score'] = round(((sentiment_promoters - sentiment_detractors) / sentiment_total) * 100, 1)
                        else:
                            record['sentiment_nps_score'] = 0
                    
                    return records
                    
                except Exception as conversion_error:
                    logger.error(f"Error converting data to JSON format: {conversion_error}")
                    # Fallback: return empty result
                    return {}
            else:
                logger.warning("No aggregated sentiments data found - this might indicate a query issue")
                return {}
    except Exception as e:
        logger.error(f"Error getting sentiments data: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error getting sentiments data: {str(e)}"
        )

from datetime import datetime
from typing import Optional, List, Dict, Any
import pandas as pd
from app.shared_services.db import get_postgres_connection

async def _get_reviews_list(
    app_id: str,
    start_date: datetime,
    end_date: datetime,
    order_by: str = 'thumbs_up_count', # Corrected default to match valid columns
    sentiment: Optional[str] = None,
    rating: Optional[str] = None,
    limit: Optional[int] = None,
    offset: Optional[int] = None
) -> List[Dict[str, Any]]:
    """
    Get a filtered and paginated list of reviews, extracting detailed sentiment and
    response recommendation data from the latest_analysis JSONB column.
    """

    # 1. Input Validation for literal values
    valid_sort_columns = ['thumbs_up_count', 'score', 'review_created_at']

    if order_by and order_by not in valid_sort_columns:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid order_by column. Must be one of: {', '.join(valid_sort_columns)}"
        )

    # 2. SQL Query with a placeholder for the ORDER BY column
    base_query = """
    SELECT
        app_id,
        review_id,
        username as Reviewer,
        user_image,
        content as review_text,
        score as rating,
        review_created_at,
        thumbs_up_count,
        reply_content as Reply,
        reply_created_at as Reply_Date,

        -- Recommended Response
        latest_analysis -> 'response_recommendation' ->> 'suggested_response' AS recommended_response_text,

        -- Overall Sentiment
        latest_analysis -> 'sentiment' -> 'overall' ->> 'classification' AS sentiment,
        (latest_analysis -> 'sentiment' -> 'overall' ->> 'score')::numeric AS overall_score,
        (latest_analysis -> 'sentiment' -> 'overall' ->> 'confidence')::numeric AS overall_confidence,

        -- Sentiment Distribution
        (latest_analysis -> 'sentiment' -> 'overall' -> 'distribution' ->> 'positive')::numeric AS dist_positive,
        (latest_analysis -> 'sentiment' -> 'overall' -> 'distribution' ->> 'neutral')::numeric AS dist_neutral,
        (latest_analysis -> 'sentiment' -> 'overall' -> 'distribution' ->> 'negative')::numeric AS dist_negative,

        -- Primary and Secondary Emotions
        latest_analysis -> 'sentiment' -> 'emotions' -> 'primary' ->> 'emotion' AS primary_emotion,
        (latest_analysis -> 'sentiment' -> 'emotions' -> 'primary' ->> 'confidence')::numeric AS primary_confidence,
        latest_analysis -> 'sentiment' -> 'emotions' -> 'secondary' ->> 'emotion' AS secondary_emotion,
        (latest_analysis -> 'sentiment' -> 'emotions' -> 'secondary' ->> 'confidence')::numeric AS secondary_confidence,

        -- Full Emotion Scores Object (for maximum flexibility in analysis)
        latest_analysis -> 'sentiment' -> 'emotions' -> 'emotion_scores' AS all_emotion_scores
    FROM
        processed_app_reviews

    WHERE
        latest_analysis IS NOT NULL -- Corrected: Removed redundant 'where' keyword
        AND app_id = %s
        AND DATE(review_created_at) BETWEEN %s AND %s
        -- Dynamic filters will be added here
    ORDER BY
        {order_by_col} DESC, -- Placeholder for the validated column
        review_created_at DESC
    """

    # 3. Build WHERE clause and parameter list
    where_parts = []
    # Parameters for app_id, start_date, and end_date
    params = [app_id, start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')]

    if sentiment:
        # Handle comma-separated sentiment values
        sentiment_list = [s.strip() for s in sentiment.split(',')]
        # Using %s placeholders for PostgreSQL parameter substitution
        placeholders = ', '.join(['%s'] * len(sentiment_list))
        where_parts.append(f"latest_analysis->'sentiment'->'overall'->>'classification' IN ({placeholders})")
        params.extend(sentiment_list)

    if rating:
        # Handle comma-separated rating values
        rating_list = [s.strip() for s in rating.split(',')]
        placeholders = ', '.join(['%s'] * len(rating_list))
        where_parts.append(f"score IN ({placeholders})")
        params.extend(rating_list)

    # 4. Final Query Construction
    where_clause = " AND " + " AND ".join(where_parts) if where_parts else ""
    order_by_col = order_by # Use the validated parameter

    # Insert the dynamic WHERE clause and the ORDER BY column name
    final_query = (
        base_query.format(order_by_col=order_by_col)
        .replace("-- Dynamic filters will be added here", where_clause)
    )

    # 5. Add pagination if specified
    if limit is not None:
        final_query += f" LIMIT {limit}"
        if offset is not None:
            final_query += f" OFFSET {offset}"

    # 6. Execute query and return data
    try:
        # NOTE: get_postgres_connection() must be implemented to work.
        # This assumes a context manager that returns a connection object suitable for pandas.read_sql
        with get_postgres_connection() as conn:
            logger.info(f"Executing reviews list query with params: {params}")
            logger.info(f"Final SQL query: {final_query}")
            
            # Debug: Show the query with actual parameter values
            debug_query = final_query
            for i, param in enumerate(params):
                debug_query = debug_query.replace('%s', f"'{param}'", 1)
            logger.info(f"Debug SQL with real params: {debug_query}")
            
            # pd.read_sql safely handles parameter substitution using the params tuple
            data = pd.read_sql(final_query, conn, params=tuple(params))
            
            if not data.empty:
                logger.info(f"List data: {len(data)} rows")
                # Convert the data to records (list of dictionaries)
                records = data.to_dict('records')
                return records
            else:
                logger.info("No reviews data found")
                return []
    except Exception as e:
        logger.error(f"Error getting reviews data: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error getting reviews data: {str(e)}"
        )

async def _get_reviews_list_count(
    app_id: str,
    start_date: datetime,
    end_date: datetime,
    sentiment: Optional[str] = None,
    rating: Optional[str] = None,
):
    """
    Get the count of reviews with optional filters.
    """
    
    # SQL Query to get the count of reviews
    base_query = """
    SELECT COUNT(*) AS count FROM processed_app_reviews
    WHERE DATE(review_created_at) BETWEEN %s AND %s AND app_id = %s
    -- Dynamic filters will be added here
    """
    
    # Build WHERE clause and parameter list
    where_parts = []
    params = [start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'), app_id]

    if sentiment:
        # Handle comma-separated sentiment values
        sentiment_list = [s.strip() for s in sentiment.split(',')]
        placeholders = ', '.join(['%s'] * len(sentiment_list))
        where_parts.append(f"latest_analysis->'sentiment'->'overall'->>'classification' IN ({placeholders})")
        params.extend(sentiment_list)
    if rating:
        rating_list = [s.strip() for s in rating.split(',')]
        placeholders = ', '.join(['%s'] * len(rating_list))
        where_parts.append(f"score IN ({placeholders})")
        params.extend(rating_list)
        
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
                logger.info(f"Filtered reviews count: {count}")
                return count
            else:
                logger.info("No reviews found with filters, count is 0")
                return 0
    except Exception as e:
        logger.error(f"Error getting filtered reviews count: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error getting filtered reviews count: {str(e)}"
        )