from fastapi import APIRouter, HTTPException, status
from app.google_reviews.reviews_scraper import ReviewScraper
from app.google_reviews.analyze_revs_db import analyze_reviews
from app.reviews_helpers.canon_main import (
    get_statements_by_review_ids,
    process_statements_for_date_async,
)
from app.shared_services.db import pooled_connection
import logging
import asyncio
from typing import Optional, Tuple, List
from datetime import datetime, timezone, date


logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/general",
    tags=["general"]
)

def get_latest_processed_timestamp(app_id: str) -> Optional[datetime]:
    """Latest processed review timestamp for scoping new work."""
    query = """
        SELECT MAX(review_created_at)
        FROM processed_app_reviews
        WHERE app_id = %s
    """
    with pooled_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query, (app_id,))
            result = cursor.fetchone()
            return result[0] if result and result[0] else None


def parse_date_param(value: Optional[str], as_date: bool = False) -> Optional[datetime]:
    """Parse ISO-like string to datetime or date (kept as datetime for flexibility)."""
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value)
        return dt.date() if as_date else dt
    except Exception:
        return None


def count_reviews(
    app_id: str,
    analyzed: bool,
    min_dt: Optional[datetime] = None,
    max_dt: Optional[datetime] = None,
) -> int:
    """Count reviews for an app with optional datetime bounds and analyzed flag."""
    query = """
        SELECT COUNT(*) FROM processed_app_reviews
        WHERE app_id = %s
          AND analyzed = %s
          AND (%s IS NULL OR review_created_at >= %s)
          AND (%s IS NULL OR review_created_at <= %s)
    """
    with pooled_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query, (app_id, analyzed, min_dt, min_dt, max_dt, max_dt))
            result = cursor.fetchone()
            return result[0] if result else 0


def get_new_review_date_range(app_id: str, cutoff: Optional[datetime]) -> Tuple[Optional[date], Optional[date]]:
    """
    Return min/max review_created_at (as date) for records newer than cutoff.
    cutoff can be None to consider all.
    """
    query = """
        SELECT
            MIN(date(review_created_at)) AS min_dt,
            MAX(date(review_created_at)) AS max_dt
        FROM processed_app_reviews
        WHERE app_id = %s
          AND (%s IS NULL OR review_created_at > %s)
    """
    with pooled_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query, (app_id, cutoff, cutoff))
            result = cursor.fetchone()
            return (result[0], result[1]) if result else (None, None)


def get_review_ids_for_app_and_dates(app_id: str, start_date: date, end_date: date) -> List[str]:
    """Return review_ids for an app within a date range (inclusive)."""
    query = """
        SELECT review_id
        FROM processed_app_reviews
        WHERE app_id = %s
          AND date(review_created_at) BETWEEN %s AND %s
        ORDER BY review_created_at
    """
    with pooled_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query, (app_id, start_date, end_date))
            rows = cursor.fetchall()
            return [row[0] for row in rows]

@router.get("/get_last_updated", status_code=status.HTTP_200_OK)
async def get_last_updated(app_id: str):
    """Get the last updated data for a given app_id"""
    try:
        last_updated = get_last_updated_helper(app_id)
        return last_updated
    except Exception as e:
        logger.error(f"Error getting last updated data for app_id: {app_id} - {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/update_app_reviews", status_code=status.HTTP_200_OK)
async def update_app_reviews(
    app_id: str,
    analyze_min_date: Optional[str] = None,
    analyze_max_date: Optional[str] = None,
    analyze_max_reviews: Optional[int] = None,
    analyze_min_reviews: Optional[int] = None,
    canon_min_date: Optional[str] = None,
    canon_max_date: Optional[str] = None,
    canon_max_reviews: Optional[int] = None,
    canon_min_reviews: Optional[int] = None,
):
    """Update the app reviews for a given app_id"""
    try:
        summary = await update_app_reviews_helper(
            app_id=app_id,
            analyze_min_date=analyze_min_date,
            analyze_max_date=analyze_max_date,
            analyze_max_reviews=analyze_max_reviews,
            analyze_min_reviews=analyze_min_reviews,
            canon_min_date=canon_min_date,
            canon_max_date=canon_max_date,
            canon_max_reviews=canon_max_reviews,
            canon_min_reviews=canon_min_reviews,
        )
        return {"message": "App reviews updated successfully", "summary": summary}
    except Exception as e:
        logger.error(f"Error updating app reviews for app_id: {app_id} - {e}")



def get_last_updated_helper(app_id: str):
    """Get the max date of canonicalized reviews for a given app_id"""

    query = f"""
        SELECT MAX(review_created_at) as max_review_created_at
        FROM processed_app_reviews 
        WHERE app_id = %s
        and analyzed = true
    
    """
    with pooled_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query, (app_id,))
            result = cursor.fetchone()
            ts = result[0] if result and result[0] else None
            return { "last_updated": ts.strftime("%Y-%m-%d %H:%M:%S") if ts else None }


async def update_app_reviews_helper(
    app_id: str,
    analyze_min_date: Optional[str] = None,
    analyze_max_date: Optional[str] = None,
    analyze_max_reviews: Optional[int] = None,
    analyze_min_reviews: Optional[int] = None,
    canon_min_date: Optional[str] = None,
    canon_max_date: Optional[str] = None,
    canon_max_reviews: Optional[int] = None,
    canon_min_reviews: Optional[int] = None,
):
    """Get reviews that dont exist in the database and add them, for a given app_id
       once retrieved, canonicalize them"""
    summary = {
        "app_id": app_id,
        "fetched": 0,
        "processed": 0,
        "analyzed": 0,
        "canonicalized": 0,
        "analysis_scope": {},
        "canonicalization_scope": {},
    }
    try:
        # Track the latest processed review timestamp before fetching to scope downstream work
        latest_before_fetch = get_latest_processed_timestamp(app_id)

        # 1) Fetch from Google Play and store in raw + processed tables
        with ReviewScraper() as scraper:
            fetched, processed = scraper.fetch_reviews(
                app_id=app_id,
                incremental=True,
                batch_size=100,
                lang="en",
                country="ke",
            )
            summary["fetched"] = fetched
            summary["processed"] = processed

        # 2) Analyze newly inserted (or any remaining unanalyzed) reviews for this app
        analyze_min_dt = parse_date_param(analyze_min_date)
        analyze_max_dt = parse_date_param(analyze_max_date)
        summary["analysis_scope"] = {
            "min_date": analyze_min_dt.isoformat() if analyze_min_dt else None,
            "max_date": analyze_max_dt.isoformat() if analyze_max_dt else None,
            "max_reviews": analyze_max_reviews,
            "min_reviews": analyze_min_reviews,
        }

        if analyze_min_reviews:
            candidates = count_reviews(app_id, analyzed=False, min_dt=analyze_min_dt, max_dt=analyze_max_dt)
            if candidates < analyze_min_reviews:
                logger.info(
                    f"Skipping analysis: only {candidates} unanalyzed reviews (min required {analyze_min_reviews})"
                )
            else:
                analyzed_count = await analyze_reviews(
                    app_id=app_id,
                    analyzed=False,  # only unanalyzed
                    reanalyze=False,
                    concurrent=True,  # enable concurrent processing
                    max_concurrent=30,  # increased concurrency for faster processing
                    batch_size=200,     # larger batches for better throughput
                    delay_between_reviews=0.0,  # removed delay - let semaphore handle rate limiting
                    min_date=analyze_min_dt,
                    max_date=analyze_max_dt,
                    max_reviews=analyze_max_reviews,
                )
                summary["analyzed"] = analyzed_count
        else:
            analyzed_count = await analyze_reviews(
                app_id=app_id,
                analyzed=False,  # only unanalyzed
                reanalyze=False,
                concurrent=True,  # enable concurrent processing
                max_concurrent=30,  # increased concurrency for faster processing
                batch_size=200,     # larger batches for better throughput
                delay_between_reviews=0.0,  # removed delay - let semaphore handle rate limiting
                min_date=analyze_min_dt,
                max_date=analyze_max_dt,
                max_reviews=analyze_max_reviews,
            )
            summary["analyzed"] = analyzed_count

        # 3) Canonicalize statements for the newly fetched/analyzed window (async)
        min_dt, max_dt = get_new_review_date_range(app_id, latest_before_fetch)
        canon_min_dt_override = parse_date_param(canon_min_date, as_date=True)
        canon_max_dt_override = parse_date_param(canon_max_date, as_date=True)

        # intersect/override ranges
        if canon_min_dt_override:
            min_dt = max(min_dt, canon_min_dt_override) if min_dt else canon_min_dt_override
        if canon_max_dt_override:
            max_dt = min(max_dt, canon_max_dt_override) if max_dt else canon_max_dt_override

        summary["canonicalization_scope"] = {
            "min_date": min_dt.isoformat() if min_dt else None,
            "max_date": max_dt.isoformat() if max_dt else None,
            "max_reviews": canon_max_reviews,
            "min_reviews": canon_min_reviews,
        }

        if min_dt and max_dt:
            review_ids = get_review_ids_for_app_and_dates(app_id, min_dt, max_dt)
            if review_ids:
                if canon_min_reviews and len(review_ids) < canon_min_reviews:
                    logger.info(
                        f"Skipping canonicalization: only {len(review_ids)} reviews in range (min required {canon_min_reviews})"
                    )
                    review_ids = []
                if canon_max_reviews is not None:
                    review_ids = review_ids[:canon_max_reviews]
                statements = get_statements_by_review_ids(review_ids)
                if statements:
                    canonicalized = await process_statements_for_date_async(
                        statements,
                        statements_per_batch=100,  # batch statements for better throughput
                        max_workers=30,  # increased workers for concurrent processing
                        stop_on_error=False,
                    )
                    summary["canonicalized"] = canonicalized
        logger.info(f"Update summary for app_id={app_id}: {summary}")
        # TODO: send email notification e.g. "Found {fetched} new reviews for {app_id}"
        return summary
    except Exception as e:
        logger.error(f"Error in update_app_reviews_helper for app_id={app_id}: {e}", exc_info=True)
        raise

