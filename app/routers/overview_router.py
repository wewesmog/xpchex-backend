"""
Overview analytics: SLA snapshot from processed_app_reviews.
"""
from __future__ import annotations

import logging
from typing import Any, Optional

from fastapi import APIRouter, HTTPException, Query, status
from psycopg2.extras import RealDictCursor

from app.shared_services.db import pooled_connection
from app.shared_services.date_ranges import TimeRange, get_date_range

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/overview", tags=["overview"])

# Reply delay uses wall-clock time from review_created_at to reply_created_at.
# Disjoint buckets for answered rows (matches UI: Critical = over3d + over5d + over7d).
_SLA_SQL = """
SELECT
    COUNT(*) AS total_reviews,
    COUNT(*) FILTER (WHERE reply_created_at IS NOT NULL) AS answered,
    COUNT(*) FILTER (WHERE reply_created_at IS NULL) AS unanswered,
    COUNT(*) FILTER (
        WHERE reply_created_at IS NOT NULL
        AND (reply_created_at - review_created_at) <= INTERVAL '24 hours'
    ) AS within1d,
    COUNT(*) FILTER (
        WHERE reply_created_at IS NOT NULL
        AND (reply_created_at - review_created_at) > INTERVAL '24 hours'
        AND (reply_created_at - review_created_at) <= INTERVAL '3 days'
    ) AS over1d,
    COUNT(*) FILTER (
        WHERE reply_created_at IS NOT NULL
        AND (reply_created_at - review_created_at) > INTERVAL '3 days'
        AND (reply_created_at - review_created_at) <= INTERVAL '5 days'
    ) AS over3d,
    COUNT(*) FILTER (
        WHERE reply_created_at IS NOT NULL
        AND (reply_created_at - review_created_at) > INTERVAL '5 days'
        AND (reply_created_at - review_created_at) <= INTERVAL '7 days'
    ) AS over5d,
    COUNT(*) FILTER (
        WHERE reply_created_at IS NOT NULL
        AND (reply_created_at - review_created_at) > INTERVAL '7 days'
    ) AS over7d
FROM processed_app_reviews
WHERE app_id = %s
  AND DATE(review_created_at) BETWEEN %s AND %s
"""


def _rate(count: int, total: int) -> dict[str, Any]:
    pct: Optional[float]
    if total <= 0:
        pct = None
    else:
        pct = round(100.0 * count / total, 1)
    return {"count": count, "pct": pct}


def _build_snapshot(row: dict[str, Any]) -> dict[str, Any]:
    total = int(row["total_reviews"] or 0)
    answered = int(row["answered"] or 0)
    unanswered = int(row["unanswered"] or 0)
    w1 = int(row["within1d"] or 0)
    o1 = int(row["over1d"] or 0)
    o3 = int(row["over3d"] or 0)
    o5 = int(row["over5d"] or 0)
    o7 = int(row["over7d"] or 0)
    return {
        "totalReviews": total,
        "answered": _rate(answered, total),
        "unanswered": _rate(unanswered, total),
        "sla": {
            "within1d": _rate(w1, total),
            "over1d": _rate(o1, total),
            "over3d": _rate(o3, total),
            "over5d": _rate(o5, total),
            "over7d": _rate(o7, total),
        },
    }


@router.get("/sla_snapshot", status_code=status.HTTP_200_OK)
async def get_sla_snapshot(
    app_id: str = Query(..., description="App ID"),
    time_range: TimeRange = Query(default=TimeRange.LAST_6_MONTHS),
):
    """
    SLA snapshot for reviews in the window (review_created_at). Uses reply_created_at for answered vs backlog and for delay buckets.
    """
    try:
        start_date, end_date = get_date_range(time_range)
        start_d = start_date.date()
        end_d = end_date.date()

        with pooled_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(_SLA_SQL, (app_id, start_d, end_d))
                row = cur.fetchone()
                if not row:
                    snap = _build_snapshot({"total_reviews": 0})
                else:
                    snap = _build_snapshot(dict(row))

        return {
            "status": "success",
            "time_range": time_range.value,
            "date_range": {
                "start": start_date.isoformat(),
                "end": end_date.isoformat(),
            },
            "data": snap,
        }
    except Exception as e:
        logger.error("Error building SLA snapshot: %s", e, exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error building SLA snapshot: {e}",
        ) from e
