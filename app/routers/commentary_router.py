from __future__ import annotations

from datetime import date
from typing import Optional

from fastapi import APIRouter, HTTPException, Query, status
from psycopg2.extras import RealDictCursor

from app.commentary.commentary_main import run_commentary_generation
from app.models.commentary_models import (
    LatestCommentaryResponse,
    RunCommentaryRequest,
    RunCommentaryResponse,
)
from app.shared_services.date_ranges import TimeRange, get_date_range
from app.shared_services.db import pooled_connection

from app.commentary.commentary_nodes import DEFAULT_SLOT_KEYS, DEFAULT_TIME_RANGES

router = APIRouter(prefix="/commentary", tags=["commentary"])

ALLOWED_SLOT_KEYS = set(DEFAULT_SLOT_KEYS)
PAGE_SLOTS: dict[str, list[str]] = {
    "overview": [
        "overview_kpi_positive_rate_footer",
        "overview_kpi_critical_issues_footer",
        "overview_kpi_delight_mentions_footer",
        "overview_kpi_recommendations_footer",
        "overview_exec_summary",
    ],
    "sentiment": ["sentiment_hero_narrative"],
    "issues": ["issues_hero_narrative"],
    "delights": ["delights_hero_narrative"],
    "recommendations": ["recommendations_hero_narrative"],
}


def _fetch_latest_snapshot(
    app_id: str,
    slot_key: str,
    time_range_preset: TimeRange,
    window_start: date,
    window_end: date,
) -> Optional[dict]:
    with pooled_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT
                  id, app_id, slot_key, time_range_preset, window_start, window_end,
                  commentary_text, max_chars, source_metrics_json, model_id, prompt_version, generated_at
                FROM analytics_commentary_snapshots
                WHERE app_id = %s
                  AND slot_key = %s
                  AND time_range_preset = %s
                  AND window_start = %s
                  AND window_end = %s
                ORDER BY generated_at DESC
                LIMIT 1
                """,
                (app_id, slot_key, time_range_preset.value, window_start, window_end),
            )
            row = cur.fetchone()
            if row:
                return dict(row)

            # Fallback: if there is no exact-window snapshot for this preset,
            # return the latest snapshot for the same app/slot/preset so
            # commentary still renders while windows roll forward.
            cur.execute(
                """
                SELECT
                  id, app_id, slot_key, time_range_preset, window_start, window_end,
                  commentary_text, max_chars, source_metrics_json, model_id, prompt_version, generated_at
                FROM analytics_commentary_snapshots
                WHERE app_id = %s
                  AND slot_key = %s
                  AND time_range_preset = %s
                ORDER BY generated_at DESC
                LIMIT 1
                """,
                (app_id, slot_key, time_range_preset.value),
            )
            fallback_row = cur.fetchone()
            return dict(fallback_row) if fallback_row else None


@router.get("/slots", status_code=status.HTTP_200_OK)
async def list_commentary_slots() -> dict:
    return {
        "status": "success",
        "slots": list(DEFAULT_SLOT_KEYS),
        "time_ranges": [tr.value for tr in DEFAULT_TIME_RANGES],
    }


@router.get("/latest", response_model=LatestCommentaryResponse, status_code=status.HTTP_200_OK)
async def get_latest_commentary(
    app_id: str = Query(..., description="App ID"),
    slot_key: str = Query(..., description="Commentary slot key"),
    time_range_preset: TimeRange = Query(...),
    window_start: date = Query(...),
    window_end: date = Query(...),
):
    if slot_key not in ALLOWED_SLOT_KEYS:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Invalid slot_key. Allowed values: {sorted(ALLOWED_SLOT_KEYS)}",
        )
    item = _fetch_latest_snapshot(
        app_id=app_id,
        slot_key=slot_key,
        time_range_preset=time_range_preset,
        window_start=window_start,
        window_end=window_end,
    )
    if not item:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No commentary snapshot found for the provided app/slot/preset/window.",
        )
    return {"status": "success", "item": item}


@router.get("/latest_for_preset", response_model=LatestCommentaryResponse, status_code=status.HTTP_200_OK)
async def get_latest_commentary_for_preset(
    app_id: str = Query(..., description="App ID"),
    slot_key: str = Query(..., description="Commentary slot key"),
    time_range_preset: TimeRange = Query(...),
):
    if slot_key not in ALLOWED_SLOT_KEYS:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Invalid slot_key. Allowed values: {sorted(ALLOWED_SLOT_KEYS)}",
        )
    start_dt, end_dt = get_date_range(time_range_preset)
    item = _fetch_latest_snapshot(
        app_id=app_id,
        slot_key=slot_key,
        time_range_preset=time_range_preset,
        window_start=start_dt.date(),
        window_end=end_dt.date(),
    )
    if not item:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No commentary snapshot found for the current resolved preset window.",
        )
    return {"status": "success", "item": item}


@router.get("/page", status_code=status.HTTP_200_OK)
async def get_page_commentary(
    app_id: str = Query(..., description="App ID"),
    page_id: str = Query(..., description="Page id: overview|sentiment|issues|delights|recommendations"),
    time_range_preset: TimeRange = Query(...),
):
    slots = PAGE_SLOTS.get(page_id)
    if not slots:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Invalid page_id. Allowed values: {sorted(PAGE_SLOTS.keys())}",
        )
    start_dt, end_dt = get_date_range(time_range_preset)
    window_start = start_dt.date()
    window_end = end_dt.date()

    items: dict[str, Optional[dict]] = {}
    for slot_key in slots:
        item = _fetch_latest_snapshot(
            app_id=app_id,
            slot_key=slot_key,
            time_range_preset=time_range_preset,
            window_start=window_start,
            window_end=window_end,
        )
        items[slot_key] = item

    return {
        "status": "success",
        "page_id": page_id,
        "time_range_preset": time_range_preset.value,
        "window_start": window_start,
        "window_end": window_end,
        "items": items,  # null means fallback to frontend default
    }


@router.post("/run", response_model=RunCommentaryResponse, status_code=status.HTTP_200_OK)
async def run_commentary(req: RunCommentaryRequest) -> RunCommentaryResponse:
    if req.slot_keys:
        unknown = [s for s in req.slot_keys if s not in ALLOWED_SLOT_KEYS]
        if unknown:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=f"Unknown slot_keys: {unknown}",
            )
    return await run_commentary_generation(
        app_id=req.app_id,
        time_ranges=req.time_ranges,
        slot_keys=req.slot_keys,
        force=req.force,
        dry_run=req.dry_run,
        as_of=req.as_of,
    )
