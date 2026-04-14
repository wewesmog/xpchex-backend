from __future__ import annotations

import argparse
import asyncio
from datetime import datetime
from typing import Optional

from app.models.commentary_models import (
    JobResult,
    RunCommentaryResponse,
    SlotKey,
)
from app.shared_services.date_ranges import TimeRange
from app.shared_services.db import pooled_connection
from app.shared_services.logger_setup import setup_logger

from .commentary_graph import commentary_graph
from .commentary_nodes import DEFAULT_SLOT_KEYS, DEFAULT_TIME_RANGES

logger = setup_logger()


def _load_target_apps(app_id: Optional[str]) -> list[str]:
    if app_id:
        return [str(app_id)]
    with pooled_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT app_id
                FROM apps
                WHERE active = TRUE
                ORDER BY app_id
                """
            )
            rows = cur.fetchall() or []
    return [str(r[0]) for r in rows if r and r[0]]


async def run_commentary_generation(
    app_id: Optional[str] = None,
    time_ranges: Optional[list[TimeRange]] = None,
    slot_keys: Optional[list[SlotKey]] = None,
    *,
    force: bool = False,
    dry_run: bool = False,
    as_of: Optional[datetime] = None,
) -> RunCommentaryResponse:
    apps = _load_target_apps(app_id)
    all_results: list[dict] = []

    for aid in apps:
        logger.info("Running commentary graph for app_id=%s", aid)
        initial_state = {
            "app_id": aid,
            "force": force,
            "dry_run": dry_run,
            "as_of": as_of,
            "requested_time_ranges": time_ranges or DEFAULT_TIME_RANGES,
            "requested_slot_keys": slot_keys or DEFAULT_SLOT_KEYS,
            "jobs": [],
            "job_index": 0,
            "results": [],
        }
        # Looping over many jobs in a single graph run needs a higher recursion limit
        # than LangGraph's default (25). Keep extra fixed headroom for graph startup/
        # shutdown transitions to avoid hitting exact boundary values.
        job_count = len(initial_state["requested_time_ranges"]) * len(initial_state["requested_slot_keys"])
        recursion_limit = max(300, (job_count * 10) + 50)
        final_state = await commentary_graph.ainvoke(
            initial_state,
            config={"recursion_limit": recursion_limit},
        )
        all_results.extend(final_state.get("results", []))

    generated = sum(1 for r in all_results if r.get("action") == "generated")
    skipped = sum(1 for r in all_results if r.get("action") == "skipped")
    errors = sum(1 for r in all_results if r.get("action") == "error")

    return RunCommentaryResponse(
        status="success",
        results=[JobResult(**r) for r in all_results],
        summary={
            "apps": len(apps),
            "jobs_total": len(all_results),
            "generated": generated,
            "skipped": skipped,
            "errors": errors,
        },
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run analytics commentary generation graph.")
    parser.add_argument("--app-id", dest="app_id", type=str, required=True)
    parser.add_argument("--force", action="store_true", help="Generate even if same metrics.")
    parser.add_argument("--dry-run", action="store_true", help="Do not insert rows.")
    args = parser.parse_args()

    async def _run() -> None:
        result = await run_commentary_generation(
            app_id=args.app_id,
            force=args.force,
            dry_run=args.dry_run,
        )
        logger.info("Commentary run summary: %s", result.summary)

    asyncio.run(_run())
