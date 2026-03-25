from __future__ import annotations
"""
Batch daily pipeline (no scrape, no Nelly — run those via scrape_nelly_pipeline.py).

Run order:
1) Analyze new reviews
2) Re-analyze reviews where stored fingerprint exists but no longer matches text/reply
3) Commentary
4) Upsert past responses → response_history
"""

import argparse
import asyncio
import os
from datetime import datetime

from app.commentary.commentary_main import run_commentary_generation
from app.google_reviews.analyze_revs_db import analyze_reviews
from app.nelly.upsert_past_responses import run_upsert_past_responses
from app.shared_services.logger_setup import setup_logger

logger = setup_logger()


async def run_pipeline(args: argparse.Namespace) -> None:
    app_id = (
        args.app_id
        or os.getenv("MVP_APP_ID")
    )
    if not app_id:
        raise SystemExit("Missing --app-id (or env MVP_APP_ID)")

    logger.info("Starting daily batch pipeline for app_id=%s", app_id)
    logger.info(
        "Order: analyze (new) -> analyze (stale) -> commentary -> upsert responses"
    )

    started = datetime.utcnow()

    if not args.skip_analysis:
        analyzed_new = await analyze_reviews(
            app_id=app_id,
            min_date=None,
            max_reviews=args.analysis_max_reviews,
            batch_size=args.analysis_batch_size,
            concurrent=True,
            max_concurrent=args.analysis_max_concurrent,
            analyzed=False,
            reanalyze=args.analysis_reanalyze_all,
            stale_analysis=False,
        )
        logger.info("Analysis (new / unanalyzed) complete processed=%s", analyzed_new)

        if not args.skip_stale_analysis_repair:
            stale_n = await analyze_reviews(
                app_id=app_id,
                min_date=None,
                max_reviews=args.stale_analysis_max_reviews,
                batch_size=args.analysis_batch_size,
                concurrent=True,
                max_concurrent=args.analysis_max_concurrent,
                analyzed=False,
                reanalyze=True,
                stale_analysis=True,
            )
            logger.info("Analysis (stale fingerprint vs text) complete processed=%s", stale_n)
        else:
            logger.info("Skipping stale-analysis repair (--skip-stale-analysis-repair)")
    else:
        logger.info("Skipping analysis steps")

    if not args.skip_commentary:
        commentary = await run_commentary_generation(app_id=app_id)
        logger.info(
            "Commentary complete generated=%s skipped=%s failed=%s jobs=%s",
            commentary.summary.get("generated"),
            commentary.summary.get("skipped"),
            commentary.summary.get("errors"),
            commentary.summary.get("jobs_total"),
        )
    else:
        logger.info("Skipping commentary step")

    if not args.skip_upsert:
        synced = run_upsert_past_responses(
            app_id=app_id,
            min_date=None,
            use_max_from_history=True,
            batch_size=args.upsert_batch_size,
            limit=None,
        )
        logger.info("Upsert past responses complete synced=%s", synced)
    else:
        logger.info("Skipping upsert step")

    elapsed = datetime.utcnow() - started
    logger.info("Daily batch pipeline completed in %s", elapsed)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Daily batch: analyze, commentary, upsert (no scrape/Nelly)")
    parser.add_argument("--app-id", default=None)
    parser.add_argument("--upsert-batch-size", type=int, default=100)
    parser.add_argument("--analysis-batch-size", type=int, default=10)
    parser.add_argument("--analysis-max-concurrent", type=int, default=5)
    parser.add_argument("--analysis-max-reviews", type=int, default=0)
    parser.add_argument("--analysis-reanalyze-all", action="store_true")
    parser.add_argument(
        "--skip-stale-analysis-repair",
        action="store_true",
        help="Skip re-analysis when review/reply changed vs stored fingerprint (default is to run this pass).",
    )
    parser.add_argument(
        "--stale-analysis-max-reviews",
        type=int,
        default=0,
        help="Max reviews per run for stale-fingerprint repair (0 = unlimited).",
    )

    parser.add_argument("--skip-upsert", action="store_true")
    parser.add_argument("--skip-analysis", action="store_true")
    parser.add_argument("--skip-commentary", action="store_true")
    return parser.parse_args()


if __name__ == "__main__":
    asyncio.run(run_pipeline(parse_args()))
