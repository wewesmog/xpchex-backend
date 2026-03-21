from __future__ import annotations
"""
Manual end-to-end pipeline runner (single file).

Run order:
1) Fetch + upsert raw/processed reviews
2) Analyze reviews
3) Generate Nelly drafts
4) Sync human replies into response_history (+ embeddings)
5) Generate analytics commentary
"""

import argparse
import asyncio
import os
from datetime import datetime

from app.commentary.commentary_main import run_commentary_generation
from app.google_reviews.analyze_revs_db import analyze_reviews
from app.google_reviews.reviews_scraper import ReviewScraper
from app.nelly.graph import cx_agent_graph
from app.nelly.nelly_main import _build_state_from_row, fetch_pending_reviews
from app.nelly.upsert_past_responses import run_upsert_past_responses
from app.shared_services.logger_setup import setup_logger

logger = setup_logger()


def run_reviews_scrape(app_id: str, country: str, lang: str, batch_size: int) -> tuple[int, int]:
    with ReviewScraper() as scraper:
        return scraper.fetch_reviews(
            app_id=app_id,
            count=0,
            lang=lang,
            country=country,
            batch_size=batch_size,
            incremental=True,
        )


async def run_nelly_drafts(app_id: str, batch: int, max_reviews: int) -> int:
    processed = 0
    batch = max(1, int(batch))
    max_reviews = max(0, int(max_reviews))

    while True:
        remaining = None if max_reviews == 0 else max_reviews - processed
        if remaining is not None and remaining <= 0:
            break
        limit = batch if remaining is None else min(batch, remaining)
        rows = fetch_pending_reviews(app_id=app_id, limit=limit)
        if not rows:
            break

        for row in rows:
            state = _build_state_from_row(app_id=app_id, row=row)
            review_id = state.get("review_id")
            logger.info("Nelly processing review_id=%s", review_id)
            final_state = await cx_agent_graph.ainvoke(state)
            processed += 1
            logger.info(
                "Nelly done review_id=%s draft_id=%s escalation_id=%s",
                review_id,
                final_state.get("draft_id"),
                final_state.get("escalation_id"),
            )
            if max_reviews and processed >= max_reviews:
                break

    return processed


async def run_pipeline(args: argparse.Namespace) -> None:
    app_id = (
        args.app_id
        or os.getenv("MVP_APP_ID")
    )
    if not app_id:
        raise SystemExit("Missing --app-id (or env MVP_APP_ID)")

    logger.info("Starting manual daily pipeline for app_id=%s", app_id)
    logger.info("Order: scrape -> analyze -> nelly -> upsert responses -> commentary")

    started = datetime.utcnow()

    if not args.skip_scrape:
        fetched, inserted = run_reviews_scrape(
            app_id=app_id,
            country=args.country,
            lang=args.lang,
            batch_size=args.scrape_batch_size,
        )
        logger.info("Scrape complete fetched=%s inserted=%s", fetched, inserted)
    else:
        logger.info("Skipping scrape step")

    if not args.skip_analysis:
        analyzed = await analyze_reviews(
            app_id=app_id,
            min_date=None,
            max_reviews=args.analysis_max_reviews,
            batch_size=args.analysis_batch_size,
            concurrent=True,
            max_concurrent=args.analysis_max_concurrent,
            analyzed=False,
            reanalyze=args.analysis_reanalyze_all,
        )
        logger.info("Analysis complete processed=%s", analyzed)
    else:
        logger.info("Skipping analysis step")

    if not args.skip_nelly:
        drafted = await run_nelly_drafts(
            app_id=app_id,
            batch=args.nelly_batch_size,
            max_reviews=args.nelly_max_reviews,
        )
        logger.info("Nelly draft generation complete processed=%s", drafted)
    else:
        logger.info("Skipping nelly step")

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

    elapsed = datetime.utcnow() - started
    logger.info("Manual daily pipeline completed in %s", elapsed)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run full daily local pipeline manually")
    parser.add_argument("--app-id", default=None)
    parser.add_argument("--country", default="ke")
    parser.add_argument("--lang", default="en")

    parser.add_argument("--scrape-batch-size", type=int, default=100)
    parser.add_argument("--upsert-batch-size", type=int, default=100)
    parser.add_argument("--analysis-batch-size", type=int, default=10)
    parser.add_argument("--analysis-max-concurrent", type=int, default=5)
    parser.add_argument("--analysis-max-reviews", type=int, default=0)
    parser.add_argument("--analysis-reanalyze-all", action="store_true")
    parser.add_argument("--nelly-batch-size", type=int, default=10)
    parser.add_argument("--nelly-max-reviews", type=int, default=0)

    parser.add_argument("--skip-scrape", action="store_true")
    parser.add_argument("--skip-upsert", action="store_true")
    parser.add_argument("--skip-analysis", action="store_true")
    parser.add_argument("--skip-nelly", action="store_true")
    parser.add_argument("--skip-commentary", action="store_true")
    return parser.parse_args()


if __name__ == "__main__":
    asyncio.run(run_pipeline(parse_args()))
