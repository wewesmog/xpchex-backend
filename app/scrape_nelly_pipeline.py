from __future__ import annotations
"""
Intraday / scheduled job: scrape Play reviews and run Nelly drafts.

Data flow:
  Scraper → raw_app_reviews → DB function process_raw_reviews() → processed_app_reviews
  Nelly (fetch_pending_reviews) reads only processed_app_reviews.

Default order in this module is scrape first (unless --skip-scrape), then Nelly, so new rows
exist before drafts. If you use --skip-scrape, Nelly only sees reviews already in
processed_app_reviews (e.g. from an earlier scrape or another job).

Does not run AI analysis, commentary, or response_history upsert — see daily_pipeline_main.py.
"""

import argparse
import asyncio
import os
from datetime import datetime

from app.google_reviews.reviews_scraper import ReviewScraper
from app.nelly.graph import cx_agent_graph
from app.nelly.nelly_main import _build_state_from_row, fetch_pending_reviews
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


async def _nelly_one_review(app_id: str, row: tuple, semaphore: asyncio.Semaphore):
    async with semaphore:
        state = _build_state_from_row(app_id=app_id, row=row)
        review_id = state.get("review_id")
        logger.info("Nelly processing review_id=%s", review_id)
        final_state = await cx_agent_graph.ainvoke(state)
        logger.info(
            "Nelly done review_id=%s draft_id=%s escalation_id=%s",
            review_id,
            final_state.get("draft_id"),
            final_state.get("escalation_id"),
        )
        return review_id, final_state


async def run_nelly_drafts(
    app_id: str,
    batch: int,
    max_reviews: int,
    *,
    concurrent: bool = True,
    max_concurrent: int = 3,
) -> int:
    processed = 0
    batch = max(1, int(batch))
    max_reviews = max(0, int(max_reviews))
    max_concurrent = max(1, int(max_concurrent))

    while True:
        remaining = None if max_reviews == 0 else max_reviews - processed
        if remaining is not None and remaining <= 0:
            break
        limit = batch if remaining is None else min(batch, remaining)
        rows = fetch_pending_reviews(app_id=app_id, limit=limit)
        if not rows:
            break

        if not concurrent or max_concurrent == 1:
            for row in rows:
                state = _build_state_from_row(app_id=app_id, row=row)
                review_id = state.get("review_id")
                logger.info("Nelly processing review_id=%s", review_id)
                await cx_agent_graph.ainvoke(state)
                processed += 1
                if max_reviews and processed >= max_reviews:
                    return processed
            continue

        semaphore = asyncio.Semaphore(max_concurrent)
        tasks = [_nelly_one_review(app_id, row, semaphore) for row in rows]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for result in results:
            if isinstance(result, Exception):
                logger.error("Nelly concurrent task failed: %s", result)
                raise result
            processed += 1
            if max_reviews and processed >= max_reviews:
                return processed

    return processed


async def run_pipeline(args: argparse.Namespace) -> None:
    app_id = args.app_id or os.getenv("MVP_APP_ID")
    if not app_id:
        raise SystemExit("Missing --app-id (or env MVP_APP_ID)")

    logger.info("Starting scrape + Nelly pipeline for app_id=%s", app_id)
    started = datetime.utcnow()

    if not args.skip_scrape:
        fetched, inserted = run_reviews_scrape(
            app_id=app_id,
            country=args.country,
            lang=args.lang,
            batch_size=args.scrape_batch_size,
        )
        logger.info(
            "Scrape complete fetched=%s processed_rows=%s (processed_app_reviews updated via process_raw_reviews)",
            fetched,
            inserted,
        )
    else:
        logger.info(
            "Skipping scrape — Nelly will only use rows already in processed_app_reviews"
        )

    if not args.skip_nelly:
        drafted = await run_nelly_drafts(
            app_id=app_id,
            batch=args.nelly_batch_size,
            max_reviews=args.nelly_max_reviews,
            concurrent=not args.nelly_sequential,
            max_concurrent=args.nelly_max_concurrent,
        )
        logger.info("Nelly draft generation complete processed=%s", drafted)
    else:
        logger.info("Skipping nelly step")

    logger.info("Scrape + Nelly pipeline completed in %s", datetime.utcnow() - started)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scrape reviews and run Nelly (intraday job)")
    parser.add_argument("--app-id", default=None)
    parser.add_argument("--country", default="ke")
    parser.add_argument("--lang", default="en")
    parser.add_argument("--scrape-batch-size", type=int, default=100)
    parser.add_argument("--nelly-batch-size", type=int, default=10)
    parser.add_argument("--nelly-max-reviews", type=int, default=0)
    parser.add_argument("--nelly-sequential", action="store_true")
    parser.add_argument("--nelly-max-concurrent", type=int, default=3)
    parser.add_argument("--skip-scrape", action="store_true")
    parser.add_argument("--skip-nelly", action="store_true")
    return parser.parse_args()


if __name__ == "__main__":
    asyncio.run(run_pipeline(parse_args()))
