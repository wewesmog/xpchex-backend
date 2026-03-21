# Main entry point for initializing the Nelly agent state

import argparse
import asyncio
import json
from typing import Any
from app.nelly.graph import cx_agent_graph

from app.nelly.state import CxAgentState
from app.shared_services.logger_setup import setup_logger
from app.shared_services.db import pooled_connection

logger = setup_logger()


def _coerce_topic_tags(raw: Any) -> list[str]:
    """
    Normalize `topic_tags` from Postgres/jsonb into `list[str]`.
    """
    if raw is None:
        return []
    if isinstance(raw, list):
        return [str(x) for x in raw]
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
            return [str(x) for x in parsed] if isinstance(parsed, list) else []
        except Exception:
            return []
    return []


def _build_state_from_row(app_id: str, row: tuple) -> CxAgentState:
    """
    Build CxAgentState from a DB row.

    Expected row shape:
      (review_id, content, score, review_created_at, sentiment, topic_tags)
    """
    review_id, content, score, review_created_at, sentiment, topic_tags_raw = row
    topic_tags = _coerce_topic_tags(topic_tags_raw)
    return CxAgentState(
        review_id=str(review_id),
        app_id=str(app_id),
        review_text=content,
        review_score=float(score) if score is not None else None,
        review_created_at=review_created_at.isoformat() if review_created_at is not None else None,
        topic_tags=topic_tags,
        sentiment=sentiment,
        review_embedding=None,
        retrieved_chunks=[],
        context_is_stale=False,
        draft_text=None,
        eval_accuracy=None,
        eval_tone=None,
        eval_completeness=None,
        agent_confidence=0.0,
        should_escalate=False,
        escalation_reason=None,
        draft_id=None,
        escalation_id=None,
        node_history_id=None,
        sources_used=None,
    )


def fetch_pending_reviews(app_id: str, limit: int) -> list[tuple]:
    """
    Fetch pending review rows that have NOT been processed by Nelly yet.
    """
    with pooled_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    review_id,
                    content,
                    score,
                    review_created_at,
                    latest_analysis -> 'sentiment' -> 'overall' ->> 'classification' AS sentiment,
                    COALESCE(latest_analysis -> 'topic_tags', '[]'::jsonb) AS topic_tags
                FROM processed_app_reviews
                WHERE app_id = %s
                  AND reply_content IS NULL
                  AND NOT EXISTS (
                      SELECT 1
                      FROM node_history nh
                      WHERE nh.review_id = processed_app_reviews.review_id
                        AND nh.app_id = processed_app_reviews.app_id
                        AND nh.agent_name = 'nelly'
                  )
                ORDER BY review_created_at DESC
                LIMIT %s;
                """,
                (app_id, limit),
            )
            return cur.fetchall() or []


def initialize_state(app_id: str) -> CxAgentState:
    """
    Build initial graph state from DB.

    If no matching review exists, returns a "null-review" state where:
    - review_text/review_score/review_created_at are None
    - topic_tags is []
    - sentiment is None
    """
    rows = fetch_pending_reviews(app_id=app_id, limit=1)
    if not rows:
        return CxAgentState(
            review_id="",
            app_id=str(app_id),
            review_text=None,
            review_score=None,
            review_created_at=None,
            topic_tags=[],
            sentiment=None,
            review_embedding=None,
            retrieved_chunks=[],
            context_is_stale=False,
            draft_text=None,
            eval_accuracy=None,
            eval_tone=None,
            eval_completeness=None,
            agent_confidence=0.0,
            should_escalate=True,
            escalation_reason="no_review_found",
            draft_id=None,
            escalation_id=None,
        )

    return _build_state_from_row(app_id=app_id, row=rows[0])


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run Nelly for pending reviews (single or batch).")
    parser.add_argument("--app-id", dest="app_id", type=str, required=True)
    parser.add_argument("--batch", dest="batch", type=int, default=1, help="Batch size per fetch. 0 means fetch 1.")
    parser.add_argument("--max", dest="max_reviews", type=int, default=0, help="Max reviews to process. 0 means unlimited.")
    args = parser.parse_args()

    app_id = args.app_id

    async def _run():
        batch_size = int(args.batch) if args.batch is not None else 1
        if batch_size <= 0:
            batch_size = 1

        max_reviews = int(args.max_reviews) if args.max_reviews is not None else 0
        processed = 0

        while True:
            remaining = None
            if max_reviews > 0:
                remaining = max_reviews - processed
                if remaining <= 0:
                    break

            limit = batch_size if remaining is None else min(batch_size, remaining)
            rows = fetch_pending_reviews(app_id=app_id, limit=limit)
            if not rows:
                break

            for row in rows:
                state = _build_state_from_row(app_id=app_id, row=row)
                review_id = state.get("review_id")
                print(f"Processing review_id={review_id} ({processed + 1}{'/' + str(max_reviews) if max_reviews > 0 else ''})")
                final_state = await cx_agent_graph.ainvoke(state)
                processed += 1
                draft_id = final_state.get("draft_id")
                escalation_id = final_state.get("escalation_id")
                print(f"Done review_id={review_id} draft_id={draft_id} escalation_id={escalation_id}")

                if max_reviews > 0 and processed >= max_reviews:
                    break

    asyncio.run(_run())

