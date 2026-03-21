from datetime import datetime, timezone, timedelta
from typing import List, Optional, Dict, Any

from psycopg2.extras import execute_values
from ..shared_services.db import pooled_connection
from ..shared_services.llm import embed_texts

from app.shared_services.logger_setup import setup_logger

logger = setup_logger()


def get_max_published_at(app_id: Optional[str] = None) -> Optional[datetime]:
    """
    Return the latest published_at in response_history.
    Use as min_date for the next sync so only newer replies are selected (reply_created_at > this).
    """
    try:
        with pooled_connection() as conn:
            with conn.cursor() as cur:
                if app_id:
                    cur.execute(
                        "SELECT MAX(published_at) FROM response_history WHERE app_id = %s",
                        (app_id,),
                    )
                else:
                    cur.execute("SELECT MAX(published_at) FROM response_history")
                row = cur.fetchone()
                return row[0] if row and row[0] else None
    except Exception as e:
        logger.error(f"Error getting max published_at from response_history: {e}")
        return None


def select_past_responses(
    app_id: str,
    min_date: Optional[datetime] = None,
    max_date: Optional[datetime] = None,
    batch_size: int = 10,
) -> List[Dict[str, Any]]:
    """
    Select past responses from processed_app_reviews by app_id.
    - min_date: only rows with reply_created_at > min_date (for incremental sync).
    - max_date: only rows with reply_created_at < max_date (for paging oldest-first).
    """
    params: List[Any] = [app_id]
    where_clauses = ["app_id = %s", "reply_content IS NOT NULL", "reply_created_at IS NOT NULL"]
    if min_date is not None:
        where_clauses.append("reply_created_at > %s")
        params.append(min_date)
    if max_date is not None:
        where_clauses.append("reply_created_at < %s")
        params.append(max_date)

    where_sql = " AND ".join(where_clauses)
    sql = f"""
        SELECT review_id, app_id, content, reply_content, review_created_at, reply_created_at
        FROM processed_app_reviews
        WHERE {where_sql}
        ORDER BY reply_created_at DESC
        LIMIT %s
    """
    params.append(batch_size)

    try:
        with pooled_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, tuple(params))
                columns = [desc[0] for desc in cur.description]
                rows = cur.fetchall()
                responses = [dict(zip(columns, row)) for row in rows]
            logger.info(f"Selected {len(responses)} past responses for app {app_id}")
            return responses
    except Exception as e:
        logger.error(f"Error selecting past responses for app {app_id}: {e}")
        return []

def upsert_past_responses(past_responses: List[Dict[str, Any]]) -> str:
    """
    Upsert past responses into response_history.
    Expects each dict from select_past_responses: review_id, app_id, content, reply_content, reply_created_at.
    Embeddings are left NULL; fill with update_embeddings_batch after.
    """
    if not past_responses:
        logger.info("No past responses to upsert")
        return "Success"

    sql = """
        INSERT INTO response_history (review_id, app_id, response_text, source, published_at)
        VALUES %s
        ON CONFLICT (review_id) DO UPDATE SET
            app_id = EXCLUDED.app_id,
            response_text = EXCLUDED.response_text,
            source = EXCLUDED.source,
            published_at = EXCLUDED.published_at
    """
    values = [
        (
            r["review_id"],
            r["app_id"],
            r["reply_content"],
            "human",
            r.get("reply_created_at"),
        )
        for r in past_responses
    ]

    try:
        with pooled_connection() as conn:
            with conn.cursor() as cur:
                execute_values(cur, sql, values, page_size=500)
                conn.commit()
            logger.info(f"Upserted {len(past_responses)} past responses into response_history")
            return "Success"
    except Exception as e:
        logger.error(f"Error upserting past responses into response_history: {e}")
        return "Failed"


def _vector_to_pg(v: List[float]) -> str:
    """Format embedding list for pgvector: '[0.1,0.2,...]'."""
    return "[" + ",".join(str(x) for x in v) + "]"


def update_embeddings_batch(
    past_responses: List[Dict[str, Any]],
    response_embeddings: List[List[float]],
    review_embeddings: List[List[float]],
) -> str:
    """Update response_history with response and review embeddings for each review_id in the batch."""
    if not past_responses or len(past_responses) != len(response_embeddings) != len(review_embeddings):
        return "Success"
    try:
        with pooled_connection() as conn:
            with conn.cursor() as cur:
                for i, r in enumerate(past_responses):
                    cur.execute(
                        """
                        UPDATE response_history
                        SET response_embedding = %s::vector, review_embedding = %s::vector
                        WHERE review_id = %s
                        """,
                        (_vector_to_pg(response_embeddings[i]), _vector_to_pg(review_embeddings[i]), r["review_id"]),
                    )
                conn.commit()
            logger.info(f"Updated embeddings for {len(past_responses)} rows in response_history")
            return "Success"
    except Exception as e:
        logger.error(f"Error updating embeddings in response_history: {e}")
        return "Failed"


def run_upsert_past_responses(
    app_id: str,
    min_date: Optional[datetime] = datetime.now(timezone.utc)-timedelta(days=60),
    max_date: Optional[datetime] = None,
    batch_size: int = 10,
    limit: Optional[int] = 50,
    use_max_from_history: bool = True,
) -> str:
    """
    Sync past responses from processed_app_reviews into response_history in batches:
    select batch -> upsert -> embed -> update embeddings. Stops when no more rows or limit reached.

    Args:
        app_id: App to sync.
        min_date: Only replies with reply_created_at > min_date. If None and use_max_from_history,
            set to get_max_published_at(app_id) for incremental sync.
        max_date: For paging: only replies with reply_created_at < max_date (set from previous batch).
        batch_size: Rows per batch.
        limit: Max total rows to process (None = no limit).
        use_max_from_history: If True and min_date is None, use get_max_published_at(app_id) as min_date.
    """
    if min_date is None and use_max_from_history:
        min_date = get_max_published_at(app_id)
        if min_date is not None:
            logger.info(f"Incremental sync for {app_id}: only reply_created_at > {min_date}")

    total = 0
    max_date_cur = max_date

    while True:
        if limit is not None and total >= limit:
            break
        take = batch_size if limit is None else min(batch_size, limit - total)
        batch = select_past_responses(app_id, min_date=min_date, max_date=max_date_cur, batch_size=take)
        if not batch:
            break

        if upsert_past_responses(batch) != "Success":
            return "Failed"

        response_texts = [r["reply_content"] or "" for r in batch]
        review_texts = [r["content"] or "" for r in batch]
        try:
            response_embeddings = embed_texts(response_texts)
            review_embeddings = embed_texts(review_texts)
        except Exception as e:
            logger.error(f"Embedding failed: {e}")
            return "Failed"

        if update_embeddings_batch(batch, response_embeddings, review_embeddings) != "Success":
            return "Failed"

        total += len(batch)
        logger.info(f"Synced batch of {len(batch)}; total so far {total}")

        if len(batch) < take:
            break
        # Next batch: only rows older than oldest in this batch
        max_date_cur = batch[-1].get("reply_created_at")
        if max_date_cur is None:
            break

    logger.info(f"run_upsert_past_responses finished for {app_id}: {total} rows processed")
    return "Success"


def select_response_history_missing_embeddings(
    app_id: Optional[str] = None,
    batch_size: int = 50,
) -> List[Dict[str, Any]]:
    """
    Select rows from response_history that have NULL response embedding (e.g. after a failed embed step).
    Returns review_id, app_id, response_text. Review text (content) must be fetched from processed_app_reviews.
    """
    params: List[Any] = []
    where = "response_embedding IS NULL AND response_text IS NOT NULL"
    if app_id is not None:
        where += " AND app_id = %s"
        params.append(app_id)
    params.append(batch_size)
    sql = f"""
        SELECT review_id, app_id, response_text
        FROM response_history
        WHERE {where}
        ORDER BY published_at DESC NULLS LAST
        LIMIT %s
    """
    try:
        with pooled_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, tuple(params))
                columns = [desc[0] for desc in cur.description]
                rows = cur.fetchall()
                return [dict(zip(columns, row)) for row in rows]
    except Exception as e:
        logger.error(f"Error selecting response_history missing embeddings: {e}")
        return []


def backfill_embeddings_for_response_history(
    app_id: Optional[str] = None,
    batch_size: int = 50,
    limit: Optional[int] = None,
) -> str:
    """
    Fill embeddings for response_history rows that have NULL embeddings (e.g. after embed/update failed).
    Selects rows with NULL response embedding, gets review text from processed_app_reviews, embeds both,
    then updates response_history. Run on a schedule or after fixing embed/API issues.
    """
    total = 0
    while True:
        if limit is not None and total >= limit:
            break
        take = batch_size if limit is None else min(batch_size, limit - total)
        rows = select_response_history_missing_embeddings(app_id=app_id, batch_size=take)
        if not rows:
            logger.info("No more response_history rows missing embeddings")
            break

        review_ids = [r["review_id"] for r in rows]
        with pooled_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT review_id, content FROM processed_app_reviews WHERE review_id = ANY(%s)",
                    (review_ids,),
                )
                content_by_review = dict(cur.fetchall())

        response_texts = [r["response_text"] or "" for r in rows]
        review_texts = [content_by_review.get(r["review_id"]) or "" for r in rows]

        try:
            response_embeddings = embed_texts(response_texts)
            review_embeddings = embed_texts(review_texts)
        except Exception as e:
            logger.error(f"Backfill embedding failed: {e}")
            return "Failed"

        # update_embeddings_batch expects list of dicts with review_id; we have rows with review_id, app_id, response_text
        if update_embeddings_batch(rows, response_embeddings, review_embeddings) != "Success":
            return "Failed"

        total += len(rows)
        logger.info(f"Backfilled embeddings for {len(rows)} rows; total {total}")

        if len(rows) < take:
            break

    logger.info(f"backfill_embeddings_for_response_history finished: {total} rows updated")
    return "Success"


if __name__ == "__main__":
    run_upsert_past_responses("com.kcb.mobilebanking.android.mbp", batch_size=10, limit=200000)