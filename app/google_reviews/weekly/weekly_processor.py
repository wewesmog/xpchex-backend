from __future__ import annotations

from datetime import date, timedelta
from typing import Any, Dict, List, Optional
import json

from app.shared_services.db import get_postgres_connection
from app.shared_services.logger_setup import setup_logger
from app.google_reviews.weekly.weekly_summary_generator import process_weekly_aggregations

logger = setup_logger()


def get_weeks_with_data(app_id: str, start_date: date, end_date: date) -> List[date]:
    """Return week_start dates (Mondays) that have analyzed reviews for the app."""
    conn = get_postgres_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT date_trunc('week', review_created_at)::date AS week_start
                FROM processed_app_reviews
                WHERE app_id = %s
                  AND review_created_at::date BETWEEN %s AND %s
                  AND latest_analysis IS NOT NULL
                ORDER BY week_start
                """,
                (app_id, start_date, end_date),
            )
            return [row[0] for row in cur.fetchall()]
    except Exception as e:
        logger.error(f"Error getting weeks with data: {e}")
        return []
    finally:
        conn.close()


def aggregate_week_for_app(app_id: str, week_start: date) -> Optional[Dict[str, Any]]:
    """Aggregate positives and issues for a single app and week (no LLM)."""
    conn = get_postgres_connection()
    try:
        week_start_param = week_start
        with conn.cursor() as cur:
            cur.execute(
                """
                WITH weekly_reviews AS (
                    SELECT review_id, review_created_at
                    FROM processed_app_reviews
                    WHERE app_id = %s
                      AND date_trunc('week', review_created_at)::date = %s
                      AND latest_analysis IS NOT NULL
                ),
                positive_mentions_expanded AS (
                    SELECT
                        wr.review_id,
                        (m->>'impact_score')::float AS positive_impact_score,
                        m->>'description' AS description,
                        m->>'impact_area' AS positive_impact_area,
                        -- exact canonical match
                        cs_exact.canonical_id AS canonical_id_exact,
                        -- alias mapping
                        ca.canonical_id AS canonical_id_alias,
                        -- trigram similarity fallback to nearest canonical statement
                        cs_sim.canonical_id AS canonical_id_sim
                    FROM weekly_reviews wr
                    CROSS JOIN LATERAL jsonb_array_elements(
                        (SELECT par.latest_analysis->'positive_feedback'->'positive_mentions'
                         FROM processed_app_reviews par
                         WHERE par.review_id = wr.review_id)
                    ) m
                    LEFT JOIN canonical_statements cs_exact ON cs_exact.statement = m->>'description'
                    LEFT JOIN canonical_aliases ca ON ca.alias = m->>'description'
                    LEFT JOIN LATERAL (
                        SELECT cs2.canonical_id
                        FROM canonical_statements cs2
                        WHERE cs2.statement IS NOT NULL
                        ORDER BY similarity(cs2.statement, m->>'description') DESC
                        LIMIT 1
                    ) cs_sim ON TRUE
                ),
                positive_mentions AS (
                    SELECT
                        COALESCE(canonical_id_alias, canonical_id_exact, canonical_id_sim, description) AS grouping_id,
                        COUNT(*) AS count,
                        ARRAY_AGG(review_id) AS review_ids,
                        ARRAY_AGG(DISTINCT positive_impact_area) AS impact_areas,
                        AVG(COALESCE(positive_impact_score, 0)) AS avg_impact
                    FROM positive_mentions_expanded
                    GROUP BY COALESCE(canonical_id_alias, canonical_id_exact, canonical_id_sim, description)
                ),
                issues_expanded AS (
                    SELECT
                        wr.review_id,
                        i->>'description' AS description,
                        i->>'type' AS issue_type,
                        i->>'severity' AS issue_severity,
                        (i->>'impact_score')::float AS issue_impact_score,
                        -- exact canonical match
                        cs_exact.canonical_id AS canonical_id_exact,
                        -- alias mapping
                        ca.canonical_id AS canonical_id_alias,
                        -- trigram similarity fallback
                        cs_sim.canonical_id AS canonical_id_sim
                    FROM weekly_reviews wr
                    CROSS JOIN LATERAL jsonb_array_elements(
                        (SELECT par.latest_analysis->'issues'->'issues'
                         FROM processed_app_reviews par
                         WHERE par.review_id = wr.review_id)
                    ) i
                    LEFT JOIN canonical_statements cs_exact ON cs_exact.statement = i->>'description'
                    LEFT JOIN canonical_aliases ca ON ca.alias = i->>'description'
                    LEFT JOIN LATERAL (
                        SELECT cs2.canonical_id
                        FROM canonical_statements cs2
                        WHERE cs2.statement IS NOT NULL
                        ORDER BY similarity(cs2.statement, i->>'description') DESC
                        LIMIT 1
                    ) cs_sim ON TRUE
                ),
                issues AS (
                    SELECT
                        COALESCE(canonical_id_alias, canonical_id_exact, canonical_id_sim, description) AS grouping_id,
                        issue_type AS type,
                        issue_severity AS severity,
                        COUNT(*) AS count,
                        AVG(COALESCE(issue_impact_score, 0)) AS avg_impact,
                        ARRAY_AGG(review_id) AS review_ids
                    FROM issues_expanded
                    GROUP BY COALESCE(canonical_id_alias, canonical_id_exact, canonical_id_sim, description), issue_type, issue_severity
                )
                SELECT
                    %s::date AS week_start,
                    (%s::date + interval '6 days')::date AS week_end,
                    COALESCE(
                        (SELECT jsonb_agg(
                            jsonb_build_object(
                                'grouping_id', grouping_id,
                                'count', count,
                                'impact_areas', impact_areas,
                                'review_ids', review_ids,
                                'avg_impact', avg_impact
                            )
                        ) FROM positive_mentions), '[]'::jsonb) AS positive_mentions,
                    COALESCE(
                        (SELECT jsonb_agg(
                            jsonb_build_object(
                                'grouping_id', grouping_id,
                                'type', type,
                                'severity', severity,
                                'count', count,
                                'avg_impact', avg_impact,
                                'review_ids', review_ids
                            )
                        ) FROM issues), '[]'::jsonb) AS issues
                """,
                (app_id, week_start_param, week_start_param, week_start_param),
            )

            row = cur.fetchone()
            if not row:
                return None

            aggregated: Dict[str, Any] = {
                'week_start': row[0].isoformat() if hasattr(row[0], 'isoformat') else str(row[0]),
                'week_end': row[1].isoformat() if hasattr(row[1], 'isoformat') else str(row[1]),
                'app_id': app_id,
                'positive_mentions': row[2] or [],
                'issues': row[3] or [],
            }
            return aggregated
    except Exception as e:
        logger.error(f"Error aggregating week {week_start} for {app_id}: {e}")
        return None
    finally:
        conn.close()


def save_week_aggregation(aggregated_data: Dict[str, Any]) -> bool:
    """Upsert the aggregated_data into weekly_aggregations."""
    conn = get_postgres_connection("weekly_aggregations")
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO weekly_aggregations (week_start, app_id, aggregated_data, processed)
                VALUES (%s, %s, %s, false)
                ON CONFLICT (week_start, app_id) DO UPDATE SET
                    aggregated_data = EXCLUDED.aggregated_data,
                    processed = weekly_aggregations.processed AND weekly_aggregations.processed,
                    updated_at = CURRENT_TIMESTAMP
                """,
                (
                    aggregated_data['week_start'],
                    aggregated_data['app_id'],
                    json.dumps(aggregated_data),
                ),
            )
            conn.commit()
            logger.info(
                f"Saved weekly aggregation for app_id={aggregated_data['app_id']} week_start={aggregated_data['week_start']}"
            )
            return True
    except Exception as e:
        logger.error(f"Error saving weekly aggregation: {e}")
        conn.rollback()
        return False
    finally:
        conn.close()


def process_week_for_app(app_id: str, week_start: date) -> bool:
    """Aggregate + save + generate LLM summaries for a single week."""
    aggregated = aggregate_week_for_app(app_id, week_start)
    if not aggregated:
        logger.info(f"No data to aggregate for {app_id} week {week_start}")
        return False

    if not save_week_aggregation(aggregated):
        return False

    return process_weekly_aggregations(app_id, week_start)


def process_app_weeks(app_id: str, start_date: date, end_date: date) -> List[date]:
    """Process all weeks with data in [start_date, end_date]."""
    processed: List[date] = []
    for wk in get_weeks_with_data(app_id, start_date, end_date):
        if process_week_for_app(app_id, wk):
            processed.append(wk)
    return processed



def main() -> None:
    # Simple CLI: process a single week or a range.
    import argparse
    parser = argparse.ArgumentParser(description="Weekly processor")
    parser.add_argument("app_id", type=str, help="Application ID")
    parser.add_argument("start", type=str, help="Start date YYYY-MM-DD or single week date")
    parser.add_argument("end", type=str, nargs="?", default=None, help="End date YYYY-MM-DD (optional)")
    args = parser.parse_args()

    if args.end:
        s = date.fromisoformat(args.start)
        e = date.fromisoformat(args.end)
        weeks = process_app_weeks(args.app_id, s, e)
        print("Processed weeks:", weeks)
    else:
        wk = date.fromisoformat(args.start)
        ok = process_week_for_app(args.app_id, wk)
        print("Processed:", ok)


if __name__ == "__main__":
    main()


# python -m app.google_reviews.weekly.weekly_processor com.kcb.mobilebanking.android.mbp 2025-01-01 2025-08-31  