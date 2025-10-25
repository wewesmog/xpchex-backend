from __future__ import annotations

from datetime import date
from typing import Any, Dict, List, Optional, Tuple
import json

from app.shared_services.db import get_postgres_connection
from app.shared_services.logger_setup import setup_logger
from app.google_reviews.weekly.weekly_summary_generator import (
    generate_overall_summary,
    generate_positive_summary,
    generate_issues_summary,
)

logger = setup_logger()


def _merge_positive_mentions(all_weeks: List[List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    by_group: Dict[str, Dict[str, Any]] = {}
    for week in all_weeks:
        for m in week or []:
            gid = m.get("grouping_id") or "unknown"
            count = int(m.get("count", 0))
            avg_imp = float(m.get("avg_impact", 0.0))
            areas = list(m.get("impact_areas", []) or [])
            rids = list(m.get("review_ids", []) or [])

            acc = by_group.setdefault(
                gid,
                {
                    "grouping_id": gid,
                    "count": 0,
                    "_weighted_impact_sum": 0.0,
                    "impact_areas": set(),
                    "review_ids": set(),
                },
            )
            acc["count"] += count
            acc["_weighted_impact_sum"] += count * avg_imp
            acc["impact_areas"].update(areas)
            acc["review_ids"].update(rids)

    result: List[Dict[str, Any]] = []
    for gid, acc in by_group.items():
        total = acc["count"] or 1
        result.append(
            {
                "grouping_id": gid,
                "count": acc["count"],
                "avg_impact": round(acc["_weighted_impact_sum"] / total, 2),
                "impact_areas": sorted(list(acc["impact_areas"])),
                # cap review_ids length for safety
                "review_ids": list(acc["review_ids"])[:200],
            }
        )
    result.sort(key=lambda x: x.get("count", 0), reverse=True)
    return result


def _merge_issues(all_weeks: List[List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    by_group: Dict[str, Dict[str, Any]] = {}
    for week in all_weeks:
        for i in week or []:
            gid = i.get("grouping_id") or "unknown"
            count = int(i.get("count", 0))
            avg_imp = float(i.get("avg_impact", 0.0))
            typ = i.get("type") or "other"
            sev = i.get("severity") or "unknown"
            rids = list(i.get("review_ids", []) or [])

            acc = by_group.setdefault(
                gid,
                {
                    "grouping_id": gid,
                    "count": 0,
                    "_weighted_impact_sum": 0.0,
                    "type_counts": {},
                    "severity_counts": {},
                    "review_ids": set(),
                },
            )
            acc["count"] += count
            acc["_weighted_impact_sum"] += count * avg_imp
            acc["type_counts"][typ] = acc["type_counts"].get(typ, 0) + count
            acc["severity_counts"][sev] = acc["severity_counts"].get(sev, 0) + count
            acc["review_ids"].update(rids)

    def _mode(d: Dict[str, int]) -> str:
        if not d:
            return "unknown"
        return max(d.items(), key=lambda kv: kv[1])[0]

    result: List[Dict[str, Any]] = []
    for gid, acc in by_group.items():
        total = acc["count"] or 1
        result.append(
            {
                "grouping_id": gid,
                "type": _mode(acc["type_counts"]),
                "severity": _mode(acc["severity_counts"]),
                "count": acc["count"],
                "avg_impact": round(acc["_weighted_impact_sum"] / total, 2),
                # cap review_ids length for safety
                "review_ids": list(acc["review_ids"])[:200],
            }
        )
    result.sort(key=lambda x: x.get("count", 0), reverse=True)
    return result


def _fetch_weeklies(app_id: str, start: Optional[date], end: Optional[date]) -> List[Dict[str, Any]]:
    conn = get_postgres_connection("weekly_aggregations")
    try:
        with conn.cursor() as cur:
            if start and end:
                cur.execute(
                    """
                    SELECT week_start, aggregated_data
                    FROM weekly_aggregations
                    WHERE app_id = %s AND processed = true
                      AND week_start BETWEEN %s AND %s
                    ORDER BY week_start
                    """,
                    (app_id, start, end),
                )
            else:
                cur.execute(
                    """
                    SELECT week_start, aggregated_data
                    FROM weekly_aggregations
                    WHERE app_id = %s AND processed = true
                    ORDER BY week_start
                    """,
                    (app_id,),
                )
            rows = cur.fetchall() or []
            return [row[1] for row in rows]
    finally:
        conn.close()


def generate_period_statements(app_id: str, start: Optional[date] = None, end: Optional[date] = None) -> Dict[str, str]:
    """Return 3 consolidated statements (overall, positive, issues) across many weeks.
    Reads from weekly_aggregations only.
    """
    weekly_payloads = _fetch_weeklies(app_id, start, end)
    if not weekly_payloads:
        return {"overall": "", "positive": "", "issues": ""}

    positives_by_week = [w.get("positive_mentions", []) for w in weekly_payloads]
    issues_by_week = [w.get("issues", []) for w in weekly_payloads]

    # Merge while preferring canonical IDs that are already present in weekly payloads
    merged_positives = _merge_positive_mentions(positives_by_week)
    merged_issues = _merge_issues(issues_by_week)

    period_data = {
        "app_id": app_id,
        # not used by prompts directly, but kept for compatibility
        "week_start": (start.isoformat() if start else "all-time"),
        "week_end": (end.isoformat() if end else "present"),
        "positive_mentions": merged_positives,
        "issues": merged_issues,
    }

    overall = generate_overall_summary(period_data)
    positive = generate_positive_summary(period_data)
    issues = generate_issues_summary(period_data)

    return {"overall": overall, "positive": positive, "issues": issues}


def save_period_statements(app_id: str, statements: Dict[str, str], merged_positives: List[Dict[str, Any]], merged_issues: List[Dict[str, Any]], start: Optional[date], end: Optional[date]) -> bool:
    """Upsert consolidated statements into period_summaries."""
    scope = 'all_time' if (start is None and end is None) else 'range'
    conn = get_postgres_connection("period_summaries")
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO period_summaries (
                    app_id, scope, period_start, period_end,
                    aggregated_positive, aggregated_issues,
                    overall_summary, positive_summary, issues_summary
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (app_id, scope, period_start, period_end) DO UPDATE SET
                    aggregated_positive = EXCLUDED.aggregated_positive,
                    aggregated_issues = EXCLUDED.aggregated_issues,
                    overall_summary = EXCLUDED.overall_summary,
                    positive_summary = EXCLUDED.positive_summary,
                    issues_summary = EXCLUDED.issues_summary,
                    updated_at = CURRENT_TIMESTAMP
                """,
                (
                    app_id,
                    scope,
                    start,
                    end,
                    json.dumps(merged_positives),
                    json.dumps(merged_issues),
                    statements.get("overall", ""),
                    statements.get("positive", ""),
                    statements.get("issues", ""),
                ),
            )
            conn.commit()
            return True
    except Exception as e:
        logger.error(f"Error saving period statements: {e}")
        conn.rollback()
        return False
    finally:
        conn.close()


def main() -> None:
    import argparse
    parser = argparse.ArgumentParser(description="Consolidated period statements from weekly_aggregations")
    parser.add_argument("app_id", type=str, help="Application ID")
    parser.add_argument("start", type=str, nargs="?", default=None, help="Start date YYYY-MM-DD (optional)")
    parser.add_argument("end", type=str, nargs="?", default=None, help="End date YYYY-MM-DD (optional)")
    args = parser.parse_args()

    s = date.fromisoformat(args.start) if args.start else None
    e = date.fromisoformat(args.end) if args.end else None

    # Generate
    weekly_payloads = _fetch_weeklies(args.app_id, s, e)
    positives_by_week = [w.get("positive_mentions", []) for w in weekly_payloads]
    issues_by_week = [w.get("issues", []) for w in weekly_payloads]
    merged_positives = _merge_positive_mentions(positives_by_week)
    merged_issues = _merge_issues(issues_by_week)

    period_data = {
        "app_id": args.app_id,
        "week_start": (s.isoformat() if s else "all-time"),
        "week_end": (e.isoformat() if e else "present"),
        "positive_mentions": merged_positives,
        "issues": merged_issues,
    }

    statements = {
        "overall": generate_overall_summary(period_data),
        "positive": generate_positive_summary(period_data),
        "issues": generate_issues_summary(period_data),
    }

    print(json.dumps(statements, ensure_ascii=False, indent=2))

    # Save
    ok = save_period_statements(args.app_id, statements, merged_positives, merged_issues, s, e)
    print("saved:", ok)


if __name__ == "__main__":
    main()



# python -m app.google_reviews.weekly.period_summary com.kcb.mobilebanking.android.mbp

# python -m app.google_reviews.weekly.period_summary com.kcb.mobilebanking.android.mbp 2025-01-01 2025-06-30

