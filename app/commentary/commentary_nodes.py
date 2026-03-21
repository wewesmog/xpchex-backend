from __future__ import annotations

import hashlib
import json
from datetime import date, datetime
from typing import Any, Optional

from psycopg2.extras import RealDictCursor

from app.models.commentary_models import CommentaryLLMOutput, SlotKey
from app.shared_services.date_ranges import TimeRange, get_date_range
from app.shared_services.db import pooled_connection
from app.shared_services.llm import call_llm_api_async
from app.shared_services.logger_setup import setup_logger

logger = setup_logger()

DEFAULT_TIME_RANGES: list[TimeRange] = [
    TimeRange.LAST_7_DAYS,
    TimeRange.LAST_30_DAYS,
    TimeRange.LAST_3_MONTHS,
    TimeRange.LAST_6_MONTHS,
    TimeRange.LAST_12_MONTHS,
    TimeRange.THIS_YEAR,
    TimeRange.ALL_TIME,
]

DEFAULT_SLOT_KEYS: list[SlotKey] = [
    "overview_kpi_positive_rate_footer",
    "overview_kpi_critical_issues_footer",
    "overview_kpi_delight_mentions_footer",
    "overview_kpi_recommendations_footer",
    "overview_exec_summary",
    "sentiment_hero_narrative",
    "issues_hero_narrative",
    "delights_hero_narrative",
    "recommendations_hero_narrative",
]

SLOT_SPECS: dict[SlotKey, dict[str, Any]] = {
    "overview_kpi_positive_rate_footer": {
        "max_chars": 120,
        "instructions": "One short line about promoter vs detractor balance.",
    },
    "overview_kpi_critical_issues_footer": {
        "max_chars": 120,
        "instructions": "One short line about critical/high issue pressure.",
    },
    "overview_kpi_delight_mentions_footer": {
        "max_chars": 120,
        "instructions": "One short line about positive/high-impact signals.",
    },
    "overview_kpi_recommendations_footer": {
        "max_chars": 120,
        "instructions": "One short line about actionable recommendations and quick wins.",
    },
    "overview_exec_summary": {
        "max_chars": 600,
        "instructions": "Executive narrative (2-4 sentences), neutral and data-grounded.",
    },
    "sentiment_hero_narrative": {
        "max_chars": 500,
        "instructions": "Narrative about sentiment direction and scale of evidence.",
    },
    "issues_hero_narrative": {
        "max_chars": 500,
        "instructions": "Narrative about key issue load and severity concentration.",
    },
    "delights_hero_narrative": {
        "max_chars": 500,
        "instructions": "Narrative about what users value and delight intensity.",
    },
    "recommendations_hero_narrative": {
        "max_chars": 500,
        "instructions": "Narrative about action backlog, quick wins, and must-do items.",
    },
}

SLOT_STYLE_HINTS: dict[SlotKey, str] = {
    "overview_kpi_positive_rate_footer": (
        "State promoter/detractor balance succinctly. Mention direction only if obvious from values."
    ),
    "overview_kpi_critical_issues_footer": (
        "Emphasize risk focus: critical first, then high-severity volume."
    ),
    "overview_kpi_delight_mentions_footer": (
        "Highlight positive momentum and high-impact positives without hype."
    ),
    "overview_kpi_recommendations_footer": (
        "Summarize actionability with quick wins and must-do balance."
    ),
    "overview_exec_summary": (
        "2-4 sentences, executive tone: what happened, why it matters, where to focus next."
    ),
    "sentiment_hero_narrative": (
        "2-3 sentences about sentiment quality and evidence volume."
    ),
    "issues_hero_narrative": (
        "2-3 sentences focusing on concentration of severe issues and probable pressure points."
    ),
    "delights_hero_narrative": (
        "2-3 sentences on what users value most and where delight is strongest."
    ),
    "recommendations_hero_narrative": (
        "2-3 sentences on workload shape and practical prioritization."
    ),
}


def _json_hash(payload: dict[str, Any]) -> str:
    normalized = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def _to_date(dt: datetime) -> date:
    return dt.date()


def _get_app_name(app_id: str) -> Optional[str]:
    with pooled_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT display_name FROM apps WHERE app_id = %s LIMIT 1", (app_id,))
            row = cur.fetchone()
            if row:
                return row[0]
    return None


def _fetch_metrics_bundle(app_id: str, window_start: date, window_end: date) -> dict[str, Any]:
    bundle: dict[str, Any] = {
        "window_start": window_start.isoformat(),
        "window_end": window_end.isoformat(),
    }
    with pooled_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Sentiment summary.
            cur.execute(
                """
                SELECT
                  COUNT(*) AS total_reviews,
                  SUM(CASE WHEN LOWER(COALESCE(p.latest_analysis -> 'sentiment' -> 'overall' ->> 'classification', '')) = 'positive' THEN 1 ELSE 0 END) AS promoters,
                  SUM(CASE WHEN LOWER(COALESCE(p.latest_analysis -> 'sentiment' -> 'overall' ->> 'classification', '')) = 'negative' THEN 1 ELSE 0 END) AS detractors,
                  SUM(CASE WHEN LOWER(COALESCE(p.latest_analysis -> 'sentiment' -> 'overall' ->> 'classification', '')) = 'neutral' THEN 1 ELSE 0 END) AS neutral
                FROM processed_app_reviews p
                WHERE p.app_id = %s
                  AND DATE(p.review_created_at) BETWEEN %s AND %s
                """,
                (app_id, window_start, window_end),
            )
            s = cur.fetchone() or {}
            total_reviews = int(s.get("total_reviews") or 0)
            promoters = int(s.get("promoters") or 0)
            detractors = int(s.get("detractors") or 0)
            bundle["sentiment"] = {
                "total_reviews": total_reviews,
                "promoters": promoters,
                "detractors": detractors,
                "neutral": int(s.get("neutral") or 0),
                "positive_rate_pct": round((promoters / total_reviews) * 100, 2) if total_reviews else 0.0,
            }

            # Issues summary.
            cur.execute(
                """
                SELECT
                  COUNT(*) AS total_issue_mentions,
                  SUM(CASE WHEN LOWER(COALESCE(v.severity, '')) = 'critical' THEN 1 ELSE 0 END) AS critical_count,
                  SUM(CASE WHEN LOWER(COALESCE(v.severity, '')) = 'high' THEN 1 ELSE 0 END) AS high_count
                FROM vw_flattened_issues v
                JOIN processed_app_reviews p ON p.review_id = v.review_id AND p.app_id = %s
                WHERE DATE(p.review_created_at) BETWEEN %s AND %s
                  AND v."desc" IS NOT NULL
                  AND NULLIF(TRIM(v."desc"), '') IS NOT NULL
                """,
                (app_id, window_start, window_end),
            )
            i = cur.fetchone() or {}
            bundle["issues"] = {
                "total_issue_mentions": int(i.get("total_issue_mentions") or 0),
                "critical_count": int(i.get("critical_count") or 0),
                "high_count": int(i.get("high_count") or 0),
            }

            # Delights summary.
            cur.execute(
                """
                SELECT
                  COUNT(*) AS total_delight_mentions,
                  SUM(CASE WHEN COALESCE(NULLIF(pm ->> 'impact_score', ''), '0')::numeric > 70 THEN 1 ELSE 0 END) AS high_impact_count
                FROM processed_app_reviews pr
                CROSS JOIN LATERAL jsonb_array_elements(COALESCE(pr.latest_analysis -> 'positive_feedback' -> 'positive_mentions', '[]'::jsonb)) pm
                WHERE pr.app_id = %s
                  AND DATE(pr.review_created_at) BETWEEN %s AND %s
                """,
                (app_id, window_start, window_end),
            )
            d = cur.fetchone() or {}
            bundle["delights"] = {
                "total_delight_mentions": int(d.get("total_delight_mentions") or 0),
                "high_impact_count": int(d.get("high_impact_count") or 0),
            }

            # Recommendations summary.
            cur.execute(
                """
                SELECT
                  COUNT(DISTINCT v."desc") AS total_actions,
                  COUNT(DISTINCT CASE
                    WHEN LOWER(COALESCE(v.estimated_effort, '')) = 'low'
                     AND LOWER(COALESCE(v.suggested_timeline, '')) = 'short-term'
                    THEN v."desc" END) AS quick_wins,
                  COUNT(DISTINCT CASE
                    WHEN LOWER(COALESCE(v.estimated_effort, '')) IN ('high', 'medium')
                     AND LOWER(COALESCE(v.suggested_timeline, '')) IN ('medium-term', 'long-term')
                    THEN v."desc" END) AS must_do
                FROM vw_flattened_actions v
                JOIN processed_app_reviews p ON p.review_id = v.review_id AND p.app_id = %s
                WHERE DATE(p.review_created_at) BETWEEN %s AND %s
                """,
                (app_id, window_start, window_end),
            )
            a = cur.fetchone() or {}
            bundle["recommendations"] = {
                "total_actions": int(a.get("total_actions") or 0),
                "quick_wins": int(a.get("quick_wins") or 0),
                "must_do": int(a.get("must_do") or 0),
            }
    return bundle


def _metrics_for_slot(slot_key: SlotKey, bundle: dict[str, Any], app_name: Optional[str]) -> dict[str, Any]:
    payload = {
        "app_name": app_name or "",
        "window_start": bundle["window_start"],
        "window_end": bundle["window_end"],
    }
    if slot_key == "overview_kpi_positive_rate_footer":
        payload.update(bundle["sentiment"])
    elif slot_key == "overview_kpi_critical_issues_footer":
        payload.update(bundle["issues"])
    elif slot_key == "overview_kpi_delight_mentions_footer":
        payload.update(bundle["delights"])
    elif slot_key == "overview_kpi_recommendations_footer":
        payload.update(bundle["recommendations"])
    elif slot_key in {"overview_exec_summary", "sentiment_hero_narrative"}:
        payload.update(bundle["sentiment"])
        payload.update(bundle["issues"])
        payload.update(bundle["recommendations"])
    elif slot_key == "issues_hero_narrative":
        payload.update(bundle["issues"])
    elif slot_key == "delights_hero_narrative":
        payload.update(bundle["delights"])
    elif slot_key == "recommendations_hero_narrative":
        payload.update(bundle["recommendations"])
    return payload


def build_jobs_node(state: dict[str, Any]) -> dict[str, Any]:
    app_id = state["app_id"]
    app_name = _get_app_name(app_id)
    selected_ranges = state.get("requested_time_ranges") or DEFAULT_TIME_RANGES
    selected_slots = state.get("requested_slot_keys") or DEFAULT_SLOT_KEYS

    jobs: list[dict[str, Any]] = []
    for tr in selected_ranges:
        start_dt, end_dt = get_date_range(TimeRange(tr))
        window_start = _to_date(start_dt)
        window_end = _to_date(end_dt)
        for slot in selected_slots:
            spec = SLOT_SPECS[slot]
            jobs.append(
                {
                    "app_id": app_id,
                    "app_name": app_name,
                    "slot_key": slot,
                    "time_range_preset": tr,
                    "window_start": window_start,
                    "window_end": window_end,
                    "max_chars": int(spec["max_chars"]),
                    "instructions": str(spec["instructions"]),
                }
            )

    return {
        "app_name": app_name,
        "jobs": jobs,
        "job_index": 0,
        "results": [],
        "metric_cache": {},
        "error": None,
    }


def load_current_job_node(state: dict[str, Any]) -> dict[str, Any]:
    idx = state.get("job_index", 0)
    jobs = state.get("jobs", [])
    if idx >= len(jobs):
        return {"current_job": {}}
    return {
        "current_job": jobs[idx],
        "existing_snapshot": None,
        "llm_output": None,
        "inserted_snapshot_id": None,
        "error": None,
    }


def check_snapshot_node(state: dict[str, Any]) -> dict[str, Any]:
    job = state["current_job"]
    with pooled_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT id, commentary_text, source_metrics_json, generated_at, model_id, prompt_version, max_chars
                FROM analytics_commentary_snapshots
                WHERE app_id = %s
                  AND slot_key = %s
                  AND time_range_preset = %s
                  AND window_start = %s
                  AND window_end = %s
                ORDER BY generated_at DESC
                LIMIT 1
                """,
                (
                    job["app_id"],
                    job["slot_key"],
                    job["time_range_preset"],
                    job["window_start"],
                    job["window_end"],
                ),
            )
            row = cur.fetchone()
    return {"existing_snapshot": dict(row) if row else None}


def collect_metrics_node(state: dict[str, Any]) -> dict[str, Any]:
    job = state["current_job"]
    cache = state.get("metric_cache", {})
    cache_key = (
        job["app_id"],
        job["time_range_preset"],
        job["window_start"].isoformat(),
        job["window_end"].isoformat(),
    )
    if cache_key not in cache:
        cache[cache_key] = _fetch_metrics_bundle(
            app_id=job["app_id"],
            window_start=job["window_start"],
            window_end=job["window_end"],
        )
    bundle = cache[cache_key]
    source_metrics_json = _metrics_for_slot(job["slot_key"], bundle, job.get("app_name"))
    return {
        "metric_cache": cache,
        "source_metrics_json": source_metrics_json,
        "source_hash": _json_hash(source_metrics_json),
    }


def decide_generate_node(state: dict[str, Any]) -> dict[str, Any]:
    if state.get("force"):
        return {"should_generate": True, "decision_reason": "forced"}

    existing = state.get("existing_snapshot")
    if not existing:
        return {"should_generate": True, "decision_reason": "missing_snapshot"}

    existing_metrics = existing.get("source_metrics_json") or {}
    existing_hash = _json_hash(existing_metrics)
    if existing_hash != state["source_hash"]:
        return {"should_generate": True, "decision_reason": "metrics_changed"}

    return {"should_generate": False, "decision_reason": "same_metrics_same_window"}


async def generate_commentary_node(state: dict[str, Any]) -> dict[str, Any]:
    job = state["current_job"]
    metrics_json = state["source_metrics_json"]
    max_chars = int(job["max_chars"])
    slot_key = job["slot_key"]
    style_hint = SLOT_STYLE_HINTS.get(slot_key, "")
    is_footer_slot = str(slot_key).endswith("_footer")
    prompt = (
        "You are a senior CX analytics writer. Produce concise, trustworthy commentary for a dashboard.\n\n"
        "NON-NEGOTIABLE RULES:\n"
        "1) Use only the metrics provided; do not fabricate numbers or trends.\n"
        "2) If evidence is weak/zero, explicitly say data is limited.\n"
        "3) Keep neutral professional tone; avoid hype, absolutes, and speculation.\n"
        "4) Do not use markdown headings, bullets, or emojis.\n"
        f"5) Output must be <= {max_chars} characters.\n"
        "6) Return JSON only matching schema: {\"commentary_text\": \"...\", \"char_count\": <int|null>}.\n\n"
        "CONTEXT:\n"
        f"- Slot key: {slot_key}\n"
        f"- Slot objective: {job['instructions']}\n"
        f"- Style hint: {style_hint}\n"
        f"- Time range preset: {job['time_range_preset']}\n"
        f"- Window: {job['window_start']} to {job['window_end']}\n"
        f"- App: {job.get('app_name') or job['app_id']}\n\n"
        f"FORMAT REQUIREMENT: {'Single sentence preferred.' if is_footer_slot else '2-4 sentences, compact narrative.'}\n\n"
        f"METRICS JSON:\n{json.dumps(metrics_json, sort_keys=True)}"
    )

    try:
        out = await call_llm_api_async(
            messages=[{"role": "user", "content": prompt}],
            response_format=CommentaryLLMOutput,
            temperature=0.2,
        )
        text = (out.commentary_text or "").strip()
        if len(text) > max_chars:
            text = text[:max_chars].rstrip()
        if not text:
            return {"error": "llm_empty_output", "llm_output": None}
        return {
            "llm_output": {
                "commentary_text": text,
                "char_count": len(text),
            },
            "error": None,
        }
    except Exception as e:
        logger.error("Commentary LLM generation failed: %s", e, exc_info=True)
        return {"error": f"llm_error:{e}", "llm_output": None}


def persist_snapshot_node(state: dict[str, Any]) -> dict[str, Any]:
    if state.get("dry_run"):
        return {"inserted_snapshot_id": None}

    job = state["current_job"]
    llm_output = state["llm_output"] or {}
    with pooled_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO analytics_commentary_snapshots
                    (app_id, slot_key, time_range_preset, window_start, window_end,
                     commentary_text, max_chars, source_metrics_json, model_id, prompt_version)
                VALUES
                    (%s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s)
                RETURNING id
                """,
                (
                    job["app_id"],
                    job["slot_key"],
                    job["time_range_preset"],
                    job["window_start"],
                    job["window_end"],
                    llm_output["commentary_text"],
                    job["max_chars"],
                    json.dumps(state["source_metrics_json"]),
                    None,
                    "commentary_v1",
                ),
            )
            snapshot_id = cur.fetchone()[0]
        conn.commit()
    return {"inserted_snapshot_id": snapshot_id}


def record_result_node(state: dict[str, Any]) -> dict[str, Any]:
    job = state["current_job"]
    action = "skipped"
    reason = state.get("decision_reason")
    if state.get("error"):
        action = "error"
        reason = state["error"]
    elif state.get("should_generate"):
        action = "generated"
        if state.get("dry_run"):
            reason = "dry_run_generate"
        else:
            reason = reason or "generated"

    result = {
        "app_id": job["app_id"],
        "slot_key": job["slot_key"],
        "time_range_preset": job["time_range_preset"],
        "window_start": job["window_start"],
        "window_end": job["window_end"],
        "action": action,
        "reason": reason,
        "snapshot_id": state.get("inserted_snapshot_id"),
    }
    return {"results": [*state.get("results", []), result]}


def advance_job_node(state: dict[str, Any]) -> dict[str, Any]:
    return {
        "job_index": int(state.get("job_index", 0)) + 1,
        "current_job": {},
        "existing_snapshot": None,
        "source_metrics_json": {},
        "source_hash": "",
        "should_generate": False,
        "llm_output": None,
        "inserted_snapshot_id": None,
        "error": None,
        "decision_reason": None,
    }


def route_has_jobs(state: dict[str, Any]) -> str:
    return "done" if not state.get("jobs") else "next"


def route_after_load_job(state: dict[str, Any]) -> str:
    idx = int(state.get("job_index", 0))
    return "done" if idx >= len(state.get("jobs", [])) else "next"


def route_should_generate(state: dict[str, Any]) -> str:
    return "generate" if bool(state.get("should_generate")) else "skip"


def route_after_generate(state: dict[str, Any]) -> str:
    return "error" if state.get("error") else "persist"
