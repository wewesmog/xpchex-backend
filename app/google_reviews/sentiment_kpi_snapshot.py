"""
Compute sentiment KPIs (same as frontend) and build an LLM prompt for commentary.
Use from a midnight job: run calculate_kpi_snapshot → pass to your LLM → store response.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any

from app.shared_services.date_ranges import TimeRange, get_date_range

logger = logging.getLogger(__name__)


def _aggregate_rows(rows: list[dict]) -> dict[str, Any]:
    """Sum period-level fields into one metrics dict (mirrors frontend calculateSentimentMetrics)."""
    total_reviews = 0
    sentiment_promoters = 0
    sentiment_detractors = 0
    sentiment_neutrals = 0
    sentiment_mixed = 0
    for r in rows:
        total_reviews += int(r.get("total_reviews") or 0)
        sentiment_promoters += int(r.get("sentiment_promoters") or 0)
        sentiment_detractors += int(r.get("sentiment_detractors") or 0)
        sentiment_neutrals += int(r.get("sentiment_neutrals") or 0)
        sentiment_mixed += int(r.get("sentiment_mixed") or 0)
    decided = sentiment_promoters + sentiment_detractors
    if decided > 0:
        sentiment_nps = round((sentiment_promoters - sentiment_detractors) / decided * 1000) / 10
    else:
        sentiment_nps = 0.0
    return {
        "total_reviews": total_reviews,
        "sentiment_promoters": sentiment_promoters,
        "sentiment_detractors": sentiment_detractors,
        "sentiment_nps": sentiment_nps,
    }


def _compute_trends(rows: list[dict]) -> dict[str, Any]:
    """First half vs second half (mirrors frontend calculateSentimentTrends). Returns {} if < 2 rows."""
    if not rows or len(rows) < 2:
        return {}
    mid = len(rows) // 2
    prev = _aggregate_rows(rows[:mid])
    curr = _aggregate_rows(rows[mid:])
    trends = {}

    def direction(c: int | float, p: int | float) -> str | None:
        if c > p:
            return "up"
        if c < p:
            return "down"
        return None

    def pct_change(c: int, p: int) -> int | None:
        if p == 0:
            return None
        return round((c - p) / p * 100)

    trends["total_reviews"] = {
        "direction": direction(curr["total_reviews"], prev["total_reviews"]),
        "pct_change": pct_change(curr["total_reviews"], prev["total_reviews"]),
    }
    trends["sentiment_promoters"] = {
        "direction": direction(curr["sentiment_promoters"], prev["sentiment_promoters"]),
        "pct_change": pct_change(curr["sentiment_promoters"], prev["sentiment_promoters"]),
    }
    trends["sentiment_detractors"] = {
        "direction": direction(curr["sentiment_detractors"], prev["sentiment_detractors"]),
        "pct_change": pct_change(curr["sentiment_detractors"], prev["sentiment_detractors"]),
    }
    nps_diff = round(curr["sentiment_nps"] - prev["sentiment_nps"])
    trends["sentiment_nps"] = {
        "direction": direction(curr["sentiment_nps"], prev["sentiment_nps"]),
        "pts_change": nps_diff if abs(nps_diff) >= 1 else None,
    }
    return trends


def _aggregate_topics(topic_rows: list[dict], top_n: int = 10) -> list[dict]:
    """Group by topic_name (case-insensitive), count, take majority sentiment. Return top N."""
    by_name: dict[str, list[str]] = {}
    for t in topic_rows:
        name = (t.get("topic_name") or "").strip()
        if not name:
            continue
        key = name.lower()
        if key not in by_name:
            by_name[key] = []
        sent = (t.get("topic_sentiment") or "neutral").lower()
        by_name[key].append(sent)
    # Majority sentiment per topic
    out = []
    for key, sents in by_name.items():
        count = len(sents)
        positive = sum(1 for s in sents if s == "positive")
        negative = sum(1 for s in sents if s == "negative")
        if positive >= negative and positive > 0:
            sentiment = "positive"
        elif negative > positive:
            sentiment = "negative"
        else:
            sentiment = "neutral"
        display_name = key[:1].upper() + key[1:] if key else ""
        out.append({"name": display_name, "count": count, "sentiment": sentiment})
    out.sort(key=lambda x: -x["count"])
    return out[:top_n]


async def calculate_kpi_snapshot(app_id: str, time_range: TimeRange) -> dict[str, Any]:
    """
    Compute the same KPIs the frontend shows for the given app and time range.
    Uses the sentiments router's aggregated data and topics data; no extra DB schema.
    """
    from app.routers.sentiments_router import (
        _get_aggregated_sentiments_data,
        _get_granularity_for_range,
        _get_topics_data,
    )

    start_date, end_date = get_date_range(time_range)
    granularity = _get_granularity_for_range(time_range)
    granularity_str = granularity.value if hasattr(granularity, "value") else str(granularity)

    sentiments_data = await _get_aggregated_sentiments_data(
        app_id, start_date, end_date, granularity_str, None, None
    )
    if not sentiments_data:
        sentiments_data = []

    topics_data = await _get_topics_data(app_id, start_date, end_date)
    if not topics_data:
        topics_data = []

    metrics = _aggregate_rows(sentiments_data)
    trends = _compute_trends(sentiments_data)
    top_topics = _aggregate_topics(topics_data, top_n=10)

    period_label = f"{start_date.strftime('%Y-%m-%d')} – {end_date.strftime('%Y-%m-%d')}"

    return {
        "app_id": app_id,
        "time_range": time_range.value,
        "date_range": {
            "start": start_date.strftime("%Y-%m-%d"),
            "end": end_date.strftime("%Y-%m-%d"),
        },
        "period_label": period_label,
        "total_reviews": metrics["total_reviews"],
        "sentiment_promoters": metrics["sentiment_promoters"],
        "sentiment_detractors": metrics["sentiment_detractors"],
        "sentiment_nps": metrics["sentiment_nps"],
        "trends": trends,
        "top_topics": top_topics,
    }


def build_llm_prompt(snapshot: dict[str, Any], app_display_name: str | None = None) -> str:
    """
    Format the KPI snapshot into a prompt string ready to send to your LLM.
    You can pass the returned string as the user message; then parse the LLM response
    and store it (e.g. in sentiment_ai_commentary).
    """
    app_id = snapshot.get("app_id", "this app")
    period_label = snapshot.get("period_label", "")
    total = snapshot.get("total_reviews", 0)
    promoters = snapshot.get("sentiment_promoters", 0)
    detractors = snapshot.get("sentiment_detractors", 0)
    nps = snapshot.get("sentiment_nps", 0)
    nps_str = f"+{int(nps)}" if nps >= 0 else str(int(nps))

    lines = [
        "You are a product analytics expert. Write concise commentary for a mobile app sentiment dashboard.",
        "",
        f"## App: {app_display_name or app_id}  |  Period: {period_label}",
        "",
        "### Overall KPIs",
        f"- Total reviews: {total:,}",
        f"- Promoters (positive sentiment): {promoters:,}",
        f"- Detractors (negative sentiment): {detractors:,}",
        f"- Sentiment NPS: {nps_str}",
        "",
    ]

    trends = snapshot.get("trends") or {}
    if trends:
        lines.append("### Trend (first half vs second half of period)")
        for key, label in [
            ("total_reviews", "Reviews"),
            ("sentiment_promoters", "Promoters"),
            ("sentiment_detractors", "Detractors"),
            ("sentiment_nps", "NPS"),
        ]:
            t = trends.get(key, {})
            direction = t.get("direction")
            pct = t.get("pct_change")
            pts = t.get("pts_change")
            if direction is None and pct is None and pts is None:
                continue
            if key == "sentiment_nps" and pts is not None:
                lines.append(f"- {label}: {pts:+d} pts ({'improving' if direction == 'up' else 'declining' if direction == 'down' else 'flat'})")
            elif pct is not None:
                lines.append(f"- {label}: {pct:+d}% ({'growing' if direction == 'up' else 'declining' if direction == 'down' else 'flat'})")
        lines.append("")

    top_topics = snapshot.get("top_topics") or []
    if top_topics:
        lines.append("### Most discussed topics")
        for i, topic in enumerate(top_topics[:10], 1):
            name = topic.get("name", "?")
            count = topic.get("count", 0)
            sentiment = topic.get("sentiment", "neutral")
            lines.append(f"{i}. {name} ({count} mentions) — {sentiment} sentiment")
        lines.append("")

    lines.extend([
        "### What to write",
        "Return a JSON object with these exact keys (short, one or two sentences per value):",
        "- hero_narrative: 2–3 sentences summarising what users feel this period.",
        "- total_reviews_comment: one-liner insight for the \"Total Reviews\" card.",
        "- promoters_comment: one-liner for the \"Promoters\" card.",
        "- detractors_comment: one-liner for the \"Detractors\" card.",
        "- nps_comment: one-liner for the \"Sentiment NPS\" card.",
        "- trend_summary: one sentence on the overall trajectory.",
        "- top_insight: single most important takeaway.",
    ])
    return "\n".join(lines)


# Convenience: run from sync context (e.g. cron script)
def calculate_kpi_snapshot_sync(app_id: str, time_range: TimeRange) -> dict[str, Any]:
    """Sync wrapper: runs calculate_kpi_snapshot in a new event loop."""
    return asyncio.run(calculate_kpi_snapshot(app_id, time_range))


if __name__ == "__main__":
    import sys
    import json
    app = sys.argv[1] if len(sys.argv) > 1 else "com.example.app"
    tr = TimeRange.LAST_6_MONTHS
    if len(sys.argv) > 2:
        tr = TimeRange(sys.argv[2])
    snapshot = asyncio.run(calculate_kpi_snapshot(app, tr))
    print(json.dumps(snapshot, indent=2))
    print("\n--- LLM prompt ---\n")
    print(build_llm_prompt(snapshot, app_display_name=app))
