import asyncio
import json, re
from datetime import datetime, timezone
from openai import AsyncOpenAI
from pydantic import BaseModel
from typing import Optional

from .state import CxAgentState


import psycopg2
from psycopg2.extras import execute_values, RealDictCursor
from ..shared_services.db import pooled_connection
from ..shared_services.llm import embed_texts, call_llm_api_async

from app.shared_services.logger_setup import setup_logger

logger = setup_logger()

oai = AsyncOpenAI()

SENSITIVE_KEYWORDS = {
    "refund", "legal", "sue", "court", "fraud", "scam",
    "police", "hack", "stolen", "lawsuit", "complaint",
}
STALE_THRESHOLD_DAYS = 90
CONFIDENCE_THRESHOLD  = 0.6

# Must match DB CHECK escalations_reason_check (see AGENTIC_RAG_DESIGN.md §2.8)
ALLOWED_ESCALATION_REASONS = frozenset(
    {
        "low_confidence",
        "sensitive_topic",
        "no_context",
        "agent_unsure",
        "manual",
        "nelly_unresolved",
    }
)


def db_escalation_reason_and_message(state: CxAgentState) -> tuple[str, Optional[str]]:
    """
    Map graph state to (reason, agent_message) for `escalations`.

    `escalation_reason` may be a short code (from our code paths) or free text from the
    self-eval LLM; only the codes above are valid in column `reason`. Free text goes to
    `agent_message` when it is not itself a valid code.
    """
    raw = state.get("escalation_reason")
    raw_s = (raw or "").strip()

    if raw_s in ALLOWED_ESCALATION_REASONS:
        return raw_s, None

    # Legacy / alternate label used in nelly_main when no row is found
    if raw_s == "no_review_found":
        return "nelly_unresolved", None

    detail = raw_s if raw_s else None
    conf = float(state.get("agent_confidence") or 0.0)

    if detail is None:
        if conf < CONFIDENCE_THRESHOLD:
            return "low_confidence", None
        return "agent_unsure", None

    if conf < CONFIDENCE_THRESHOLD:
        return "low_confidence", detail
    return "agent_unsure", detail


# ── classify_review ────────────────────────────────────────────────────────────

async def classify_review(state: CxAgentState) -> dict:
    prompt = f"""Classify this app review.
Review: "{state['review_text']}"
Stars: {state['review_score']}

Return JSON only:
{{"sentiment": "positive|neutral|negative",
  "topic_tags": ["tag1", "tag2"],
  "urgency": "low|medium|high"}}

Valid topic tags: login_issue, payment_issue, app_crash, feature_request,
general_praise, ui_complaint, security_concern, account_issue, performance,
data_loss, slow_loading, transaction_failure."""

    resp = await oai.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": prompt}],
        response_format={"type": "json_object"},
    )
    data = json.loads(resp.choices[0].message.content)
    return {
        "sentiment":  data.get("sentiment", "neutral"),
        "topic_tags": data.get("topic_tags", []),
    }


# ── retrieve_context ───────────────────────────────────────────────────────────

def _retrieve_context_chunks(state: CxAgentState, review_emb) -> list:
    """DB-heavy work for retrieve_context (sync)."""
    chunks: list = []

    with pooled_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # ── response_history: semantic (review_embedding) ──────────────────────
            cur.execute(
                """
                SELECT id, response_text, score, recency_weight,
                       COALESCE(user_helpful_pct, -1) AS user_helpful_pct,
                       published_at,
                       1 - (review_embedding <=> %s::vector) AS similarity
                FROM response_history
                WHERE app_id = %s
                ORDER BY similarity DESC
                LIMIT 20
                """,
                (review_emb, state["app_id"]),
            )
            rows = cur.fetchall()
            for r in rows:
                uhp = r["user_helpful_pct"]
                user_boost = (0.8 + 0.4 * uhp) if uhp >= 0 else 1.0
                chunks.append({
                    "source": "response_history",
                    "chunk_id": r["id"],
                    "text": r["response_text"],
                    "similarity": float(r["similarity"]),
                    "score": r["score"] or 3,
                    "recency_weight": float(r["recency_weight"]),
                    "user_boost": user_boost,
                    "published_at": r["published_at"].isoformat() if r["published_at"] else None,
                })

            # ── response_history: keyword (hybrid search) ──────────────────────────
            keyword = state["review_text"][:200]
            cur.execute(
                """
                SELECT id, response_text, score, recency_weight,
                       COALESCE(user_helpful_pct, -1) AS user_helpful_pct,
                       published_at,
                       ts_rank(
                         to_tsvector('english', response_text),
                         plainto_tsquery('english', %s)
                       ) AS similarity
                FROM response_history
                WHERE app_id = %s
                  AND to_tsvector('english', response_text) @@ plainto_tsquery('english', %s)
                ORDER BY similarity DESC
                LIMIT 10
                """,
                (keyword, state["app_id"], keyword),
            )
            kw_rows = cur.fetchall()

            existing_ids = {c["chunk_id"] for c in chunks}
            for r in kw_rows:
                if r["id"] not in existing_ids:
                    uhp = r["user_helpful_pct"]
                    user_boost = (0.8 + 0.4 * uhp) if uhp >= 0 else 1.0
                    chunks.append({
                        "source": "response_history",
                        "chunk_id": r["id"],
                        "text": r["response_text"],
                        "similarity": float(r["similarity"]),
                        "score": r["score"] or 3,
                        "recency_weight": float(r["recency_weight"]),
                        "user_boost": user_boost,
                        "published_at": r["published_at"].isoformat() if r["published_at"] else None,
                    })

            # ── knowledge_chunks ────────────────────────────────────────────────────
            cur.execute(
                """
                SELECT kc.id, kc.chunk_text, kd.title AS doc_title,
                       1 - (kc.embedding <=> %s::vector) AS similarity
                FROM knowledge_chunks kc
                JOIN knowledge_documents kd ON kd.id = kc.document_id
                WHERE kd.app_id = %s AND kd.is_active = TRUE
                ORDER BY similarity DESC
                LIMIT 10
                """,
                (review_emb, state["app_id"]),
            )
            rows = cur.fetchall()
            for r in rows:
                chunks.append({
                    "source": "knowledge",
                    "chunk_id": r["id"],
                    "text": r["chunk_text"],
                    "document_title": r["doc_title"],
                    "similarity": float(r["similarity"]),
                    "score": 4,  # docs are always trusted
                    "recency_weight": 1.0,  # docs don't decay
                    "user_boost": 1.0,
                    "published_at": None,
                })

        # ── Nelly feedback insights (last 60 days) ────────────────────────────
        # These come directly from reviewers post-reply — highest real-world signal.
        # rows = await conn.fetch("""
        #     SELECT fi.id, fi.insight_text, fi.issue_resolved,
        #            fi.sentiment_shift, fi.topic_tags,
        #            1 - (fi.embedding <=> $1::vector) AS similarity
        #     FROM feedback_insights fi
        #     WHERE fi.app_id = $2
        #       AND fi.embedding IS NOT NULL
        #       AND fi.created_at > NOW() - INTERVAL '60 days'
        #     ORDER BY similarity DESC
        #     LIMIT 8
        # """, review_emb, state["app_id"])

        # for r in rows:
        #     resolved_note = ""
        #     if r["issue_resolved"] is True:
        #         resolved_note = " [user confirmed resolved]"
        #     elif r["issue_resolved"] is False:
        #         resolved_note = " [user said NOT resolved]"
        #     chunks.append({
        #         "source":        "nelly_feedback",
        #         "chunk_id":      r["id"],
        #         "text":          r["insight_text"] + resolved_note,
        #         "similarity":    float(r["similarity"]),
        #         "score":         5 if r["issue_resolved"] else 4,
        #         "recency_weight":1.0,  # decays via scheduler like response_history
        #         "user_boost":    1.2 if r["issue_resolved"] else 0.9,
        #         "published_at":  None,
        #         "sentiment_shift": r["sentiment_shift"],
        #     })

        # ── conversation_messages (last 30 days, human only, internal CX staff) ──
        # rows = await conn.fetch("""
        #     SELECT cm.id, cm.content,
        #            1 - (cm.embedding <=> $1::vector) AS similarity
        #     FROM conversation_messages cm
        #     JOIN conversation_threads ct ON ct.id = cm.thread_id
        #     WHERE ct.app_id = $2
        #       AND ct.thread_type IN ('review_chat', 'general', 'feedback')
        #       AND cm.role = 'human'
        #       AND cm.embedding IS NOT NULL
        #       AND cm.created_at > NOW() - INTERVAL '30 days'
        #     ORDER BY similarity DESC
        #     LIMIT 5
        # """, review_emb, state["app_id"])

        # for r in rows:
        #     chunks.append({
        #         "source":        "conversation",
        #         "chunk_id":      r["id"],
        #         "text":          r["content"],
        #         "similarity":    float(r["similarity"]),
        #         "score":         4,
        #         "recency_weight":1.0,
        #         "user_boost":    1.0,
        #         "published_at":  None,
        #     })

    return chunks


async def retrieve_context(state: CxAgentState) -> dict:
    """Multi-source retrieval. Embeds the review once and caches it in state."""
    # embed_texts is synchronous in `app.shared_services.llm`,
    # so we must not `await` it here.
    review_emb = embed_texts([state["review_text"]])[0]

    # pooled_connection() is synchronous; long remote DB sessions can drop mid-query.
    # Retry transient disconnects so the pool does not hand out a dead socket.
    _TRANSIENT = (psycopg2.OperationalError, psycopg2.InterfaceError)
    for attempt in range(3):
        try:
            chunks = _retrieve_context_chunks(state, review_emb)
            return {"review_embedding": review_emb, "retrieved_chunks": chunks}
        except _TRANSIENT as e:
            logger.warning(
                "retrieve_context: DB connection lost (attempt %s/3): %s",
                attempt + 1,
                e,
            )
            if attempt < 2:
                await asyncio.sleep(0.5 * (2**attempt))
            else:
                raise


# ── score_and_rerank ───────────────────────────────────────────────────────────

async def score_and_rerank(state: CxAgentState) -> dict:
    """final_score = similarity × (score/5) × recency_weight × user_boost"""
    for chunk in state["retrieved_chunks"]:
        chunk["final_score"] = (
            chunk["similarity"]
            * (chunk["score"] / 5.0)
            * chunk["recency_weight"]
            * chunk.get("user_boost", 1.0)
        )
    ranked = sorted(state["retrieved_chunks"], key=lambda c: c["final_score"], reverse=True)
    return {"retrieved_chunks": ranked[:10]}


# ── check_staleness ────────────────────────────────────────────────────────────

async def check_staleness(state: CxAgentState) -> dict:
    history = [c for c in state["retrieved_chunks"] if c["source"] == "response_history"]
    if not history:
        return {"context_is_stale": False}  # knowledge docs will cover it
    top = history[0]
    if not top.get("published_at"):
        return {"context_is_stale": False}
    age_days = (
        datetime.now(timezone.utc) - datetime.fromisoformat(top["published_at"])
    ).days
    return {"context_is_stale": age_days > STALE_THRESHOLD_DAYS and top["score"] < 3}


# ── retrieve_fresh_only ────────────────────────────────────────────────────────

async def retrieve_fresh_only(state: CxAgentState) -> dict:
    """Re-query response_history restricted to last 60 days. Reuse cached embedding."""
    review_emb = state["review_embedding"]  # cached — no extra API call
    non_history = [c for c in state["retrieved_chunks"] if c["source"] != "response_history"]

    with pooled_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT id, response_text, score, recency_weight,
                       COALESCE(user_helpful_pct, -1) AS user_helpful_pct,
                       published_at,
                       1 - (review_embedding <=> %s::vector) AS similarity
                FROM response_history
                WHERE app_id = %s
                  AND published_at > NOW() - INTERVAL '60 days'
                ORDER BY similarity DESC
                LIMIT 10
                """,
                (review_emb, state["app_id"]),
            )
            rows = cur.fetchall()

    fresh = []
    for r in rows:
        uhp = r["user_helpful_pct"]
        user_boost = (0.8 + 0.4 * uhp) if uhp >= 0 else 1.0
        chunk = {
            "source":        "response_history",
            "chunk_id":      r["id"],
            "text":          r["response_text"],
            "similarity":    float(r["similarity"]),
            "score":         r["score"] or 3,
            "recency_weight":float(r["recency_weight"]),
            "user_boost":    user_boost,
            "published_at":  r["published_at"].isoformat(),
        }
        chunk["final_score"] = (
            chunk["similarity"] * (chunk["score"] / 5.0)
            * chunk["recency_weight"] * user_boost
        )
        fresh.append(chunk)

    all_chunks = non_history + fresh
    ranked = sorted(all_chunks, key=lambda c: c["final_score"], reverse=True)

    # If still no usable context, flag for escalation
    if not ranked or ranked[0]["final_score"] < 0.3:
        return {
            "retrieved_chunks": ranked,
            "should_escalate":  True,
            "escalation_reason":"no_context",
        }
    return {"retrieved_chunks": ranked[:10]}


# ── draft_response ─────────────────────────────────────────────────────────────

async def draft_response(state: CxAgentState) -> dict:
    context_blocks = []
    for i, c in enumerate(state["retrieved_chunks"][:5], 1):
        label = c["source"].upper()
        if c["source"] == "knowledge":
            label = f"FAQ DOCUMENT  │ {c.get('document_title', 'doc')}"
        elif c["source"] == "conversation":
            label = "TEAM CHAT"
        elif c["source"] == "nelly_feedback":
            shift = c.get("sentiment_shift") or "unknown"
            label = f"REVIEWER FEEDBACK (Nelly)  │ sentiment_shift:{shift}"
        else:
            days_ago = ""
            if c.get("published_at"):
                age = (datetime.now(timezone.utc) - datetime.fromisoformat(c["published_at"])).days
                days_ago = f" │ {age} days ago"
            label = f"PAST RESPONSE  │ score:{c['score']}/5{days_ago}"

        # Include a stable reference (chunk_id / document_title) so the UI can show sources.
        ref = ""
        if c.get("source") == "knowledge":
            ref = f" │ document_title:{c.get('document_title')}"
        else:
            ref = f" │ chunk_id:{c.get('chunk_id')}"
        context_blocks.append(
            f"[{i}] {label}{ref} │ similarity:{c['similarity']:.2f}\n{c['text']}"
        )

    context_text = "\n\n".join(context_blocks)
    tags_str = ", ".join(state.get("topic_tags", []))

    prompt = f"""You are a professional CX agent for a mobile banking app.

TONE (must be consistent in every sentence):
- empathetic and reassuring
- calm, respectful, and professional
- no blame, no technical jargon, no competitors

REPLY STYLE:
- maximum 3 sentences total
- acknowledge the user’s experience in sentence 1
- give one or two actionable steps if applicable
- do not promise fix timelines or guarantee outcomes

REVIEW ({state.get('sentiment', 'unknown')}, {state['review_score']}★, topics: {tags_str}):
"{state['review_text']}"

CONTEXT (ranked by relevance — use these to inform your response):
{context_text}

---
Write a concise, empathetic, professional reply.
Rules:
- Acknowledge the user's experience directly.
- Give one or two actionable steps if applicable.
- Do not make promises about fix timelines.
- Do not mention competitor apps.
- Maximum 3 sentences.
Return JSON only with:
{{
  "reply_text": "string",
  "sources_used": [1-5]
}}

Notes:
- Only use information from the provided CONTEXT blocks.
- Pick indices that directly support the facts/actions in your reply.
- If you used none of the context blocks, set sources_used to an empty list.
"""

    class DraftWithSources(BaseModel):
        reply_text: str
        sources_used: list[int]

    data = await call_llm_api_async(
        model="gpt-4o",
        messages=[{"role": "user", "content": prompt}],
        response_format=DraftWithSources,
    )

    return {"draft_text": str(data.reply_text).strip(), "sources_used": data.sources_used}


# ── self_evaluate ──────────────────────────────────────────────────────────────

async def self_evaluate(state: CxAgentState) -> dict:
    # Hard-check for sensitive keywords before calling LLM
    lower = state["review_text"].lower()
    if any(kw in lower for kw in SENSITIVE_KEYWORDS):
        return {
            "eval_accuracy":    None,
            "eval_tone":        None,
            "eval_completeness":None,
            "agent_confidence": 0.2,
            "should_escalate":  True,
            "escalation_reason":"sensitive_topic",
        }

    class SelfEvaluationResponse(BaseModel):
        accuracy: int
        tone: int
        completeness: int
        confidence: float
        escalate: bool
        reason: Optional[str]

    prompt = f"""Review: "{state['review_text']}"
Draft: "{state['draft_text']}"

Evaluate the draft using the SAME TONE rules you were given:
- empathetic and reassuring
- calm, respectful, professional
- max 3 sentences
- one or two actionable steps when applicable
- no promises about fix timelines
- do not mention competitor apps
- MUST be grounded in the provided CONTEXT blocks (do not introduce new facts)

Rate the draft. Return JSON only:
{{
  "accuracy": 1-5,
  "tone": 1-5,
  "completeness": 1-5,
  "confidence": 0.0-1.0,
  "escalate": true|false,
  "reason": "brief reason if escalating, else null"
}}"""

    data = await call_llm_api_async(
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": prompt}],
        response_format=SelfEvaluationResponse,
    )
    confidence = float(getattr(data, "confidence", 1.0))

    return {
        "eval_accuracy":    getattr(data, "accuracy", None),
        "eval_tone":        getattr(data, "tone", None),
        "eval_completeness":getattr(data, "completeness", None),
        "agent_confidence": confidence,
        "should_escalate":  getattr(data, "escalate", False) or confidence < CONFIDENCE_THRESHOLD,
        "escalation_reason":getattr(data, "reason", None),
    }


# ── save_draft ─────────────────────────────────────────────────────────────────

async def save_draft(state: CxAgentState) -> dict:
    snapshot = json.dumps({
        "confidence": state["agent_confidence"],
        "eval": {
            "accuracy":     state["eval_accuracy"],
            "tone":         state["eval_tone"],
            "completeness": state["eval_completeness"],
        },
        "chunks": state["retrieved_chunks"][:5],
    })

    with pooled_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO response_drafts
                    (review_id, app_id, draft_text, source, status, context_snapshot, created_by)
                VALUES (%s, %s, %s, 'agent', 'draft', %s::jsonb, 'agent')
                RETURNING id
                """,
                (state["review_id"], state["app_id"], state["draft_text"], snapshot),
            )
            draft_id = cur.fetchone()[0]
        conn.commit()

    return {"draft_id": draft_id}


# ── escalate_node ──────────────────────────────────────────────────────────────

async def escalate_node(state: CxAgentState) -> dict:
    reason_code, agent_message = db_escalation_reason_and_message(state)
    snapshot = json.dumps({
        "confidence":  state["agent_confidence"],
        "reason":      state.get("escalation_reason"),
        "reason_code": reason_code,
        "chunks":      state["retrieved_chunks"][:3],
    })

    with pooled_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO response_drafts
                    (review_id, app_id, draft_text, source, status, context_snapshot, created_by)
                VALUES (%s, %s, %s, 'agent', 'escalated', %s::jsonb, 'agent')
                RETURNING id
                """,
                (
                    state["review_id"],
                    state["app_id"],
                    state.get("draft_text") or "",
                    snapshot,
                ),
            )
            draft_id = cur.fetchone()[0]

            cur.execute(
                """
                INSERT INTO escalations
                    (review_id, app_id, draft_id, reason, confidence_score, status, agent_message)
                VALUES (%s, %s, %s, %s, %s, 'open', %s)
                RETURNING id
                """,
                (
                    state["review_id"],
                    state["app_id"],
                    draft_id,
                    reason_code,
                    state["agent_confidence"],
                    agent_message,
                ),
            )
            escalation_id = cur.fetchone()[0]
        conn.commit()

    return {"draft_id": draft_id, "escalation_id": escalation_id}


# ── write_node_history ─────────────────────────────────────────────────────

async def write_node_history(state: CxAgentState) -> dict:
    """
    Persist a snapshot of the final agent run for debugging/auditing.
    This is intentionally a *small* snapshot (top chunks + key fields).
    """
    should_escalate = bool(state.get("should_escalate"))
    run_status = "escalated" if should_escalate else "drafted"
    failure_reason = state.get("escalation_reason") if should_escalate else None

    # Keep snapshot small to avoid huge JSON rows.
    snapshot = json.dumps(
        {
            "review_id": state.get("review_id"),
            "app_id": state.get("app_id"),
            "context_is_stale": state.get("context_is_stale"),
            "should_escalate": state.get("should_escalate"),
            "escalation_reason": state.get("escalation_reason"),
            "agent_confidence": state.get("agent_confidence"),
            "sources_used": state.get("sources_used"),
            "eval": {
                "accuracy": state.get("eval_accuracy"),
                "tone": state.get("eval_tone"),
                "completeness": state.get("eval_completeness"),
            },
            "draft_text_present": bool(state.get("draft_text")),
            "draft_text": (state.get("draft_text") or "")[:2000] if state.get("draft_text") else None,
            "draft_id": state.get("draft_id"),
            "escalation_id": state.get("escalation_id"),
            "retrieved_chunks": (state.get("retrieved_chunks") or [])[:10],
        }
    )

    with pooled_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO node_history
                    (review_id, app_id, agent_name, run_status, failure_reason, should_escalate, context_is_stale, draft_id, escalation_id, snapshot)
                VALUES
                    (%s, %s, 'nelly', %s, %s, %s, %s, %s, %s, %s::jsonb)
                RETURNING id
                """,
                (
                    state.get("review_id"),
                    state.get("app_id"),
                    run_status,
                    failure_reason,
                    should_escalate,
                    bool(state.get("context_is_stale")),
                    state.get("draft_id"),
                    state.get("escalation_id"),
                    snapshot,
                ),
            )
            node_history_id = cur.fetchone()[0]
        conn.commit()

    return {"node_history_id": node_history_id}


# ── Helpers ────────────────────────────────────────────────────────────────────

async def _embed(text: str) -> list[float]:
    resp = await oai.embeddings.create(
        model="text-embedding-3-small",
        input=text[:8000],  # model token limit guard
    )
    return resp.data[0].embedding