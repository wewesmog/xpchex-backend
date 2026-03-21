from typing import Optional, TypedDict


class CxAgentState(TypedDict):
    # ── Input (set at graph entry) ─────────────────────────────────────────────
    review_id:         str
    app_id:            str
    review_text:       Optional[str]
    review_score:      Optional[float]
    review_created_at: Optional[str]

    # ── Classification (also at entry after canonicalization) ─────────────────────────────────────────────────────────
    topic_tags:        list[str]
    sentiment:         Optional[str]

    # ── Retrieval ──────────────────────────────────────────────────────────────
    review_embedding:  Optional[list[float]]   # cached — computed also during upserting, once, reused
    retrieved_chunks:  list[dict]              # [{source, text, similarity, score, recency_weight, final_score, ...}]
    context_is_stale:  bool

    # ── Draft ──────────────────────────────────────────────────────────────────
    draft_text:        Optional[str]
    # Indices into the context blocks inside `draft_response` prompt (1-based).
    # Used for grounding / showing which sources were relied on.
    sources_used:      Optional[list[int]]

    # ── Self-evaluation ────────────────────────────────────────────────────────
    eval_accuracy:     Optional[int]
    eval_tone:         Optional[int]
    eval_completeness: Optional[int]
    agent_confidence:  float
    should_escalate:   bool
    escalation_reason: Optional[str]

    # ── Output IDs ─────────────────────────────────────────────────────────────
    draft_id:          Optional[int]
    escalation_id:     Optional[int]
    node_history_id:   Optional[int]