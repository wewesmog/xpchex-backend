from langgraph.graph import StateGraph, END
from .state import CxAgentState
from .nodes import (
    classify_review, retrieve_context, score_and_rerank,
    check_staleness, retrieve_fresh_only, draft_response,
    self_evaluate, save_draft, escalate_node,
)

from .nodes import write_node_history

def _route_staleness(state: CxAgentState) -> str:
    return "retrieve_fresh_only" if state["context_is_stale"] else "draft_response"

def _route_escalation(state: CxAgentState) -> str:
    return "escalate_node" if state["should_escalate"] else "save_draft"

def _route_after_draft(state: CxAgentState) -> str:
    # If a previous stage already decided to escalate (e.g. no context),
    # skip self-evaluation and go straight to escalation.
    return "escalate_node" if state.get("should_escalate") else "self_evaluate"

builder = StateGraph(CxAgentState)

#builder.add_node("classify_review",     classify_review)
builder.add_node("retrieve_context",    retrieve_context)
builder.add_node("score_and_rerank",    score_and_rerank)
builder.add_node("check_staleness",     check_staleness)
builder.add_node("retrieve_fresh_only", retrieve_fresh_only)
builder.add_node("draft_response",      draft_response)
builder.add_node("self_evaluate",       self_evaluate)
builder.add_node("save_draft",          save_draft)
builder.add_node("escalate_node",       escalate_node)
builder.add_node("write_node_history", write_node_history)

builder.set_entry_point("retrieve_context")
#builder.add_edge("classify_review",  "retrieve_context")
builder.add_edge("retrieve_context", "score_and_rerank")
builder.add_edge("score_and_rerank", "check_staleness")

builder.add_conditional_edges("check_staleness", _route_staleness, {
    "retrieve_fresh_only": "retrieve_fresh_only",
    "draft_response":      "draft_response",
})

builder.add_edge("retrieve_fresh_only", "draft_response")
builder.add_conditional_edges("draft_response", _route_after_draft, {
    "escalate_node": "escalate_node",
    "self_evaluate": "self_evaluate",
})

builder.add_conditional_edges("self_evaluate", _route_escalation, {
    "escalate_node": "escalate_node",
    "save_draft":    "save_draft",
})

builder.add_edge("save_draft", "write_node_history")
builder.add_edge("escalate_node", "write_node_history")
builder.add_edge("write_node_history", END)

cx_agent_graph = builder.compile()