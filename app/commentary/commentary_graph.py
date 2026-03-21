from __future__ import annotations

from langgraph.graph import END, StateGraph

from app.models.commentary_models import CommentaryGraphState

from .commentary_nodes import (
    advance_job_node,
    build_jobs_node,
    check_snapshot_node,
    collect_metrics_node,
    decide_generate_node,
    generate_commentary_node,
    load_current_job_node,
    persist_snapshot_node,
    record_result_node,
    route_after_generate,
    route_after_load_job,
    route_has_jobs,
    route_should_generate,
)


builder = StateGraph(CommentaryGraphState)

builder.add_node("build_jobs", build_jobs_node)
builder.add_node("load_current_job", load_current_job_node)
builder.add_node("check_snapshot", check_snapshot_node)
builder.add_node("collect_metrics", collect_metrics_node)
builder.add_node("decide_generate", decide_generate_node)
builder.add_node("generate_commentary", generate_commentary_node)
builder.add_node("persist_snapshot", persist_snapshot_node)
builder.add_node("record_result", record_result_node)
builder.add_node("advance_job", advance_job_node)

builder.set_entry_point("build_jobs")
builder.add_conditional_edges(
    "build_jobs",
    route_has_jobs,
    {
        "next": "load_current_job",
        "done": END,
    },
)
builder.add_conditional_edges(
    "load_current_job",
    route_after_load_job,
    {
        "next": "check_snapshot",
        "done": END,
    },
)
builder.add_edge("check_snapshot", "collect_metrics")
builder.add_edge("collect_metrics", "decide_generate")
builder.add_conditional_edges(
    "decide_generate",
    route_should_generate,
    {
        "generate": "generate_commentary",
        "skip": "record_result",
    },
)
builder.add_conditional_edges(
    "generate_commentary",
    route_after_generate,
    {
        "persist": "persist_snapshot",
        "error": "record_result",
    },
)
builder.add_edge("persist_snapshot", "record_result")
builder.add_edge("record_result", "advance_job")
builder.add_edge("advance_job", "load_current_job")

commentary_graph = builder.compile()
