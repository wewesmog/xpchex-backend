from __future__ import annotations

from datetime import date, datetime
from typing import Any, Literal, Optional, TypedDict

from pydantic import BaseModel, Field, field_validator

from app.shared_services.date_ranges import TimeRange

SlotKey = Literal[
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


class CommentaryLLMInput(BaseModel):
    slot_key: SlotKey
    app_id: str
    app_name: Optional[str] = None
    time_range_preset: TimeRange
    window_start: date
    window_end: date
    max_chars: int = Field(default=120, ge=1, le=5000)
    source_metrics_json: dict[str, Any] = Field(default_factory=dict)
    instructions: str


class CommentaryLLMOutput(BaseModel):
    commentary_text: str = Field(min_length=1)
    char_count: Optional[int] = None

    @field_validator("commentary_text")
    @classmethod
    def normalize_commentary_text(cls, value: str) -> str:
        return value.strip()


class SnapshotRecord(BaseModel):
    id: int
    app_id: str
    slot_key: SlotKey
    time_range_preset: TimeRange
    window_start: date
    window_end: date
    commentary_text: str
    max_chars: int
    source_metrics_json: dict[str, Any]
    model_id: Optional[str] = None
    prompt_version: Optional[str] = None
    generated_at: datetime


class LatestCommentaryResponse(BaseModel):
    status: Literal["success"]
    item: SnapshotRecord


class RunCommentaryRequest(BaseModel):
    app_id: Optional[str] = None
    time_ranges: Optional[list[TimeRange]] = None
    slot_keys: Optional[list[SlotKey]] = None
    force: bool = False
    dry_run: bool = False
    as_of: Optional[datetime] = None


class JobResult(BaseModel):
    app_id: str
    slot_key: SlotKey
    time_range_preset: TimeRange
    window_start: date
    window_end: date
    action: Literal["generated", "skipped", "error"]
    reason: Optional[str] = None
    snapshot_id: Optional[int] = None


class RunCommentaryResponse(BaseModel):
    status: Literal["success"]
    results: list[JobResult]
    summary: dict[str, int]


class CommentaryGraphState(TypedDict):
    app_id: str
    app_name: Optional[str]
    force: bool
    dry_run: bool
    as_of: Optional[datetime]
    jobs: list[dict[str, Any]]
    job_index: int
    current_job: dict[str, Any]
    existing_snapshot: Optional[dict[str, Any]]
    source_metrics_json: dict[str, Any]
    source_hash: str
    should_generate: bool
    llm_input: dict[str, Any]
    llm_output: Optional[dict[str, Any]]
    inserted_snapshot_id: Optional[int]
    results: list[dict[str, Any]]
    error: Optional[str]
