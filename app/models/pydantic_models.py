from pydantic import BaseModel, Field, field_validator
from typing import List, Optional, Union, Literal, Dict, Any, Annotated
from datetime import datetime


class Review(BaseModel):
    id: int
    app_id: str
    review_id: str
    username: str
    user_image: Optional[str]
    content: str
    score: float
    thumbs_up_count: int
    review_created_at: datetime
    reply_content: Optional[str]
    reply_created_at: Optional[datetime]
    app_version: Optional[str]
    analyzed: bool = False
    last_analyzed_on: Optional[datetime] = None
    latest_analysis: Optional[dict] = None
    latest_analysis_id: Optional[int] = None

    @field_validator('content')
    @classmethod
    def validate_content(cls, v: str) -> str:
        """Validate that content is not empty."""
        if not v or not v.strip():
            raise ValueError("Review content cannot be empty")
        return v.strip()

class ReviewFilter(BaseModel):
    app_id: Optional[str] = None
    username: Optional[str] = None
    review_id: Optional[str] = None
    limit: int = 50
    offset: int = 0
    order_by: str = "review_created_at"
    order_direction: str = "desc"
    from_date: Optional[datetime] = None
    to_date: Optional[datetime] = None
    analyzed: Optional[bool] = None   # None = do not filter (e.g. CX Agent "select all"); True/False = filter
    date_list: Optional[List[datetime]] = None
    min_score: Optional[float] = None  # stars filter: e.g. 1 for 1-star
    max_score: Optional[float] = None  # stars filter: e.g. 1.99 for 1-star
    sentiment: Optional[str] = None   # positive | neutral | negative (from latest_analysis)
    inbox_id: Optional[str] = None    # CX Agent: all | unanswered | answered | ...
    replied: Optional[bool] = None     # False = unanswered (reply_created_at IS NULL), True = answered (IS NOT NULL), None = no filter
    # When True: analyzed rows where latest_analysis has _analysis_input_fingerprint and it != hash of current text/reply.
    stale_analysis: Optional[bool] = None


class ReviewListItem(BaseModel):
    """Minimal fields for list/table view."""
    id: int
    review_id: str
    username: Optional[str]
    score: float
    review_created_at: datetime
    content: str
    reply_created_at: Optional[datetime] = None   # null = unanswered (for CX bolding)


class ReviewListResponse(BaseModel):
    """GET /reviews/list response: always includes total_count; when cx_agent includes inbox counts for hover."""
    status: str
    data: List[ReviewListItem]
    total_count: int
    all_count: Optional[int] = None
    unanswered_count: Optional[int] = None
    answered_count: Optional[int] = None


     


    