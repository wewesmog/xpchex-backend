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
    analyzed: bool = False
    date_list: Optional[List[datetime]] = None


     


    