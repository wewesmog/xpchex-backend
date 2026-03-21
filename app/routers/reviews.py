from fastapi import APIRouter, HTTPException, Query, status
from app.google_reviews.get_reviews import get_reviews, get_reviews_count
from app.google_reviews.app_details_scraper import AppDetailsScraper
from app.google_reviews.app_search import search_app_id
from app.models.pydantic_models import ReviewFilter, Review, ReviewListResponse
from datetime import datetime
import logging
from typing import Optional

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/reviews",
    tags=["reviews"]
)

@router.get("/list", status_code=status.HTTP_200_OK, response_model=ReviewListResponse)
async def list_reviews(
    app_id: str = None,
    username: str = None,
    review_id: str = None,
    limit: int = Query(default=50, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    order_by: str = Query(default="review_created_at", pattern="^(review_created_at|thumbs_up_count)$"),
    order_direction: str = Query(default="desc", pattern="^(asc|desc)$"),
    from_date: datetime = None,
    to_date: datetime = None,
    min_score: Optional[float] = Query(default=None, ge=0, le=5),
    max_score: Optional[float] = Query(default=None, ge=0, le=5),
    sentiment: Optional[str] = Query(default=None, pattern="^(positive|neutral|negative)$"),
    inbox_id: Optional[str] = Query(default=None, description="CX Agent inbox filter: all | unanswered | answered | analyzed | escalated | draft | closed."),
    cx_agent: bool = Query(default=False, description="When true, select all reviews with no analyzed filter (CX Agent inbox)."),
):
    """List Reviews with minimal data for table view. Filters: from_date, to_date (when), min_score/max_score (stars), sentiment, inbox_id (unanswered/answered via reply_created_at). Set cx_agent=1 for inbox."""
    try:
        # Map inbox_id to filters (case-insensitive):
        # - unanswered: reply_created_at IS NULL
        # - answered: reply_created_at IS NOT NULL
        # - analyzed: analyzed = TRUE
        replied = None
        analyzed_filter: Optional[bool] = None
        if inbox_id:
            inbox_lower = inbox_id.strip().lower()
            if inbox_lower == "unanswered":
                replied = False
            elif inbox_lower == "answered":
                replied = True
            elif inbox_lower == "analyzed":
                analyzed_filter = True

        filters = ReviewFilter(
            app_id=app_id,
            username=username,
            review_id=review_id,
            limit=limit,
            offset=offset,
            order_by=order_by,
            order_direction=order_direction,
            from_date=from_date,
            to_date=to_date,
            min_score=min_score,
            max_score=max_score,
            sentiment=sentiment,
            inbox_id=inbox_id,
            replied=replied,
            analyzed=analyzed_filter if inbox_id and inbox_id.strip().lower() == "analyzed" else None,
        )
        reviews = await get_reviews(filters)
        reviews_count = await get_reviews_count(filters)

        all_count = None
        unanswered_count = None
        answered_count = None
        if cx_agent:
            base = dict(app_id=app_id, from_date=from_date, to_date=to_date, min_score=min_score, max_score=max_score, sentiment=sentiment, analyzed=None, limit=1, offset=0, order_by=order_by, order_direction=order_direction)
            all_count = await get_reviews_count(ReviewFilter(**base, replied=None))
            unanswered_count = await get_reviews_count(ReviewFilter(**base, replied=False))
            answered_count = await get_reviews_count(ReviewFilter(**base, replied=True))
        logger.info(f"CX list: inbox_id={inbox_id} replied={replied} total={reviews_count} unanswered={unanswered_count} answered={answered_count}")

        review_responses = [
            {
                "id": review.id,
                "review_id": review.review_id,
                "username": review.username,
                "score": review.score,
                "review_created_at": review.review_created_at,
                "content": review.content[:100] if review.content else "",
                "reply_created_at": review.reply_created_at,
            }
            for review in reviews
        ]
        return ReviewListResponse(
            status="success",
            data=review_responses,
            total_count=int(reviews_count),
            all_count=int(all_count) if all_count is not None else None,
            unanswered_count=int(unanswered_count) if unanswered_count is not None else None,
            answered_count=int(answered_count) if answered_count is not None else None,
        )
    except Exception as e:
        logger.error(f"Error listing reviews: {str(e)}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error listing reviews: {str(e)}")

@router.get("/{review_id}/details", status_code=status.HTTP_200_OK)
async def get_review_details(review_id: str):
    """Get detailed information for a specific review"""
    try:
        filters = ReviewFilter(
            review_id=review_id,
            limit=1
        )
        reviews = await get_reviews(filters)
        if not reviews:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Review not found")
        
        # Return full review details
        review = reviews[0]
        review_payload = Review(
            id=review.id,
            app_id=review.app_id,
            review_id=review.review_id,
            username=review.username,
            user_image=review.user_image,
            content=review.content,
            score=review.score,
            thumbs_up_count=review.thumbs_up_count,
            review_created_at=review.review_created_at,
            reply_content=review.reply_content,
            reply_created_at=review.reply_created_at,
            app_version=review.app_version,
            latest_analysis=review.latest_analysis
        ).model_dump()

        # Prefer draft from response_drafts/node_history pipeline for CX inbox draft card.
        # This is best-effort; if tables are unavailable we still return base review details.
        draft_text = None
        draft_source = None
        draft_id = None
        node_history_id = None
        node_history_snapshot = None
        try:
            from app.shared_services.db import pooled_connection

            with pooled_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT id, draft_text, source
                        FROM response_drafts
                        WHERE review_id = %s
                          AND app_id = %s
                        ORDER BY id DESC
                        LIMIT 1
                        """,
                        (review.review_id, review.app_id),
                    )
                    row = cur.fetchone()
                    if row:
                        draft_id, draft_text, draft_source = row[0], row[1], row[2]

                    cur.execute(
                        """
                        SELECT id, snapshot
                        FROM node_history
                        WHERE review_id = %s
                          AND app_id = %s
                          AND agent_name = 'nelly'
                        ORDER BY id DESC
                        LIMIT 1
                        """,
                        (review.review_id, review.app_id),
                    )
                    nh_row = cur.fetchone()
                    if nh_row:
                        node_history_id = nh_row[0]
                        node_history_snapshot = nh_row[1]
        except Exception as e:
            logger.warning("Draft/node_history lookup failed for review_id=%s: %s", review_id, e)

        review_payload["draft_id"] = draft_id
        review_payload["draft_text"] = draft_text
        review_payload["draft_source"] = draft_source
        review_payload["node_history_id"] = node_history_id
        review_payload["node_history_snapshot"] = node_history_snapshot

        return {
            "status": "success",
            "data": review_payload
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting review details: {str(e)}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error getting review details: {str(e)}")
