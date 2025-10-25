from fastapi import APIRouter, HTTPException, Query, status
from app.google_reviews.get_reviews import get_reviews
from app.google_reviews.app_details_scraper import AppDetailsScraper
from app.google_reviews.app_search import search_app_id
from app.models.pydantic_models import ReviewFilter, Review
from datetime import datetime
import logging
from typing import Optional

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/reviews",
    tags=["reviews"]
)

@router.get("/list", status_code=status.HTTP_200_OK)
async def list_reviews(
    app_id: str = None,
    username: str = None,
    review_id: str = None,
    limit: int = Query(default=50, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    order_by: str = Query(default="review_created_at", pattern="^(review_created_at)$"),
    order_direction: str = Query(default="desc", pattern="^(asc|desc)$"),
    from_date: datetime = None,
    to_date: datetime = None
):
    """List Reviews with minimal data for table view"""
    try:
        filters = ReviewFilter(
            app_id=app_id,
            username=username,
            review_id=review_id,
            limit=limit,
            offset=offset,
            order_by=order_by,
            order_direction=order_direction,
            from_date=from_date,
            to_date=to_date
        )
        reviews = await get_reviews(filters)
        # Return only essential fields for table view
        review_responses = [
            {
                "id": review.id,
                "review_id": review.review_id,
                "username": review.username,
                "score": review.score,
                "review_created_at": review.review_created_at,
                "content": review.content[:100] + "..." if review.content and len(review.content) > 100 else review.content,  # Truncate long content
            }
            for review in reviews
        ]
        return {"status": "success", "data": review_responses}
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
        return {
            "status": "success",
            "data": Review(
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
                app_version=review.app_version
            )
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting review details: {str(e)}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error getting review details: {str(e)}")
