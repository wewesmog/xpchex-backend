from fastapi import APIRouter, HTTPException, status
import logging
from app.models.review_analysis_models import ReviewAnalysisRequest, AppReviewAnalysis
from app.google_reviews.review_analyzer import perform_review_analysis

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/reviewsAnalysis",
    tags=["reviewsAnalysis"]
)

@router.post(
    "/analyze",
    status_code=status.HTTP_200_OK,
    response_model=AppReviewAnalysis
)
async def analyze_review(review: ReviewAnalysisRequest) -> AppReviewAnalysis:
    try:
        logger.info(f"Received request to analyze review: {review.review_content[:100]}...")
        analysis_result = await perform_review_analysis(review.review_content)
        logger.info(f"Successfully analyzed review: {analysis_result.review_id}")
        return analysis_result
    except Exception as e:
        logger.error(f"Error analyzing review: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error analyzing review: {str(e)}"
        )






