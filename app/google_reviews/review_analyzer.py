from uuid import uuid4
import logging
from ..models.review_analysis_models import (
    ReviewAnalysisRequest, MainState, AppReviewAnalysis, Error,
    SentimentAnalysis, AspectAnalysis, IssueAnalysis, Opportunities,
    Roadmap, ProductStrengths, ResponseRecommendation,
    IdentifiedFeature, Topic, UserIntent, Issue, Action,
    MarketOpportunity, Evidence, ImpactAnalysis, Implementation, CompetitivePosition,
    ExecutionPlan, QuarterPlan, SuccessMetric, KeyDeliverable, ResourceAllocation,
    ResponseContext, ResponseStrategy, ResponseTone, PositiveMention
)
from ..graph.review_analysis_graph import build_graph
from ..agents.review_wise.review_wise_agents import sentiment_analysis_node
from ..models.summary_models import ProcessingError
from datetime import datetime
from typing import Optional, List, Dict, Any
from ..shared_services.logger_setup import setup_logger

logger = setup_logger()

async def perform_review_analysis(review_content: str, test_mode: bool = False) -> dict:
    """
    Perform analysis on a single review.
    """
    # Generate a unique review ID
    review_id = str(uuid4())
    
    # Create the review analysis request
    review_request = ReviewAnalysisRequest(
        review_id=review_id,
        review_content=review_content.strip() if review_content else None,
        app_id=None,  # Will be set later if needed
        review_created_at=None  # Will be set later if needed
    )
    
    # Create the initial review analysis object
    review_analysis = AppReviewAnalysis(
        review_id=review_id,
        app_id=None,
        review_created_at=None,
        content=review_content.strip() if review_content else None,  # Set content here
        score=None,
        sentiment=SentimentAnalysis(
            error=None,
            overall=None,
            emotions=None,
            segments=None
        ),
        sentiment_attempts=0,
        aspects=AspectAnalysis(
            error=None,
            identified_features=[],
            topics=[],
            user_intent=UserIntent(
                primary="unknown",
                secondary=None,
                confidence=0.0,
                intents=None,
                total_intents=None
            )
        ),
        aspects_attempts=0,
        action_items=IssueAnalysis(
            error=None,
            issues=[]
        ),
        action_items_attempts=0,
        opportunities=Opportunities(
            error=None,
            market_opportunities=[]
        ),
        opportunities_attempts=0,
        roadmap=Roadmap(
            error=None,
            strategic_initiatives=None,
            execution_plan=None
        ),
        roadmap_attempts=0,
        positive_feedback=ProductStrengths(
            error=None,
            positive_mentions=[],
            overall_satisfaction_score=0.0,
            brand_advocacy_rate=0.0,
            competitive_advantages={},
            growth_opportunities=[]
        ),
        positive_feedback_attempts=0,
        response_recommendation=ResponseRecommendation(
            error=None,
            response_required=True,  # Default to true since we want to respond to all reviews
            response_id=None,
            context=ResponseContext(
                related_issues=[],
                related_positives=[],
                user_sentiment=0.0,
                user_segment=None,
                platform=None
            ),
            strategy=ResponseStrategy(
                should_respond=True,
                priority="high",
                key_points=[],
                tone=ResponseTone(
                    primary_tone="appreciative",
                    secondary_tone=None,
                    formality_level="neutral",
                    personalization_level=0.8
                ),
                commitments=None,
                public_response=True
            ),
            suggested_response=None,
            alternative_responses=None,
            response_guidelines=None,
            follow_up_needed=False,
            follow_up_timeline=None,
            metadata={}
        ),
        response_recommendation_attempts=0
    )

    # Create the initial state with both review request and analysis
    initial_state = MainState(
        review_analysis_request=review_request,  # Set the request here
        review_analysis=review_analysis,
        node_history=[],
        current_step="sentiment_analysis",
        error=None
    )

    logger.debug(f"Initialized state with review_id: {review_id}")
    logger.debug(f"Review content in request: {review_request.review_content[:100]}...")
    logger.debug(f"Review content in analysis: {review_analysis.content[:100]}...")

    try:
        if test_mode:
            # For testing, just do sentiment analysis
            state_dict = initial_state.model_dump()
            result = await sentiment_analysis_node(state_dict)
            final_state = MainState(**result)
        else:
            # Full analysis using the graph
            graph = build_graph()
            results = await graph.abatch([initial_state.model_dump()])
            final_state = MainState(**results[0])

        logger.info(f"Completed internal analysis for review_id: {final_state.review_analysis.review_id}")
        
        # Convert the analysis results to a dictionary, excluding node history
        analysis_results = final_state.review_analysis.model_dump()
        
        # Log node history for debugging but don't include it in the results
        for node in final_state.node_history:
            logger.debug(f"Node {node.get('node_name')} completed with response: {node.get('response')}")
            
        return analysis_results

    except Exception as e:
        error_msg = f"Error in review analysis: {str(e)}"
        logger.error(error_msg)
        
        # Set error in each component's error field
        review_analysis.sentiment.error = error_msg if review_analysis.sentiment else None
        review_analysis.aspects.error = error_msg if review_analysis.aspects else None
        review_analysis.action_items.error = error_msg if review_analysis.action_items else None
        review_analysis.opportunities.error = error_msg if review_analysis.opportunities else None
        review_analysis.roadmap.error = error_msg if review_analysis.roadmap else None
        review_analysis.positive_feedback.error = error_msg if review_analysis.positive_feedback else None
        review_analysis.response_recommendation.error = error_msg if review_analysis.response_recommendation else None
        
        return review_analysis.model_dump()
