from typing import Dict, Any
import json
from app.shared_services.llm import call_llm_api
from app.shared_services.logger_setup import setup_logger
from app.models.review_analysis_models import MainState, ReviewAnalysisRequest, Error, Roadmap, AppReviewAnalysis
#from app.models.daily_summary_models import Roadmap as RoadmapAnalysis, ExecutionPlan
from app.prompts.review_wise.roadmap_analysis_prompt import get_roadmap_analysis_prompt

logger = setup_logger()

async def analyze_roadmap(content: str) -> Roadmap:
    """
    Analyze review content to generate roadmap insights.
    """
    try:
        # Get the analysis prompt
        user_prompt = get_roadmap_analysis_prompt(review_content=content)
        
        # Get the LLM response
        response = await call_llm_api(user_prompt)
        
        # Parse the JSON response
        try:
            roadmap_data = json.loads(response)
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse JSON response: {str(e)}")
            raise
        
        # Create and return the Roadmap object
        return Roadmap(**roadmap_data)
        
    except Exception as e:
        logger.error(f"Error in roadmap analysis: {str(e)}")
        return Roadmap(
            error="Error occurred during roadmap analysis",
            strategic_initiatives=[],
            execution_plan={
                "quarters": [],
                "success_metrics": []
            }
        )

async def roadmap_analysis_node(state: Dict[str, Any]) -> Dict[str, Any]:
    """Process a review to extract roadmap insights."""
    current_state = None
    try:
        # Convert state dict to MainState if needed
        if isinstance(state, MainState):
            current_state = state
        else:
            current_state = MainState(**state)
            
        # Get the review content from review_analysis_request
        if not current_state.review_analysis_request or not current_state.review_analysis_request.review_content:
            raise ValueError("No review content provided")
            
        # Analyze the roadmap
        roadmap_analysis = await analyze_roadmap(current_state.review_analysis_request.review_content)
        
        # Update the state with roadmap analysis
        if not hasattr(current_state, 'review_analysis'):
            current_state.review_analysis = AppReviewAnalysis()
        current_state.review_analysis.roadmap = roadmap_analysis
        
    except Exception as e:
        logger.error(f"Error in roadmap analysis node: {str(e)}")
        if current_state is None:
            if isinstance(state, MainState):
                current_state = state
            else:
                current_state = MainState(**state)
                
        if not hasattr(current_state, 'review_analysis'):
            current_state.review_analysis = AppReviewAnalysis()
        current_state.review_analysis.roadmap = Roadmap(
            error=f"Error in roadmap analysis: {str(e)}",
            strategic_initiatives=[],
            execution_plan={
                "quarters": [],
                "success_metrics": []
            }
        )
        
    return current_state.model_dump()