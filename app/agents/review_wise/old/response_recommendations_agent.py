from typing import Dict, Any
import logging
from dotenv import load_dotenv

#Shared Services
from app.shared_services.llm import call_llm_api

#Models
from app.models.review_analysis_models import MainState, ReviewAnalysisRequest, ResponseRecommendation, Error

#Prompts
from app.prompts.review_wise.response_recommendations_prompt import get_response_recommendations_prompt

# Load environment variables
load_dotenv()

#logger
logger = logging.getLogger(__name__)

async def response_recommendations_node(state: Dict[str, Any]) -> Dict[str, Any]:
    """Generate response recommendations based on review analysis."""
    
    # Convert state dict to MainState, ensuring nested objects are properly converted
    if isinstance(state, MainState):
        current_state = state
    else:
        # Convert the review analysis request first
        review_analysis_request = ReviewAnalysisRequest(**state.get("review_analysis_request", {}))
        
        # Create MainState with the converted request
        state["review_analysis_request"] = review_analysis_request
        current_state = MainState(**state)
    
    try:
        # Get the review content
        review_content = current_state.review_analysis_request.review_content
        if not review_content:
            raise ValueError("Review content is missing from the state")
            
        logger.debug(f"Processing review content: {review_content[:100]}...")
        
        # Get sentiment score if available
        sentiment_score = (
            current_state.review_analysis.sentiment.overall.score 
            if current_state.review_analysis.sentiment and current_state.review_analysis.sentiment.overall 
            else None
        )

        # Get the system prompt with context
        user_prompt = get_response_recommendations_prompt(
            review_content=review_content,
            sentiment_score=sentiment_score,
            issues=current_state.review_analysis.action_items if current_state.review_analysis.action_items else None,
            positives=current_state.review_analysis.positive_feedback if current_state.review_analysis.positive_feedback else None
        )
        
        messages = [
            {"role": "system", "content": "You are a response recommendations agent. Return response in JSON format."},
            {"role": "user", "content": user_prompt}
        ]
        
        response_rec = call_llm_api(
            messages=messages,
            temperature=0.7,
            response_format=ResponseRecommendation
        )

        # Update state
        current_state.review_analysis.response_recommendation = response_rec
        current_state.node_history.append({
            "node_name": "response_recommendations_node",
            "response": response_rec.model_dump()  # Convert to dict for serialization
        })

        # Return state as dict
        return current_state.model_dump()
    
    except Exception as e:
        error_msg = f"Error in response recommendations node: {str(e)}"
        logger.error(error_msg)
        
        # Create Error object instead of appending to list
        current_state.error = Error(
            agent="response_recommendations_agent",
            error_message=error_msg
        )

        current_state.node_history.append({
            "node_name": "response_recommendations_node",
            "error": {
                "agent": "response_recommendations_agent",
                "error_message": error_msg
            }
        })
        
        return current_state.model_dump()  # Use model_dump() instead of dict()
