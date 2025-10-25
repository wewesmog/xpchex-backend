from typing import Dict, Any
import logging
from app.shared_services.llm import call_llm_api
from app.models.review_analysis_models import MainState, ProductStrengths, Error
from app.prompts.review_wise.positives_analysis_prompt import get_positives_analysis_prompt

logger = logging.getLogger(__name__)

async def positives_analysis_node(state: Dict[str, Any]) -> Dict[str, Any]:
    """Analyze positive aspects and strengths from the review."""
    
    # Convert state dict to MainState
    current_state = state if isinstance(state, MainState) else MainState(**state)
    
    try:
        # Get the system prompt
        user_prompt = get_positives_analysis_prompt(
            review_content=current_state.review_analysis_request.review_content
        )
        
        # Call LLM
        messages = [
            {"role": "system", "content": "You are a positive analysis agent. Return response in JSON format."},
            {"role": "user", "content": user_prompt}
        ]
        
        # Get response
        positives_analysis = call_llm_api(
            messages=messages,
            temperature=0.7,
            response_format=ProductStrengths
        )

        # Update state
        current_state.review_analysis.positive_feedback = positives_analysis
        current_state.node_history.append({
            "node_name": "positives_analysis_node",
            "response": positives_analysis
        })

        return {
            "node_history": current_state.node_history,
            "current_step": "positives_analysis_node",
            "review_analysis": current_state.review_analysis,
            "review_analysis_request": current_state.review_analysis_request
        }
    
    except Exception as e:
        error_msg = f"Error in positives analysis node: {str(e)}"
        logger.error(error_msg)
        
        # Create Error object instead of appending to list
        current_state.error = Error(
            agent="positives_analysis_agent",
            error_message=error_msg
        )

        current_state.node_history.append({
            "node_name": "positives_analysis_node",
            "error": {
                "agent": "positives_analysis_agent",
                "error_message": error_msg
            }
        })
        
        return current_state.dict()
