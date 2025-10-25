# --- Aspect Analysis Agent ---
# --- Import Libraries ---
import asyncio
import os
import logging
from typing import Optional, List, Dict, Any, Literal, Annotated
from dotenv import load_dotenv
from pydantic import BaseModel, Field
import pprint

#Shared Services
from app.shared_services.llm import call_llm_api

#Models
from app.models.review_analysis_models import MainState, ReviewAnalysisRequest, AspectAnalysis, Error
from app.prompts.review_wise.aspect_analysis_prompt import get_aspect_analysis_prompt

# Load environment variables
load_dotenv()

#logger
logger = logging.getLogger(__name__)

async def aspect_analysis_node(state: Dict[str, Any]) -> Dict[str, Any]:
    """Analyze aspects of a review."""
    
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
        
        # Get the system prompt with context
        user_prompt = get_aspect_analysis_prompt(
            review_content=review_content,
            sentiment=current_state.review_analysis.sentiment if current_state.review_analysis.sentiment else None
        )
        
        messages = [
            {"role": "system", "content": "You are an aspect analysis agent. Return response in JSON format."},
            {"role": "user", "content": user_prompt}
        ]
        
        aspects = call_llm_api(
            messages=messages,
            temperature=0.7,
            response_format=AspectAnalysis
        )

        # Update state
        current_state.review_analysis.aspects = aspects
        current_state.node_history.append({
            "node_name": "aspect_analysis_node",
            "response": aspects.model_dump()  # Convert to dict for serialization
        })

        # Return state as dict
        return current_state.model_dump()
    
    except Exception as e:
        error_msg = f"Error in aspect analysis node: {str(e)}"
        logger.error(error_msg)
        
        # Create Error object instead of appending to list
        current_state.error = Error(
            agent="aspect_analysis_agent",
            error_message=error_msg
        )

        current_state.node_history.append({
            "node_name": "aspect_analysis_node",
            "error": {
                "agent": "aspect_analysis_agent",
                "error_message": error_msg
            }
        })
        
        return current_state.model_dump()  # Use model_dump() instead of dict()

