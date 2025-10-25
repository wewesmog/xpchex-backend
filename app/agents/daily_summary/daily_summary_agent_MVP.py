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
from app.models.summary_models import DailySummaryState, DailySummary, DailySummaryError
from app.prompts.daily_wise.daily_analysis_prompt import get_daily_analysis_prompt

# Load environment variables
load_dotenv()

#logger
logger = logging.getLogger(__name__)

async def daily_summary_node(state: Dict[str, Any]) -> Dict[str, Any]:
    """Summarize the reviews for a given day."""
    
    # Convert state dict to MainState
    current_state = state if isinstance(state, DailySummaryState) else DailySummaryState(**state)
    
    try:
        # Get the system prompt with context
        user_prompt = get_daily_analysis_prompt(
            daily_summary_request=current_state.daily_summary_request
        )
        
        messages = [
            {"role": "system", "content": "You are a daily summary agent. Return response in JSON format."},
            {"role": "user", "content": user_prompt}
        ]
        
        daily_summary = call_llm_api(
            messages=messages,
            temperature=0.7,
            response_format=DailySummary
        )

        # Update state
        current_state.daily_summary = daily_summary
        current_state.node_history.append({
            "node_name": "summarize_reviews",
            "response": daily_summary
        })

        return {
            "node_history": current_state.node_history,
            "current_step": "summarize_reviews",
            "daily_summary_request": current_state.daily_summary_request,
            "daily_summary": current_state.daily_summary
        }
    
    except Exception as e:
        error_msg = f"Error in daily summary node: {str(e)}"
        logger.error(error_msg)
        
        # Create Error object instead of appending to list
        current_state.error = DailySummaryError(
            agent="daily_summary_agent",
            error_message=error_msg
        )

        current_state.node_history.append({
            "node_name": "summarize_reviews",
            "error": {
                "agent": "daily_summary_agent",
                "error_message": error_msg
            }
        })
        
        return current_state.dict()

