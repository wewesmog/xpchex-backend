# --- Review Wise Agents ---
# Have all the agents in one file
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
from app.models.review_analysis_models import MainState, ReviewAnalysisRequest, AspectAnalysis, Error, SentimentAnalysis, ProductStrengths, Opportunities, Roadmap, ResponseRecommendation, IssueAnalysis

#Prompts
from app.prompts.review_wise.aspect_analysis_prompt import get_aspect_analysis_prompt
from app.prompts.review_wise.sentiment_analysis_prompt import get_sentiment_analysis_prompt
from app.prompts.review_wise.opportunities_analysis_prompt import get_opportunities_analysis_prompt
from app.prompts.review_wise.roadmap_analysis_prompt import get_roadmap_analysis_prompt
from app.prompts.review_wise.response_recommendations_prompt import get_response_recommendations_prompt
from app.prompts.review_wise.issue_analysis_prompt import get_issue_analysis_prompt
from app.prompts.review_wise.positives_analysis_prompt import get_positives_analysis_prompt

# Load environment variables
load_dotenv()

#logger
logger = logging.getLogger(__name__)

# Node Functions
# Aspect Analysis Agent
def aspect_analysis_node(state: Dict[str, Any]) -> Dict[str, Any]:
    state = review_wise_agent(state, "aspect_analysis_node", get_aspect_analysis_prompt, AspectAnalysis)
    return state

# Sentiment Analysis Agent
def sentiment_analysis_node(state: Dict[str, Any]) -> Dict[str, Any]:
    state = review_wise_agent(state, "sentiment_analysis_node", get_sentiment_analysis_prompt, SentimentAnalysis)
    return state

# Opportunities Analysis Agent
def opportunities_analysis_node(state: Dict[str, Any]) -> Dict[str, Any]:
    state = review_wise_agent(state, "opportunities_analysis_node", get_opportunities_analysis_prompt, Opportunities)
    return state

# Roadmap Analysis Agent
def roadmap_analysis_node(state: Dict[str, Any]) -> Dict[str, Any]:
    state = review_wise_agent(state, "roadmap_analysis_node", get_roadmap_analysis_prompt, Roadmap)
    return state

# Response Recommendations Agent
def response_recommendations_node(state: Dict[str, Any]) -> Dict[str, Any]:
    state = review_wise_agent(state, "response_recommendations_node", get_response_recommendations_prompt, ResponseRecommendation)
    return state

# Issue Analysis Agent
def issue_analysis_node(state: Dict[str, Any]) -> Dict[str, Any]:
    state = review_wise_agent(state, "issue_analysis_node", get_issue_analysis_prompt, IssueAnalysis)
    return state

# Positives Analysis Agent
def positives_analysis_node(state: Dict[str, Any]) -> Dict[str, Any]:
    state = review_wise_agent(state, "positives_analysis_node", get_positives_analysis_prompt, ProductStrengths)
    return state

def review_wise_agent(state: Dict[str, Any], agent_name: str, prompt: str, response_model: BaseModel) -> Dict[str, Any]:
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
        
        # Get the system prompt with context - just pass review_content
        user_prompt = prompt(review_content=review_content)
        
        messages = [
            {"role": "system", "content": f"You are an {agent_name} agent. Return response in JSON format."},
            {"role": "user", "content": user_prompt}
        ]
        
        response = call_llm_api(
            messages=messages,
            temperature=0.7,
            response_format=response_model
        )

        # Update state with the response based on agent name
        if agent_name == "aspect_analysis_node":
            current_state.review_analysis.aspects = response
        elif agent_name == "sentiment_analysis_node":
            current_state.review_analysis.sentiment = response
        elif agent_name == "opportunities_analysis_node":
            current_state.review_analysis.opportunities = response
        elif agent_name == "roadmap_analysis_node":
            current_state.review_analysis.roadmap = response
        elif agent_name == "response_recommendations_node":
            current_state.review_analysis.response_recommendation = response
        elif agent_name == "issue_analysis_node":
            current_state.review_analysis.issues = response
        elif agent_name == "positives_analysis_node":
            current_state.review_analysis.positive_feedback = response

        # Add to node history
        current_state.node_history.append({
            "node_name": agent_name,
            "response": response.model_dump() if hasattr(response, 'model_dump') else response
        })

        return current_state.model_dump()
    
    except Exception as e:
        error_msg = f"Error in {agent_name} node: {str(e)}"
        logger.error(error_msg)
        
        current_state.error = Error(
            agent=agent_name,
            error_message=error_msg
        )

        current_state.node_history.append({
            "node_name": agent_name,
            "error": {
                "agent": agent_name,
                "error_message": error_msg
            }
        })
        
        return current_state.model_dump()

