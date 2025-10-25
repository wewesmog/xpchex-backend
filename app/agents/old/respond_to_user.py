# quiz-generator/app/agents/respond_to_user.py
# Agent to stream the response to user and potentially the artifact (quiz) to the user

from typing import Dict, Any
# from langchain_core.messages import HumanMessage
from app.models.pydantic_models import QuizState, MessageToUser

import logging
logger = logging.getLogger(__name__)

async def respond_to_user_node(state: Dict[str, Any]) -> Dict[str, Any]:
    """Node for handling user communication and updating conversation history."""
    logger.info("===== Entering Respond to User Node ======")
  
    current_state = state if isinstance(state, QuizState) else QuizState(**state)
    
    # Process message to user if there is one
    if current_state.message_to_user:
        logger.info(f"Processing message to user: {current_state.message_to_user}")
        # Add assistant's message to conversation history
        current_state.conversation_history.append({
            "role": "assistant",
            "content": current_state.message_to_user
        })
    
    return_state = {
        "conversation_history": current_state.conversation_history,
        "response_to_user_attempts": current_state.response_to_user_attempts + 1,
        "current_step": "respond_to_user",
        # Keep the message_to_user in the state so it can be sent in the API response
        "message_to_user": current_state.message_to_user
    }
    
    logger.info("===== Exiting Respond to User Node ======")
    
    return return_state