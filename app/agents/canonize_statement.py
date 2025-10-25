from app.models.canonization_models import CanonizationRequest, CanonizationLLMResponse, CanonizationState
from app.shared_services.db import get_postgres_connection
from app.shared_services.llm import call_llm_api
from app.prompts.canonization import canonize_statement as get_canonization_prompt

import asyncio
import os
import logging
from typing import Optional, List, Dict, Any, Literal, Annotated
from dotenv import load_dotenv
from pydantic import BaseModel, Field
import pprint

#Load environment variables
load_dotenv()

#Logger
logger = logging.getLogger(__name__)

def save_to_db(state: CanonizationState) -> None:
    """Save canonization state to database"""
    try:
        conn = get_postgres_connection()
        cur = conn.cursor()
        
        # Convert request and response to JSON strings if they exist
        request_json = state.canonization_request.model_dump_json() if state.canonization_request else None
        response_json = state.canonization_response.model_dump_json() if state.canonization_response else None
        
        # Call the insert function
        query = """
            SELECT * FROM insert_canonization_state(%s, %s, %s, %s, %s, %s);
        """
        params = (
            request_json,
            response_json,
            state.canonization_status,
            state.current_step,
            state.canonization_attempt,
            getattr(state, 'canonization_result', None)  # Use getattr in case field doesn't exist
        )
        
        cur.execute(query, params)
        
        result = cur.fetchone()
        success, error_msg, inserted_id = result
        
        if not success:
            logger.error(f"Failed to save canonization state: {error_msg}")
        else:
            logger.info(f"Saved canonization state with ID: {inserted_id}")
            
        conn.commit()
        cur.close()
        conn.close()
        
    except Exception as e:
        logger.error(f"Error saving canonization state to database: {e}")
        raise  # Re-raise the exception for proper error handling

def canonize_statement_node(state: Dict[str, Any]) -> Dict[str, Any]:
    """
    Canonize a statement
    """
    try:
        # For existing statements, create a minimal state
        if state.get("canonization_status") == "already_exists":
            # Create a minimal request with the correct statement
            request = CanonizationRequest(
                statement=state.get("statement", ""),
                review_id=state.get("review_id", ""),
                review_section=state.get("review_section", ""),
                existing_pairs=[]  # Empty since we already know it exists
            )
            
            # Create response object
            response = CanonizationLLMResponse(
                canonical_id=state.get("canonical_id"),
                reasoning=f"Statement already exists with canonical ID: {state.get('canonical_id')}",
                error=None
            )
            
            canonization_state = CanonizationState(
                canonization_request=request,
                canonization_response=response,
                canonization_status="already_exists",
                current_step="canonization_skipped",
                canonization_attempt=0,
                canonical_id=state.get("canonical_id")
            )
            
            # Save to db with already_exists status
            save_to_db(canonization_state)
            return canonization_state

        # For new statements, proceed with normal flow
        canonization_state = CanonizationState(**state)
        canonization_state.current_step = "canonization_requested"
        canonization_state.canonization_attempt = 0
        canonization_state.canonization_response = None

        # Get the canonization request
        canonization_request = canonization_state.canonization_request
        if not canonization_request:
            raise ValueError("Canonization request is missing from the state")
            
        # Heuristic: reduce existing_pairs to top-N nearest textual candidates
        # to keep the prompt small and focused.
        candidates = canonization_request.existing_pairs
        try:
            # naive sort by token overlap length (fallback when no embedding service)
            base = canonization_request.statement.lower()
            def score(pair):
                s = pair.statement.lower()
                overlap = len(set(base.split()) & set(s.split()))
                return (overlap, -abs(len(s) - len(base)))
            candidates = sorted(candidates, key=score, reverse=True)[:12]
        except Exception:
            candidates = candidates[:12]

        # Get the system prompt with context (strengthened few-shot prompt)
        user_prompt = get_canonization_prompt(
            statement=canonization_request.statement,
            existing_statements=candidates
        )
        
        messages = [
            {"role": "system", "content": "You are a canonization agent. Return response in JSON format."},
            {"role": "user", "content": user_prompt}
        ]
        
        # Get response from LLM
        try:
            response = call_llm_api(
                messages=messages,
                temperature=0.2,  # more deterministic for taxonomy mapping
                response_format=CanonizationLLMResponse
            )
        except Exception as e:
            logger.error(f"Error calling LLM: {e}")
            raise e
        
        if response.is_successful:
            canonization_state.current_step = "canonization_completed"
            canonization_state.canonization_attempt += 1
            canonization_state.canonization_response = response
            canonization_state.canonization_status = "completed"
        else:
            canonization_state.current_step = "canonization_failed"
            canonization_state.canonization_attempt += 1
            canonization_state.canonization_status = "failed"
            canonization_state.canonization_result = response.reasoning

        # Save to db
        save_to_db(canonization_state)
        return canonization_state
        
    except Exception as e:
        logger.error(f"Error in canonize_statement_node: {e}")
        return {
            "canonization_status": "failed",
            "current_step": "canonization_failed",
            "error": str(e)
        }