"""
Async canonicalization workflow - replaces LangGraph with simple if/then logic
Fully async for maximum I/O concurrency
"""
import asyncio
from app.models.canonicalization_models import CanonicalizationState
from app.reviews_helpers.canonicalization import (
    get_exact_match_async,
    get_lexical_similarity_async, 
    get_vector_similarity_async,
    get_hybrid_similarity_async,
    enrich_hybrid_results_async,
    get_llm_input,
    save_canonicalization_result_async
)


async def run_canonicalization_workflow_async(state: CanonicalizationState) -> CanonicalizationState:
    """
    Run the complete canonicalization workflow with async support.
    This replaces the LangGraph workflow with simple if/then logic.
    
    Workflow:
    1. Try exact match
    2. If no match, try lexical + vector similarity
    3. Calculate hybrid similarity
    4. If good candidates, enrich them
    5. Use LLM to select/create canonical ID
    6. Save result
    
    Args:
        state: Initial canonicalization state
        
    Returns:
        Final canonicalization state with canonical_id (or failure info)
    """
    # Step 1: Try exact match (async DB)
    state = await get_exact_match_async(state)
    
    # If we found exact match, skip to save
    if state.canonical_id:
        state = await save_canonicalization_result_async(state)
        return state
    
    # Step 2: Get lexical similarity (async DB)
    state = await get_lexical_similarity_async(state)
    
    # Step 3: Get vector similarity (async DB)
    state = await get_vector_similarity_async(state)
    
    # Step 4: Calculate hybrid similarity (async combining)
    state = await get_hybrid_similarity_async(state)
    
    # If hybrid found a great match, skip to save
    if state.canonical_id:
        state = await asyncio.to_thread(save_canonicalization_result, state)
        return state
    
    # Step 5: Enrich candidates if we have hybrid results
    if state.hybrid_similarity_result and len(state.hybrid_similarity_result) > 0:
        state = await enrich_hybrid_results_async(state)
    
    # Step 6: Use LLM to select or create canonical ID (async LLM call)
    state = await get_llm_input(state)
    
    # Step 7: Save result (async DB writes)
    state = await save_canonicalization_result_async(state)
    
    return state

