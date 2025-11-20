# Langgraph imports
from langgraph.graph import StateGraph, START, END
from langgraph.graph.message import add_messages

# import the nodes
from app.reviews_helpers.canonicalization import get_exact_match, get_lexical_similarity, get_vector_similarity, get_hybrid_similarity, enrich_hybrid_results, get_llm_input, save_canonicalization_result, final_canonical_id

# import model
from app.models.canonicalization_models import CanonicalizationState

# Function to check wether the canonical_id field has a value
def has_canonical_id(state: CanonicalizationState) -> bool:
    if state.canonical_id is not None:
        return True
    else:
        return False
def hybrid_decision(state: CanonicalizationState) -> str:
    if state.canonical_id is not None:
        return "save_canonicalization_result"
    elif state.hybrid_similarity_result and len(state.hybrid_similarity_result) > 0:
        return "enrich_hybrid_results"
    else:
        return "get_llm_input"

def build_graph():
    # Build the graph
    workflow = StateGraph(CanonicalizationState)

    # Add nodes
    workflow.add_node("get_exact_match", get_exact_match)
    workflow.add_node("get_lexical_similarity", get_lexical_similarity)
    workflow.add_node("get_vector_similarity", get_vector_similarity)
    workflow.add_node("get_hybrid_similarity", get_hybrid_similarity)
    workflow.add_node("enrich_hybrid_results", enrich_hybrid_results)
    workflow.add_node("get_llm_input", get_llm_input)
    workflow.add_node("save_canonicalization_result", save_canonicalization_result)
    # Add edges
    workflow.add_edge(START, "get_exact_match")
    workflow.add_conditional_edges(
        "get_exact_match",
        has_canonical_id,
        {
            True: "save_canonicalization_result",
            False: "get_lexical_similarity"
        }
    )



    workflow.add_edge("get_lexical_similarity", "get_vector_similarity")
    workflow.add_edge("get_vector_similarity", "get_hybrid_similarity")

    workflow.add_conditional_edges(
        "get_hybrid_similarity",
        hybrid_decision,
        {
            "save_canonicalization_result": "save_canonicalization_result",
            "enrich_hybrid_results": "enrich_hybrid_results",
            "get_llm_input": "get_llm_input"
        }
    )
   
    workflow.add_edge("enrich_hybrid_results", "get_llm_input")
    workflow.add_edge("get_llm_input", "save_canonicalization_result")
    workflow.add_edge("save_canonicalization_result", END)

    # Return the compiled graph     
    return workflow.compile()



