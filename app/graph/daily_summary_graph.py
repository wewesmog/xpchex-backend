"""
Daily Summary Graph
"""
from typing import Dict, Any
from langgraph.graph import StateGraph, END, START



from app.models.summary_models import DailySummaryState

#Nodes
from app.agents.daily_summary.daily_summary_agent_MVP import daily_summary_node

def save_graph_state(state: Dict[str, Any]) -> Dict[str, Any]:
    """The node to save the graph state. it runs just before end node.
    Its work it to only save the graph in postgres DB.
    """
    current_state = state if isinstance(state, DailySummaryState) else DailySummaryState(**state)
    return current_state.model_dump()
  
def build_graph(state: Dict[str, Any] = None) -> StateGraph:
    """Build the daily summary workflow graph."""
    workflow = StateGraph(DailySummaryState)
    
    # Add nodes
    workflow.add_node("summarize_reviews", daily_summary_node)
    # Add edges
    workflow.add_edge(START, "summarize_reviews")
    workflow.add_edge("summarize_reviews", END)
    
    # Return the compiled graph
    return workflow.compile()


    
