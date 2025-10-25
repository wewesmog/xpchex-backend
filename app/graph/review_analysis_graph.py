"""
Graph Builder Module for No Frameworks
"""
from typing import Dict, Any
from langgraph.graph import StateGraph, END, START
import logging

from ..models.review_analysis_models import MainState, ReviewAnalysisRequest, IssueAnalysis, SentimentAnalysis, ProductStrengths, ResponseRecommendation, CompetitorComparison, Prioritization, MarketOpportunity, SourceDerivation, BusinessGoal, StrategicInitiative, PositiveMention, ImpactAnalysis, Implementation, RoadmapImpactAssessment, RoadmapPrioritization, RoadmapTimeline, Opportunities, Roadmap    

#Nodes
from ..agents.review_wise.review_wise_agents import sentiment_analysis_node, issue_analysis_node, positives_analysis_node, response_recommendations_node

#Logging

logger = logging.getLogger(__name__)

def convert_state(state: Dict[str, Any]) -> MainState:
    """Convert a dictionary state to MainState, ensuring nested objects are properly converted."""
    if isinstance(state, MainState):
        return state
        
    # Convert the review analysis request first
    review_analysis_request = ReviewAnalysisRequest(**state.get("review_analysis_request", {}))
    review_content = review_analysis_request.review_content
    
    # Create MainState with the converted request
    state["review_analysis_request"] = review_analysis_request
    
    # Ensure review content is set in the analysis and all nested components
    if "review_analysis" in state:
        # Set content in main analysis
        if "content" not in state["review_analysis"] or not state["review_analysis"]["content"]:
            state["review_analysis"]["content"] = review_content
            
        # Convert and ensure content in nested components
        if "sentiment" in state["review_analysis"]:
            sentiment_dict = state["review_analysis"]["sentiment"] or {}
            if sentiment_dict.get("error") == "No review content provided for analysis.":
                sentiment_dict["error"] = None
            state["review_analysis"]["sentiment"] = SentimentAnalysis(**sentiment_dict)
            
        if "issues" in state["review_analysis"]:
            issues_dict = state["review_analysis"]["issues"] or {}
            if issues_dict.get("error") == "No review content provided for analysis.":
                issues_dict["error"] = None
            state["review_analysis"]["issues"] = IssueAnalysis(**issues_dict)
            
        if "positive_feedback" in state["review_analysis"]:
            feedback_dict = state["review_analysis"]["positive_feedback"] or {}
            if feedback_dict.get("error") == "No review content provided for analysis.":
                feedback_dict["error"] = None
            state["review_analysis"]["positive_feedback"] = ProductStrengths(**feedback_dict)
            
        if "opportunities" in state["review_analysis"]:
            opps_dict = state["review_analysis"]["opportunities"] or {}
            if opps_dict.get("error") == "No review content provided for analysis.":
                opps_dict["error"] = None
            state["review_analysis"]["opportunities"] = Opportunities(**opps_dict)
            
        if "roadmap" in state["review_analysis"]:
            roadmap_dict = state["review_analysis"]["roadmap"] or {}
            if roadmap_dict.get("error") == "No review content provided for analysis.":
                roadmap_dict["error"] = None
            state["review_analysis"]["roadmap"] = Roadmap(**roadmap_dict)
            
        if "positive_feedback" in state["review_analysis"]:
            feedback_dict = state["review_analysis"]["positive_feedback"] or {}
            if feedback_dict.get("error") == "No review content provided for analysis.":
                feedback_dict["error"] = None
            state["review_analysis"]["positive_feedback"] = ProductStrengths(**feedback_dict)
            
        if "response_recommendation" in state["review_analysis"]:
            response_dict = state["review_analysis"]["response_recommendation"] or {}
            if response_dict.get("error") == "No review content provided.":
                response_dict["error"] = None
            state["review_analysis"]["response_recommendation"] = ResponseRecommendation(**response_dict)
            
    return MainState(**state)

def save_graph_state(state: Dict[str, Any]) -> Dict[str, Any]:
    """The node to save the graph state. it runs just before end node."""
    current_state = convert_state(state)
    
    # Log the state for debugging
    logger.debug(f"Saving graph state with review_id: {current_state.review_analysis.review_id}")
    logger.debug(f"Review content: {current_state.review_analysis_request.review_content[:100]}...")
    logger.debug(f"Review content in analysis: {current_state.review_analysis.content[:100]}...")
    
    return current_state.model_dump()
  
def build_graph(state: Dict[str, Any] = None) -> StateGraph:
    """Build the review analysis workflow graph."""
    workflow = StateGraph(MainState)
    
    # Add nodes
    workflow.add_node("sentiment_analysis", sentiment_analysis_node)
   # workflow.add_node("aspect_analysis", aspect_analysis_node)
    workflow.add_node("issue_analysis", issue_analysis_node)
    #workflow.add_node("opportunities_analysis", opportunities_analysis_node)
    workflow.add_node("positives_analysis", positives_analysis_node)
    #workflow.add_node("roadmap_analysis", roadmap_analysis_node)
    workflow.add_node("response_recommendations", response_recommendations_node)
    
    # Add edges
    workflow.add_edge(START, "sentiment_analysis")
    #workflow.add_edge("sentiment_analysis", "aspect_analysis")
    #workflow.add_edge("aspect_analysis", "issue_analysis")
    workflow.add_edge("sentiment_analysis", "issue_analysis")
    #workflow.add_edge("issue_analysis", "opportunities_analysis")
    workflow.add_edge("issue_analysis", "positives_analysis")
    workflow.add_edge("positives_analysis", "response_recommendations")
    #workflow.add_edge("roadmap_analysis", "response_recommendations")
    workflow.add_edge("response_recommendations", END)
    
    # Return the compiled graph
    return workflow.compile()


    
