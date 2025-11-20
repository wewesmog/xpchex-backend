"""
Models for canonicalization workflow
"""
from enum import Enum
from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any


class Category(Enum):
    """High-level categories for canonical issues"""
    PERFORMANCE = "Performance"
    UX = "UX"
    UI = "UI"
    AUTHENTICATION = "Authentication"
    PAYMENTS = "Payments"
    FEATURE = "Feature"
    STABILITY = "Stability"
    SECURITY = "Security"
    SUPPORT = "Support"
    NOTIFICATIONS = "Notifications"
    CONTENT = "Content"
    COMPETITIVE_ANALYSIS = "Competitive Analysis"
    ACCESSIBILITY = "Accessibility"
    CUSTOMIZATION = "Customization"
    INTEGRATIONS = "Integrations"


class Subcategory(Enum):
    """Subcategories used in prompts"""
    PROCESSING = "Processing"
    CRASH = "Crash"
    NAVIGATION = "Navigation"
    FEES = "Fees"
    GAP = "Gap"
    PERFORMANCE = "Performance"
    UI_LAYOUT = "UI Layout"
    AUTH_FLOW = "Authentication Flow"
    BILLING = "Billing"
    DISCOVERABILITY = "Discoverability"
    STABILITY = "Stability"
    SECURITY = "Security"
    NOTIFICATIONS = "Notifications"
    CONTENT = "Content"
    SUPPORT = "Support"
    GENERAL = "General"


class node_history(BaseModel):
    """Node history entry for tracking workflow steps"""
    node_name: str
    timestamp: str


class llm_input(BaseModel):
    """Input model for LLM canonicalization"""
    statement: str
    enriched_candidates: Optional[List[Dict[str, Any]]] = None


class llm_output(BaseModel):
    """Output model from LLM canonicalization"""
    canonical_id: str
    existing_canonical_id: bool
    reasoning: Optional[str] = None
    confidence: Optional[float] = None


class CanonicalizationResult(BaseModel):
    """Result of canonicalization process"""
    canonical_id: Optional[str] = None
    existing_canonical_id: bool = False
    source: Optional[str] = None
    confidence_score: Optional[float] = None


class CanonicalizationState(BaseModel):
    """State model for canonicalization workflow"""
    # Input fields
    input_statement: str
    review_section: Optional[str] = None
    review_id: Optional[str] = None
    review_created_at: Optional[str] = None
    
    # Output fields
    canonical_id: Optional[str] = None
    existing_canonical_id: bool = False
    source: Optional[str] = None
    confidence_score: Optional[float] = None
    results: Optional[str] = None
    
    # LLM fields
    llm_used: bool = False
    llm_with_examples_result: Optional[str] = None
    llm_without_examples_result: Optional[str] = None
    llm_with_examples_error: Optional[str] = None
    llm_without_examples_error: Optional[str] = None
    
    # Similarity results
    exact_match_result: Optional[str] = None
    exact_match_error: Optional[str] = None
    lexical_similarity_result: Optional[List[tuple]] = None
    lexical_similarity_error: Optional[str] = None
    vector_similarity_result: Optional[List[tuple]] = None
    vector_similarity_error: Optional[str] = None
    hybrid_similarity_result: Optional[List[tuple]] = None
    hybrid_similarity_error: Optional[str] = None
    
    # Enrichment
    enriched_candidates: Optional[List[Dict[str, Any]]] = None
    enrich_hybrid_results_result: Optional[str] = None
    enrich_hybrid_results_error: Optional[str] = None
    
    # Tracking
    node_history: List[node_history] = Field(default_factory=list)
    error: List[Dict[str, Any]] = Field(default_factory=list)
    
    class Config:
        arbitrary_types_allowed = True

