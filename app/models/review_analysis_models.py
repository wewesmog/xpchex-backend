from typing import List, Optional, Dict, Literal, Union, Any
from datetime import datetime
from pydantic import BaseModel, Field, field_validator

# This file defines the Pydantic models for a complete, structured analysis
# of a single app review. It has been corrected to precisely match the structure
# of the provided example JSON object.

class ReviewAnalysisRequest(BaseModel):
    """
    A request to analyze a review.
    """
    review_id: str
    review_content: str
    app_id: Optional[str] = None
    review_created_at: Optional[datetime] = None

    @field_validator('review_content')
    @classmethod
    def validate_content(cls, v: str) -> str:
        """Validate that content is not empty and properly formatted."""
        if not v or not v.strip():
            raise ValueError("Review content cannot be empty")
        return v.strip()

    @field_validator('review_id')
    @classmethod
    def validate_review_id(cls, v: str) -> str:
        """Validate that review_id is not empty."""
        if not v or not v.strip():
            raise ValueError("Review ID cannot be empty")
        return v.strip()


# ==============================================================================
# 1. SENTIMENT AND EMOTION ANALYSIS MODELS
# ==============================================================================

class Span(BaseModel):
    start: int
    end: int

class SegmentSentiment(BaseModel):
    label: Literal["positive", "negative", "neutral"]
    score: float
    confidence: float

class SentimentSegment(BaseModel):
    text: str
    span: Span
    sentiment: SegmentSentiment

class SentimentDistribution(BaseModel):
    positive: float
    neutral: float
    negative: float

class OverallSentiment(BaseModel):
    score: float
    classification: Literal["positive", "negative", "neutral", "mixed"]
    confidence: float
    distribution: SentimentDistribution

class Emotion(BaseModel):
    emotion: Literal["joy", "sadness", "anger", "fear", "surprise", "disgust", "frustration", "anticipation", "trust", "other"]
    confidence: float

class EmotionAnalysis(BaseModel):
    primary: Emotion
    secondary: Optional[Emotion] = None
    emotion_scores: Dict[str, float]

class SentimentAnalysis(BaseModel):
    # This is a single object in the JSON, not a list
    error: Optional[str] = None
    overall: Optional[OverallSentiment] = None
    emotions: Optional[EmotionAnalysis] = None
    segments: Optional[List[SentimentSegment]] = None

# ==============================================================================
# 2. FEATURE AND ASPECT ANALYSIS MODELS
# ==============================================================================

class Mention(BaseModel):
    text: str
    span: Optional[Span] = None
    context: Optional[str] = None

class AspectSentiment(BaseModel):
    label: Literal["positive", "negative", "neutral"]
    count: Optional[int] = None
    score: Optional[float] = None
    confidence: Optional[float] = None

class IdentifiedFeature(BaseModel):
    name: str
    category: Optional[Literal["UI/UX", "Functionality", "Performance", "Security", "Login", "Payment", "Profile", "Search", "General", "Other"]] = None
    sentiment: AspectSentiment
    mentions: List[Mention]
    importance_score: float
    occurrence_count: Optional[int] = None

class Topic(BaseModel):
    name: str
    confidence: Optional[float] = None
    keywords: List[str]
    sentiment: Literal["positive", "negative", "neutral", "mixed"]
    occurrence_count: Optional[int] = None
    keyword_counts: Optional[Dict[str, int]] = None

class UserIntent(BaseModel):
    # This is a single object in the JSON, not a list
    primary: Literal["report_issue", "provide_feedback", "ask_question", "praise", "request_feature", "other", "unknown"]
    secondary: Optional[Literal["report_issue", "provide_feedback", "ask_question", "praise", "request_feature", "other", "unknown"]] = None
    confidence: float
    intents: Optional[Dict[str, int]] = None
    total_intents: Optional[int] = None

class AspectAnalysis(BaseModel):
    # This is a single object in the JSON, not a list
    error: Optional[str] = None # Added to indicate error in analysis
    identified_features: List[IdentifiedFeature]
    topics: List[Topic]
    user_intent: UserIntent

# ==============================================================================
# 3. ISSUES MODELS
# ==============================================================================

class Action(BaseModel):
    type: Literal["fix", "improvement", "investigation", "user_communication", "documentation"]
    description: str
    confidence: float
    estimated_effort: Literal["low", "medium", "high"]
    suggested_timeline: Optional[Literal["short-term", "medium-term", "long-term"]] = None



class Issue(BaseModel):
    type: Literal["bug", "performance", "feature_request", "ux_issue", "security", "other"]
    description: str
    impact_score: float # 0-100, how much the issue impacts the overall sentiment score for the review
    severity: Literal["low", "medium", "high", "critical"]
    key_words: Optional[List[str]] = None # Keywords mentioned in the review that are related to the issue
    snippet: Optional[List[str]] = None # Relevant text snippets from the review
    actions: Optional[List[Action]] = None  # Actions that can address this issue
   
 

class IssueAnalysis(BaseModel):
    # This is a single object in the JSON, not a list
    error: Optional[str] = None # Added to indicate error in analysis
    issues: List[Issue]
 
# ==============================================================================
# 4. OPPORTUNITIES MODELS
# ==============================================================================

class ReviewSnippet(BaseModel):
    text: str
    review_id: str
    sentiment: float

class Evidence(BaseModel):
    review_count: int
    user_sentiment: float
    mention_frequency: float
    review_snippets: List[ReviewSnippet]

class AffectedMetric(BaseModel):
    metric: Literal["user_acquisition", "user_retention", "app_rating", "user_engagement", "conversion_rate", "other"]
    predicted_impact: float
    confidence: float

class CompetitorComparison(BaseModel):
    competitor: str
    status: Literal["ahead", "behind", "equal"]

class CompetitivePosition(BaseModel):
    current_state: str
    potential_advantage: str
    competitor_comparison: Optional[List[CompetitorComparison]] = None

class ImpactAnalysis(BaseModel):
    potential_impact: Literal["low", "medium", "high", "very_high"]
    affected_metrics: List[AffectedMetric]
    user_segments: List[str]
    competitive_position: CompetitivePosition

class Risk(BaseModel):
    description: str
    severity: Literal["low", "medium", "high"]
    mitigation: str

class Implementation(BaseModel):
    complexity: Literal["low", "medium", "high", "very_high"]
    required_resources: List[str]
    estimated_timeline: str
    dependencies: List[str]
    risks: List[Risk]

class PrioritizationFactor(BaseModel):
    name: Literal["market_demand", "strategic_alignment", "revenue_potential", "user_impact", "effort", "other"]
    weight: float
    score: float

class Prioritization(BaseModel):
    score: float
    factors: List[PrioritizationFactor]
    time_sensitivity: Literal["low", "medium", "high"]
    strategic_alignment: float

class MarketOpportunity(BaseModel):
    id: str
    type: Literal["feature_gap", "user_need", "competitive_advantage", "market_trend", "new_market", "other"]
    title: str
    description: str
    confidence_score: float
    evidence: Evidence
    impact_analysis: ImpactAnalysis
    implementation: Implementation
    prioritization: Prioritization

class SupportingData(BaseModel):
    review_trend: List[float] # Corrected from int to float based on example
    time_period: str

class Opportunities(BaseModel):
    # This is a single object in the JSON, not a list
    error: Optional[str] = None # Added to indicate error in analysis
    market_opportunities: List[MarketOpportunity]

class SourceDerivation(BaseModel):
    type: Literal["user_feedback", "competitor_analysis", "market_trend", "internal_data"]
    reference_id: str
    confidence: float

class SupportingMetric(BaseModel):
    metric_name: str
    current_value: float
    target_value: float
    impact_confidence: float

class Source(BaseModel):
    derived_from: List[SourceDerivation]
    supporting_metrics: List[SupportingMetric]

class Milestone(BaseModel):
    title: str
    description: str
    target_date: str

class RoadmapTimeline(BaseModel):
    phase: Literal["short_term", "medium_term", "long_term"]
    estimated_duration: str
    target_quarter: str
    dependencies: List[str]
    milestones: List[Milestone]

class BusinessGoalMetric(BaseModel):
    name: str
    predicted_change: float

class BusinessGoal(BaseModel):
    goal: str
    impact_score: float
    metrics: List[BusinessGoalMetric]

class UserExperienceImpact(BaseModel):
    affected_journeys: List[str]
    improvement_areas: List[str]
    predicted_satisfaction_impact: float

class RiskAssessment(BaseModel):
    risk_type: str
    probability: float
    impact: float
    mitigation_strategy: str

class RoadmapImpactAssessment(BaseModel):
    business_goals: List[BusinessGoal]
    user_experience: UserExperienceImpact
    risk_assessment: List[RiskAssessment]

class RoadmapPrioritizationFactor(BaseModel):
    name: str
    weight: float
    score: float
    justification: str

class RoadmapPrioritization(BaseModel):
    overall_score: float
    factors: List[RoadmapPrioritizationFactor]
    strategic_alignment: float
    cost_benefit_ratio: float

class StrategicInitiative(BaseModel):
    id: str
    title: str
    description: str
    type: Literal["feature", "improvement", "fix", "innovation", "research", "other"]
    status: Literal["proposed", "planned", "in_progress", "completed", "on_hold", "cancelled"]
    source: Source
    timeline: RoadmapTimeline
    impact_assessment: RoadmapImpactAssessment
    prioritization: RoadmapPrioritization

class KeyDeliverable(BaseModel):
    initiative_id: str
    deliverable: str
    success_criteria: List[str]

class ResourceAllocation(BaseModel):
    team: Literal["engineering", "product", "design", "qa", "marketing", "other"]
    allocation_percentage: float

class QuarterPlan(BaseModel):
    quarter: str
    theme: str
    key_deliverables: List[KeyDeliverable]
    resource_allocation: List[ResourceAllocation]

class SuccessMetric(BaseModel):
    metric: str
    current_value: float
    target_value: float
    measurement_frequency: Literal["daily", "weekly", "monthly", "quarterly"]
    data_source: str

class ExecutionPlan(BaseModel):
    quarters: List[QuarterPlan]
    success_metrics: List[SuccessMetric]

class Roadmap(BaseModel):
    # This is a single object in the JSON, not a list
    error: Optional[str] = None # Added to indicate error in analysis
    strategic_initiatives: Optional[List[StrategicInitiative]] = None
    execution_plan: Optional[ExecutionPlan] = None

# ==============================================================================
# 5. POSITIVES MODELS
# ==============================================================================

class PositiveMention(BaseModel):
    description: str  # Clear statement describing what users like
    impact_score: float # 0-100, how much the positive mention impacts the overall sentiment score for the review
    quote: Optional[str] = None  # Representative user quote
    keywords: List[str] = Field(default_factory=list)  # Keywords for word cloud
    impact_area: Literal["functionality", "usability", "performance", "security", "support", "innovation", "other"]
    user_segments: List[str] = Field(default_factory=list)  # Which user segments particularly like this
    metrics: Optional[Dict[str, float]] = None  # Any relevant metrics (e.g. usage rate, satisfaction)

class ProductStrengths(BaseModel):
    """Top-level model for tracking wins and positive feedback"""
    error: Optional[str] = None
    positive_mentions: List[PositiveMention] = Field(default_factory=list)
    overall_satisfaction_score: float = Field(default=0.0)
    brand_advocacy_rate: float = Field(default=0.0)  # Percentage of users likely to recommend
    competitive_advantages: Dict[str, str] = Field(default_factory=dict)  # Area -> advantage description
    growth_opportunities: List[str] = Field(default_factory=list)  # Areas where positive feedback suggests growth potential


# ==============================================================================
# 6. RESPONSE MODELS
# ==============================================================================

class ResponseContext(BaseModel):
    related_issues: List[str]  # Issue IDs
    related_positives: List[str]  # Positive feedback IDs
    user_sentiment: float
    user_segment: Optional[str] = None
    platform: Optional[Literal["app_store", "play_store", "support_email", "twitter", "other"]] = None  # e.g., "app_store", "play_store", "support_email"

class ResponseTone(BaseModel):
    primary_tone: Literal["apologetic", "appreciative", "informative", "enthusiastic", "professional", "empathetic"]
    secondary_tone: Optional[Literal["apologetic", "appreciative", "informative", "enthusiastic", "professional", "empathetic", "neutral", "friendly", "casual"]] = None
    formality_level: Literal["casual", "neutral", "formal"]
    personalization_level: float  # 0-1, how personalized the response should be

class ActionCommitment(BaseModel):
    commitment_type: Literal["fix", "investigate", "implement", "consider", "clarify", "monitor"]
    timeline: Optional[str] = None
    confidence_level: float
    conditions: Optional[List[str]] = None

class ResponseStrategy(BaseModel):
    should_respond: bool
    priority: Literal["low", "medium", "high", "urgent"]
    key_points: List[str]
    tone: ResponseTone
    commitments: Optional[List[ActionCommitment]] = None
    public_response: bool  # Whether this can be shared publicly

class ResponseRecommendation(BaseModel):
    """Top-level model for response recommendations"""
    error: Optional[str] = None # Added to indicate error in analysis
    response_required: bool
    response_id: Optional[str] = None
    context: Optional[ResponseContext] = None
    strategy: Optional[ResponseStrategy] = None
    suggested_response: Optional[str] = None
    alternative_responses: Optional[List[str]] = None  # Different versions/tones
    response_guidelines: Optional[List[str]] = None  # Do's and Don'ts for this response
    follow_up_needed: Optional[bool] = None
    follow_up_timeline: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None  # Additional context like response channel, timing, etc.

class AppReviewAnalysis(BaseModel):
    """
    The complete, structured analysis for a single app review. This model has
    been validated against the example JSON to ensure structural correctness.
    """
    review_id: str
    app_id: Optional[str] = None
    review_created_at: Optional[datetime] = None
    content: Optional[str] = None
    score: Optional[float] = None
    sentiment: Optional[SentimentAnalysis] = None
    sentiment_attempts: int = 0
    # aspects: Optional[AspectAnalysis] = None
    #aspects_attempts: int = 0
    issues: Optional[IssueAnalysis] = None
    issues_attempts: int = 0
    #opportunities: Optional[Opportunities] = None
    #opportunities_attempts: int = 0
    #roadmap: Optional[Roadmap] = None
    #roadmap_attempts: int = 0
    positive_feedback: Optional[ProductStrengths] = None
    positive_feedback_attempts: int = 0
    response_recommendation: Optional[ResponseRecommendation] = None
    response_recommendation_attempts: int = 0

    @field_validator('content')
    @classmethod
    def validate_content(cls, v: Optional[str]) -> Optional[str]:
        """Validate that content is not empty if provided."""
        if v is not None and not v.strip():
            raise ValueError("Review content cannot be empty")
        return v.strip() if v else None

class Error(BaseModel):
    agent: str
    error_message: str

class MainState(BaseModel):
    review_analysis: AppReviewAnalysis
    review_analysis_request: ReviewAnalysisRequest
    node_history: List[Dict[str, Any]]
    current_step: str
    error: Optional[Error] = None

    @field_validator('review_analysis')
    @classmethod
    def validate_review_analysis(cls, v: AppReviewAnalysis, info: Dict[str, Any]) -> AppReviewAnalysis:
        """Ensure review content is consistent between request and analysis."""
        if 'review_analysis_request' in info.data:
            request = info.data['review_analysis_request']
            if not v.content and request.review_content:
                v.content = request.review_content
        return v



