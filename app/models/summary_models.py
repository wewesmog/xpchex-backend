from typing import List, Optional, Dict, Literal, Union, Any
from pydantic import BaseModel, Field, field_validator
from datetime import date, datetime

# This file defines the Pydantic models for a complete, structured analysis
# of a single app review. It has been corrected to precisely match the structure
# of the provided example JSON object.

class ReviewAnalysisRequest(BaseModel):
    review_content: str
    review_id: str

    @field_validator('review_content')
    def validate_content(cls, v: str) -> str:
        """Validate that content is not empty."""
        if not v or not v.strip():
            raise ValueError("Review content cannot be empty")
        return v.strip()


# ==============================================================================
# 1. SENTIMENT AND EMOTION ANALYSIS MODELS
# ==============================================================================

class Span(BaseModel):
    start: int
    end: int

class SegmentSentiment(BaseModel):
    count: int = Field(default=0)
    label: str
    score: float
    confidence: float

class SentimentSegment(BaseModel):
    count: int = Field(default=0)
    text: str
    span: Span
    sentiment: SegmentSentiment

class SentimentDistribution(BaseModel):
    positive: float = Field(default=0.0)
    neutral: float = Field(default=0.0)
    negative: float = Field(default=0.0)

class OverallSentiment(BaseModel):
    score: float = Field(default=0.0)
    classification: Literal["positive", "negative", "neutral", "mixed"] = Field(default="neutral")
    confidence: float = Field(default=0.0)
    distribution: SentimentDistribution

class Emotion(BaseModel):
    emotion: str = Field(default="")
    confidence: float = Field(default=0.0)

class EmotionAnalysis(BaseModel):
    primary: Emotion = Field(default=Emotion())
    secondary: Optional[Emotion] = Field(default=None)
    emotion_scores: Dict[str, float] = Field(default={})

class SentimentAnalysis(BaseModel):
    # This is a single object in the JSON, not a list
    analysis_error: Optional[str] = Field(default=None)
    overall: Optional[OverallSentiment] = Field(default=None)
    emotions: Optional[EmotionAnalysis] = Field(default=None)
    segments: Optional[List[SentimentSegment]] = Field(default=None)

# ==============================================================================
# 2. FEATURE AND ASPECT ANALYSIS MODELS
# ==============================================================================

class Mention(BaseModel):
    text: str
    span: Span
    context: Optional[str] = None

class AspectSentiment(BaseModel):
    label: str
    score: float
    confidence: float

class IdentifiedFeature(BaseModel):
    name: str
    category: str
    mention_count: int = Field(default=0)
    sentiment: AspectSentiment
    mentions: List[Mention]
    importance_score: float

class Topic(BaseModel):
    name: str
    count: int = Field(default=0)
    confidence: float
    keywords: List[str]
    sentiment: str

class UserIntent(BaseModel):
    # This is a single object in the JSON, not a list
    primary: str
    secondary: Optional[str] = None
    confidence: float

class AspectAnalysis(BaseModel):
    # This is a single object in the JSON, not a list
    analysis_error: Optional[str] = None
    identified_features: List[IdentifiedFeature]
    topics: List[Topic]
    user_intent: UserIntent

# ==============================================================================
# 3. ISSUES AND ACTIONS MODELS
# ==============================================================================

class IssueContext(BaseModel):
    feature_area: str
    reproducibility: Optional[str] = None
    impact: str

class Issue(BaseModel):
    id: str
    type: Literal["bug", "performance", "feature_request", "ux_issue"] = None
    description: str
    occurrence_count: int = Field(default=1)
    severity: Literal["low", "medium", "high", "critical"]
    confidence: float
    context: IssueContext
    priority_score: float
    is_actionable: bool
    needs_investigation: bool
    similar_issues: List[str] = Field(default_factory=list)

class Action(BaseModel):
    issue_id: str
    type: Literal["fix", "improvement", "investigation", "user_communication"] = None
    description: str
    occurrence_count: int = Field(default=1)
    confidence: float
    estimated_effort: Literal["low", "medium", "high"]
    prerequisites: Optional[List[str]] = None
    suggested_timeline: Optional[str] = None


class MetricsImpact(BaseModel):
    user_retention: Optional[bool] = None
    app_rating: Optional[bool] = None
    user_acquisition: Optional[bool] = None

class BusinessImpact(BaseModel):
    severity: Literal["low", "medium", "high"]
    affected_areas: List[str]
    metrics_impact: MetricsImpact
    recommendation: str
    confidence: float  # Added to indicate confidence in impact assessment

class ActionItems(BaseModel):
    # This is a single object in the JSON, not a list
    analysis_error: Optional[str] = None
    issues: List[Issue]
    actions: Optional[List[Action]] = None
    business_impact: BusinessImpact
    unactionable_reason: Optional[str] = None

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
    metric: str
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
    potential_impact: Literal["low", "medium", "high"]
    affected_metrics: List[AffectedMetric]
    user_segments: List[str]
    competitive_position: CompetitivePosition

class Risk(BaseModel):
    description: str
    severity: Literal["low", "medium", "high"]
    mitigation: str

class Implementation(BaseModel):
    complexity: Literal["low", "medium", "high"]
    required_resources: List[str]
    estimated_timeline: str
    dependencies: List[str]
    risks: List[Risk]

class PrioritizationFactor(BaseModel):
    name: str
    weight: float
    score: float

class Prioritization(BaseModel):
    score: float
    factors: List[PrioritizationFactor]
    time_sensitivity: Literal["low", "medium", "high"]
    strategic_alignment: float

class MarketOpportunity(BaseModel):
    id: str
    type: Literal["feature_gap", "user_need", "competitive_advantage", "market_trend"] = None
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
    analysis_error: Optional[str] = None
    market_opportunities: List[MarketOpportunity]

# ==============================================================================
# 5. ROADMAP RECOMMENDATIONS MODELS
# ==============================================================================

class SourceDerivation(BaseModel):
    type: Literal["user_feedback", "competitor_analysis", "market_trend", "internal_data"] = None
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
    type: Literal["feature", "improvement", "fix", "innovation", "user_communication"] = None
    status: Literal["proposed", "planned", "in_progress", "completed"]
    source: Source
    timeline: RoadmapTimeline
    impact_assessment: RoadmapImpactAssessment
    prioritization: RoadmapPrioritization

class KeyDeliverable(BaseModel):
    initiative_id: str
    deliverable: str
    success_criteria: List[str]

class ResourceAllocation(BaseModel):
    team: str
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
    measurement_frequency: str
    data_source: str

class ExecutionPlan(BaseModel):
    quarters: List[QuarterPlan]
    success_metrics: List[SuccessMetric]

class Roadmap(BaseModel):
    # This is a single object in the JSON, not a list
    analysis_error: Optional[str] = None
    strategic_initiatives: List[StrategicInitiative]
    execution_plan: ExecutionPlan

# ==============================================================================
# POSITIVE FEEDBACK AND WINS MODELS
# ==============================================================================

class UserValue(BaseModel):
    description: str
    impact_area: Literal["productivity", "satisfaction", "efficiency", "delight", "cost_saving", "other"] = None
    user_quote: Optional[str] = None
    mention_count: int = Field(default=0)
    sentiment_score: float

class FeatureSuccess(BaseModel):
    feature_name: str
    success_metrics: Dict[str, float]  # e.g., {"usage_rate": 0.85, "satisfaction": 0.92}
    positive_mentions: List[str]
    user_segments: List[str]  # Which user segments are particularly happy
    competitive_advantage: Optional[str] = None

class UserDelight(BaseModel):
    description: str
    wow_factor: float  # How impressed users are (0-1)
    unexpected_benefit: Optional[str] = None  # Benefits users discovered beyond intended use
    viral_potential: float  # Likelihood of users sharing this (0-1)

class RetentionDriver(BaseModel):
    feature_or_aspect: str
    retention_impact: float
    user_stickiness_factor: float
    repeat_usage_rate: Optional[float] = None
    user_testimonials: List[str]

class SuccessStory(BaseModel):
    title: str
    description: str
    impact: str
    supporting_metrics: Dict[str, float]
    user_segment: str
    replication_potential: float  # How easily this success can be replicated in other areas

class ProductStrengths(BaseModel):
    """Top-level model for tracking wins and positive feedback"""
    analysis_error: Optional[str] = None
    key_values: List[UserValue] = Field(default_factory=list)
    successful_features: List[FeatureSuccess] = Field(default_factory=list)
    delight_factors: List[UserDelight] = Field(default_factory=list)
    retention_drivers: List[RetentionDriver] = Field(default_factory=list)
    success_stories: List[SuccessStory] = Field(default_factory=list)
    competitive_edges: Dict[str, str] = Field(default_factory=dict)
    satisfaction_score: float = Field(default=0.0)
    brand_advocates_percentage: float = Field(default=0.0)
    growth_opportunities: List[str] = Field(default_factory=list)


# ==============================================================================
# RESPONSE RECOMMENDATIONS MODEL
# ==============================================================================

class ResponseContext(BaseModel):
    related_issues: List[str]  # Issue IDs
    related_positives: List[str]  # Positive feedback IDs
    user_sentiment: float
    user_segment: Optional[str] = None
    platform: Optional[str] = None  # e.g., "app_store", "play_store", "support_email"

class ResponseTone(BaseModel):
    primary_tone: Literal["apologetic", "appreciative", "informative", "enthusiastic", "professional"] = None
    secondary_tone: Optional[str] = None
    formality_level: Literal["casual", "neutral", "formal"]
    personalization_level: float  # 0-1, how personalized the response should be

class ActionCommitment(BaseModel):
    commitment_type: Literal["fix", "investigate", "implement", "consider", "clarify"] = None
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
    analysis_error: Optional[str] = None
    response_required: bool
    response_id: Optional[str] = None
    context: Optional[ResponseContext] = None
    strategy: Optional[ResponseStrategy] = None
    suggested_response: Optional[str] = None
    alternative_responses: Optional[List[str]] = None
    response_guidelines: Optional[List[str]] = None
    follow_up_needed: Optional[bool] = None
    follow_up_timeline: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None


# ==============================================================================
# FINAL TOP-LEVEL MODEL
# ==============================================================================

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
    sentiment_attempts: int
    aspects: Optional[AspectAnalysis] = None
    aspects_attempts: int
    action_items: Optional[ActionItems] = None
    action_items_attempts: int
    opportunities: Optional[Opportunities] = None
    opportunities_attempts: int
    roadmap: Optional[Roadmap] = None
    roadmap_attempts: int
    positive_feedback: Optional[ProductStrengths] = None
    positive_feedback_attempts: int
    response_recommendation: Optional[ResponseRecommendation] = None
    response_recommendation_attempts: int

    @field_validator('content')
    def validate_content(cls, v: Optional[str]) -> Optional[str]:
        """Validate that content is not empty if provided."""
        if v is not None and not v.strip():
            raise ValueError("Review content cannot be empty")
        return v.strip() if v else None

class DailySummaryError(BaseModel):
    """Error type specific to daily summary processing"""
    agent: str
    error_message: str

class ProcessingError(BaseModel):
    """Error type for general processing failures"""
    agent: str
    error_message: str

class MainState(BaseModel):
    review_analysis: AppReviewAnalysis
    review_analysis_request: ReviewAnalysisRequest
    node_history: List[Dict[str, Any]]
    current_step: str
    error: Optional[DailySummaryError] = None

# New Summary specific models
class IssueGroupSummary(BaseModel):
    normalized_description: str
    count: int = Field(default=0)
    similar_descriptions: List[str] = Field(default_factory=list)
    severity_distribution: Dict[str, int] = Field(default_factory=dict)
    confidence_sum: float = Field(default=0.0)
    avg_priority_score: float = Field(default=0.0)
    feature_areas: Dict[str, int] = Field(default_factory=dict)

class FeatureAreaSummary(BaseModel):
    total_issues: int = Field(default=0)
    severity_distribution: Dict[str, int] = Field(default_factory=dict)
    issue_types: Dict[str, int] = Field(default_factory=dict)
    avg_sentiment: float = Field(default=0.0)

class ActionSummary(BaseModel):
    type: str
    count: int = Field(default=0)
    effort_distribution: Dict[str, int] = Field(default_factory=dict)
    timeline_distribution: Dict[str, int] = Field(default_factory=dict)
    related_issues: List[str] = Field(default_factory=list)

class DailySummary(BaseModel):
    """Top-level model for daily aggregation of reviews"""
    summary_date: date = Field(default_factory=date.today)
    app_id: str
    daily_summary_statement: str = Field(default="")
    sentiment_distribution: SentimentDistribution
    issue_groups: List[IssueGroupSummary] = Field(default_factory=list)
    feature_areas: Dict[str, FeatureAreaSummary] = Field(default_factory=dict)
    actions: List[ActionSummary] = Field(default_factory=list)
    business_impact: BusinessImpact
    error: Optional[DailySummaryError] = None



class DailySummaryRequest(BaseModel):
    review_date: date
    app_id: str
    reviews: List[dict]


class DailySummaryState(BaseModel):
    daily_summary: DailySummary
    daily_summary_request: DailySummaryRequest
    node_history: List[Dict[str, Any]]
    current_step: str
    error: Optional[DailySummaryError] = None



