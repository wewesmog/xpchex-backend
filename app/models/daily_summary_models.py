from typing import List, Optional, Dict, Literal, Union, Any
from pydantic import BaseModel, Field
from datetime import date, datetime

# ==============================================================================
# 1. ASPECTS DAILY SUMMARY MODELS
# ==============================================================================

class ReviewContent(BaseModel):
    content: str

class Mention(BaseModel):
    text: str
    count: int = 1  # Track how many times this exact mention appears

class AspectSentiment(BaseModel):
    label: str
    count: int = 1  # Track how many times this sentiment appears

class IdentifiedFeature(BaseModel):
    name: str
    sentiment: AspectSentiment
    mentions: List[Mention]
    importance_score: float
    occurrence_count: int = 1  # Track how many times this feature appears

class IdentifiedFeatureCategory(BaseModel):
    name: str
    features: List[IdentifiedFeature]
    count: int = 1  # Track category occurrences

class Topic(BaseModel):
    name: str
    keywords: List[str]
    sentiment: str
    occurrence_count: int = 1  # Track how many times this topic appears
    keyword_counts: Dict[str, int] = Field(default_factory=dict)  # Track counts for each keyword

class UserIntent(BaseModel):
    # List of intents with their counts
    intents: Dict[str, int] = Field(default_factory=dict)  # Maps intent to count
    total_intents: int = 0  # Total number of intents processed

class AspectAnalysis(BaseModel):
    # This is a single object in the JSON, not a list
    analysis_error: Optional[str] = None  # Changed from 'error' to be more specific
    features: List[IdentifiedFeature]  # Changed from 'identified_features' to be more concise
    topic_list: List[Topic]  # Changed from 'topics' to avoid potential clash
    intent_data: UserIntent  # Changed from 'user_intent' to be more specific
    total_reviews_analyzed: int = 0  # Track total number of reviews processed
    review_contents: List[ReviewContent] = Field(default_factory=list)  # Add review contents here
    analysis_date: date = Field(default_factory=date.today)  # Changed from 'date' to be more specific
    app_id: str

# ==============================================================================
# Roadmap Models
# ==============================================================================

class Milestone(BaseModel):
    title: str
    description: str
    target_date: str

class ResourceAllocation(BaseModel):
    team: str
    allocation_percentage: int

class SuccessMetric(BaseModel):
    metric: str
    data_source: str
    target_value: float
    current_value: float
    measurement_frequency: str

class KeyDeliverable(BaseModel):
    deliverable: str
    initiative_id: str
    success_criteria: List[str]

class Quarter(BaseModel):
    theme: str
    quarter: str
    key_deliverables: List[KeyDeliverable]
    resource_allocation: List[ResourceAllocation]

class ExecutionPlan(BaseModel):
    quarters: List[Quarter]
    success_metrics: List[SuccessMetric]

class ImpactMetric(BaseModel):
    name: str
    predicted_change: float

class BusinessGoal(BaseModel):
    goal: str
    metrics: List[ImpactMetric]
    impact_score: float

class UserExperience(BaseModel):
    affected_journeys: List[str]
    improvement_areas: List[str]
    predicted_satisfaction_impact: float

class ImpactAssessment(BaseModel):
    business_goals: List[BusinessGoal]
    risk_assessment: List[Any] = Field(default_factory=list)
    user_experience: UserExperience

class PrioritizationFactor(BaseModel):
    name: str
    score: float
    weight: float
    justification: str

class Prioritization(BaseModel):
    factors: List[PrioritizationFactor]
    overall_score: float
    cost_benefit_ratio: float
    strategic_alignment: float

class Source(BaseModel):
    derived_from: List[Dict[str, Any]]
    supporting_metrics: List[Any] = Field(default_factory=list)

class Timeline(BaseModel):
    phase: str
    milestones: List[Milestone]
    dependencies: List[str]
    target_quarter: str
    estimated_duration: str

class StrategicInitiative(BaseModel):
    id: str
    type: str
    title: str
    source: Source
    status: str
    timeline: Timeline
    description: str
    prioritization: Prioritization
    impact_assessment: ImpactAssessment

class RoadmapAnalysis(BaseModel):
    analysis_error: Optional[str] = None
    execution_plan: ExecutionPlan
    strategic_initiatives: List[StrategicInitiative]

# ==============================================================================
# Sentiment Models
# ==============================================================================

class SentimentSpan(BaseModel):
    start: int
    end: int

class SentimentSegment(BaseModel):
    text: str
    sentiment: Dict[str, float]
    count: int = Field(default=1)  # Number of times this segment appeared

class Emotion(BaseModel):
    emotion: str
    confidence: float

class EmotionAnalysis(BaseModel):
    primary: Emotion
    secondary: Emotion
    emotion_scores: Dict[str, float]

class OverallSentiment(BaseModel):
    score: float
    confidence: float
    distribution: Dict[str, float]
    classification: str

class SentimentAnalysis(BaseModel):
    analysis_error: Optional[str] = None
    overall: OverallSentiment
    emotions: EmotionAnalysis
    segments: List[SentimentSegment]

# ==============================================================================
# Action Items Models
# ==============================================================================

class IssueContext(BaseModel):
    impact: str
    feature_area: str
    reproducibility: Optional[str]

class Issue(BaseModel):
    id: str
    type: str
    context: IssueContext
    severity: str
    confidence: float
    description: str
    is_actionable: bool
    priority_score: float
    needs_investigation: bool

class Action(BaseModel):
    type: str
    issue_id: str
    confidence: float
    description: str
    prerequisites: List[str]
    estimated_effort: str
    suggested_timeline: str

class BusinessImpact(BaseModel):
    severity: str
    confidence: float
    affected_areas: List[str]
    metrics_impact: Dict[str, bool]
    recommendation: str

class ActionItemsAnalysis(BaseModel):
    analysis_error: Optional[str] = None
    issues: List[Issue]
    actions: List[Action]
    business_impact: BusinessImpact
    unactionable_reason: Optional[str]

# ==============================================================================
# Opportunities Models
# ==============================================================================

class ReviewSnippet(BaseModel):
    text: str
    review_id: str
    sentiment: float

class Evidence(BaseModel):
    review_count: int
    user_sentiment: float
    review_snippets: List[ReviewSnippet]
    mention_frequency: float

class Risk(BaseModel):
    severity: str
    mitigation: str
    description: str

class Implementation(BaseModel):
    risks: List[Risk]
    complexity: str
    dependencies: List[str]
    estimated_timeline: str
    required_resources: List[str]

class ImpactMetrics(BaseModel):
    metric: str
    confidence: float
    predicted_impact: float

class CompetitivePosition(BaseModel):
    current_state: str
    potential_advantage: str
    competitor_comparison: List[Dict[str, str]]

class ImpactAnalysis(BaseModel):
    user_segments: List[str]
    affected_metrics: List[ImpactMetrics]
    potential_impact: str
    competitive_position: CompetitivePosition

class MarketOpportunity(BaseModel):
    id: str
    type: str
    title: str
    evidence: Evidence
    description: str
    implementation: Implementation
    prioritization: Dict[str, Any]
    impact_analysis: ImpactAnalysis
    confidence_score: float

class OpportunitiesAnalysis(BaseModel):
    analysis_error: Optional[str] = None
    market_opportunities: List[MarketOpportunity]

# ==============================================================================
# Response Recommendation Models
# ==============================================================================

class ResponseContext(BaseModel):
    platform: Optional[str]
    user_segment: Optional[str]
    related_issues: List[str]
    user_sentiment: float
    related_positives: List[str]

class ResponseTone(BaseModel):
    primary_tone: str
    secondary_tone: str
    formality_level: str
    personalization_level: float

class ResponseCommitment(BaseModel):
    timeline: str
    conditions: Optional[str]
    commitment_type: str
    confidence_level: float

class ResponseStrategy(BaseModel):
    tone: ResponseTone
    priority: str
    key_points: List[str]
    commitments: List[ResponseCommitment]
    should_respond: bool
    public_response: bool

class ResponseRecommendation(BaseModel):
    analysis_error: Optional[str] = None
    context: ResponseContext
    metadata: Dict[str, Any] = Field(default_factory=dict)
    strategy: ResponseStrategy
    response_id: Optional[str]
    follow_up_needed: bool
    response_required: bool
    follow_up_timeline: str
    suggested_response: str
    response_guidelines: List[str]
    alternative_responses: List[str]

# ==============================================================================
# Positive Feedback Models
# ==============================================================================

class KeyValue(BaseModel):
    frequency: int
    user_quote: str
    description: str
    impact_area: str
    sentiment_score: float

class DelightFactor(BaseModel):
    wow_factor: float
    description: str
    viral_potential: float
    unexpected_benefit: Optional[str] = None  # Made optional to match UserDelight model

class SuccessStoryMetrics(BaseModel):
    time_saved_hours: Optional[float] = None  # Changed from int to float and made optional
    increased_engagement_percentage: Optional[float] = None  # Changed from int to float and made optional

class SuccessStory(BaseModel):
    title: str
    impact: str
    description: str
    user_segment: str
    supporting_metrics: SuccessStoryMetrics
    replication_potential: float

class FeatureSuccessMetrics(BaseModel):
    efficiency_score: Optional[float] = None
    time_saved_minutes: Optional[float] = None
    satisfaction_score: Optional[float] = None
    usage_rate: Optional[float] = None
    usability_score: Optional[float] = None
    time_saving_score: Optional[float] = None

class SuccessfulFeature(BaseModel):
    feature_name: str
    user_segments: List[str]
    success_metrics: FeatureSuccessMetrics
    positive_mentions: List[str]
    competitive_advantage: str

class RetentionDriver(BaseModel):
    retention_impact: float
    feature_or_aspect: str
    repeat_usage_rate: Optional[float]
    user_testimonials: List[str]
    user_stickiness_factor: float

class PositiveFeedbackAnalysis(BaseModel):
    analysis_error: Optional[str] = None
    key_values: List[KeyValue]
    delight_factors: List[DelightFactor]
    success_stories: List[SuccessStory]
    competitive_edges: Dict[str, str]
    retention_drivers: List[RetentionDriver]
    satisfaction_score: float
    successful_features: List[SuccessfulFeature]
    growth_opportunities: List[str]
    brand_advocates_percentage: float


