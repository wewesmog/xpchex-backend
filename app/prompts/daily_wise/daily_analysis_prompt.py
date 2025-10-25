from app.models.summary_models import DailySummary, DailySummaryRequest
from pydantic import BaseModel, Field


def get_daily_analysis_prompt(daily_summary_request: DailySummaryRequest) -> str:
    prompt = """
    Agent Name: daily_analysis_agent
    Description: You are an expert in summarizing daily app review analysis data into a concise, actionable daily summary.

    Daily Summary Request:
    {daily_summary_request.model_dump_json(indent=2)}

    Task: Generate a comprehensive daily summary of app review analysis data. Your analysis must be detailed, evidence-based, and strictly adhere to the `DailySummary` model.

    Required Output Format:
    You must return a JSON object that strictly conforms to the `DailySummary` model. The JSON schema is:
    {DailySummary.model_json_schema(indent=2)}

    Rules:
    1.  All fields must be present. Use empty lists (`[]`) or empty objects (`{}`) if no data is available.
    2.  All scores must be between 0.0 and 1.0.
    3.  Ensure all counts and distributions are accurate based on the provided `daily_summary_request`.
    4.  If you cannot complete the analysis, populate the `error` field with a descriptive message and leave other fields as empty lists or null.

    Example Daily Summary Request:
    {
        "summary_date": "2025-06-27",
        "app_id": "com.example.app",
        "review_analysis": [
            {
                "review_id": "rev_1",
                "sentiment": {
                    "overall": {
                        "score": 0.8,
                        "classification": "positive",
                        "confidence": 0.9,
                        "distribution": {"positive": 0.8, "neutral": 0.1, "negative": 0.1}
                    }
                },
                "aspects": {
                    "identified_features": [
                        {
                            "name": "UI",
                            "category": "User Experience",
                            "sentiment": {"label": "positive", "score": 0.7, "confidence": 0.8},
                            "mentions": [],
                            "importance_score": 0.8
                        }
                    ],
                    "topics": [],
                    "user_intent": {"primary": "praise", "confidence": 0.9}
                },
                "action_items": {
                    "issues": [],
                    "actions": [],
                    "business_impact": {
                        "severity": "low",
                        "affected_areas": [],
                        "metrics_impact": {"user_retention": null, "app_rating": null, "user_acquisition": null},
                        "recommendation": "",
                        "confidence": 0.0
                    }
                },
                "opportunities": {"market_opportunities": []},
                "roadmap": {"strategic_initiatives": [], "execution_plan": {"quarters": [], "success_metrics": []}},
                "positive_feedback": {
                    "key_values": [
                        {
                            "description": "Great UI",
                            "impact_area": "satisfaction",
                            "user_quote": "The UI is so intuitive",
                            "frequency": 1,
                            "sentiment_score": 0.85
                        }
                    ],
                    "successful_features": [],
                    "delight_factors": [],
                    "retention_drivers": [],
                    "success_stories": [],
                    "competitive_edges": {},
                    "satisfaction_score": 0.9,
                    "brand_advocates_percentage": 0.7,
                    "growth_opportunities": []
                },
                "response_recommendation": {"response_required": false}
            },
            {
                "review_id": "rev_2",
                "sentiment": {
                    "overall": {
                        "score": -0.6,
                        "classification": "negative",
                        "confidence": 0.8,
                        "distribution": {"positive": 0.1, "neutral": 0.1, "negative": 0.8}
                    }
                },
                "aspects": {
                    "identified_features": [
                        {
                            "name": "Crashes",
                            "category": "Stability",
                            "sentiment": {"label": "negative", "score": -0.9, "confidence": 0.95},
                            "mentions": [],
                            "importance_score": 0.9
                        }
                    ],
                    "topics": [],
                    "user_intent": {"primary": "report_bug", "confidence": 0.95}
                },
                "action_items": {
                    "issues": [
                        {
                            "id": "ISS-001",
                            "type": "bug",
                            "description": "App crashes frequently",
                            "occurrence_count": 1,
                            "severity": "critical",
                            "confidence": 0.95,
                            "context": {"feature_area": "core_functionality", "impact": "app unusable"},
                            "priority_score": 0.95,
                            "is_actionable": true,
                            "needs_investigation": false
                        }
                    ],
                    "actions": [
                        {
                            "issue_id": "ISS-001",
                            "type": "fix",
                            "description": "Investigate and fix crash issues",
                            "occurrence_count": 1,
                            "confidence": 0.9,
                            "estimated_effort": "high",
                            "prerequisites": [],
                            "suggested_timeline": "1 week"
                        }
                    ],
                    "business_impact": {
                        "severity": "high",
                        "affected_areas": ["user_experience", "app_rating"],
                        "metrics_impact": {"user_retention": true, "app_rating": true, "user_acquisition": false},
                        "recommendation": "Prioritize fixing crashes",
                        "confidence": 0.95
                    }
                },
                "opportunities": {"market_opportunities": []},
                "roadmap": {"strategic_initiatives": [], "execution_plan": {"quarters": [], "success_metrics": []}},
                "positive_feedback": {
                    "key_values": [],
                    "successful_features": [],
                    "delight_factors": [],
                    "retention_drivers": [],
                    "success_stories": [],
                    "competitive_edges": {},
                    "satisfaction_score": 0.0,
                    "brand_advocates_percentage": 0.0,
                    "growth_opportunities": []
                },
                "response_recommendation": {"response_required": true}
            }
        ]
    }

    Example JSON Output:
    {
        "summary_date": "2025-06-27",
        "app_id": "com.example.app",
        "daily_summary_statement": "The app has a positive sentiment with a score of 0.45. The app has 1 issue group with a normalized description of 'App crashes frequently'. The issue group has a severity of 'critical' and a confidence sum of 0.95. The issue group has a feature area of 'core_functionality'.",
        "sentiment_distribution": {
            "positive": 0.45,
            "neutral": 0.1,
            "negative": 0.45
        },
        "issue_groups": [
            {
                "normalized_description": "App crashes frequently",
                "count": 1,
                "similar_descriptions": ["App crashes frequently"],
                "severity_distribution": {"critical": 1},
                "confidence_sum": 0.95,
                "avg_priority_score": 0.95,
                "feature_areas": {"core_functionality": 1}
            }
        ],
        "feature_areas": {
            "User Experience": {
                "total_issues": 0,
                "severity_distribution": {},
                "issue_types": {},
                "avg_sentiment": 0.8
            },
            "Stability": {
                "total_issues": 1,
                "severity_distribution": {"critical": 1},
                "issue_types": {"bug": 1},
                "avg_sentiment": -0.9
            }
        },
        "actions": [
            {
                "type": "fix",
                "count": 1,
                "effort_distribution": {"high": 1},
                "timeline_distribution": {"1 week": 1},
                "related_issues": ["ISS-001"]
            }
        ],
        "business_impact": {
            "severity": "high",
            "affected_areas": ["user_experience", "app_rating"],
            "metrics_impact": {"user_retention": true, "app_rating": true, "user_acquisition": false},
            "recommendation": "Prioritize fixing crashes",
            "confidence": 0.95
        }
    }

    Return valid JSON matching the required format."""
    return prompt