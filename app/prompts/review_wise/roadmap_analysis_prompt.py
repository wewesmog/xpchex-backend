from app.models.review_analysis_models import Roadmap

def get_roadmap_analysis_prompt(review_content: str) -> str:
    """Generate a prompt for roadmap analysis based on review content."""
    
    return f"""Based on the following app review, analyze potential roadmap implications.

Review: {review_content}

Analyze this review and provide roadmap suggestions in the following format:
{{
    "error": null,
    "strategic_initiatives": [
        {{
            "id": "SI-001",
            "title": "Initiative title",
            "description": "Detailed description",
            "type": "feature|improvement|fix|innovation|research|other",
            "status": "proposed|planned|in_progress|completed|on_hold|cancelled",
            "source": {{
                "derived_from": [
                    {{
                        "type": "user_feedback",
                        "reference_id": "ref-001",
                        "confidence": 0.8
                    }}
                ],
                "supporting_metrics": [
                    {{
                        "metric_name": "metric name",
                        "current_value": 0.0,
                        "target_value": 0.0,
                        "impact_confidence": 0.8
                    }}
                ]
            }},
            "timeline": {{
                "phase": "short_term|medium_term|long_term",
                "estimated_duration": "duration in weeks/months",
                "target_quarter": "Q1/Q2/Q3/Q4",
                "dependencies": ["dep1", "dep2"],
                "milestones": [
                    {{
                        "title": "milestone title",
                        "description": "milestone description",
                        "target_date": "Q1 2025"
                    }}
                ]
            }},
            "impact_assessment": {{
                "business_goals": [
                    {{
                        "goal": "goal description",
                        "impact_score": 0.8,
                        "metrics": [
                            {{
                                "name": "metric name",
                                "predicted_change": 0.2
                            }}
                        ]
                    }}
                ],
                "user_experience": {{
                    "affected_journeys": ["journey1", "journey2"],
                    "improvement_areas": ["area1", "area2"],
                    "predicted_satisfaction_impact": 0.8
                }},
                "risk_assessment": [
                    {{
                        "risk_type": "technical|business|user|other",
                        "probability": 0.3,
                        "impact": 0.7,
                        "mitigation_strategy": "strategy description"
                    }}
                ]
            }},
            "prioritization": {{
                "overall_score": 0.8,
                "factors": [
                    {{
                        "name": "user_impact",
                        "weight": 0.4,
                        "score": 0.8,
                        "justification": "reason for score"
                    }}
                ],
                "strategic_alignment": 0.8,
                "cost_benefit_ratio": 0.7
            }}
        }}
    ],
    "execution_plan": {{
        "quarters": [
            {{
                "quarter": "Q1",
                "theme": "theme description",
                "key_deliverables": [
                    {{
                        "initiative_id": "SI-001",
                        "deliverable": "deliverable description",
                        "success_criteria": ["criteria1", "criteria2"]
                    }}
                ],
                "resource_allocation": [
                    {{
                        "team": "engineering|product|design|qa|marketing|other",
                        "allocation_percentage": 0.4
                    }}
                ]
            }}
        ],
        "success_metrics": [
            {{
                "metric": "metric name",
                "current_value": 0.0,
                "target_value": 0.0,
                "measurement_frequency": "daily|weekly|monthly|quarterly",
                "data_source": "data source description"
            }}
        ]
    }}
}}"""
