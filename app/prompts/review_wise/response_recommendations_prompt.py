from app.models.review_analysis_models import ResponseRecommendation

def get_response_recommendations_prompt(review_content: str) -> str:
    """Generate a prompt for response recommendations based on review content."""
    
    return f"""Based on the following app review, suggest appropriate response recommendations.

Review: {review_content}

For example, if a review says "App keeps crashing but customer support was very helpful", the analysis would be:
(This is just an example to demonstrate the structure)

{{
    "error": null,
    "response_required": true,
    "response_id": "RR-001",
    "context": {{
        "platform": "play_store",
        "user_segment": "active_user",
        "related_issues": ["app crashes"],
        "related_positives": ["customer support"],
        "user_sentiment": 0.5
    }},
    "strategy": {{
        "should_respond": true,
        "priority": "high",
        "key_points": [
            "Thank for feedback",
            "Acknowledge positive support experience",
            "Address crash issue"
        ],
        "tone": {{
            "primary_tone": "appreciative",
            "secondary_tone": "empathetic",
            "formality_level": "formal",
            "personalization_level": 0.8
        }},
        "commitments": [
            {{
                "commitment_type": "fix",
                "timeline": "next release",
                "confidence_level": 0.9,
                "conditions": ["after thorough testing"]
            }}
        ],
        "public_response": true
    }},
    "suggested_response": "Thank you for your feedback! We're glad our support team could help. We're actively working on fixing the crash issues you've experienced.",
    "alternative_responses": [
        "We appreciate your feedback and are happy our support team was helpful. Our engineers are investigating the crash issues."
    ],
    "response_guidelines": [
        "Be empathetic about technical issues",
        "Highlight positive aspects"
    ],
    "follow_up_needed": true,
    "follow_up_timeline": "1 week",
    "metadata": {{
        "response_channel": "play_store",
        "priority_level": "high",
        "response_category": "technical_issue"
    }}
}}

Now analyze the provided review and return in the same format. Note that formality_level MUST be one of: 'casual', 'neutral', or 'formal'."""