from app.models.review_analysis_models import ReviewAnalysisRequest, ProductStrengths
from pydantic import BaseModel, Field

def get_positives_analysis_prompt(review_content: str) -> str:
    """Generate a prompt for positive feedback analysis based on review content."""
    
    return f"""Based on the following app review, identify positive feedback and strengths.

Review: {review_content}

For each positive aspect mentioned in the review:
1. Write a clear statement describing what users like
2. Extract relevant quotes that support this
3. Identify key words that could be used in a word cloud
4. Categorize the impact area
5. Note which user segments are particularly happy with this aspect
6. Include any relevant metrics if mentioned
7. Impact score is a number between 0 and 100, based on how much the positive mention impacts the overall sentiment score for the review.

For example, if a review says "Love the new dark mode and the app is much faster now. Great job on the UI!", the analysis would be:
(This is just an example to demonstrate the structure)

{{
    "positive_mentions": [
        {{
            "impact_score": 80,
            "description": "Users appreciate the new dark mode theme option which enhances visual comfort",
            "quote": "Love the new dark mode",
            "keywords": ["dark mode", "theme", "UI", "visual"],
            "impact_area": "usability",
            "user_segments": ["night-time users", "power users"],
            "metrics": {{"satisfaction": 0.9}}
        }},
        {{
            "impact_score": 50,
            "description": "The app's performance improvements have been well received",
            "quote": "app is much faster now",
            "keywords": ["faster", "performance", "speed", "improvement"],
            "impact_area": "performance",
            "user_segments": ["all users"],
            "metrics": {{"performance_satisfaction": 0.8}}
        }}
    ],
    "overall_satisfaction_score": 0.9,
    "brand_advocacy_rate": 0.8,
    "competitive_advantages": {{
        "UI/UX": "Strong visual customization options with dark mode",
        "Performance": "Notable speed improvements"
    }},
    "growth_opportunities": [
        "Could expand theme customization options",
        "Leverage performance improvements in marketing"
    ]
}}

Now analyze the provided review and return in the same format. Focus on:
1. Clear, actionable descriptions of what users like
2. Relevant quotes that capture the sentiment
3. Keywords that represent the positive aspects
4. Accurate categorization of impact areas
5. Identifying affected user segments
6. Any metrics mentioned or implied in the review"""