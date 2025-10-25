from app.models.review_analysis_models import ReviewAnalysisRequest, Opportunities
from pydantic import BaseModel, Field



def get_opportunities_analysis_prompt(review_content: str) -> str:
    """Generate a prompt for opportunities analysis based on review content."""
    
    return f"""Based on the following app review, identify potential market opportunities.

Review: {review_content}

Analyze this review and provide opportunities in the following format:
{{
    "market_opportunities": [
        {{
            "name": "Opportunity name",
            "description": "Brief description",
            "priority": "high/medium/low",
            "potential_impact": "Description of impact",
            "confidence_score": 0.0 to 1.0
        }}
    ]
}}"""


