from app.models.review_analysis_models import ReviewAnalysisRequest, AspectAnalysis
from pydantic import BaseModel, Field



def get_aspect_analysis_prompt(review_content: str) -> str:
    # Get the schema first
    schema = AspectAnalysis.model_json_schema()
    
    # Create the example output
    example_output = {
        "error": None,
        "identified_features": [
            {
                "name": "User Interface",
                "category": "UI/UX",
                "sentiment": {
                    "label": "positive",
                    "score": 0.8,
                    "confidence": 0.9
                },
                "mentions": [
                    {
                        "text": "new UI is beautiful and easy to use",
                        "span": {"start": 4, "end": 35},
                        "context": "The new UI is beautiful and easy to use"
                    },
                    {
                        "text": "new dark mode feature",
                        "span": {"start": 100, "end": 121},
                        "context": "Love the new dark mode feature!"
                    }
                ],
                "importance_score": 0.85
            },
            {
                "name": "Photo Upload",
                "category": "Functionality",
                "sentiment": {
                    "label": "negative",
                    "score": -0.7,
                    "confidence": 0.85
                },
                "mentions": [
                    {
                        "text": "crashes when uploading photos",
                        "span": {"start": 50, "end": 77},
                        "context": "app crashes when uploading photos"
                    }
                ],
                "importance_score": 0.9
            },
            {
                "name": "Battery Usage",
                "category": "Performance",
                "sentiment": {
                    "label": "negative",
                    "score": -0.8,
                    "confidence": 0.9
                },
                "mentions": [
                    {
                        "text": "drains my battery too fast",
                        "span": {"start": 82, "end": 108},
                        "context": "drains my battery too fast"
                    }
                ],
                "importance_score": 0.95
            }
        ],
        "topics": [
            {
                "name": "App Performance",
                "confidence": 0.85,
                "keywords": ["crashes", "battery drain"],
                "sentiment": "negative"
            },
            {
                "name": "User Interface",
                "confidence": 0.9,
                "keywords": ["UI", "beautiful", "easy to use", "dark mode"],
                "sentiment": "positive"
            }
        ],
        "user_intent": {
            "primary": "report_issue",
            "secondary": "provide_feedback",
            "confidence": 0.85
        }
    }
    
    # Create the prompt
    return f"""
    Agent Name: aspect_analysis_agent
    Description: You are an expert in analyzing features, topics, and user intent from app reviews.

    Review to analyze:
    {review_content}



    Task: Analyze the features, topics, and user intent in this review. Your analysis must be detailed, evidence-based, and strictly adhere to the `AspectAnalysis` model.

    Required Output Format:
    You must return a JSON object that strictly conforms to the `AspectAnalysis` model. The JSON schema is:
    {schema}

    Example Review: "The new UI is beautiful and easy to use. However, the app crashes when uploading photos and drains my battery too fast. Love the new dark mode feature!"
    
    Example JSON Output:
    {example_output}

    **CRITICAL REMINDER:** The above example is for structural reference only. Your entire analysis and all field values **MUST** be derived *exclusively* from the user's review provided under 'Review to analyze'. Do not copy or use any data from the example output. Do not constrain free-text fields like 'description' based on the example.

    Validation Rules:
    1.  The output must be a single, valid JSON object.
    2.  Every feature must have at least one text mention with a correct span.
    3.  Feature sentiments must be justified by the mention context.
    4.  If you cannot meet these requirements or are unsure, populate the `error` field with a descriptive message and leave other fields as empty lists.

    Return your analysis in the exact format shown in the example.
    """


