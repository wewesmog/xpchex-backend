from app.models.review_analysis_models import SentimentAnalysis, ReviewAnalysisRequest
from pydantic import BaseModel, Field



def get_sentiment_analysis_prompt(review_content: str) -> str:
    # Get the schema first
    schema = SentimentAnalysis.model_json_schema()
    
    # Create the base prompt
    base_prompt = f"""
    Agent Name: sentiment_analysis_agent
    Description: You are an expert in detailed sentiment and emotion analysis of app reviews.
    
    Review to analyze:
    {review_content}

    Task: Analyze the sentiment, emotions, and specific segments of this review. Your analysis must be detailed and evidence-based, and strictly adhere to the `SentimentAnalysis` model.

    Required Output Format:
    You must return a JSON object that strictly conforms to the `SentimentAnalysis` model. The JSON schema is:
    """
    
    # Create the example section
    example_section = """
    Example Input:
    review_content: "I love this app! It's so easy to use and has all the features I need. I'm so frustrated with the app because it keeps crashing. I'm so happy with the app because it's so easy to use."

    Example Output:
    {
        "error": null,
        "overall": {
            "score": 0.0,
            "classification": "mixed",
            "confidence": 0.9,
            "distribution": {
                "positive": 0.5,
                "neutral": 0.0,
                "negative": 0.5
            }
        },
        "emotions": {
            "primary": {
                "emotion": "joy",
                "confidence": 0.7
            },
            "secondary": {
                "emotion": "frustration",
                "confidence": 0.6
            },
            "emotion_scores": {
                "joy": 0.5,
                "frustration": 0.5
            }
        },
        "segments": [
            {
                "text": "I love this app! It's so easy to use and has all the features I need.",
                "span": {
                    "start": 0,
                    "end": 67
                },
                "sentiment": {
                    "label": "positive",
                    "score": 0.9,
                    "confidence": 0.95
                }
            },
            {
                "text": "I'm so frustrated with the app because it keeps crashing.",
                "span": {
                    "start": 68,
                    "end": 125
                },
                "sentiment": {
                    "label": "negative",
                    "score": -0.8,
                    "confidence": 0.9
                }
            },
            {
                "text": "I'm so happy with the app because it's so easy to use.",
                "span": {
                    "start": 126,
                    "end": 180
                },
                "sentiment": {
                    "label": "positive",
                    "score": 0.85,
                    "confidence": 0.92
                }
            }
        ]
    }

    **CRITICAL REMINDER:** The above example is for structural reference only. Your entire analysis and all field values **MUST** be derived *exclusively* from the user's review provided under 'Review to analyze'. Do not copy or use any data from the example output. Do not constrain free-text fields like 'description' based on the example.

    Validation Rules:
    1.  The output must be a single, valid JSON object.
    2.  Every sentiment score must be justified by specific text.
    3.  Segment spans must match the exact text location.
    4.  Emotion scores must sum to 1.0.
    5.  If you cannot complete the analysis, populate the `error` field with a descriptive message and leave other fields as empty lists or null.

    Return your analysis in the exact format shown in the example.
    """
    
    # Combine all parts
    return base_prompt + str(schema) + example_section
