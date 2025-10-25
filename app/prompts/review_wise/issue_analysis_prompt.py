from app.models.review_analysis_models import IssueAnalysis


def get_issue_analysis_prompt(review_content: str) -> IssueAnalysis:
    """Generate a prompt for issue analysis based on review content."""
    
    return f"""Based on the following app review, identify potential negative issues and action items to resolve them, as well as key words that are related to the issue.
Please note that issues are negative feedback only. Do not include any positive feedback.
If there is no negative feedback, return an empty list & mention in error field that there are no issues.
Review: {review_content}

Important Notes:
1. Issue types MUST be one of: ["bug", "performance", "feature_request", "ux_issue", "security", "other"]
2. Issue categorization guidelines:
   - "bug": Functionality not working as intended (crashes, errors, failures)
   - "performance": Speed, responsiveness, or resource usage issues
   - "feature_request": Missing features or functionality
   - "ux_issue": User interface, navigation, or usability problems
   - "security": Security or authentication related issues
   - "other": Only use if issue doesn't fit above categories
3. When listing an issue, state it from the negative perspective, it should not be like a suggestion.
e.g if a user says "Please add a feature to allow users to upload large files", the issue should be "Feature to upload large files is not working/missing".
4. Impact score is a number between 0 and 100, based on how much the issue impacts the overall sentiment score for the review.

For example, if a review says "The app crashes when uploading large files and dark mode doesn't work in settings", the analysis would be:
(This is just an example to demonstrate the structure)

{{
    "error": null,
    "issues": [
        {{
            "type": "bug",
            "impact_score": 80,
            "description": "App crashes when uploading large files",
            "severity": "high",
            "key_words": ["crash", "upload", "large files"],
            "snippet": ["The app crashes when uploading large files"],
            "actions": [
                {{
                    "type": "fix",
                    "description": "Investigate and fix file upload crashes",
                    "confidence": 0.9,
                    "estimated_effort": "medium",
                    "suggested_timeline": "short-term"
                }},
                {{
                    "type": "investigation",
                    "description": "Analyze crash logs for large file uploads",
                    "confidence": 0.85,
                    "estimated_effort": "low",
                    "suggested_timeline": "short-term"
                }}
            ]
        }},
        {{
            "type": "ux_issue",
            "impact_score": 50,
            "description": "Dark mode not working in settings menu",
            "severity": "medium",
            "key_words": ["dark mode", "settings", "not working"],
            "snippet": ["Dark mode not working in settings menu"],
            "actions": [
                {{
                    "type": "fix",
                    "description": "Fix dark mode functionality in settings",
                    "confidence": 0.85,
                    "estimated_effort": "low",
                    "suggested_timeline": "medium-term"
                }}
            ]
        }}
    ]
}}

Now analyze the provided review and return in the same format. Remember to use ONLY the allowed issue types listed above."""