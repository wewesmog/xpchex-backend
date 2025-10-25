import json
from typing import List, Dict, Any, Optional, TypedDict, Union

def extract_and_parse_json(text: str) -> dict:
    """
    Extracts and parses JSON from text with detailed error handling and debugging
    """
    try:
        # Debug: Print original text
        print("\nOriginal text:")
        print(text)
        
        # 1. Find JSON boundaries
        start_index = text.find('{')
        end_index = text.rfind('}')
        
        if start_index == -1 or end_index == -1:
            raise ValueError("No JSON object found in text")
            
        # 2. Extract JSON content
        json_content = text[start_index:end_index + 1]
            # Debug: Print extracted content
        print("\nExtracted JSON content:")
        print(json_content)
        
        # 3. Clean the content
        # Remove any markdown formatting
        cleaned_content = json_content.replace("```json", "").replace("```", "")
        # Remove any potential unicode or special characters
        cleaned_content = cleaned_content.encode('ascii', 'ignore').decode()
        # Remove any whitespace at the start/end
        cleaned_content = cleaned_content.strip()
        
        # Debug: Print cleaned content
        print("\nCleaned JSON content:")
        print(cleaned_content)
        
        # 4. Parse JSON
        parsed_json = json.loads(cleaned_content)
        
        # Debug: Print parsed JSON
        print("\nParsed JSON:")
        print(json.dumps(parsed_json, indent=2))
        
        return parsed_json
    except json.JSONDecodeError as e:
        print(f"\nJSON Parsing Error: {str(e)}")
        print(f"Error at position {e.pos}: {e.msg}")
        print(f"Line: {e.lineno}, Column: {e.colno}")
        raise
    except Exception as e:
        print(f"\nGeneral Error: {str(e)}")
        raise