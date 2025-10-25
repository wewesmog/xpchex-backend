from openai import OpenAI
from typing import List, Dict, Any, Optional
import os
from dotenv import load_dotenv
from pydantic import BaseModel
import instructor
from groq import Groq
#from google.generativeai import configure, GenerativeModel
#import google.generativeai as genai
from instructor import patch
import logging


load_dotenv()

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

# Initialize OpenAI client with instructor for structured outputs
openai_client = instructor.patch(OpenAI(api_key=OPENAI_API_KEY), mode=instructor.Mode.JSON)

# Configure Google Gemini
# genai.configure(api_key=os.getenv("GOOGLE_API_KEY"))

def call_llm_api_openai(messages: List[Dict[str, str]], 
                model: str = "gpt-4o",
                response_format: Optional[BaseModel] = None,
                temperature: float = 0.3) -> Any:
    """
    Make a call to the OpenAI API for chat completions.
    """
    try:
        # If a response model is provided, use it for structured output
        if response_format:
            response = openai_client.chat.completions.create(
                model=model,
                messages=messages,
                temperature=temperature,
                response_model=response_format,
                max_retries=3
            )
            # Return the parsed response directly
            return response
        else:
            # For unstructured responses
            response = openai_client.chat.completions.create(
                model=model,
                messages=messages,
                temperature=temperature,
                max_retries=3
            )
            return response.choices[0].message.content
    except Exception as e:
        print(f"Error in OpenAI API call: {e}")
        raise

    # Groq API


# Patch Groq() with instructor, this is where the magic happens!
groq_client = instructor.from_groq(Groq(api_key=os.getenv("GROQ_API_KEY")), mode=instructor.Mode.JSON)

def call_llm_api_1(messages: List[Dict[str, str]],
                model: str = "llama3-70b-8192",
                response_format: Optional[BaseModel] = None,
                max_tokens: int = 2000,
                temperature: float = 0.3) -> Any:
    """
    Make a call to the Groq API for chat completions.
    """
    try:
        # If a response model is provided, use it for structured output
        if response_format:
            response = groq_client.chat.completions.create(
                model=model,
                messages=messages,
                max_tokens=max_tokens,
                temperature=temperature,
                response_model=response_format,
                max_retries=3
            )
            # Return the parsed response directly
            return response
        else:
            # For unstructured responses
            response = groq_client.chat.completions.create(
                model=model,
                messages=messages,
                max_tokens=max_tokens,
                temperature=temperature,
                max_retries=3
            )
            return response.choices[0].message.content
    except Exception as e:
        print(f"Error in Groq API call: {e}")
        raise


# OpenRouter API

openrouter_client = OpenAI(
    base_url="https://openrouter.ai/api/v1",
    api_key=os.getenv("OPENROUTER_API_KEY"),
)

# Patch OpenRouter client with instructor for structured outputs
openrouter_client = instructor.patch(openrouter_client, mode=instructor.Mode.JSON)

def call_llm_api(messages: List[Dict[str, str]],
                model: str = "google/gemini-2.5-flash-lite-preview-06-17",
                response_format: Optional[BaseModel] = None,
                max_tokens: int = 8000,
                temperature: float = 0.3) -> Any:
    """
    Make a call to the OpenRouter API for chat completions with structured output support.
    Args:
        messages: List of message dictionaries
        model: Model to use (default: gemini-2.5-flash-lite-preview-06-17)
        response_format: Optional Pydantic model for structured output
        max_tokens: Maximum tokens in response
        temperature: Temperature for response generation
    Returns:
        Either structured output matching response_format or raw text response
    """
    try:
        # If a response model is provided, use it for structured output
        if response_format:
            response = openrouter_client.chat.completions.create(
                model=model,
                messages=messages,
                max_tokens=max_tokens,
                temperature=temperature,
                response_model=response_format,
                max_retries=3,
                extra_headers={
                    "HTTP-Referer": "https://mwalimu.ai", # Optional. Site URL for rankings on openrouter.ai.
                    "X-Title": "Mwalimu", # Optional. Site title for rankings on openrouter.ai.
                }
            )  # Close the create() call
    
            return response
        else:
            # For unstructured responses
            response = openrouter_client.chat.completions.create(
                model=model,
                messages=messages,
                max_tokens=max_tokens,
                temperature=temperature,
                max_retries=3
            )
            return response.choices[0].message.content
    except Exception as e:
        logger.error(f"Error in OpenRouter API call: {e}")
        raise


# Patch instructor with gemini

# gemini_client = genai.Client(api_key=os.getenv("GOOGLE_API_KEY"))
# async def call_llm_api_gemini(
#     messages: List[Dict[str, str]], 
#     model: str = "gemini-pro",
#     response_format: Optional[BaseModel] = None,
#     temperature: float = 0.3
# ) -> Any:
#     """
#     Make a call to the Gemini API for chat completions.
#     """
#     try:
#         # Create model instance
#         model = genai.GenerativeModel('gemini-pro')
        
#         # Convert messages to Gemini format
#         chat = model.start_chat(history=[])
        
#         # Add message history
#         for message in messages[:-1]:  # All messages except the last one
#             if message["role"] == "user":
#                 chat.send_message(message["content"])
#             # Assistant messages are automatically handled by the response
        
#         # If a response model is provided, use instructor for structured output
#         if response_format:
#             # Use instructor's Gemini integration
#             patched_chat = instructor.patch(chat, mode=instructor.Mode.TOOLS)
#             response = patched_chat.send_message(
#                 messages[-1]["content"],
#                 response_model=response_format
#             )
#             return response
#         else:
#             # For unstructured responses
#             response = chat.send_message(messages[-1]["content"])
#             return response.text
            
#     except Exception as e:
#         print(f"Error in Gemini API call: {e}, now falling back to OpenAI API")
#         return call_llm_api(messages, model="gpt-4o-mini", response_format=response_format, temperature=temperature)

