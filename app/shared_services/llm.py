from openai import OpenAI, AsyncOpenAI
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
import asyncio
import time
from openai import APIError, RateLimitError

logger = logging.getLogger(__name__)


class QuotaExceededError(Exception):
    """Custom exception for OpenAI quota exceeded errors"""
    pass

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
    Raises QuotaExceededError for quota/429 errors to allow immediate stopping.
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
    except RateLimitError as e:
        # Check if it's a quota error (not just rate limit)
        error_str = str(e).lower()
        if 'quota' in error_str or 'insufficient_quota' in error_str:
            logger.error(f"OpenAI quota exceeded - stopping execution: {e}")
            raise QuotaExceededError(f"OpenAI quota exceeded: {e}") from e
        # For regular rate limits, still raise but don't stop execution
        logger.warning(f"OpenAI rate limit hit: {e}")
        raise
    except APIError as e:
        # Check for quota errors in APIError as well
        error_str = str(e).lower()
        if 'quota' in error_str or 'insufficient_quota' in error_str or (hasattr(e, 'code') and e.code == 'insufficient_quota'):
            logger.error(f"OpenAI quota exceeded - stopping execution: {e}")
            raise QuotaExceededError(f"OpenAI quota exceeded: {e}") from e
        logger.error(f"OpenAI API error: {e}")
        raise
    except Exception as e:
        error_str = str(e).lower()
        # Check for quota errors in generic exceptions too
        if 'quota' in error_str or 'insufficient_quota' in error_str:
            logger.error(f"OpenAI quota exceeded - stopping execution: {e}")
            raise QuotaExceededError(f"OpenAI quota exceeded: {e}") from e
        logger.error(f"Error in OpenAI API call: {e}")
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

# Async OpenRouter client
async_openrouter_client = AsyncOpenAI(
    base_url="https://openrouter.ai/api/v1",
    api_key=os.getenv("OPENROUTER_API_KEY"),
)
# Patch async client with instructor
async_openrouter_client = instructor.patch(async_openrouter_client, mode=instructor.Mode.JSON)

def call_llm_api(messages: List[Dict[str, str]],
                #model: str = "google/gemini-2.5-flash-lite-preview-06-17",
                mode: str = "openai/gpt-oss-120b:exacto",
                response_format: Optional[BaseModel] = None,
                max_tokens: int = 8000,
                temperature: float = 0.3,
                rate_limit_delay: float = 0) -> Any:
    """
    Make a call to the OpenRouter API for chat completions with structured output support.
    Args:
        messages: List of message dictionaries
        model: Model to use (default: gemini-2.5-flash-lite-preview-06-17)
        response_format: Optional Pydantic model for structured output
        max_tokens: Maximum tokens in response
        temperature: Temperature for response generation
        rate_limit_delay: Delay in seconds before making API call (default: 60.0s / 1 minute to avoid rate limits)
    Returns:
        Either structured output matching response_format or raw text response
    """
    # Rate limiting: Add delay before API call to avoid hitting free tier limits
    if rate_limit_delay > 0:
        time.sleep(rate_limit_delay)
    
    try:
        # If a response model is provided, use it for structured output
        if response_format:
            response = openrouter_client.chat.completions.create(
                model=mode,
                messages=messages,
                max_tokens=max_tokens,
                temperature=temperature,
                response_model=response_format,
                max_retries=3,
                extra_headers={
                    "HTTP-Referer": "https://xpchex.com", # Optional. Site URL for rankings on openrouter.ai.
                    "X-Title": "Xpchex", # Optional. Site title for rankings on openrouter.ai.
                }
            )  # Close the create() call
    
            return response
        else:
            # For unstructured responses
            response = openrouter_client.chat.completions.create(
                model=mode,
                messages=messages,
                max_tokens=max_tokens,
                temperature=temperature,
                max_retries=3
            )
            return response.choices[0].message.content
    except Exception as e:
        logger.error(f"Error in OpenRouter API call: {e}")
        raise


async def call_llm_api_async(messages: List[Dict[str, str]],
                mode: str = "openai/gpt-oss-120b:exacto",
                response_format: Optional[BaseModel] = None,
                max_tokens: int = 8000,
                temperature: float = 0.3,
                rate_limit_delay: float = 0) -> Any:
    """
    Async version of call_llm_api for non-blocking LLM calls.
    Make a call to the OpenRouter API for chat completions with structured output support.
    Args:
        messages: List of message dictionaries
        mode: Model to use (default: openai/gpt-oss-120b:exacto)
        response_format: Optional Pydantic model for structured output
        max_tokens: Maximum tokens in response
        temperature: Temperature for response generation
        rate_limit_delay: Delay in seconds before making API call (use asyncio.sleep instead)
    Returns:
        Either structured output matching response_format or raw text response
    Raises:
        QuotaExceededError: If quota is exceeded (stops execution)
    """
    # Rate limiting: Add delay before API call to avoid hitting free tier limits
    if rate_limit_delay > 0:
        await asyncio.sleep(rate_limit_delay)
    
    try:
        # If a response model is provided, use it for structured output
        if response_format:
            response = await async_openrouter_client.chat.completions.create(
                model=mode,
                messages=messages,
                max_tokens=max_tokens,
                temperature=temperature,
                response_model=response_format,
                max_retries=3,
                extra_headers={
                    "HTTP-Referer": "https://xpchex.com",
                    "X-Title": "Xpchex",
                }
            )
            return response
        else:
            # For unstructured responses
            response = await async_openrouter_client.chat.completions.create(
                model=mode,
                messages=messages,
                max_tokens=max_tokens,
                temperature=temperature,
                max_retries=3
            )
            return response.choices[0].message.content
    except RateLimitError as e:
        # Check if it's a quota error
        error_str = str(e).lower()
        if 'quota' in error_str or 'insufficient_quota' in error_str:
            logger.error(f"OpenRouter quota exceeded - stopping execution: {e}")
            raise QuotaExceededError(f"OpenRouter quota exceeded: {e}") from e
        logger.warning(f"OpenRouter rate limit hit: {e}")
        raise
    except APIError as e:
        # Check for quota errors in APIError as well
        error_str = str(e).lower()
        if 'quota' in error_str or 'insufficient_quota' in error_str or (hasattr(e, 'code') and e.code == 'insufficient_quota'):
            logger.error(f"OpenRouter quota exceeded - stopping execution: {e}")
            raise QuotaExceededError(f"OpenRouter quota exceeded: {e}") from e
        logger.error(f"OpenRouter API error: {e}")
        raise
    except Exception as e:
        error_str = str(e).lower()
        # Check for quota errors in generic exceptions too
        if 'quota' in error_str or 'insufficient_quota' in error_str:
            logger.error(f"OpenRouter quota exceeded - stopping execution: {e}")
            raise QuotaExceededError(f"OpenRouter quota exceeded: {e}") from e
        logger.error(f"Error in OpenRouter async API call: {e}")
        raise


