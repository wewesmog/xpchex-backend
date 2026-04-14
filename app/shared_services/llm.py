from openai import OpenAI, AsyncOpenAI
from typing import List, Dict, Any, Optional
import os
from dotenv import load_dotenv
from pydantic import BaseModel
import instructor
from groq import Groq
from instructor import patch
import logging
import asyncio
import time
import re
from openai import APIError, RateLimitError

logger = logging.getLogger(__name__)


class QuotaExceededError(Exception):
    """Custom exception for OpenAI quota exceeded errors"""
    pass

load_dotenv()

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

# Initialize OpenAI client with instructor for structured outputs
openai_client = instructor.patch(OpenAI(api_key=OPENAI_API_KEY), mode=instructor.Mode.JSON)

# Async OpenAI client
async_openai_client = instructor.patch(AsyncOpenAI(api_key=OPENAI_API_KEY), mode=instructor.Mode.JSON)

# Plain OpenAI client for embeddings (no instructor)
_embedding_client = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None

# Google Gemini — via OpenAI-compatible endpoint so logfire.instrument_openai() captures calls
GOOGLE_API_KEY = os.getenv("GEMINI_API_KEY")
_GEMINI_BASE_URL = "https://generativelanguage.googleapis.com/v1beta/openai/"

if GOOGLE_API_KEY:
    gemini_client = instructor.patch(
        OpenAI(base_url=_GEMINI_BASE_URL, api_key=GOOGLE_API_KEY),
        mode=instructor.Mode.JSON,
    )
    async_gemini_client = instructor.patch(
        AsyncOpenAI(base_url=_GEMINI_BASE_URL, api_key=GOOGLE_API_KEY),
        mode=instructor.Mode.JSON,
    )
else:
    gemini_client = None
    async_gemini_client = None

def call_llm_api_openai_provider(messages: List[Dict[str, str]], 
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


def embed_texts(
    texts: List[str],
    model: str = "text-embedding-3-small",
) -> List[List[float]]:
    """
    Embed one or more texts with OpenAI (text-embedding-3-small, 1536 dims).
    Use for response_history and RAG retrieval; keep model fixed so all vectors are comparable.
    """
    if not texts:
        return []
    if not _embedding_client:
        raise ValueError("OPENAI_API_KEY not set; cannot embed")
    # API accepts up to 2048 inputs per request; we batch in chunks to avoid token limits
    batch_size = 100
    all_embeddings: List[List[float]] = []
    for i in range(0, len(texts), batch_size):
        batch = texts[i : i + batch_size]
        response = _embedding_client.embeddings.create(model=model, input=batch)
        # Preserve order: response.data is in same order as input
        order = {e.index: e.embedding for e in response.data}
        all_embeddings.extend([order[j] for j in range(len(batch))])
    return all_embeddings


# Groq API


# Patch Groq() with instructor, this is where the magic happens!
groq_client = instructor.from_groq(Groq(api_key=os.getenv("GROQ_API_KEY")), mode=instructor.Mode.JSON)

def call_llm_api_groq_provider(messages: List[Dict[str, str]],
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

def call_llm_api_openrouter_provider(messages: List[Dict[str, str]],
                mode: str = "openai/gpt-oss-120b:exacto",
                response_format: Optional[BaseModel] = None,
                max_tokens: int = 8000,
                temperature: float = 0.3,
                rate_limit_delay: float = 0) -> Any:
    """
    Make a call to the OpenRouter API for chat completions with structured output support.
    Args:
        messages: List of message dictionaries
        mode: Model to use (default: openai/gpt-oss-120b:exacto)
        response_format: Optional Pydantic model for structured output
        max_tokens: Maximum tokens in response
        temperature: Temperature for response generation
        rate_limit_delay: Delay in seconds before making API call (default: 0)
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
                    "HTTP-Referer": "https://xpchex.com",
                    "X-Title": "Xpchex",
                }
            )
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
    
    
async def call_llm_api_openrouter_provider_async(messages: List[Dict[str, str]],
                mode: str = "openai/gpt-oss-120b:exacto",
                response_format: Optional[BaseModel] = None,
                max_tokens: int = 8000,
                temperature: float = 0.3,
                rate_limit_delay: float = 0) -> Any:
    """
    Async version of call_llm_api_1 for non-blocking LLM calls.
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
    
    
def call_llm_api(messages: List[Dict[str, str]],
                mode: str = "gemini-3.0-flash-lite",
                response_format: Optional[BaseModel] = None,
                max_tokens: int = 8000,
                temperature: float = 0.3,
                rate_limit_delay: float = 0) -> Any:
    """
    Call Google Gemini via the OpenAI-compatible endpoint.
    Uses the same openai SDK client as other providers so logfire captures all calls.
    Messages stay in standard OpenAI format — no conversion required.
    """
    if rate_limit_delay > 0:
        time.sleep(rate_limit_delay)

    if not gemini_client:
        raise ValueError("GOOGLE_API_KEY not set")

    try:
        if response_format:
            return gemini_client.chat.completions.create(
                model=mode,
                messages=messages,
                max_tokens=max_tokens,
                temperature=temperature,
                response_model=response_format,
                max_retries=3,
            )
        else:
            response = gemini_client.chat.completions.create(
                model=mode,
                messages=messages,
                max_tokens=max_tokens,
                temperature=temperature,
                max_retries=3,
            )
            return response.choices[0].message.content
    except RateLimitError as e:
        error_str = str(e).lower()
        if 'quota' in error_str or 'insufficient_quota' in error_str or 'quota exceeded' in error_str:
            logger.error(f"Gemini quota exceeded - stopping execution: {e}")
            raise QuotaExceededError(f"Gemini quota exceeded: {e}") from e
        logger.warning(f"Gemini rate limit hit: {e}")
        raise
    except Exception as e:
        error_str = str(e).lower()
        if 'quota' in error_str or 'insufficient_quota' in error_str or 'quota exceeded' in error_str:
            logger.error(f"Gemini quota exceeded - stopping execution: {e}")
            raise QuotaExceededError(f"Gemini quota exceeded: {e}") from e
        logger.error(f"Error in Gemini API call: {e}")
        raise
    
    
async def call_llm_api_async(messages: List[Dict[str, str]],
                mode: str = "gemini-2.0-flash-lite",
                response_format: Optional[BaseModel] = None,
                max_tokens: int = 8000,
                temperature: float = 0.3,
                rate_limit_delay: float = 0) -> Any:
    """
    Async call to Google Gemini via the OpenAI-compatible endpoint.
    Truly non-blocking (AsyncOpenAI) — no asyncio.to_thread wrapper needed.
    logfire.instrument_openai() captures these calls automatically.
    """
    if rate_limit_delay > 0:
        await asyncio.sleep(rate_limit_delay)

    if not async_gemini_client:
        raise ValueError("GOOGLE_API_KEY not set")

    try:
        if response_format:
            return await async_gemini_client.chat.completions.create(
                model=mode,
                messages=messages,
                max_tokens=max_tokens,
                temperature=temperature,
                response_model=response_format,
                max_retries=3,
            )
        else:
            response = await async_gemini_client.chat.completions.create(
                model=mode,
                messages=messages,
                max_tokens=max_tokens,
                temperature=temperature,
                max_retries=3,
            )
            return response.choices[0].message.content
    except RateLimitError as e:
        error_str = str(e).lower()
        if 'quota' in error_str or 'insufficient_quota' in error_str or 'quota exceeded' in error_str:
            logger.error(f"Gemini quota exceeded - stopping execution: {e}")
            raise QuotaExceededError(f"Gemini quota exceeded: {e}") from e
        logger.warning(f"Gemini rate limit hit: {e}")
        raise
    except Exception as e:
        error_str = str(e).lower()
        if 'quota' in error_str or 'insufficient_quota' in error_str or 'quota exceeded' in error_str:
            logger.error(f"Gemini quota exceeded - stopping execution: {e}")
            raise QuotaExceededError(f"Gemini quota exceeded: {e}") from e
        logger.error(f"Error in Gemini async API call: {e}")
        raise
    

async def call_llm_api_openai_async_provider(messages: List[Dict[str, str]], 
                model: str = "gpt-4o",
                response_format: Optional[BaseModel] = None,
                temperature: float = 0.3) -> Any:
    """
    Primary async OpenAI helper used by the app.
    Thin wrapper around call_llm_api_openai_async.
    """
    return await call_llm_api_openai_async(
        messages=messages,
        model=model,
        response_format=response_format,
        temperature=temperature,
    )
    
    
async def call_llm_api_openai_async(messages: List[Dict[str, str]], 
                model: str = "gpt-4o",
                response_format: Optional[BaseModel] = None,
                temperature: float = 0.3) -> Any:
    """
    Async version of call_llm_api_openai for non-blocking LLM calls.
    Make a call to the OpenAI API for chat completions.
    Raises QuotaExceededError for quota/429 errors to allow immediate stopping.
    """
    try:
        # If a response model is provided, use it for structured output
        if response_format:
            response = await async_openai_client.chat.completions.create(
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
            response = await async_openai_client.chat.completions.create(
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
        logger.error(f"Error in OpenAI async API call: {e}")
        raise


