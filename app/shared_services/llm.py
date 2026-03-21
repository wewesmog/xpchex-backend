from openai import OpenAI, AsyncOpenAI
from typing import List, Dict, Any, Optional
import os
from dotenv import load_dotenv
from pydantic import BaseModel
import instructor
from groq import Groq
from google import genai
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

# Configure Google Gemini
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")

# Try to use instructor with Gemini, fallback to native client
if GOOGLE_API_KEY:
    try:
        gemini_instructor_client = instructor.from_provider(
            "google/gemini-2.0-flash-lite",
            api_key=GOOGLE_API_KEY
        )
    except Exception as e:
        logger.warning(f"Could not initialize instructor for Gemini: {e}, using native client")
        gemini_instructor_client = None
    
    # Always initialize native client as fallback
    gemini_client = genai.Client(api_key=GOOGLE_API_KEY)
else:
    gemini_instructor_client = None
    gemini_client = None

def call_llm_api(messages: List[Dict[str, str]], 
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
    
    
def call_llm_api_gemini_provider(messages: List[Dict[str, str]],
                mode: str = "gemini-2.0-flash-lite",
                response_format: Optional[BaseModel] = None,
                max_tokens: int = 8000,
                temperature: float = 0.3,
                rate_limit_delay: float = 0) -> Any:
    """
    Make a call to the Google Gemini API for chat completions with structured output support.
    Args:
        messages: List of message dictionaries (OpenAI format)
        mode: Model to use (default: gemini-2.0-flash-lite)
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
        # Convert OpenAI message format to Gemini format
        contents_parts = []
        for msg in messages:
            role = msg.get('role', 'user')
            content = msg.get('content', '')
            if role == 'system':
                contents_parts.append(f"System: {content}")
            elif role == 'user':
                contents_parts.append(f"User: {content}")
            elif role == 'assistant':
                contents_parts.append(f"Assistant: {content}")
        
        contents = "\n".join(contents_parts)
        
        # Use instructor if available and response_format is requested
        if response_format and gemini_instructor_client:
            try:
                response = gemini_instructor_client.create(
                    response_model=response_format,
                    messages=[{"role": "user", "content": contents}],
                    # Note: Gemini doesn't accept max_tokens, it uses max_output_tokens
                    # Instructor should handle this internally
                )
                return response
            except Exception as e:
                logger.warning(f"Instructor failed, falling back to native client: {e}")
        
        # Fallback to native Gemini client
        if not gemini_client:
            raise ValueError("GOOGLE_API_KEY not set")
            
        response = gemini_client.models.generate_content(
            model=mode,
            contents=contents
        )
        
        response_text = response.text
        
        # If structured output requested, parse JSON manually
        if response_format:
            import json
            try:
                # Strip markdown code blocks if present
                response_text_clean = response_text.strip()
                if response_text_clean.startswith('```'):
                    response_text_clean = re.sub(r'^```(?:json)?\s*\n?', '', response_text_clean)
                    response_text_clean = re.sub(r'\n?```\s*$', '', response_text_clean)
                
                response_json = json.loads(response_text_clean)
                return response_format(**response_json)
            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse JSON response from Gemini: {e}")
                logger.error(f"Response text: {response_text}")
                raise
        else:
            return response_text
            
    except Exception as e:
        error_str = str(e).lower()
        # Check for quota errors
        if 'quota' in error_str or 'insufficient_quota' in error_str or 'quota exceeded' in error_str:
            logger.error(f"Gemini quota exceeded - stopping execution: {e}")
            raise QuotaExceededError(f"Gemini quota exceeded: {e}") from e
        # Check for rate limit errors
        if 'rate limit' in error_str or '429' in error_str:
            logger.warning(f"Gemini rate limit hit: {e}")
            raise RateLimitError(f"Gemini rate limit: {e}") from e
        logger.error(f"Error in Gemini API call: {e}")
        raise
    
    
async def call_llm_api_gemini_provider_async(messages: List[Dict[str, str]],
                mode: str = "gemini-2.0-flash-lite",
                response_format: Optional[BaseModel] = None,
                max_tokens: int = 8000,
                temperature: float = 0.3,
                rate_limit_delay: float = 0) -> Any:
    """
    Async version of call_llm_api for non-blocking LLM calls.
    Make a call to the Google Gemini API for chat completions with structured output support.
    Args:
        messages: List of message dictionaries (OpenAI format)
        mode: Model to use (default: gemini-2.0-flash-lite)
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
        # Convert OpenAI message format to Gemini format
        contents_parts = []
        for msg in messages:
            role = msg.get('role', 'user')
            content = msg.get('content', '')
            if role == 'system':
                contents_parts.append(f"System: {content}")
            elif role == 'user':
                contents_parts.append(f"User: {content}")
            elif role == 'assistant':
                contents_parts.append(f"Assistant: {content}")
        
        contents = "\n".join(contents_parts)
        
        # Use instructor if available and response_format is requested
        # Run in thread pool since instructor's Gemini client is sync
        if response_format and gemini_instructor_client:
            try:
                response = await asyncio.to_thread(
                    gemini_instructor_client.create,
                    response_model=response_format,
                    messages=[{"role": "user", "content": contents}],
                    # Note: Gemini doesn't accept max_tokens, it uses max_output_tokens
                    # Instructor should handle this internally
                )
                return response
            except Exception as e:
                logger.warning(f"Instructor failed, falling back to native client: {e}")
        
        # Fallback to native Gemini client
        if not gemini_client:
            raise ValueError("GOOGLE_API_KEY not set")
        
        # Run in thread pool since SDK is sync
        response = await asyncio.to_thread(
            gemini_client.models.generate_content,
            model=mode,
            contents=contents
        )
        
        response_text = response.text
        
        # If structured output requested, parse JSON manually
        if response_format:
            import json
            try:
                # Strip markdown code blocks if present
                response_text_clean = response_text.strip()
                if response_text_clean.startswith('```'):
                    response_text_clean = re.sub(r'^```(?:json)?\s*\n?', '', response_text_clean)
                    response_text_clean = re.sub(r'\n?```\s*$', '', response_text_clean)
                
                response_json = json.loads(response_text_clean)
                return response_format(**response_json)
            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse JSON response from Gemini: {e}")
                logger.error(f"Response text: {response_text}")
                raise
        else:
            return response_text
            
    except Exception as e:
        error_str = str(e).lower()
        # Check for quota errors
        if 'quota' in error_str or 'insufficient_quota' in error_str or 'quota exceeded' in error_str:
            logger.error(f"Gemini quota exceeded - stopping execution: {e}")
            raise QuotaExceededError(f"Gemini quota exceeded: {e}") from e
        # Check for rate limit errors
        if 'rate limit' in error_str or '429' in error_str:
            logger.warning(f"Gemini rate limit hit: {e}")
            raise RateLimitError(f"Gemini rate limit: {e}") from e
        logger.error(f"Error in Gemini async API call: {e}")
        raise
    

async def call_llm_api_async(messages: List[Dict[str, str]], 
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


