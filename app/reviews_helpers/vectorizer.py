"""
Vectorizer module for generating embeddings using OpenAI
"""
import os
from typing import Optional
from openai import OpenAI, APIError, RateLimitError
from dotenv import load_dotenv
from app.shared_services.logger_setup import setup_logger
from app.shared_services.llm import QuotaExceededError

logger = setup_logger()

# Load environment variables from .env file
load_dotenv()

# Get OpenAI API key from environment
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
if not OPENAI_API_KEY:
    logger.warning("OPENAI_API_KEY not found in environment variables")

# Initialize OpenAI client
openai_client = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None

# Default embedding model and target dimensionality (must match DB schema)
EMBEDDING_MODEL = os.getenv("EMBEDDING_MODEL", "text-embedding-3-small")
EMBEDDING_DIMENSIONS = int(os.getenv("EMBEDDING_DIMENSIONS", "768"))


def get_embedding(text: str, model: str = EMBEDDING_MODEL) -> Optional[list]:
    """
    Generate embedding for a given text using OpenAI's embedding model.
    
    Args:
        text: The text to generate embedding for
        model: The embedding model to use (default: text-embedding-3-small)
    
    Returns:
        List of floats representing the embedding vector, or None if error
    """
    if not text or not text.strip():
        logger.warning("Empty text provided for embedding")
        return None
    
    if not openai_client:
        logger.error("OpenAI client not initialized. Check OPENAI_API_KEY environment variable")
        return None
    
    try:
        # Generate embedding (force dimension to match vector column, e.g., 768)
        embedding_kwargs = {
            "model": model,
            "input": text.strip(),
        }
        if EMBEDDING_DIMENSIONS:
            embedding_kwargs["dimensions"] = EMBEDDING_DIMENSIONS

        response = openai_client.embeddings.create(**embedding_kwargs)
        
        # Extract embedding vector
        embedding = response.data[0].embedding
        logger.debug(f"Generated embedding for text: {text[:50]}...")
        return embedding
        
    except RateLimitError as e:
        # Check if it's a quota error (not just rate limit)
        error_str = str(e).lower()
        if 'quota' in error_str or 'insufficient_quota' in error_str:
            logger.error(f"OpenAI quota exceeded in embedding generation - stopping: {e}")
            raise QuotaExceededError(f"OpenAI quota exceeded: {e}") from e
        logger.warning(f"OpenAI rate limit hit in embedding generation: {e}")
        return None
    except APIError as e:
        # Check for quota errors in APIError as well
        error_str = str(e).lower()
        if 'quota' in error_str or 'insufficient_quota' in error_str or (hasattr(e, 'code') and e.code == 'insufficient_quota'):
            logger.error(f"OpenAI quota exceeded in embedding generation - stopping: {e}")
            raise QuotaExceededError(f"OpenAI quota exceeded: {e}") from e
        logger.error(f"OpenAI API error in embedding generation: {e}")
        return None
    except Exception as e:
        error_str = str(e).lower()
        # Check for quota errors in generic exceptions too
        if 'quota' in error_str or 'insufficient_quota' in error_str:
            logger.error(f"OpenAI quota exceeded in embedding generation - stopping: {e}")
            raise QuotaExceededError(f"OpenAI quota exceeded: {e}") from e
        logger.error(f"Error generating embedding for text '{text[:50]}...': {e}")
        return None

