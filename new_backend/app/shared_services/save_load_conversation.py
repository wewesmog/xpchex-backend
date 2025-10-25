from .db import get_postgres_connection
from .logger_setup import setup_logger
from psycopg2.extras import Json
from datetime import datetime, timezone

logger = setup_logger()

def save_conversation(result: dict):
    """Save conversation result to database"""
    conn = get_postgres_connection("conversations")
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO maswali_conversations 
                (log_timestamp, user_id, session_id, conversation_id, user_input, state, created_at)
                VALUES
                (NOW(), %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (conversation_id) 
                DO UPDATE SET
                    log_timestamp = NOW(),
                    user_id = EXCLUDED.user_id,
                    session_id = EXCLUDED.session_id,
                    user_input = EXCLUDED.user_input,
                    state = EXCLUDED.state
                RETURNING id;
            """, (
                result.get("user_id"),
                result.get("session_id", None),  # Make session_id optional
                result.get("conversation_id"),
                result.get("user_input"),
                Json(result)
            ))
            inserted_id = cur.fetchone()[0]
        conn.commit()
        logger.info(f"Conversation upserted with ID: {inserted_id}")
        return inserted_id
    except Exception as e:
        logger.error(f"Error saving conversation: {e}")
        raise
    finally:
        conn.close()

def load_conversation(conversation_id: str):
    """Load conversation from database"""
    conn = get_postgres_connection("conversations")
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT state FROM maswali_conversations 
                WHERE conversation_id = %s
                ORDER BY log_timestamp DESC 
                LIMIT 1
            """, (conversation_id,))
            result = cur.fetchone()
            return result[0] if result else None
    except Exception as e:
        logger.error(f"Error loading conversation: {e}")
        raise
    finally:
        conn.close()

