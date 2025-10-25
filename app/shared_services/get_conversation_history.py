import os
import json
from datetime import datetime
from typing import Dict, Any
from .db import get_postgres_connection
from .logger_setup import setup_logger
from psycopg2.extras import RealDictCursor


logger = setup_logger()

def get_conversation_history(
    user_id: str, 
    session_id: str,
    conversation_id: str,
    limit: int,
) -> Dict[str, Any]:
    """
    Extract and log conversation history from state column ordered by latest first
    """
    conn = get_postgres_connection("conversations")
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT 
                    state->'current_conversation_history' as conversation_history
                FROM uliza_conversations 
                WHERE user_id = %s 
                AND session_id = %s 
                ORDER BY log_timestamp DESC
                LIMIT %s;
            """, (user_id, session_id, limit))
            
            results = cur.fetchall()
            
            if not results:
                logger.info(f"No conversations found for user_id: {user_id}")
                return {
                    "status": "no_data",
                    "metadata": {
                        "user_id": user_id,
                        "session_id": session_id,
                        "timestamp": datetime.now().isoformat(),
                        "query_limit": limit
                    },
                    "conversations": []
                }
            
            # Extract conversations and sort by timestamp
            conversations = []
            for result in results:
                if result['conversation_history']:
                    conversations.extend(result['conversation_history'])
            
            # Sort by timestamp within conversation_history
            sorted_conversations = sorted(
                conversations,
                key=lambda x: datetime.fromisoformat(x['timestamp']),
                reverse=True  # Newest first
            )
            
            output = {
                "status": "success",
                "metadata": {
                    "user_id": user_id,
                    "session_id": session_id,
                    "timestamp": datetime.now().isoformat(),
                    "total_messages": len(sorted_conversations),
                    "query_limit": limit
                },
                "conversations": sorted_conversations
            }
            
            # Log to file
            log_dir = "conversation_logs"
            if not os.path.exists(log_dir):
                os.makedirs(log_dir)

              
            filename = f"{log_dir}/conversation_{conversation_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(output, f, indent=2, ensure_ascii=False)
            
            logger.info(f"Conversation history extracted and saved to {filename}")
            
            # Print formatted JSON to console
            print("\nExtracted Conversation History:")
            print(json.dumps(output, indent=2, ensure_ascii=False))
            
            return output
            
    except Exception as e:
        error_response = {
            "status": "error",
            "metadata": {
                "user_id": user_id,
                "session_id": session_id,
                "conversation_id": conversation_id,
                "timestamp": datetime.now().isoformat(),
                "error": str(e)
            },
            "conversations": []
        }
        logger.error(f"Error retrieving conversation history: {e}")
        return error_response
    finally:
        conn.close()