from .db import get_postgres_connection

TABLES = {
    "conversations": """
        CREATE TABLE IF NOT EXISTS maswali_conversations (
            id SERIAL PRIMARY KEY,
            log_timestamp TIMESTAMP WITH TIME ZONE,
            user_id VARCHAR(255),
            session_id VARCHAR(255),
            conversation_id VARCHAR(255),
            user_input TEXT,
            state JSONB,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        );
    """,
    
    "artifacts": """
        CREATE TABLE IF NOT EXISTS artifacts (
            response_id UUID PRIMARY KEY,
            content TEXT NOT NULL,
            quiz_parameters JSONB NOT NULL,
            user_id TEXT NOT NULL,
            created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
            is_published BOOLEAN NOT NULL DEFAULT FALSE,
            published_at TIMESTAMP WITH TIME ZONE
        );

        -- Create indexes for common queries
        CREATE INDEX IF NOT EXISTS idx_artifacts_user_id ON artifacts(user_id);
        CREATE INDEX IF NOT EXISTS idx_artifacts_is_published ON artifacts(is_published);
        CREATE INDEX IF NOT EXISTS idx_artifacts_created_at ON artifacts(created_at);
    """
}

def init_database():
    """Initialize all database tables"""
    for table_name, create_statement in TABLES.items():
        conn = get_postgres_connection(table_name)
        try:
            with conn.cursor() as cur:
                cur.execute(create_statement)
            conn.commit()
            print(f"{table_name} table created or already exists")
        except Exception as e:
            print(f"Error creating {table_name} table: {e}")
            raise
        finally:
            conn.close()

if __name__ == "__main__":
    print("Initializing database tables...")
    init_database()
    print("Database initialization complete!") 