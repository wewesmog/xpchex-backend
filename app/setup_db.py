import os
from pathlib import Path
from shared_services.logger_setup import setup_logger
from shared_services.db import get_postgres_connection

logger = setup_logger()

def setup_database():
    """Set up the database tables and functions"""
    try:
        # Get connection using existing function
        conn = get_postgres_connection("app_reviews")  # table name doesn't matter for setup
        conn.autocommit = False  # We want transaction control
        cursor = conn.cursor()

        # Read and execute the SQL file
        sql_file_path = Path(__file__).parent.parent / 'create_tables.sql'
        with open(sql_file_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()

        # Replace the connection command with our connection
        sql_content = sql_content.replace(r'\c :dbname\n', '')
        
        try:
            cursor.execute(sql_content)
            conn.commit()
            logger.info("Successfully set up database tables and functions")
        except Exception as e:
            conn.rollback()
            logger.error(f"Error executing SQL: {e}")
            raise

    except Exception as e:
        logger.error(f"Database setup failed: {e}")
        raise
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    setup_database() 