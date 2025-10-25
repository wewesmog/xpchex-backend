from pathlib import Path
from app.shared_services.db import get_postgres_connection
from app.shared_services.logger_setup import setup_logger

logger = setup_logger()

def run_migration(migration_file: str):
    """Run a specific migration file"""
    try:
        # Get connection
        conn = get_postgres_connection("app_reviews")
        conn.autocommit = False  # We want transaction control
        cursor = conn.cursor()

        # Read and execute the migration file
        migration_path = Path(__file__).parent.parent.parent / 'migrations' / migration_file
        with open(migration_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        try:
            cursor.execute(sql_content)
            conn.commit()
            logger.info(f"Successfully ran migration: {migration_file}")
        except Exception as e:
            conn.rollback()
            logger.error(f"Error executing migration: {e}")
            raise

    except Exception as e:
        logger.error(f"Migration failed: {e}")
        raise
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    run_migration('add_analysis_failure_tracking.sql') 