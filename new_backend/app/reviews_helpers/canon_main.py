import logging
from app.reviews_helpers.canon_graph import build_graph
from app.models.canonicalization_models import CanonicalizationState
from app.reviews_helpers.canonicalization import get_statements_by_date_range
from datetime import datetime, timedelta

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

def process_statements(start_date: str, end_date: str, date_range: int = 1):
    """Process statements between dates in batches"""
    logger.info(f"Processing statements from {start_date} to {end_date}")
    # Convert string dates to datetime objects
    current_date = datetime.strptime(start_date, '%Y-%m-%d')
    end_date_dt = datetime.strptime(end_date, '%Y-%m-%d')
    
    # Loop through each date in the range, and process the statements for that date
    while current_date <= end_date_dt:
        current_date_str = current_date.strftime('%Y-%m-%d')
        logger.info(f"Processing statements for date: {current_date_str}")
        statements = get_statements_by_date_range(current_date_str, current_date_str)
        total = len(statements)
        if not statements:
            logger.info("No statements found")
            current_date += timedelta(days=1)
            continue
        logger.info(f"Found {total} statements")
        process_statements_for_date(statements)
        current_date += timedelta(days=date_range)
    
    # Get all statements for the date range

    # loop through each statement and process it
def process_statements_for_date(statements):
    # Build graph once
    graph = build_graph()
    for section_type, free_text_description, review_id, review_created_at in statements:
        try:
            logger.info(f"Processing statement: {free_text_description}")
            state = CanonicalizationState(
                input_statement=free_text_description,
                review_section=section_type,
                review_id=review_id,
                review_created_at=review_created_at
            )
            try:
                result = graph.invoke(state)
            except Exception as e:
                logger.error(f"Error invoking graph: {e}")
                continue
            status = "Success" if result.get('canonical_id') else "Failed"
            logger.info(f"Status: {status}")
            logger.info(f"Canonical ID: {result.get('canonical_id')}")
        except Exception as e:
            logger.error(f"Error processing statement: {e}")

    
if __name__ == "__main__":
    # Set your parameters here
    start_date = '2025-02-28'
    end_date = '2025-02-28'
    batch_size = 5
    
    try:
        process_statements(start_date, end_date, batch_size)
    except Exception as e:
        logger.error(f"Processing failed: {e}")
        raise





