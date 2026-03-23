import logging
import asyncio
from concurrent.futures import ThreadPoolExecutor, as_completed
from app.reviews_helpers.canon_graph import build_graph
from app.reviews_helpers.canon_workflow_async import run_canonicalization_workflow_async
from app.models.canonicalization_models import CanonicalizationState
from app.shared_services.db import get_postgres_connection, init_connection_pool, release_thread_connection, close_connection_pool
from app.shared_services.db_async import init_async_pool, close_async_pool
from datetime import datetime, timedelta
from typing import List, Tuple, Optional

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

def get_min_max_uncanonized_dates() -> Tuple[Optional[str], Optional[str]]:
    """Get minimum and maximum dates from uncanonized records
    
    Returns:
        tuple: (min_date, max_date) in 'YYYY-MM-DD' format, or (None, None) if no records found
    """
    conn = None
    cursor = None
    try:
        conn = get_postgres_connection()
        cursor = conn.cursor()
        cursor.execute("""
            WITH all_statements AS (
                SELECT
                    issue_data->>'description' AS free_text_description,
                    review_created_at
                FROM
                    processed_app_reviews,
                    jsonb_array_elements(latest_analysis->'issues'->'issues') AS issue_data
                WHERE
                    issue_data->>'description' IS NOT NULL

                UNION ALL

                SELECT
                    action_data->>'description' AS free_text_description,
                    review_created_at
                FROM
                    processed_app_reviews,
                    jsonb_array_elements(latest_analysis->'issues'->'issues') AS issue_data,
                    jsonb_array_elements(issue_data->'actions') AS action_data
                WHERE
                    action_data->>'description' IS NOT NULL

                UNION ALL

                SELECT
                    positive_data->>'description' AS free_text_description,
                    review_created_at
                FROM
                    processed_app_reviews,
                    jsonb_array_elements(latest_analysis->'positive_feedback'->'positive_mentions') AS positive_data
                WHERE
                    positive_data->>'description' IS NOT NULL
            )
            SELECT 
                MIN(date(a.review_created_at)) as min_date,
                MAX(date(a.review_created_at)) as max_date
            FROM all_statements a
            LEFT OUTER JOIN canonical_statements b
            ON (a.free_text_description = b.statement)
            WHERE b.statement IS NULL
        """)
        result = cursor.fetchone()
        if result and result[0] and result[1]:
            min_date = result[0].strftime('%Y-%m-%d') if result[0] else None
            max_date = result[1].strftime('%Y-%m-%d') if result[1] else None
            return (min_date, max_date)
        return (None, None)
    except Exception as e:
        logger.error(f"Error fetching min/max uncanonized dates: {e}")
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def get_reviews_by_date_range(start_date: str, end_date: str) -> List[str]:
    """Get all review IDs for a date range ordered by review date"""
    conn = None
    cursor = None
    try:
        conn = get_postgres_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT review_id 
            FROM processed_app_reviews 
            WHERE date(review_created_at) BETWEEN %s AND %s
            ORDER BY review_created_at
        """, (start_date, end_date))
        review_ids = [row[0] for row in cursor.fetchall()]
        return review_ids
    except Exception as e:
        logger.error(f"Error fetching review IDs: {e}")
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def get_statements_by_review_ids(review_ids: List[str]) -> List[Tuple]:
    """Get statements from specific review IDs"""
    if not review_ids:
        return []
    
    conn = None
    cursor = None
    try:
        conn = get_postgres_connection()
        cursor = conn.cursor()
        placeholders = ','.join(['%s'] * len(review_ids))
        # Use parameterized query to avoid SQL injection
        query = f"""
            WITH all_statements AS (
                SELECT
                    issue_data->>'description' AS free_text_description,
                    'issue' as section_type,
                    review_id,
                    review_created_at
                FROM
                    processed_app_reviews,
                    jsonb_array_elements(latest_analysis->'issues'->'issues') AS issue_data
                WHERE
                    issue_data->>'description' IS NOT NULL 
                    AND review_id IN ({placeholders})

                UNION ALL

                SELECT
                    action_data->>'description' AS free_text_description,
                    'issue_action' as section_type,
                    review_id,
                    review_created_at
                FROM
                    processed_app_reviews,
                    jsonb_array_elements(latest_analysis->'issues'->'issues') AS issue_data,
                    jsonb_array_elements(issue_data->'actions') AS action_data
                WHERE
                    action_data->>'description' IS NOT NULL 
                    AND review_id IN ({placeholders})

                UNION ALL

                SELECT
                    positive_data->>'description' AS free_text_description,
                    'positive' as section_type,
                    review_id,
                    review_created_at
                FROM
                    processed_app_reviews,
                    jsonb_array_elements(latest_analysis->'positive_feedback'->'positive_mentions') AS positive_data
                WHERE
                    positive_data->>'description' IS NOT NULL 
                    AND review_id IN ({placeholders})
            )
            SELECT 
                a.section_type,
                a.free_text_description,
                a.review_id,
                a.review_created_at
            FROM all_statements a
            LEFT OUTER JOIN canonical_statements b
            ON (a.free_text_description = b.statement)
            WHERE b.statement is null
        """
        # Pass review_ids 3 times (once for each UNION ALL part)
        cursor.execute(query, review_ids * 3)
        statements = cursor.fetchall()
        return statements
    except Exception as e:
        logger.error(f"Error fetching statements for review IDs: {e}")
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def process_statements(start_date: Optional[str] = None, end_date: Optional[str] = None, date_range: int = 1, 
                       review_limit: Optional[int] = None,
                       reviews_per_batch: Optional[int] = None, statements_per_batch: Optional[int] = None,
                       max_workers: int = 5, stop_on_error: bool = False):
    """Process statements between dates with concurrent processing
    
    Args:
        start_date: Start date in 'YYYY-MM-DD' format (None = use min date from uncanonized records)
        end_date: End date in 'YYYY-MM-DD' format (None = use max date from uncanonized records)
        date_range: Number of days to increment per iteration (default: 1)
        review_limit: Maximum number of reviews to process total (None = no limit, for testing)
        reviews_per_batch: Number of reviews to process per batch (None = all reviews at once)
        statements_per_batch: Number of statements to submit at once (None = all statements, workers pick when free)
        max_workers: Maximum number of concurrent workers for ThreadPoolExecutor (default: 5)
        stop_on_error: If True, abort processing on the first statement error (default: False)
    """
    # If no dates provided, get min/max from uncanonized records
    if start_date is None or end_date is None:
        logger.info("No date range provided. Fetching min/max dates from uncanonized records...")
        min_date, max_date = get_min_max_uncanonized_dates()
        if not min_date or not max_date:
            logger.warning("No uncanonized records found. Exiting.")
            return
        if start_date is None:
            start_date = min_date
            logger.info(f"Using minimum uncanonized date as start_date: {start_date}")
        if end_date is None:
            end_date = max_date
            logger.info(f"Using maximum uncanonized date as end_date: {end_date}")
    
    # If dates are provided, check the actual min date in uncanonized records
    # and use that as start if it's earlier than provided start_date
    logger.info(f"Checking minimum date in uncanonized records...")
    actual_min_date, actual_max_date = get_min_max_uncanonized_dates()
    if actual_min_date:
        provided_start = datetime.strptime(start_date, '%Y-%m-%d')
        actual_min = datetime.strptime(actual_min_date, '%Y-%m-%d')
        if actual_min < provided_start:
            logger.info(f"Found earlier uncanonized records. Adjusting start_date from {start_date} to {actual_min_date}")
            start_date = actual_min_date
        else:
            logger.info(f"Using provided start_date: {start_date}")
    else:
        logger.warning("No uncanonized records found. Exiting.")
        return
    
    logger.info(f"Processing statements from {start_date} to {end_date}")
    if review_limit is not None:
        logger.info(f"REVIEW LIMIT: Processing only {review_limit} reviews")
    else:
        logger.info("REVIEW LIMIT: No limit (processing all reviews)")
    logger.info(
        "Concurrent processing: reviews_per_batch=%s, statements_per_batch=%s, max_workers=%s, stop_on_error=%s",
        reviews_per_batch,
        statements_per_batch,
        max_workers,
        stop_on_error,
    )
    
    # Convert string dates to datetime objects
    current_date = datetime.strptime(start_date, '%Y-%m-%d')
    end_date_dt = datetime.strptime(end_date, '%Y-%m-%d')
    
    total_reviews_processed = 0
    
    # Loop through each date in the range
    while current_date <= end_date_dt:
        current_date_str = current_date.strftime('%Y-%m-%d')
        logger.info(f"Processing statements for date: {current_date_str}")
        
        # Get all reviews for this date
        review_ids = get_reviews_by_date_range(current_date_str, current_date_str)
        if not review_ids:
            logger.info("No reviews found")
            current_date += timedelta(days=1)
            continue
        
        logger.info(f"Found {len(review_ids)} reviews for date {current_date_str}")
        
        # Process reviews in batches if reviews_per_batch is set
        if reviews_per_batch:
            review_batches = [review_ids[i:i + reviews_per_batch] for i in range(0, len(review_ids), reviews_per_batch)]
            logger.info(f"Processing {len(review_batches)} review batches (max {reviews_per_batch} reviews per batch)")
        else:
            review_batches = [review_ids]
        
        for batch_num, review_batch in enumerate(review_batches, 1):
            # Check review limit at start of batch
            if review_limit is not None and total_reviews_processed >= review_limit:
                logger.info(f"Reached review limit of {review_limit} reviews. Stopping.")
                return
            
            # Apply review limit to current batch if needed
            if review_limit is not None:
                remaining_reviews = review_limit - total_reviews_processed
                logger.info(f"Review limit check: limit={review_limit}, processed={total_reviews_processed}, remaining={remaining_reviews}, batch_size={len(review_batch)}")
                if remaining_reviews <= 0:
                    logger.info(f"Review limit reached. No more reviews to process.")
                    return
                if len(review_batch) > remaining_reviews:
                    logger.info(f"Limiting batch from {len(review_batch)} to {remaining_reviews} reviews to respect review limit")
                    review_batch = review_batch[:remaining_reviews]
            
            # Skip empty batches
            if not review_batch:
                logger.info(f"Review batch {batch_num} is empty after applying limit. Skipping.")
                continue
            
            logger.info(f"Processing review batch {batch_num}/{len(review_batches)} ({len(review_batch)} reviews)")
            
            # Get statements from this batch of reviews
            statements = get_statements_by_review_ids(review_batch)
            if not statements:
                logger.info(f"No statements found in review batch {batch_num}")
                # Still count these reviews as processed (they had no uncanonized statements)
                total_reviews_processed += len(review_batch)
                continue
            
            logger.info(f"Found {len(statements)} statements from review batch {batch_num}")
            
            # Process statements (workers will pick them when free)
            process_statements_for_date(
                statements,
                statements_per_batch=statements_per_batch,
                max_workers=max_workers,
                stop_on_error=stop_on_error,
            )
            # Count reviews as processed after successfully processing their statements
            total_reviews_processed += len(review_batch)
            
            # Check limit after processing
            if review_limit is not None and total_reviews_processed >= review_limit:
                logger.info(f"Reached review limit of {review_limit} reviews. Stopping.")
                return
        
        current_date += timedelta(days=date_range)
    
def process_statements_for_date(
    statements: List[Tuple],
    statements_per_batch: Optional[int] = None,
    max_workers: int = 5,
    stop_on_error: bool = False,
) -> int:
    """Process statements with concurrent processing - workers pick statements when free
    
    Args:
        statements: List of tuples (section_type, free_text_description, review_id, review_created_at)
        statements_per_batch: Number of statements to submit at once (None = all at once, workers pick when free)
        max_workers: Maximum number of concurrent workers for ThreadPoolExecutor
        stop_on_error: If True, abort and bubble up the first exception encountered
    
    Returns:
        int: Number of successfully processed statements
    """
    if not statements:
        logger.info("No statements to process")
        return 0
    
    # Build graph once (shared across all workers)
    graph = build_graph()
    total_statements = len(statements)
    
    logger.info(f"Processing {total_statements} statements with {max_workers} workers")
    if statements_per_batch:
        logger.info(f"Submitting statements in batches of {statements_per_batch}")
    else:
        logger.info("Submitting all statements at once - workers will pick when free")
    
    processed_count = 0
    failed_count = 0
    
    # Submit statements - either all at once or in batches
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        if statements_per_batch:
            # Submit in batches
            batches = [statements[i:i + statements_per_batch] for i in range(0, total_statements, statements_per_batch)]
            logger.info(f"Created {len(batches)} statement batches")
            
            for batch_num, batch in enumerate(batches, 1):
                logger.info(f"Submitting statement batch {batch_num}/{len(batches)} ({len(batch)} statements)")
                
                # Submit all statements in this batch
                future_to_statement = {}
                for idx, statement in enumerate(batch):
                    statement_num = (batch_num - 1) * statements_per_batch + idx + 1
                    future = executor.submit(
                        process_single_statement,
                        graph,
                        statement,
                        statement_num,
                        stop_on_error,
                    )
                    future_to_statement[future] = (statement, statement_num)
                
                # Collect results as they complete (workers pick when free)
                for future in as_completed(future_to_statement):
                    statement, statement_num = future_to_statement[future]
                    try:
                        success = future.result()
                        if success:
                            processed_count += 1
                        else:
                            failed_count += 1
                    except Exception as e:
                        logger.error(f"Error processing statement {statement_num}: {e}")
                        if stop_on_error:
                            raise
                        failed_count += 1
                
                logger.info(f"Batch {batch_num} completed. Processed: {processed_count}, Failed: {failed_count}")
        else:
            # Submit all statements at once - ThreadPoolExecutor handles work distribution
            logger.info("Submitting all statements - workers will automatically pick when free")
            future_to_statement = {}
            for idx, statement in enumerate(statements):
                statement_num = idx + 1
                future = executor.submit(
                    process_single_statement,
                    graph,
                    statement,
                    statement_num,
                    stop_on_error,
                )
                future_to_statement[future] = (statement, statement_num)
            
            # Collect results as they complete (workers pick statements automatically when free)
            for future in as_completed(future_to_statement):
                statement, statement_num = future_to_statement[future]
                try:
                    success = future.result()
                    if success:
                        processed_count += 1
                    else:
                        failed_count += 1
                except Exception as e:
                    logger.error(f"Error processing statement {statement_num}: {e}")
                    if stop_on_error:
                        raise
                    failed_count += 1
    
    logger.info(f"Finished processing. Total: {processed_count} succeeded, {failed_count} failed")
    return processed_count


# Async variants -------------------------------------------------

async def process_single_statement_async(
    statement: Tuple,
    statement_num: int,
    stop_on_error: bool = False,
) -> bool:
    """Run single statement processing asynchronously (fully async workflow)."""
    return await process_single_statement_async_native(
        statement,
        statement_num,
        stop_on_error,
    )


async def process_statements_for_date_async(
    statements: List[Tuple],
    statements_per_batch: Optional[int] = None,
    max_workers: int = 5,
    stop_on_error: bool = False,
) -> int:
    """Async processing of statements with asyncio.gather and thread offload."""
    if not statements:
        logger.info("No statements to process")
        return 0

    total_statements = len(statements)
    logger.info(f"[async] Processing {total_statements} statements (fully async workflow)")
    if statements_per_batch:
        logger.info(f"[async] Submitting statements in batches of {statements_per_batch}")

    processed_count = 0
    failed_count = 0

    async def run_batch(batch_statements, offset):
        nonlocal processed_count, failed_count
        tasks = [
            process_single_statement_async(
                statement,
                offset + idx + 1,
                stop_on_error,
            )
            for idx, statement in enumerate(batch_statements)
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for res in results:
            if res is True:
                processed_count += 1
            elif res is False:
                failed_count += 1
            elif isinstance(res, Exception):
                logger.error(f"[async] Error processing statement: {res}")
                if stop_on_error:
                    raise res
                failed_count += 1

    if statements_per_batch:
        batches = [statements[i:i + statements_per_batch] for i in range(0, total_statements, statements_per_batch)]
        logger.info(f"[async] Created {len(batches)} statement batches")
        for batch_num, batch in enumerate(batches, 1):
            logger.info(f"[async] Submitting statement batch {batch_num}/{len(batches)} ({len(batch)} statements)")
            await run_batch(batch, (batch_num - 1) * statements_per_batch)
    else:
        logger.info("[async] Submitting all statements at once")
        await run_batch(statements, 0)

    logger.info(f"[async] Finished processing. Total: {processed_count} succeeded, {failed_count} failed")
    return processed_count


async def process_single_statement_async_native(statement: Tuple, statement_num: int, stop_on_error: bool = False) -> bool:
    """Process a single statement through the async workflow (fully async, no LangGraph)
    
    Args:
        statement: Tuple of (section_type, free_text_description, review_id, review_created_at)
        statement_num: Statement number for logging
        stop_on_error: Whether to stop on errors
    
    Returns:
        bool: True if successful, False otherwise
    """
    section_type, free_text_description, review_id, review_created_at = statement
    
    # Convert review_created_at to string if it's a datetime object
    if isinstance(review_created_at, datetime):
        review_created_at_str = review_created_at.isoformat()
    else:
        review_created_at_str = str(review_created_at) if review_created_at else None
    
    logger.info(f"[async] Processing statement [{statement_num}]: {free_text_description[:100]}...")
    try:
        state = CanonicalizationState(
            input_statement=free_text_description,
            review_section=section_type,
            review_id=review_id,
            review_created_at=review_created_at_str
        )
        
        try:
            # Use async workflow (simple if/then logic)
            result = await run_canonicalization_workflow_async(state)
            status = "Success" if result.canonical_id else "Failed"
            canonical_id = result.canonical_id or 'None'
            logger.info(f"[async] Statement [{statement_num}] - Status: {status}, Canonical ID: {canonical_id}")
            return result.canonical_id is not None
        except Exception as e:
            logger.error(f"[async] Error in workflow for statement [{statement_num}]: {e}")
            if stop_on_error:
                raise
            return False
    except Exception as e:
        logger.error(f"[async] Error processing statement [{statement_num}]: {e}")
        if stop_on_error:
            raise
        return False
    finally:
        # Release thread-local connection back to pool when worker finishes
        release_thread_connection()


def process_single_statement(graph, statement: Tuple, statement_num: int, stop_on_error: bool = False) -> bool:
    """Process a single statement (sync wrapper for backward compatibility with old code)
    
    Args:
        graph: The compiled LangGraph workflow (kept for backward compatibility, but not used)
        statement: Tuple of (section_type, free_text_description, review_id, review_created_at)
        statement_num: Statement number for logging
        stop_on_error: Whether to stop on errors
    
    Returns:
        bool: True if successful, False otherwise
    """
    # Run the async version in a new event loop (for backward compatibility)
    return asyncio.run(process_single_statement_async_native(statement, statement_num, stop_on_error))
    
def get_failed_canonicalizations(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    limit: Optional[int] = None,
    error_type: Optional[str] = None
) -> List[Tuple]:
    """Get failed canonicalizations from failed_canonicalizations table
    
    Args:
        start_date: Filter by created_at >= start_date (YYYY-MM-DD format)
        end_date: Filter by created_at <= end_date (YYYY-MM-DD format)
        limit: Maximum number of failed records to return (None = no limit)
        error_type: Filter by specific error_type (e.g., 'canonicalization_failed')
    
    Returns:
        List of tuples: (input_statement, review_id, review_section, created_at, error_message)
    """
    conn = None
    cursor = None
    try:
        conn = get_postgres_connection()
        cursor = conn.cursor()
        
        query = """
            SELECT 
                input_statement,
                review_id,
                review_section,
                created_at,
                error_message
            FROM failed_canonicalizations
            WHERE 1=1
        """
        params = []
        
        if start_date:
            query += " AND date(created_at) >= %s"
            params.append(start_date)
        
        if end_date:
            query += " AND date(created_at) <= %s"
            params.append(end_date)
        
        if error_type:
            query += " AND error_type = %s"
            params.append(error_type)
        
        query += " ORDER BY created_at DESC"
        
        if limit:
            query += " LIMIT %s"
            params.append(limit)
        
        cursor.execute(query, params)
        results = cursor.fetchall()
        logger.info(f"Found {len(results)} failed canonicalizations")
        return results
        
    except Exception as e:
        logger.error(f"Error fetching failed canonicalizations: {e}")
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def rerun_failed_canonicalizations(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    limit: Optional[int] = None,
    error_type: Optional[str] = None,
    clear_old_failures: bool = False,
    statements_per_batch: Optional[int] = None,
    max_workers: int = 5,
    stop_on_error: bool = False
) -> int:
    """Rerun failed canonicalizations
    
    Args:
        start_date: Filter failed records by created_at >= start_date (YYYY-MM-DD format)
        end_date: Filter failed records by created_at <= end_date (YYYY-MM-DD format)
        limit: Maximum number of failed records to rerun (None = all matching)
        error_type: Filter by specific error_type
        clear_old_failures: If True, delete old failed records before rerunning (default: False)
        statements_per_batch: Number of statements to submit at once (None = all at once)
        max_workers: Maximum number of concurrent workers (default: 5)
        stop_on_error: If True, abort on first error (default: False)
    
    Returns:
        int: Number of statements successfully reprocessed
    """
    logger.info("=" * 60)
    logger.info("RERUNNING FAILED CANONICALIZATIONS")
    logger.info("=" * 60)
    
    # Get failed canonicalizations
    failed_records = get_failed_canonicalizations(
        start_date=start_date,
        end_date=end_date,
        limit=limit,
        error_type=error_type
    )
    
    if not failed_records:
        logger.info("No failed canonicalizations found to rerun")
        return 0
    
    logger.info(f"Found {len(failed_records)} failed canonicalizations to rerun")
    
    # Optionally clear old failures
    if clear_old_failures:
        logger.info("Clearing old failed records from failed_canonicalizations table...")
        conn = None
        cursor = None
        try:
            conn = get_postgres_connection()
            cursor = conn.cursor()
            
            delete_query = "DELETE FROM failed_canonicalizations WHERE 1=1"
            delete_params = []
            
            if start_date:
                delete_query += " AND date(created_at) >= %s"
                delete_params.append(start_date)
            
            if end_date:
                delete_query += " AND date(created_at) <= %s"
                delete_params.append(end_date)
            
            if error_type:
                delete_query += " AND error_type = %s"
                delete_params.append(error_type)
            
            cursor.execute(delete_query, delete_params)
            deleted_count = cursor.rowcount
            conn.commit()
            logger.info(f"Deleted {deleted_count} old failed records")
            
        except Exception as e:
            logger.error(f"Error clearing old failures: {e}")
            if conn:
                conn.rollback()
            raise
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
    
    # Convert failed records to statement format
    # Format: (section_type, free_text_description, review_id, review_created_at)
    # Note: We don't have review_created_at in failed_canonicalizations, so we'll use current timestamp
    statements = []
    for record in failed_records:
        input_statement, review_id, review_section, created_at, error_message = record
        # Use created_at from failed record, or current time if None
        review_created_at = created_at if created_at else datetime.now()
        statements.append((
            review_section or 'issues',  # section_type
            input_statement,              # free_text_description
            review_id,                     # review_id
            review_created_at              # review_created_at
        ))
    
    logger.info(f"Prepared {len(statements)} statements for reprocessing")
    
    # Process statements using existing function
    processed_count = process_statements_for_date(
        statements,
        statements_per_batch=statements_per_batch,
        max_workers=max_workers,
        stop_on_error=stop_on_error,
    )
    
    logger.info(f"Successfully reprocessed {processed_count}/{len(statements)} failed canonicalizations")
    return processed_count


async def main_async():
    # Initialize connection pools (sync and async) for production throughput
    try:
        init_connection_pool()
        await init_async_pool()
        logger.info("Connection pools initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize connection pools: {e}")
        raise
    
    # Set your parameters here
    # Set to None to automatically use min/max dates from uncanonized records
    start_date = None  # 'YYYY-MM-DD' format or None for auto-detect
    end_date = None  # 'YYYY-MM-DD' format or None for auto-detect
    date_range = 1  # Days to increment per iteration
    review_limit = None  # Maximum number of reviews to process (None = no limit, for testing)
    reviews_per_batch = None  # Number of reviews to process per batch (None = all reviews at once)
    statements_per_batch = None  # Number of statements to submit at once (None = all at once, workers pick when free)
    max_workers = 50  # Maximum number of concurrent workers (threads) - used by to_thread offload
    stop_on_error = False  # Continues on DB errors, but LLM errors will still stop
    
    try:
        # Step 1: Normal processing (process uncanonized statements) using async path
        logger.info("=" * 60)
        logger.info("STEP 1: Processing uncanonized statements (async)")
        logger.info("=" * 60)
        try:
            # Use async date loop leveraging existing sync helpers for dates
            # We reuse process_statements logic but swap to async per-day execution
            # Simpler: call existing process_statements (sync) via to_thread to avoid rewrite of date logic
            await asyncio.to_thread(
                process_statements,
                start_date,
                end_date,
                date_range,
                review_limit,
                reviews_per_batch,
                statements_per_batch,
                max_workers,
                stop_on_error,
            )
            logger.info("Step 1 completed successfully")
        except Exception as e:
            logger.error(f"Step 1 failed: {e}")
            # Don't raise - continue to retry step
        
        # Step 2: Retry failed canonicalizations once
        logger.info("=" * 60)
        logger.info("STEP 2: Retrying failed canonicalizations (async wrapper)")
        logger.info("=" * 60)
        try:
            retry_count = await asyncio.to_thread(
                rerun_failed_canonicalizations,
                None,  # start_date
                None,  # end_date
                None,  # limit
                None,  # error_type
                False,  # clear_old_failures
                statements_per_batch,
                max_workers,
                stop_on_error,
            )
            if retry_count > 0:
                logger.info(f"Step 2 completed: Successfully reprocessed {retry_count} failed statements")
            else:
                logger.info("Step 2 completed: No failed statements to retry")
        except Exception as e:
            logger.error(f"Step 2 failed: {e}")
            # Don't raise - we've done our best
        
        logger.info("=" * 60)
        logger.info("Processing complete - all steps finished")
        logger.info("=" * 60)
        
    except Exception as e:
        logger.error(f"Critical error during processing: {e}")
        raise
    finally:
        # Clean up connection pools on exit
        close_connection_pool()
        await close_async_pool()
        logger.info("Connection pools closed")


if __name__ == "__main__":
    asyncio.run(main_async())





