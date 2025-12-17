# How to Rerun Failed Canonicalizations

## Overview

Failed canonicalizations are stored in the `failed_canonicalizations` table. Since they are **NOT** saved to `canonical_statements`, they will be automatically picked up again in the next normal run. However, you can also explicitly rerun them using the dedicated function.

---

## Method 1: Automatic Rerun (Default Behavior)

**How it works:**
- Failed statements are saved to `failed_canonicalizations` but **NOT** to `canonical_statements`
- The query in `get_statements_by_review_ids()` filters: `WHERE b.statement IS NULL`
- Since failed statements aren't in `canonical_statements`, they will be picked up automatically

**To rerun:**
Just run the normal processing - failed statements will be included:

```python
python -m app.reviews_helpers.canon_main
```

---

## Method 2: Explicit Rerun (New Function)

Use the new `rerun_failed_canonicalizations()` function to explicitly rerun only failed ones.

### Basic Usage

```python
from app.reviews_helpers.canon_main import rerun_failed_canonicalizations

# Rerun all failed canonicalizations
rerun_failed_canonicalizations()
```

### Advanced Usage

```python
# Rerun failed canonicalizations from a specific date range
rerun_failed_canonicalizations(
    start_date='2025-01-01',
    end_date='2025-01-31',
    limit=100,  # Only rerun first 100
    clear_old_failures=True  # Delete old failures before rerunning
)
```

### Parameters

| Parameter | Type | Default | Description |
|----------|------|---------|-------------|
| `start_date` | `str \| None` | `None` | Filter by `created_at >= start_date` (YYYY-MM-DD) |
| `end_date` | `str \| None` | `None` | Filter by `created_at <= end_date` (YYYY-MM-DD) |
| `limit` | `int \| None` | `None` | Maximum number of failed records to rerun |
| `error_type` | `str \| None` | `None` | Filter by specific error_type (e.g., 'canonicalization_failed') |
| `clear_old_failures` | `bool` | `False` | Delete old failed records before rerunning |
| `statements_per_batch` | `int \| None` | `None` | Number of statements per batch |
| `max_workers` | `int` | `5` | Number of concurrent workers |
| `stop_on_error` | `bool` | `False` | Abort on first error |

---

## Method 3: Query Failed Records First

Before rerunning, you can query what failed:

```python
from app.reviews_helpers.canon_main import get_failed_canonicalizations

# Get all failed canonicalizations
failed = get_failed_canonicalizations()

# Get failed from date range
failed = get_failed_canonicalizations(
    start_date='2025-01-01',
    end_date='2025-01-31',
    limit=50
)

# Get specific error type
failed = get_failed_canonicalizations(
    error_type='canonicalization_failed'
)

# Print results
for record in failed:
    statement, review_id, section, created_at, error_msg = record
    print(f"Failed: {statement[:50]}... | Review: {review_id} | Error: {error_msg}")
```

---

## Example: Rerun from Command Line

### Option 1: Modify `canon_main.py`

Edit the `if __name__ == "__main__":` section:

```python
if __name__ == "__main__":
    # Rerun failed canonicalizations
    rerun_failed_canonicalizations(
        start_date=None,  # All dates
        end_date=None,
        limit=None,  # All failed records
        error_type=None,  # All error types
        clear_old_failures=False,  # Keep old records
        statements_per_batch=None,
        max_workers=5,
        stop_on_error=False
    )
```

Then run:
```bash
python -m app.reviews_helpers.canon_main
```

### Option 2: Create a Separate Script

Create `rerun_failed.py`:

```python
from app.reviews_helpers.canon_main import rerun_failed_canonicalizations

if __name__ == "__main__":
    rerun_failed_canonicalizations(
        start_date='2025-01-01',
        limit=100,
        clear_old_failures=True
    )
```

Run:
```bash
python rerun_failed.py
```

---

## Common Scenarios

### Scenario 1: Rerun All Failed from Last Week

```python
from datetime import datetime, timedelta

last_week = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
rerun_failed_canonicalizations(
    start_date=last_week,
    clear_old_failures=False
)
```

### Scenario 2: Rerun Specific Error Type

```python
rerun_failed_canonicalizations(
    error_type='canonicalization_failed',
    limit=50
)
```

### Scenario 3: Clear Old Failures and Rerun

```python
rerun_failed_canonicalizations(
    start_date='2025-01-01',
    clear_old_failures=True  # Deletes old failures first
)
```

### Scenario 4: Rerun with Higher Concurrency

```python
rerun_failed_canonicalizations(
    max_workers=10,  # More workers
    statements_per_batch=20
)
```

---

## SQL Queries (Direct Database Access)

### Count Failed Canonicalizations

```sql
SELECT COUNT(*) 
FROM failed_canonicalizations;
```

### Get Failed by Date Range

```sql
SELECT 
    input_statement,
    review_id,
    error_message,
    created_at
FROM failed_canonicalizations
WHERE date(created_at) BETWEEN '2025-01-01' AND '2025-01-31'
ORDER BY created_at DESC;
```

### Get Failed by Error Type

```sql
SELECT 
    error_type,
    COUNT(*) as count
FROM failed_canonicalizations
GROUP BY error_type;
```

### Delete Old Failures (Manual)

```sql
-- Delete failures from specific date range
DELETE FROM failed_canonicalizations
WHERE date(created_at) < '2025-01-01';

-- Delete all failures (use with caution!)
DELETE FROM failed_canonicalizations;
```

---

## Important Notes

1. **Automatic Rerun**: Failed statements are automatically included in normal processing runs
2. **No Duplicates**: The system prevents duplicate processing via `canonical_statements` table
3. **Audit Trail**: All failures are logged in `canonicalization_results` table for debugging
4. **Transaction Safety**: Each rerun is processed in the same transaction-safe manner as normal processing
5. **Clear Old Failures**: Use `clear_old_failures=True` carefully - it deletes records before rerunning

---

## Troubleshooting

### Q: Why are failed statements not being rerun automatically?

**A:** Check if they're actually in `canonical_statements`:
```sql
SELECT * FROM canonical_statements 
WHERE statement = 'your_failed_statement';
```

If they are, they won't be reprocessed. You may need to delete them first.

### Q: How to rerun only specific failed statements?

**A:** Use `get_failed_canonicalizations()` to filter, then create a custom script to process only those.

### Q: Can I rerun without deleting old failures?

**A:** Yes, set `clear_old_failures=False` (default). Old failures will remain in the table for audit purposes.

### Q: What happens if a statement fails again?

**A:** A new record is inserted into `failed_canonicalizations` with a new `created_at` timestamp. Old records remain unless you explicitly delete them.




