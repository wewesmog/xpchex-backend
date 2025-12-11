# Async Migration Strategy & Changes Summary

## What is `asyncio.to_thread()`?

`asyncio.to_thread()` is a bridge between sync and async code. It takes a **synchronous function** and runs it in a **separate thread** so it doesn't block the async event loop.

### How it works:

```python
# Sync function (blocks the event loop if called directly)
def process_statements(...):
    # This is sync code - uses psycopg2, blocking calls, etc.
    ...

# Wrap it with to_thread to run in background thread
await asyncio.to_thread(process_statements, arg1, arg2, ...)
```

### What happens:

1. **Without `to_thread`** (BAD):
   ```python
   async def main():
       process_statements(...)  # Blocks entire event loop!
       # Nothing else can run while this executes
   ```

2. **With `to_thread`** (GOOD):
   ```python
   async def main():
       await asyncio.to_thread(process_statements, ...)
       # Event loop can handle other tasks while this runs in background thread
   ```

### Visual Example:

```
Event Loop (Main Thread):
├─ Task 1: Processing statements (via to_thread in background thread)
├─ Task 2: Can still run other async tasks
└─ Task 3: Event loop not blocked!

Background Thread:
└─ process_statements() running here (doesn't block main loop)
```

### Why use it?

- **Sync code exists**: You have legacy sync functions you don't want to rewrite
- **Not in hot path**: These functions run infrequently (date queries, retries)
- **Quick fix**: Wraps sync code without rewriting it
- **Non-blocking**: Event loop stays responsive

---

## Migration Strategy Overview

### Philosophy: Optimize the Hot Path, Keep Simple the Rest

We focused async conversion on **per-statement execution** (runs thousands of times) and left **orchestration/helper functions** (run infrequently) as sync, wrapped with `to_thread()`.

---

## What We Changed (Async Conversion)

### ✅ 1. Core Canonicalization Functions (Hot Path)

**Location**: `canonicalization.py`

Converted to async with `asyncpg`:

- `get_exact_match_async()` - DB lookup for exact matches
- `get_lexical_similarity_async()` - PostgreSQL trigram similarity search
- `get_vector_similarity_async()` - Vector similarity search (pgvector)
- `get_hybrid_similarity_async()` - Combines lexical + vector scores
- `enrich_hybrid_results_async()` - Enriches candidates with metadata
- `save_canonicalization_result_async()` - Saves results to multiple tables
- `get_llm_input()` - Already async (LLM API calls)

**Why**: These run **thousands of times** per batch. Async = massive throughput gain.

**Before**:
```python
def get_exact_match(state):
    conn = psycopg2.connect(...)  # Blocking!
    cursor = conn.cursor()
    cursor.execute(...)  # Blocks event loop
    return state
```

**After**:
```python
async def get_exact_match_async(state):
    pool = await get_async_pool()
    async with pool.acquire() as conn:
        results = await conn.fetch(...)  # Non-blocking!
    return state
```

---

### ✅ 2. Workflow Orchestration (Hot Path)

**Location**: `canon_workflow_async.py`

- `run_canonicalization_workflow_async()` - Orchestrates the full workflow
  - Calls all async canonicalization functions
  - Replaces LangGraph with simple if/then logic
  - Fully async (no `to_thread` needed)

**Why**: This is the core workflow that processes each statement. Must be async.

**Flow**:
```
1. get_exact_match_async() → If found, save and return
2. get_lexical_similarity_async() → Get lexical matches
3. get_vector_similarity_async() → Get vector matches
4. get_hybrid_similarity_async() → Combine scores
5. enrich_hybrid_results_async() → Add metadata
6. get_llm_input() → LLM arbitration
7. save_canonicalization_result_async() → Save results
```

---

### ✅ 3. Statement Processing (Hot Path)

**Location**: `canon_main.py`

- `process_single_statement_async_native()` - Processes one statement (fully async)
- `process_statements_for_date_async()` - Processes multiple statements concurrently
  - Uses `asyncio.gather()` to run many statements **at the same time**
  - This is where the magic happens!

**Why**: This is where we process 50+ statements concurrently.

**Before** (ThreadPoolExecutor):
```python
with ThreadPoolExecutor(max_workers=50) as executor:
    futures = [executor.submit(process_single_statement, ...) for stmt in statements]
    # Each statement gets a thread
```

**After** (asyncio.gather):
```python
tasks = [process_single_statement_async(stmt) for stmt in statements]
results = await asyncio.gather(*tasks)  # All run concurrently!
# No threads needed - event loop handles it
```

---

### ✅ 4. Database Connection Pooling

**Location**: `db_async.py` (NEW FILE)

- `init_async_pool()` - Creates asyncpg connection pool
- `get_async_pool()` - Gets the pool instance
- `close_async_pool()` - Closes pool on shutdown

**Why**: Connection pooling is crucial for async. Reuses connections across thousands of operations.

**Pool Configuration**:
```python
pool = await asyncpg.create_pool(
    min_size=10,   # Always keep 10 connections ready
    max_size=50,   # Can grow to 50 connections under load
    ...
)
```

---

## What We Left as Sync (Wrapped with `to_thread`)

### 🔄 1. Date/Query Helper Functions

**Location**: `canon_main.py`

- `get_min_max_uncanonized_dates()` - Gets date range from DB
- `get_reviews_by_date_range()` - Gets review IDs for date range
- `get_statements_by_review_ids()` - Gets statements from review IDs
- `get_failed_canonicalizations()` - Gets failed records

**Why**: These run **once or twice** per batch (not thousands of times). Not worth converting.

**Usage**:
```python
# Called inside sync functions, which are wrapped with to_thread
def process_statements(...):
    min_date, max_date = get_min_max_uncanonized_dates()  # Sync, but OK
    ...
```

---

### 🔄 2. Main Orchestration Functions

**Location**: `canon_main.py`

- `process_statements()` - Main date loop function
  - Calls sync helpers
  - Calls `process_statements_for_date()` (sync version)
  - Wrapped with `asyncio.to_thread()` in `main_async()`

- `rerun_failed_canonicalizations()` - Retry logic
  - Calls sync helpers
  - Wrapped with `asyncio.to_thread()` in `main_async()`

**Why**: These orchestrate the overall process but don't process individual statements. The actual statement processing happens in the async path.

**Usage in `main_async()`**:
```python
async def main_async():
    # Step 1: Process statements (sync function in background thread)
    await asyncio.to_thread(
        process_statements,  # Sync function
        start_date, end_date, ...
    )
    
    # Step 2: Retry failures (sync function in background thread)
    await asyncio.to_thread(
        rerun_failed_canonicalizations,  # Sync function
        ...
    )
```

**What happens**:
1. `process_statements()` runs in a **background thread** (doesn't block event loop)
2. Inside it, calls `process_statements_for_date()` (sync version)
3. Which calls `process_single_statement_async_native()` (async!)
4. The async statement processing happens on the **main event loop**
5. So we get: sync orchestration (background thread) + async processing (event loop)

---

## Architecture Flow

### Overall Flow:

```
main_async() [Async Entry Point]
│
├─ Step 1: await asyncio.to_thread(process_statements, ...)
│   │
│   └─ process_statements() [Sync, runs in background thread]
│       │
│       ├─ get_min_max_uncanonized_dates() [Sync helper]
│       ├─ get_reviews_by_date_range() [Sync helper]
│       ├─ get_statements_by_review_ids() [Sync helper]
│       │
│       └─ process_statements_for_date() [Sync orchestrator]
│           │
│           └─ process_single_statement_async_native() [Async!]
│               │
│               └─ run_canonicalization_workflow_async() [Async workflow]
│                   │
│                   ├─ get_exact_match_async() [Async DB]
│                   ├─ get_lexical_similarity_async() [Async DB]
│                   ├─ get_vector_similarity_async() [Async DB]
│                   ├─ get_hybrid_similarity_async() [Async]
│                   ├─ enrich_hybrid_results_async() [Async DB]
│                   ├─ get_llm_input() [Async LLM]
│                   └─ save_canonicalization_result_async() [Async DB]
│
└─ Step 2: await asyncio.to_thread(rerun_failed_canonicalizations, ...)
    │
    └─ rerun_failed_canonicalizations() [Sync, runs in background thread]
        └─ (Similar flow as above)
```

### Key Insight:

- **Sync layer** (orchestration): Runs in background threads via `to_thread`
- **Async layer** (hot path): Runs on main event loop for maximum concurrency

---

## Performance Impact

### Before (Sync + ThreadPoolExecutor):

```
50 statements × 3 seconds each = 150 seconds (sequential)
With 50 threads: ~3 seconds (but 50 threads = high overhead)
```

### After (Async + asyncio.gather):

```
50 statements × 3 seconds = 3 seconds (concurrent, no thread overhead)
Event loop handles all 50 efficiently
```

### Key Benefits:

1. **Higher throughput**: Process 50+ statements concurrently
2. **Lower overhead**: No thread creation/destruction
3. **Better resource usage**: Event loop is more efficient than threads
4. **Scalability**: Can handle thousands of statements efficiently

---

## When to Use `asyncio.to_thread()`

### ✅ Use `to_thread` when:

1. **Legacy sync code** you don't want to rewrite
2. **Infrequent operations** (date queries, setup, cleanup)
3. **CPU-bound tasks** (though `to_thread` helps, but not ideal)
4. **Quick migration** (bridge to async without full rewrite)

### ❌ Don't use `to_thread` for:

1. **Hot path operations** (convert to async instead)
2. **I/O-bound operations** (use async libraries)
3. **Operations that run thousands of times** (convert to async)

---

## Summary

### What Changed:

1. ✅ **Per-statement execution**: Fully async (hot path)
2. ✅ **Statement batching**: Uses `asyncio.gather()` for concurrency
3. ✅ **Database access**: Migrated to `asyncpg` (async)
4. ✅ **LLM calls**: Already async, kept async
5. 🔄 **Orchestration**: Left sync, wrapped with `to_thread()`

### Strategy:

- **Optimize the hot path** (per-statement processing) → Full async
- **Keep simple the rest** (orchestration) → Sync + `to_thread()`
- **Maximum throughput** where it matters (statement processing)
- **Minimal changes** where it doesn't (helper functions)

### Result:

- Can process **50+ statements concurrently**
- **No thread overhead** for statement processing
- **Event loop** handles concurrency efficiently
- **Production-ready** for thousands of reviews

---

## Key Takeaways

1. **`asyncio.to_thread()`** = Run sync code in background thread (doesn't block event loop)
2. **`asyncio.gather()`** = Run multiple async tasks concurrently
3. **Hot path** = Fully async (statement processing)
4. **Cold path** = Sync + `to_thread()` (orchestration/helpers)
5. **Connection pooling** = Critical for async performance

This hybrid approach gives you **maximum performance** where it matters while keeping **simple code** where it doesn't.

