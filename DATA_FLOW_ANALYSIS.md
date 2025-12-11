# Data Flow Analysis: Canonicalization Pipeline

## Overview
This document traces the complete data flow from `canon_main.py` through the LangGraph workflow to database tables.

---

## High-Level Flow

```
canon_main.py → LangGraph Workflow → Database Tables
     ↓                ↓                    ↓
  Get Reviews    Process Statements    Save Results
```

---

## Stage-by-Stage Data Flow

### **STAGE 1: Data Extraction (`canon_main.py`)**

#### 1.1 Get Date Range
- **Function**: `get_min_max_uncanonized_dates()`
- **Query**: Finds uncanonized statements from `processed_app_reviews`
- **Returns**: `(min_date, max_date)` in 'YYYY-MM-DD' format
- **Tables Read**: 
  - `processed_app_reviews` (source)
  - `canonical_statements` (filter: WHERE b.statement IS NULL)

#### 1.2 Get Review IDs
- **Function**: `get_reviews_by_date_range(start_date, end_date)`
- **Query**: Gets all `review_id` for date range
- **Returns**: `List[str]` of review IDs
- **Tables Read**: `processed_app_reviews`

#### 1.3 Get Statements from Reviews
- **Function**: `get_statements_by_review_ids(review_ids)`
- **Query**: Extracts statements from JSONB fields in `processed_app_reviews`
- **Returns**: `List[Tuple]` = `(section_type, free_text_description, review_id, review_created_at)`
- **Tables Read**: 
  - `processed_app_reviews` (extracts from `latest_analysis->'issues'`, `'actions'`, `'positive_feedback'`)
  - `canonical_statements` (filter: WHERE b.statement IS NULL - only uncanonized)
- **Data Extracted**:
  - Issues: `latest_analysis->'issues'->'issues'->>'description'`
  - Actions: `latest_analysis->'issues'->'issues'->'actions'->>'description'`
  - Positives: `latest_analysis->'positive_feedback'->'positive_mentions'->>'description'`

---

### **STAGE 2: LangGraph Workflow (`canon_graph.py`)**

The workflow processes each statement through these nodes in sequence:

```
START → get_exact_match → [if no match] → get_lexical_similarity 
  → get_vector_similarity → get_hybrid_similarity → [decision] 
  → enrich_hybrid_results → get_llm_input → save_canonicalization_result → END
```

#### **Node 1: `get_exact_match`**
- **Function**: `get_exact_match(state: CanonicalizationState)`
- **Purpose**: Check for exact string match
- **Tables Read**:
  - `statement_taxonomy` (match on `display_label`, `description`, `canonical_id`, `examples`)
  - `canonical_aliases` (match on `alias`)
  - `canonical_statements` (match on `statement`)
- **State Updated**: 
  - If match: `canonical_id`, `existing_canonical_id=True`, `source='exact_match'`, `confidence_score=1.0`
  - If no match: `exact_match_result="No exact match found"`
- **Database Writes**: ❌ None (read-only)
- **Decision**: If `canonical_id` exists → go to `save_canonicalization_result`, else → continue

#### **Node 2: `get_lexical_similarity`**
- **Function**: `get_lexical_similarity(state: CanonicalizationState)`
- **Purpose**: PostgreSQL `pg_trgm` similarity search
- **Tables Read**: `statement_taxonomy` (using `similarity(description, statement)`)
- **State Updated**: `lexical_similarity_result` = Top 15 matches with similarity scores
- **Database Writes**: ❌ None (read-only)
- **Always Executes**: Yes (even if exact match found, but result may not be used)

#### **Node 3: `get_vector_similarity`**
- **Function**: `get_vector_similarity(state: CanonicalizationState)`
- **Purpose**: Vector embedding similarity search
- **Process**:
  1. Generate embedding for input statement (via `get_embedding()`)
  2. Query using cosine distance (`<=>` operator)
- **Tables Read**: 
  - `statement_taxonomy` (using `statement_embedding`)
  - `canonical_aliases` (using `alias_embedding`)
- **State Updated**: `vector_similarity_result` = Top 15 matches with similarity scores (>0.3 threshold)
- **Database Writes**: ❌ None (read-only)
- **Always Executes**: Yes

#### **Node 4: `get_hybrid_similarity`**
- **Function**: `get_hybrid_similarity(state: CanonicalizationState)`
- **Purpose**: Combine lexical + vector scores
- **Calculation**:
  - If `vector_score > 0.95`: Use `vector_score` directly
  - Else: `combined_score = 0.05 * pg_score + 0.95 * vector_score`
- **State Updated**: 
  - `hybrid_similarity_result` = Top 15 combined results
  - If top score > 0.95: Sets `canonical_id`, `existing_canonical_id=True`, `source='hybrid_similarity'`
- **Database Writes**: ❌ None (read-only)
- **Decision**: 
  - If `canonical_id` exists → `save_canonicalization_result`
  - If `hybrid_similarity_result` exists → `enrich_hybrid_results`
  - Else → `get_llm_input`

#### **Node 5: `enrich_hybrid_results`**
- **Function**: `enrich_hybrid_results(state: CanonicalizationState)`
- **Purpose**: Add metadata for top 5 candidates (for LLM context)
- **Tables Read**: 
  - `statement_taxonomy` (get `display_label`, `description`, `examples`)
  - `canonical_aliases` (aggregate aliases per `canonical_id`)
- **State Updated**: `enriched_candidates` = List of dicts with full metadata
- **Database Writes**: ❌ None (read-only)
- **Always Executes**: Only if `hybrid_similarity_result` exists

#### **Node 6: `get_llm_input`**
- **Function**: `get_llm_input(state: CanonicalizationState)`
- **Purpose**: LLM decides canonical_id or creates new one
- **Process**:
  1. If `enriched_candidates` exist → use `canonization_with_examples` prompt
  2. Else → use `canonization_without_examples` prompt
  3. Call OpenAI API via `call_llm_api()`
- **State Updated**: 
  - `canonical_id` (from LLM response)
  - `existing_canonical_id` (True if LLM selected existing, False if created new)
  - `source` = `'llm_with_examples'` or `'llm_without_examples'`
  - `llm_used = True`
- **Database Writes**: ❌ None (API call only)
- **Always Executes**: Only if no high-confidence match found

#### **Node 7: `save_canonicalization_result`** ⭐ **MAIN SAVE POINT**
- **Function**: `save_canonicalization_result(state, app_id, review_id, review_section)`
- **Purpose**: Save all results to database
- **Tables Written** (in order):

##### **7.1 If `canonical_id` exists AND `existing_canonical_id=False` AND `llm_used=True`:**

**Table: `statement_taxonomy`** (NEW canonical_id created)
- **When**: LLM created a new canonical_id
- **Data Saved**:
  ```sql
  INSERT INTO statement_taxonomy (
    canonical_id,           -- From state.canonical_id
    review_section,         -- From parameter (default: 'issues')
    category,               -- Default: 'General'
    subcategory,            -- Default: 'General'
    display_label,         -- Auto-generated from canonical_id
    description,           -- Auto-generated: "Auto-generated canonical ID for: {statement}"
    source,                -- 'llm_created'
    statement_embedding    -- Generated via get_embedding()
  )
  ```
- **Conflict Handling**: `ON CONFLICT (canonical_id) DO UPDATE` (updates existing)

**Table: `canonical_aliases`** (NEW alias for new canonical_id)
- **When**: New canonical_id created OR source is LLM/hybrid
- **Data Saved**:
  ```sql
  INSERT INTO canonical_aliases (
    alias,                 -- state.input_statement (the original statement)
    canonical_id,         -- state.canonical_id
    source,               -- 'llm_created'
    confidence,           -- state.confidence_score
    alias_embedding       -- Generated via get_embedding()
  )
  ```
- **Conflict Handling**: `ON CONFLICT (alias) DO UPDATE` (updates existing)

##### **7.2 If `canonical_id` exists (always):**

**Table: `canonical_statements`** (Statement → Canonical ID mapping)
- **When**: ALWAYS if `canonical_id` exists
- **Purpose**: Audit trail of which statements map to which canonical_id
- **Data Saved**:
  ```sql
  INSERT INTO canonical_statements (
    statement,             -- state.input_statement (original statement text)
    canonical_id,         -- state.canonical_id
    source,               -- state.source ('exact_match', 'hybrid_similarity', 'llm_with_examples', etc.)
    confidence,           -- state.confidence_score
    statement_embedding,   -- NULL (to avoid duplication)
    review_section        -- From parameter (default: 'issues')
  )
  ```
- **Conflict Handling**: `ON CONFLICT (statement) DO UPDATE` (updates existing mapping)

##### **7.3 Always (success or failure):**

**Table: `canonicalization_results`** (Complete audit log)
- **When**: ALWAYS (both success and failure)
- **Purpose**: Full debugging/audit trail of entire canonicalization process
- **Data Saved**:
  ```sql
  INSERT INTO canonicalization_results (
    input_statement,              -- Original statement
    canonical_id,                 -- Result (NULL if failed)
    existing_canonical_id,        -- True/False
    source,                       -- 'exact_match', 'hybrid_similarity', 'llm_with_examples', etc.
    confidence_score,             -- 0.0 to 1.0
    results,                      -- Human-readable result string
    llm_used,                     -- True/False
    node_history,                 -- JSONB: Array of all nodes executed
    errors,                       -- JSONB: Array of errors encountered
    enriched_candidates,          -- JSONB: Top candidates for LLM
    enrich_hybrid_results_result, -- Result string
    llm_with_examples_result,     -- LLM output if used
    llm_without_examples_result,  -- LLM output if used
    llm_with_examples_error,      -- Error if LLM failed
    llm_without_examples_error,    -- Error if LLM failed
    exact_match_result,           -- "Exact match found" or "No exact match found"
    exact_match_error,            -- Error if exact match failed
    lexical_similarity_result,   -- JSONB: Top 15 lexical matches
    lexical_similarity_error,     -- Error if lexical failed
    vector_similarity_result,    -- JSONB: Top 15 vector matches
    vector_similarity_error,       -- Error if vector failed
    hybrid_similarity_result,     -- JSONB: Combined results
    hybrid_similarity_error,      -- Error if hybrid failed
    enrich_hybrid_results_error   -- Error if enrichment failed
  )
  ```
- **No Conflict Handling**: Always inserts (historical record)

##### **7.4 If `canonical_id` exists (SUCCESS):**

**Table: `review_statements`** (Review → Canonical ID linkage)
- **When**: SUCCESS case only (`canonical_id` is not NULL)
- **Purpose**: Link reviews to canonicalized statements
- **Data Saved**:
  ```sql
  INSERT INTO review_statements (
    review_id,                    -- From parameter (from original review)
    app_id,                       -- From parameter (default: 'unknown')
    canonical_id,                 -- state.canonical_id
    review_section,              -- From parameter (default: 'unknown')
    severity,                     -- Default: 'medium'
    impact_score,                 -- Default: 50.0
    confidence,                   -- state.confidence_score
    source,                       -- state.source
    canonicalization_status,      -- 'success'
    node_history,                 -- JSONB: Execution path
    errors                        -- JSONB: Errors (if any)
  )
  ```
- **Conflict Handling**: `ON CONFLICT (review_id, canonical_id) DO UPDATE` (prevents duplicates)

##### **7.5 If `canonical_id` is NULL (FAILURE):**

**Table: `failed_canonicalizations`** (Failed attempts)
- **When**: FAILURE case only (`canonical_id` is NULL)
- **Purpose**: Track statements that couldn't be canonicalized
- **Data Saved**:
  ```sql
  INSERT INTO failed_canonicalizations (
    review_id,                    -- From parameter
    app_id,                       -- From parameter (default: 'unknown')
    input_statement,              -- Original statement that failed
    review_section,              -- From parameter
    severity,                     -- Default: 'medium'
    impact_score,                 -- Default: 50.0
    confidence,                   -- state.confidence_score (likely 0.0)
    source,                       -- state.source (likely 'failed')
    canonicalization_status,     -- 'failed'
    error_type,                   -- 'canonicalization_failed'
    error_message,               -- 'Failed to generate canonical_id'
    node_history,                 -- JSONB: Execution path
    errors                        -- JSONB: All errors encountered
  )
  ```
- **No Conflict Handling**: Always inserts (track all failures)

---

## Summary Table: Database Operations by Stage

| Stage | Function | Tables Read | Tables Written | Purpose |
|-------|----------|-------------|----------------|---------|
| **1.1** | `get_min_max_uncanonized_dates()` | `processed_app_reviews`, `canonical_statements` | ❌ | Find date range |
| **1.2** | `get_reviews_by_date_range()` | `processed_app_reviews` | ❌ | Get review IDs |
| **1.3** | `get_statements_by_review_ids()` | `processed_app_reviews`, `canonical_statements` | ❌ | Extract statements |
| **2.1** | `get_exact_match()` | `statement_taxonomy`, `canonical_aliases`, `canonical_statements` | ❌ | Exact match check |
| **2.2** | `get_lexical_similarity()` | `statement_taxonomy` | ❌ | pg_trgm similarity |
| **2.3** | `get_vector_similarity()` | `statement_taxonomy`, `canonical_aliases` | ❌ | Vector similarity |
| **2.4** | `get_hybrid_similarity()` | (uses results from 2.2, 2.3) | ❌ | Combine scores |
| **2.5** | `enrich_hybrid_results()` | `statement_taxonomy`, `canonical_aliases` | ❌ | Enrich for LLM |
| **2.6** | `get_llm_input()` | (none - API call) | ❌ | LLM decision |
| **2.7** | `save_canonicalization_result()` | (none) | **5 tables** | **SAVE ALL RESULTS** |

---

## Database Tables Written (Final Stage)

### **Always Written:**
1. ✅ `canonicalization_results` - Complete audit log (success + failure)

### **Written if `canonical_id` exists:**
2. ✅ `canonical_statements` - Statement → Canonical ID mapping
3. ✅ `review_statements` - Review → Canonical ID linkage (SUCCESS)

### **Written if NEW canonical_id created:**
4. ✅ `statement_taxonomy` - New canonical_id entry
5. ✅ `canonical_aliases` - Alias for new canonical_id

### **Written if `canonical_id` is NULL:**
6. ✅ `failed_canonicalizations` - Failed canonicalization attempts

---

## Key Points

1. **No writes until final node**: All database writes happen in `save_canonicalization_result()` only
2. **Read-heavy workflow**: Most nodes only read from database
3. **Audit trail**: `canonicalization_results` captures everything for debugging
4. **Deduplication**: `canonical_statements` prevents reprocessing same statement
5. **Failure tracking**: `failed_canonicalizations` tracks all failures separately
6. **Transaction safety**: All writes in `save_canonicalization_result()` are in a single transaction (commit/rollback)

---

## Data Flow Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│ canon_main.py                                                    │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │ 1. get_min_max_uncanonized_dates()                       │  │
│  │    READ: processed_app_reviews, canonical_statements      │  │
│  └──────────────────────────────────────────────────────────┘  │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │ 2. get_reviews_by_date_range()                            │  │
│  │    READ: processed_app_reviews                            │  │
│  └──────────────────────────────────────────────────────────┘  │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │ 3. get_statements_by_review_ids()                         │  │
│  │    READ: processed_app_reviews, canonical_statements     │  │
│  │    RETURNS: List[(section_type, statement, review_id)]  │  │
│  └──────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────────┐
│ LangGraph Workflow (per statement)                              │
│  ┌──────────────────────────────────────────────────────────┐ │
│  │ get_exact_match()                                          │ │
│  │   READ: statement_taxonomy, canonical_aliases,              │ │
│  │         canonical_statements                              │ │
│  └──────────────────────────────────────────────────────────┘ │
│  ┌──────────────────────────────────────────────────────────┐ │
│  │ get_lexical_similarity()                                  │ │
│  │   READ: statement_taxonomy (pg_trgm)                      │ │
│  └──────────────────────────────────────────────────────────┘ │
│  ┌──────────────────────────────────────────────────────────┐ │
│  │ get_vector_similarity()                                   │ │
│  │   READ: statement_taxonomy, canonical_aliases (vectors)   │ │
│  └──────────────────────────────────────────────────────────┘ │
│  ┌──────────────────────────────────────────────────────────┐ │
│  │ get_hybrid_similarity()                                   │ │
│  │   (combines lexical + vector, no DB read)                  │ │
│  └──────────────────────────────────────────────────────────┘ │
│  ┌──────────────────────────────────────────────────────────┐ │
│  │ enrich_hybrid_results()                                   │ │
│  │   READ: statement_taxonomy, canonical_aliases             │ │
│  └──────────────────────────────────────────────────────────┘ │
│  ┌──────────────────────────────────────────────────────────┐ │
│  │ get_llm_input()                                           │ │
│  │   (API call, no DB)                                       │ │
│  └──────────────────────────────────────────────────────────┘ │
│  ┌──────────────────────────────────────────────────────────┐ │
│  │ save_canonicalization_result() ⭐                         │ │
│  │   WRITE: 5 tables (see details above)                    │ │
│  └──────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────────┐
│ Database Tables Written                                         │
│  ✅ canonicalization_results (always)                           │
│  ✅ canonical_statements (if success)                            │
│  ✅ review_statements (if success)                              │
│  ✅ statement_taxonomy (if new canonical_id)                   │
│  ✅ canonical_aliases (if new canonical_id)                     │
│  ✅ failed_canonicalizations (if failure)                      │
└─────────────────────────────────────────────────────────────────┘
```



