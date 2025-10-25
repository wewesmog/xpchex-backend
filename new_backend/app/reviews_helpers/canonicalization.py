#canonicalization.py
#1. Get statement from reviwise
#2. search for the statement using pg_trm, in statement_taxonomy & aliases and assign likelhood %
#3. search for the statement using vector similarity, in statement_taxonomy & aliases and assign likelhood %
#4. Calculate the weighted vector * 0.7 + pg_trm * 0.3
#5. Return top 15
#6. If the first statement is >0.7, return the canonical_id
#7. If the first statement is <0.7, return the top 15
#8. Pass the top 15 to the LLM to choose the best canonical_id with %
#9. If the top likelihood is >0.7, return the canonical_id
#10. If the top likelihood is <0.7, return the top 15
#11. Ask LLM to create a new canonical_id

#Whenever a canonical_id is selected (not new):
# 1. Insert into canonical statements table (for audit purposes)
# 2. Insert into  review statements table( if statement is not in the table, insert it)

#Whenever a new canonical_id is created:
# 1. Insert into canonical statements table (for audit purposes)
# 2. Insert into statement_taxonomy table 
# 3. Insert into  review statements table( if statement is not in the table, insert it)

from app.reviews_helpers.vectorizer import get_embedding
from app.shared_services.db import get_postgres_connection
from app.shared_services.logger_setup import setup_logger
from typing import List, Tuple, Optional, Dict
import json
from datetime import datetime

from app.models.canonicalization_models import llm_input, llm_output, CanonicalizationResult, CanonicalizationState, node_history
from app.shared_services.llm import call_llm_api_openai
from app.prompts.canonicalization_prompts import canonization_with_examples, canonization_without_examples

logger = setup_logger()

def get_statements_by_date_range(start_date: str, end_date: str) -> List[Tuple[str, str, str]]:
    """
    Get all relevant text descriptions for a date range.
    Returns: List of tuples containing (section_type, description, review_id)
    """
    conn = None
    cursor = None
    statements = []
    try:
        conn = get_postgres_connection()
        cursor = conn.cursor()
        # Log the query parameters
        logger.info(f"Executing query with date range: {start_date} to {end_date}")
        
        # First check if we have any data for this date range
        cursor.execute("""
            SELECT COUNT(*) FROM processed_app_reviews 
            WHERE date(review_created_at) BETWEEN %s AND %s
        """, (start_date, end_date))
        count = cursor.fetchone()[0]
        logger.info(f"Found {count} reviews in processed_app_reviews for date range")
        
        cursor.execute("""
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
                    AND date(review_created_at) BETWEEN %s AND %s

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
                    AND date(review_created_at) BETWEEN %s AND %s

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
                    AND date(review_created_at) BETWEEN %s AND %s
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
        """, (start_date, end_date, start_date, end_date, start_date, end_date))
        
        statements = cursor.fetchall()
        logger.info(f"Found {len(statements)} statements for date range {start_date} to {end_date}")
        return statements

    except Exception as e:
        logger.error(f"Error fetching statements for date range {start_date} to {end_date}: {e}")
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def get_exact_match(state: CanonicalizationState) -> CanonicalizationState:
    """Get exact match from statement_taxonomy or canonical_aliases."""
    import json
    from datetime import datetime
    
    statement_lower = state.input_statement.lower()
    logger.info(f"Finding exact match for {statement_lower}")
    conn = get_postgres_connection()
    try:
        cursor = conn.cursor()
        # Fix: Properly format JSON array contains query
        cursor.execute("""
            SELECT canonical_id 
            FROM statement_taxonomy 
            WHERE 
                lower(trim(rtrim(display_label, '.'))) = %s
                OR lower(trim(rtrim(description, '.'))) = %s
               OR lower(trim(rtrim(canonical_id, '.'))) = %s
               OR examples @> %s  -- Use JSONB array contains (no lower() on JSONB)
            UNION
            SELECT canonical_id
            FROM canonical_aliases
            WHERE lower(trim(rtrim(alias, '.'))) = lower(trim(rtrim(%s, '.')))
            UNION 
            SELECT canonical_id
            FROM canonical_statements
            WHERE lower(trim(rtrim(statement, '.'))) = lower(trim(rtrim(%s, '.')))
        """, (
            statement_lower,  # match display_label
            statement_lower,  # match description
            statement_lower,  # match canonical_id
            json.dumps([statement_lower]),  # properly format as JSON array for @>
            statement_lower,   # match alias
            statement_lower   # match statement
        ))
        result = cursor.fetchone()
        if result:
            logger.info(f"Exact match found for {state.input_statement}: {result[0]}") 
            state.exact_match_result = "Exact match found"
            state.canonical_id = result[0]
            state.existing_canonical_id = True
            state.source = 'exact_match'
            state.confidence_score = 1.0
            state.node_history.append(node_history(node_name='exact_match', timestamp=datetime.now().isoformat()))
            return state
        else:
            logger.info(f"No exact match found for {state.input_statement}")
            state.exact_match_result = "No exact match found"
            state.node_history.append(node_history(node_name='exact_match', timestamp=datetime.now().isoformat()))
            return state
    except Exception as e:
        logger.error(f"Error in exact match: {e}")
        # Update state with error
        state.exact_match_result = "Error in exact match"
        state.exact_match_error = str(e)
        state.node_history.append(node_history(node_name='exact_match', timestamp=datetime.now().isoformat()))
        return state
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# Get match with pg_trm
#Take a statement and match against all statements using pg_trm, while returning the similiarity

def get_lexical_similarity(state: CanonicalizationState) -> CanonicalizationState:
    """Get similarity from statement_taxonomy or canonical_aliases."""
    statement = state.input_statement
    conn = get_postgres_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT  canonical_id,
                       description,
                       similarity(description, %s) as similarity
                      
            FROM statement_taxonomy
            WHERE description IS NOT NULL
            ORDER BY similarity DESC
            LIMIT 15
        """, (statement,))
        results = cursor.fetchall()
        if results:
            logger.info(f"Lexical Similarity found for {statement}: {results[:5]}")
            state.lexical_similarity_result = results     
            state.node_history.append(node_history(node_name='lexical_similarity', timestamp=datetime.now().isoformat()))
            return state
        else:
            logger.info(f"No Lexical Similarity found for {statement}")
            state.lexical_similarity_result = None
            state.node_history.append(node_history(node_name='lexical_similarity', timestamp=datetime.now().isoformat()))
            return state
    except Exception as e:
        print(f"Error in lexical similarity: {e}")
        state.lexical_similarity_error = str(e)
        state.node_history.append(node_history(node_name='lexical_similarity', timestamp=datetime.now().isoformat()))
        return state
    finally:
        conn.close()

def get_vector_similarity(state: CanonicalizationState) -> CanonicalizationState:
    """Get vector similarity from statement_taxonomy or canonical_aliases."""
    statement = state.input_statement
    # Get the embedding of the statement
    embedding = get_embedding(statement)
    if not embedding:
        logger.warning(f"Could not get embedding for statement: {statement}")
        state.vector_similarity_result = None
        state.vector_similarity_error = "Could not get embedding for statement"
        state.node_history.append(node_history(node_name='vector_similarity', timestamp=datetime.now().isoformat()))
        return state
        
    conn = get_postgres_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT canonical_id, existing_statement, similarity
            FROM (
                SELECT  canonical_id,
                       description as existing_statement,
                       1 - (statement_embedding <=> %s::vector) / 2 as similarity
                FROM statement_taxonomy
                WHERE statement_embedding IS NOT NULL
                and  1 - (statement_embedding <=> %s::vector) / 2 > 0.3
                       
                UNION ALL
                       
                SELECT  canonical_id,
                       alias as existing_statement,
                       1 - (alias_embedding <=> %s::vector) / 2 as similarity
                FROM canonical_aliases
                WHERE alias_embedding IS NOT NULL
                and  1 - (alias_embedding <=> %s::vector) / 2 > 0.3
            ) combined_results
                
            ORDER BY similarity DESC
            LIMIT 15
        """, (embedding, embedding,embedding,embedding))
        results = cursor.fetchall()
        if results:
            logger.info(f"Vector similarity found for {statement}: {results[:5]}")
            state.vector_similarity_result = results
            state.node_history.append(node_history(node_name='vector_similarity', timestamp=datetime.now().isoformat()))
            return state
        else:
            logger.info(f"No vector similarity found for {statement}")
            state.vector_similarity_result = None
            state.vector_similarity_error = "No vector similarity found for statement"
            state.node_history.append(node_history(node_name='vector_similarity', timestamp=datetime.now().isoformat()))
            return state
    except Exception as e:
        print(f"Error in vector similarity: {e}")
        state.vector_similarity_error = str(e)
        state.node_history.append(node_history(node_name='vector_similarity', timestamp=datetime.now().isoformat()))
        return state
    finally:
        conn.close()

# Get weighted similarity, 0.3 * pg_trm + 0.7 * vector similarity. 
# Pick the top 15 from both similarity and vector similarity (already done by the two functions)
# If statement is appearing both then do  0.3 * pg_trm + 0.7 * vector similarity
# If statement is appearing in vector only, do 0.3 * 0 + 0.7 * vector similarity
# If statement is appearing in pg_trm only, do 0.3 * pg_trm + 0.7 * 0, 
# Return the top 15

def get_hybrid_similarity(state: CanonicalizationState) -> CanonicalizationState:
    """Get combined pg_trgm + vector similarity scores with deduplication and tie-breaking."""
    try:
        # Get lexical and vector similarity results
        state = get_lexical_similarity(state)
        state = get_vector_similarity(state)
        
        # Check if we have any results
        if not state.lexical_similarity_result and not state.vector_similarity_result:
            state.hybrid_similarity_result = None
            state.hybrid_similarity_error = "No similarity score for statement"
            state.node_history.append(node_history(node_name='hybrid_similarity', timestamp=datetime.now().isoformat()))
            return state
        
        # Use canonical_id as key to prevent duplicates
        combined_dict = {}
        
        # Process pg_trgm results
        if state.lexical_similarity_result:
            for canonical_id, description, pg_score in state.lexical_similarity_result:
                if canonical_id not in combined_dict:
                    combined_dict[canonical_id] = {
                        'pg_score': pg_score,
                        'vector_score': 0.0,
                        'statement_text': description
                    }
                else:
                    # Take higher score if same canonical_id appears again
                    combined_dict[canonical_id]['pg_score'] = max(
                        combined_dict[canonical_id]['pg_score'], 
                        pg_score
                    )
        
        # Process vector results
        if state.vector_similarity_result:
            for canonical_id, existing_statement, vector_score in state.vector_similarity_result:
                if canonical_id not in combined_dict:
                    combined_dict[canonical_id] = {
                        'pg_score': 0.0,
                        'vector_score': vector_score,
                        'statement_text': existing_statement
                    }
                else:
                    # Update existing entry
                    combined_dict[canonical_id]['vector_score'] = max(
                        combined_dict[canonical_id]['vector_score'], 
                        vector_score
                    )
                    # Keep statement text from method with higher score
                    if vector_score > combined_dict[canonical_id]['pg_score']:
                        combined_dict[canonical_id]['statement_text'] = existing_statement
        
        # Convert to final format
        combined_results = []
        for canonical_id, data in combined_dict.items():
            if data['vector_score'] > 0.95:
                combined_score = data['vector_score']
                combined_results.append((
                canonical_id, 
                data['statement_text'], 
                data['pg_score'], 
                data['vector_score'], 
                combined_score
            ))
            else:
                combined_score = 0.05 * data['pg_score'] + 0.95 * data['vector_score']
                combined_results.append((
                canonical_id, 
                data['statement_text'], 
                data['pg_score'], 
                data['vector_score'], 
                combined_score
            ))
        combined_results.sort(key=lambda x: x[4], reverse=True)
        combined_results = combined_results[:15]

        # Store the full results list
        state.hybrid_similarity_result = combined_results
        
        # Assign canonical_id if the highest score is > 0.95
        if combined_results and combined_results[0][4] > 0.95:
            state.canonical_id = combined_results[0][0]
            state.existing_canonical_id = True
            state.source = 'hybrid_similarity'
            state.confidence_score = combined_results[0][4]
            state.results = f"High confidence hybrid match: {combined_results[0][0]} (score: {combined_results[0][4]:.3f})"
        else:
            state.canonical_id = None
            state.results = f"Low confidence hybrid match, top score: {combined_results[0][4]:.3f}" if combined_results else "No hybrid matches found"
        
        state.node_history.append(node_history(node_name='hybrid_similarity', timestamp=datetime.now().isoformat()))
        return state
        
    except Exception as e:
        logger.error(f"Error in hybrid similarity: {e}")
        state.hybrid_similarity_error = str(e)
        state.error.append({
            "source": "hybrid_similarity",
            "timestamp": datetime.now().isoformat(),
            "message": f"Hybrid similarity error: {str(e)}"
        })
        state.node_history.append(node_history(node_name='hybrid_similarity', timestamp=datetime.now().isoformat()))
        return state
 
    
def enrich_hybrid_results(state: CanonicalizationState) -> CanonicalizationState:
        """Enrich hybrid similarity results with display_label, description, examples, and aliases."""
        try:
            if not state.hybrid_similarity_result:
                logger.warning("No hybrid similarity results to enrich")
                state.enrich_hybrid_results_error = "No hybrid similarity results to enrich"
                state.node_history.append(node_history(node_name='enrich_hybrid_results', timestamp=datetime.now().isoformat()))
                return state
                
            conn = get_postgres_connection()
            cursor = conn.cursor()
            
            # Enrich top 5 candidates from hybrid results
            enriched_candidates = []
            # Handle both single tuple and list of tuples
            hybrid_results = state.hybrid_similarity_result if isinstance(state.hybrid_similarity_result, list) else [state.hybrid_similarity_result]
            
            for cid, text, pg_score, vector_score, combined_score in hybrid_results[:5]:  # Top 5 only
                cursor.execute("""
                    WITH aggregated_aliases AS (
                        SELECT
                            t.canonical_id,
                            ARRAY_AGG(t.alias ORDER BY t.alias) AS aliases
                        FROM
                            canonical_aliases t 
                        GROUP BY
                            t.canonical_id
                    )
                    SELECT
                        st.canonical_id,
                        st.display_label,
                        st.description,
                        st.examples,
                        COALESCE(aa.aliases, ARRAY[]::text[]) as aliases
                    FROM
                        statement_taxonomy st
                    LEFT OUTER JOIN
                        aggregated_aliases aa ON st.canonical_id = aa.canonical_id
                    WHERE
                        st.canonical_id = %s;
                """, (cid,))
                result = cursor.fetchone()
                if result:
                    logger.info(f"Enriched canonical_id: {cid} with display_label: {result[1]}, description: {result[2]}")
                    enriched_candidates.append({
                        'canonical_id': result[0],
                        'display_label': result[1],
                        'description': result[2],
                        'examples': result[3],
                        'aliases': result[4],
                        'combined_score': combined_score,
                        'pg_score': pg_score,
                        'vector_score': vector_score
                    })
                else:
                    logger.warning(f"No data found for canonical_id: {cid}")
            
            state.enriched_candidates = enriched_candidates
            state.enrich_hybrid_results_result = f"Enriched {len(enriched_candidates)} candidates"
            state.results = f"Enriched {len(enriched_candidates)} candidates for LLM"
            state.node_history.append(node_history(node_name='enrich_hybrid_results', timestamp=datetime.now().isoformat()))
            return state
            
        except Exception as e:
            logger.error(f"Error enriching hybrid results: {e}")
            state.enrich_hybrid_results_error = str(e)
            state.error.append({
                "source": "enrich_hybrid_results",
                "timestamp": datetime.now().isoformat(),
                "message": f"Enrich hybrid results error: {str(e)}"
            })
            state.node_history.append(node_history(node_name='enrich_hybrid_results', timestamp=datetime.now().isoformat()))
            return state
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

def get_llm_input(state: CanonicalizationState) -> CanonicalizationState:
    """Get LLM input for canonicalization."""
    logger.info("Starting llm canonicalization")
    
    try:
        # Create llm_input object from state
        input_data = llm_input(
            statement=state.input_statement,
            enriched_candidates=state.enriched_candidates if hasattr(state, 'enriched_candidates') else None
        )
        
        # Generate prompts
        examples_prompt = canonization_with_examples(input_data)
        no_examples_prompt = canonization_without_examples(input_data)

        if input_data.enriched_candidates:
            logger.info(f"Using examples for LLM canonicalization with {len(input_data.enriched_candidates)} candidates")
            logger.info(f"Enriched candidates: {input_data.enriched_candidates}")
            try:
                messages = [
                    {"role": "system", "content": "You are a helpful assistant that canonicalizes statements."},
                    {"role": "user", "content": examples_prompt}
                ]
                result = call_llm_api_openai(messages, response_format=llm_output)
                logger.info(f"LLM output: {result}")
                if result:
                    state.llm_with_examples_result = result.canonical_id
                    state.canonical_id = result.canonical_id
                    state.existing_canonical_id = result.existing_canonical_id
                    state.source = 'llm_with_examples'
                    state.results = f"LLM selected: {result.canonical_id} (existing: {result.existing_canonical_id})"
                else:
                    state.llm_with_examples_result = None
                    state.results = "LLM returned no result"
                state.llm_used = True
                state.node_history.append(node_history(node_name='llm_with_examples', timestamp=datetime.now().isoformat()))
                return state
            except Exception as e:
                logger.error(f"Error in LLM API call: {e}")
                state.llm_with_examples_error = str(e)
                state.node_history.append(node_history(node_name='llm_with_examples', timestamp=datetime.now().isoformat()))
                return state
        else:
            logger.info(f"No examples found for LLM canonicalization")
            try:
                messages = [
                    {"role": "system", "content": "You are a helpful assistant that canonicalizes statements."},
                    {"role": "user", "content": no_examples_prompt}
                ]
                result = call_llm_api_openai(messages, response_format=llm_output)
                logger.info(f"LLM output: {result}")
                if result:
                    state.llm_without_examples_result = result.canonical_id
                    state.canonical_id = result.canonical_id
                    state.existing_canonical_id = result.existing_canonical_id
                    state.source = 'llm_without_examples'
                    state.results = f"LLM created new: {result.canonical_id}"
                else:
                    state.llm_without_examples_result = None
                    state.results = "LLM returned no result"
                state.llm_used = True
                state.node_history.append(node_history(node_name='llm_without_examples', timestamp=datetime.now().isoformat()))
                return state
            except Exception as e:
                logger.error(f"Error in LLM API call: {e}")
                state.llm_without_examples_error = str(e)
                state.error.append({
                    "source": "llm_without_examples",
                    "timestamp": datetime.now().isoformat(),
                    "message": f"LLM without examples error: {str(e)}"
                })
                state.node_history.append(node_history(node_name='llm_without_examples', timestamp=datetime.now().isoformat()))
                return state
                
    except Exception as e:
        logger.error(f"Error in get_llm_input: {e}")
        state.llm_without_examples_error = str(e)
        state.error.append({
            "source": "llm_input",
            "timestamp": datetime.now().isoformat(),
            "message": f"LLM input error: {str(e)}"
        })
        state.node_history.append(node_history(node_name='llm_input', timestamp=datetime.now().isoformat()))
        return state
    
def save_canonicalization_result(state: CanonicalizationState, app_id: str = None, review_id: str = None, review_section: str = None) -> CanonicalizationState:
    """
    Save canonicalization result to database (both success and failure).
    Failure is determined by whether canonical_id is NULL.
    """
    try:
        conn = get_postgres_connection()
        cursor = conn.cursor()
        
        # Convert Pydantic models to dictionaries for JSONB storage
        node_history_dict = [node.dict() for node in state.node_history] if state.node_history else []
        
        # Only save to other tables if we have a canonical_id (success case)
        if state.canonical_id:
            # 2. If LLM created new canonical_id, save to statement_taxonomy FIRST
            if not state.existing_canonical_id and state.llm_used:
                logger.info(f"LLM created new canonical_id: {state.canonical_id}, source: {state.source}, llm_used: {state.llm_used}")
                # Extract canonical_fields from LLM output
                category = 'General'
                subcategory = 'General'
                display_label = state.canonical_id.replace('_', ' ').title()
                description = f"Auto-generated canonical ID for: {state.input_statement}"
                
                # Generate embedding for the taxonomy entry
                statement_embedding = get_embedding(state.input_statement) if state.input_statement else None
                
                cursor.execute("""
                    INSERT INTO statement_taxonomy 
                    (canonical_id, review_section, category, subcategory, display_label, description, source, statement_embedding)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (canonical_id) DO UPDATE SET
                        category = EXCLUDED.category,
                        subcategory = EXCLUDED.subcategory,
                        display_label = EXCLUDED.display_label,
                        description = EXCLUDED.description,
                        statement_embedding = EXCLUDED.statement_embedding,
                        updated_at = CURRENT_TIMESTAMP
                """, (
                    state.canonical_id,
                    review_section or 'issues',
                    category,
                    subcategory,
                    display_label,
                    description,
                    'llm_created',
                    statement_embedding
                ))
                
                # 3. Save to canonical_aliases (for new canonical IDs)
                if state.source in ['llm_with_examples', 'llm_without_examples', 'hybrid_similarity']:
                    # Always include the input statement as an alias
                    aliases_to_save = [state.input_statement]
                    
                    # Save each alias with its embedding
                    for alias in aliases_to_save:
                        if alias and alias.strip():  # Skip empty aliases
                            # Generate embedding for the alias
                            alias_embedding = get_embedding(alias) if alias else None
                            
                            cursor.execute("""
                                INSERT INTO canonical_aliases 
                                (alias, canonical_id, source, confidence, alias_embedding)
                                VALUES (%s, %s, %s, %s, %s)
                                ON CONFLICT (alias) DO UPDATE SET
                                    canonical_id = EXCLUDED.canonical_id,
                                    confidence = EXCLUDED.confidence,
                                    alias_embedding = EXCLUDED.alias_embedding,
                                    updated_at = CURRENT_TIMESTAMP
                            """, (
                                alias.strip(),
                                state.canonical_id,
                                'llm_created',
                                state.confidence_score,
                                alias_embedding
                            ))
            
                        # 1. Save to canonical_statements (statement → canonical_id mapping) - AFTER ensuring canonical_id exists
            cursor.execute("""
                INSERT INTO canonical_statements 
                (statement, canonical_id, source, confidence, statement_embedding, review_section)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (statement) DO UPDATE SET
                    canonical_id = EXCLUDED.canonical_id,
                    source = EXCLUDED.source,
                    confidence = EXCLUDED.confidence,
                    statement_embedding = EXCLUDED.statement_embedding,
                    updated_at = CURRENT_TIMESTAMP
            """, (
                state.input_statement,
                state.canonical_id,
                state.source,
                state.confidence_score,
                None,  # statement_embedding - pass NULL to avoid duplication
                review_section or 'issues'
            ))
        
        # 4. Save to canonicalization_results log table (for debugging)
        cursor.execute("""
            INSERT INTO canonicalization_results (
                input_statement, canonical_id, existing_canonical_id, source, confidence_score, 
                results, llm_used, node_history, errors, enriched_candidates, 
                enrich_hybrid_results_result, llm_with_examples_result, llm_without_examples_result, 
                llm_with_examples_error, llm_without_examples_error, exact_match_result, 
                exact_match_error, lexical_similarity_result, lexical_similarity_error,
                vector_similarity_result, vector_similarity_error, hybrid_similarity_result,
                hybrid_similarity_error, enrich_hybrid_results_error
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            state.input_statement,
            state.canonical_id,
            state.existing_canonical_id,
            state.source,
            state.confidence_score,
            state.results,
            state.llm_used,
            json.dumps(node_history_dict),
            json.dumps(state.error),
            json.dumps(state.enriched_candidates) if state.enriched_candidates else None,
            state.enrich_hybrid_results_result,
            state.llm_with_examples_result,
            state.llm_without_examples_result,
            state.llm_with_examples_error,
            state.llm_without_examples_error,
            state.exact_match_result,
            state.exact_match_error,
            json.dumps(state.lexical_similarity_result) if state.lexical_similarity_result else None,
            state.lexical_similarity_error,
            json.dumps(state.vector_similarity_result) if state.vector_similarity_result else None,
            state.vector_similarity_error,
            json.dumps(state.hybrid_similarity_result) if state.hybrid_similarity_result else None,
            state.hybrid_similarity_error,
            state.enrich_hybrid_results_error
        ))
        
        # 5. Save to appropriate table based on success/failure
        if state.canonical_id:
            # Success case: Save to review_statements
            cursor.execute("""
                INSERT INTO review_statements (
                    review_id, app_id, canonical_id, review_section, severity, 
                    impact_score, confidence, source, canonicalization_status,
                    node_history, errors
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (review_id, canonical_id) DO UPDATE SET
                    canonicalization_status = EXCLUDED.canonicalization_status,
                    error_type = NULL,
                    error_message = NULL,
                    node_history = EXCLUDED.node_history,
                    errors = EXCLUDED.errors,
                    created_at = CURRENT_TIMESTAMP
            """, (
                review_id or 'unknown',
                app_id or 'unknown',
                state.canonical_id,  # Use actual canonical_id
                review_section or 'unknown',
                'medium',  # default severity
                50.0,      # default impact score
                state.confidence_score or 0.0,
                state.source or 'success',
                'success',
                json.dumps(node_history_dict),
                json.dumps(state.error) if state.error else None
            ))
        else:
            # Failure case: Save to failed_canonicalizations
            cursor.execute("""
                INSERT INTO failed_canonicalizations (
                    review_id, app_id, input_statement, review_section, severity, 
                    impact_score, confidence, source, canonicalization_status,
                    error_type, error_message, node_history, errors
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                review_id or 'unknown',
                app_id or 'unknown',
                state.input_statement,  # Store the original input statement
                review_section or 'unknown',
                'medium',  # default severity
                50.0,      # default impact score
                state.confidence_score or 0.0,
                state.source or 'failed',
                'failed',
                'canonicalization_failed',  # error_type
                'Failed to generate canonical_id',  # error_message
                json.dumps(node_history_dict),
                json.dumps(state.error) if state.error else None
            ))
        
        conn.commit()
        canonicalization_status = 'success' if state.canonical_id else 'failed'
        state.results = canonicalization_status
        logger.info(f"Successfully saved canonicalization result to all tables for: {state.input_statement} (status: {canonicalization_status})")
        return state
        
    except Exception as e:
        logger.error(f"Error saving canonicalization result: {e}")
        state.error.append({
            "source": "save_canonicalization_result",
            "timestamp": datetime.now().isoformat(),
            "message": f"Save error: {str(e)}"
        })
        if conn:
            conn.rollback()
        return state
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()




def final_canonical_id(statement: str) -> Optional[str]:
    """Get canonical_id using hierarchical approach: exact match → high confidence hybrid → LLM arbitration."""
    
    # Step 1: Try exact match first
    exact_match = get_exact_match(statement)
    if exact_match:
        logger.info(f"Exact match found for '{statement}': {exact_match}")
        return exact_match
    
    logger.info(f"No exact match found for '{statement}', trying hybrid similarity...")
    
    # Step 2: Get hybrid similarity results
    similar_cids = get_hybrid_similarity(statement)
    if not similar_cids:
        logger.warning(f"No similar cids found for '{statement}', sending to LLM to create New canonical id")
        result = get_llm_input(llm_input(statement=statement))
        logger.info(f"LLM output: {result}")
        return result
    
    # Step 3: Check if top result has high confidence (≥0.7)
    top_cid, top_text, top_pg_score, top_vector_score, top_combined_score = similar_cids[0]
    
    if top_combined_score >= 0.95:
        logger.info(f"High confidence match (≥0.95) found for '{statement}': {top_cid} (score: {top_combined_score:.3f})")
        return top_cid
    
    logger.info(f"No high confidence match (≥0.95) found. Top score: {top_combined_score:.3f} Sending to LLM with examples")
    
    # Step 4: Enrich candidates for LLM (but no LLM for now, just display)
    logger.info("Enriching candidates for LLM arbitration (display only):")
    
    enriched_candidates = []
    for cid, text, pg_score, vector_score, combined_score in similar_cids[:5]:  # Top 5 candidates
        enriched_data = enrich_cid(cid)
        if enriched_data:
            canonical_id, display_label, description, examples, aliases = enriched_data
            enriched_candidates.append({
                'canonical_id': canonical_id,
                'display_label': display_label,
                'description': description,
                'examples': examples,
                'aliases': aliases,
                'combined_score': combined_score,
                'pg_score': pg_score,
                'vector_score': vector_score
            })
            logger.info(f"  Candidate: {canonical_id} (score: {combined_score:.3f})")
            logger.info(f"    Display: {display_label}")
            logger.info(f"    Description: {description}")
            logger.info(f"    Aliases: {aliases}")
        else:
            logger.warning(f"Could not enrich canonical_id: {cid}")
    
    # For now, return the top result (even if confidence < 0.7)
    logger.info(f"Sending for LLM arbitration with examples: {enriched_candidates}")
    
    # Create llm_input object
    input_data = llm_input(
        statement=statement,
        enriched_candidates=enriched_candidates
    )
    
    result = get_llm_input(input_data)
    if result:
        top_cid = result.canonical_id
    else:
        top_cid = None
    #logger.info(f"LLM input: {llm_input}")
    # Todo : send to LLM to create a new canonical_id
    

    return top_cid

def get_unprocessed_statements_by_date_range(start_date: str, end_date: str, batch_size: int = 100) -> List[Dict]:
    """
    Get statements from processed_app_reviews for a date range that haven't been canonicalized yet.
    
    Args:
        start_date: 'YYYY-MM-DD' format
        end_date: 'YYYY-MM-DD' format  
        batch_size: Number of statements to return
    
    Returns:
        List of dicts with: review_id, statement, section_type, review_date
    """
    conn = None
    cursor = None
    try:
        conn = get_postgres_connection()
        cursor = conn.cursor()
        
        # Query to get statements not already in canonical_statements table
        cursor.execute("""
            WITH all_statements AS (
                -- Issues
                SELECT 
                    review_id,
                    'issue' as section_type,
                    issue_data->>'description' AS statement,
                    review_date
                FROM processed_app_reviews,
                     jsonb_array_elements(latest_analysis->'issues'->'issues') AS issue_data
                WHERE issue_data->>'description' IS NOT NULL 
                  AND review_date BETWEEN %s AND %s
                
                UNION ALL
                
                -- Actions
                SELECT 
                    review_id,
                    'issue_action' as section_type,
                    action_data->>'description' AS statement,
                    review_date
                FROM processed_app_reviews,
                     jsonb_array_elements(latest_analysis->'issues'->'issues') AS issue_data,
                     jsonb_array_elements(issue_data->'actions') AS action_data
                WHERE action_data->>'description' IS NOT NULL 
                  AND review_date BETWEEN %s AND %s
                
                UNION ALL
                
                -- Positives
                SELECT 
                    review_id,
                    'positive' as section_type,
                    positive_data->>'description' AS statement,
                    review_date
                FROM processed_app_reviews,
                     jsonb_array_elements(latest_analysis->'positive_feedback'->'positive_mentions') AS positive_data
                WHERE positive_data->>'description' IS NOT NULL 
                  AND review_date BETWEEN %s AND %s
            )
            SELECT DISTINCT
                s.review_id,
                s.statement,
                s.section_type,
                s.review_date
            FROM all_statements s
            LEFT JOIN canonical_statements cs ON s.statement = cs.statement
            WHERE cs.statement IS NULL  -- Only statements not already canonicalized
            ORDER BY s.review_date, s.review_id
            LIMIT %s
        """, (start_date, end_date, start_date, end_date, start_date, end_date, batch_size))
        
        results = cursor.fetchall()
        
        # Convert to list of dicts
        statements = []
        for row in results:
            statements.append({
                'review_id': row[0],
                'statement': row[1], 
                'section_type': row[2],
                'review_date': row[3]
            })
        
        logger.info(f"Found {len(statements)} unprocessed statements for date range {start_date} to {end_date}")
        return statements
        
    except Exception as e:
        logger.error(f"Error fetching unprocessed statements: {e}")
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


if __name__ == "__main__":
    # Test the complete canonicalization pipeline
    test_statement = "Menu is easy to access"
    result = final_canonical_id(test_statement)
    #print(f"\nFinal result for '{test_statement}': {result}")
 
