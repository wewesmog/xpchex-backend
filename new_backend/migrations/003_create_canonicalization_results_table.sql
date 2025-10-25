CREATE TABLE IF NOT EXISTS canonicalization_results (
    id SERIAL PRIMARY KEY,
    
    input_statement TEXT NOT NULL,
    
    canonical_id VARCHAR(255),
    existing_canonical_id BOOLEAN,
    source VARCHAR(100),
    confidence_score DECIMAL(5,4),
    
    llm_used BOOLEAN DEFAULT FALSE,
    processing_time_ms INTEGER,
    
    exact_match_result TEXT,
    exact_match_error TEXT,
    
    lexical_similarity_result JSONB,
    lexical_similarity_error TEXT,
    
    vector_similarity_result JSONB,
    vector_similarity_error TEXT,
    
    hybrid_similarity_result JSONB,
    hybrid_similarity_error TEXT,
    
    enriched_candidates JSONB,
    enrich_hybrid_results_error TEXT,
    enrich_hybrid_results_result TEXT,
    
    llm_with_examples_result VARCHAR(255),
    llm_without_examples_result VARCHAR(255),
    llm_with_examples_error TEXT,
    llm_without_examples_error TEXT,
    
    node_history JSONB,
    
    errors JSONB,
    
    results TEXT,
    
    app_id VARCHAR(255),
    review_id VARCHAR(255),
    review_section VARCHAR(50),
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_canonicalization_results_input_statement ON canonicalization_results(input_statement);
CREATE INDEX IF NOT EXISTS idx_canonicalization_results_canonical_id ON canonicalization_results(canonical_id);
CREATE INDEX IF NOT EXISTS idx_canonicalization_results_source ON canonicalization_results(source);
CREATE INDEX IF NOT EXISTS idx_canonicalization_results_llm_used ON canonicalization_results(llm_used);
CREATE INDEX IF NOT EXISTS idx_canonicalization_results_created_at ON canonicalization_results(created_at);
CREATE INDEX IF NOT EXISTS idx_canonicalization_results_app_id ON canonicalization_results(app_id);
CREATE INDEX IF NOT EXISTS idx_canonicalization_results_review_id ON canonicalization_results(review_id);

CREATE OR REPLACE FUNCTION update_canonicalization_results_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_update_canonicalization_results_updated_at
    BEFORE UPDATE ON canonicalization_results
    FOR EACH ROW
    EXECUTE FUNCTION update_canonicalization_results_updated_at();


