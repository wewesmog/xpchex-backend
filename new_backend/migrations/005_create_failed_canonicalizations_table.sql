-- Create table for failed canonicalizations
CREATE TABLE IF NOT EXISTS failed_canonicalizations (
    id SERIAL PRIMARY KEY,
    review_id TEXT NOT NULL,
    app_id TEXT NOT NULL,
    input_statement TEXT NOT NULL,
    review_section TEXT NOT NULL,
    severity TEXT NOT NULL DEFAULT 'medium',
    impact_score NUMERIC NOT NULL DEFAULT 50.0 CHECK (impact_score >= 0 AND impact_score <= 100),
    confidence NUMERIC NULL CHECK (confidence IS NULL OR (confidence >= 0 AND confidence <= 1)),
    source TEXT NULL,
    canonicalization_status VARCHAR(20) DEFAULT 'failed' CHECK (canonicalization_status IN ('failed')),
    error_type VARCHAR(100),
    error_message TEXT,
    node_history JSONB,
    errors JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for the failed canonicalizations table
CREATE INDEX IF NOT EXISTS idx_failed_canon_review_id ON failed_canonicalizations(review_id);
CREATE INDEX IF NOT EXISTS idx_failed_canon_app_id ON failed_canonicalizations(app_id);
CREATE INDEX IF NOT EXISTS idx_failed_canon_status ON failed_canonicalizations(canonicalization_status);
CREATE INDEX IF NOT EXISTS idx_failed_canon_error_type ON failed_canonicalizations(error_type);
CREATE INDEX IF NOT EXISTS idx_failed_canon_created_at ON failed_canonicalizations(created_at);
