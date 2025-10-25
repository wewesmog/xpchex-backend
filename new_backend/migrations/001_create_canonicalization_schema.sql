-- Canonicalization schema (drop-before-create, PostgreSQL)

-- 0) Extensions (safe idempotent)
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE EXTENSION IF NOT EXISTS vector;

-- 1) Drop existing objects in dependency order
DROP TABLE IF EXISTS review_statements CASCADE;
DROP TABLE IF EXISTS canonical_statements CASCADE;
DROP TABLE IF EXISTS canonical_aliases CASCADE;
DROP TABLE IF EXISTS statement_taxonomy CASCADE;

-- -- 2) Enum types (validation)
-- DO $$
-- BEGIN
--   -- Categories (edit to your needs)
--   IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'issue_category') THEN
--     CREATE TYPE issue_category AS ENUM (
--       'Performance','Stability','Authentication','OTP','Payments',
--       'Account/Balances','UX','Security','Support','Update/Compatibility','Feature','Other'
--     );
--   END IF;

--   IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'issue_type_enum') THEN
--     CREATE TYPE issue_type_enum AS ENUM ('bug','performance','feature_request','ux_issue','security','other');
--   END IF;

--   IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'severity_enum') THEN
--     CREATE TYPE severity_enum AS ENUM ('low','medium','high','critical');
--   END IF;

--   IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'mapping_source_enum') THEN
--     CREATE TYPE mapping_source_enum AS ENUM ('exact','alias','fuzzy','vector','hybrid','llm','import');
--   END IF;
-- END$$;

-- 3) Timestamp touch function (shared)
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_proc WHERE proname = 'touch_updated_at') THEN
    CREATE OR REPLACE FUNCTION touch_updated_at()
    RETURNS TRIGGER AS $f$
    BEGIN
      NEW.updated_at := CURRENT_TIMESTAMP;
      RETURN NEW;
    END;$f$ LANGUAGE plpgsql;
  END IF;
END$$;

DROP TABLE IF EXISTS statement_taxonomy;
-- 4) Master taxonomy (source of truth)
CREATE TABLE statement_taxonomy (
  id SERIAL,
  canonical_id TEXT PRIMARY KEY,
  review_section TEXT NOT NULL, --CAN BE ISSUE OR POSITIVE_FEEDBACK
  category TEXT NOT NULL,
  subcategory TEXT NULL,
  subcategory2 TEXT NULL,
  display_label TEXT NOT NULL,
  description TEXT NULL,
  examples JSONB NULL,
  owner TEXT NULL,
  source TEXT NOT NULL DEFAULT 'manual',
  notes TEXT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT chk_canonical_id_snake_case CHECK (canonical_id ~ '^[a-z0-9]+(_[a-z0-9]+){0,5}$'),
  CONSTRAINT chk_review_section CHECK (review_section IN ('issues', 'positives', 'actions'))
);

CREATE TRIGGER trg_issue_taxonomy_touch
BEFORE UPDATE ON statement_taxonomy
FOR EACH ROW EXECUTE FUNCTION touch_updated_at();

-- 5) Curated aliases (phrase → canonical_id)
CREATE TABLE canonical_aliases (
  id SERIAL,
  alias TEXT PRIMARY KEY,
  canonical_id TEXT NOT NULL REFERENCES statement_taxonomy(canonical_id) ON UPDATE CASCADE,
  locale TEXT NULL,
  source TEXT NOT NULL DEFAULT 'manual',
  confidence NUMERIC NULL CHECK (confidence IS NULL OR (confidence >= 0 AND confidence <= 1)),
  is_active BOOLEAN NOT NULL DEFAULT TRUE,
  is_seed BOOLEAN NOT NULL DEFAULT TRUE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);  

CREATE TRIGGER trg_canonical_aliases_touch
BEFORE UPDATE ON canonical_aliases
FOR EACH ROW EXECUTE FUNCTION touch_updated_at();

-- 6) Statement → canonical mapping (current resolution + provenance)
CREATE TABLE canonical_statements (
  id SERIAL,
  statement TEXT PRIMARY KEY,
  canonical_id TEXT NOT NULL REFERENCES statement_taxonomy(canonical_id) ON UPDATE CASCADE,
  normalized_statement TEXT NULL,
  review_section TEXT NULL, -- e.g., 'issues', 'positives'
  source TEXT NULL,
  confidence NUMERIC NULL CHECK (confidence IS NULL OR (confidence >= 0 AND confidence <= 1)),
  first_seen_review_id TEXT NULL,
  language TEXT NULL,
  statement_embedding VECTOR(768) NULL, -- requires pgvector; optional
  created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TRIGGER trg_canonical_statements_touch
BEFORE UPDATE ON canonical_statements
FOR EACH ROW EXECUTE FUNCTION touch_updated_at();

-- Helpful indexes
CREATE INDEX IF NOT EXISTS idx_canon_stmt_trgm ON canonical_statements USING gin (statement gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_canon_stmt_norm_trgm ON canonical_statements USING gin (normalized_statement gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_canon_stmt_canonical_id ON canonical_statements(canonical_id);
CREATE INDEX IF NOT EXISTS idx_canon_stmt_vec ON canonical_statements USING ivfflat (statement_embedding vector_cosine_ops) WITH (lists = 100);

CREATE INDEX IF NOT EXISTS idx_canon_alias_trgm ON canonical_aliases USING gin (alias gin_trgm_ops);

-- 7) Normalized per-review facts (for deterministic analytics)
CREATE TABLE review_statements (
  id SERIAL,
  review_id TEXT NOT NULL,
  app_id TEXT NOT NULL,
  canonical_id TEXT NOT NULL REFERENCES statement_taxonomy(canonical_id) ON UPDATE CASCADE,
  review_section TEXT NOT NULL,
  severity TEXT NOT NULL,
  impact_score NUMERIC NOT NULL CHECK (impact_score >= 0 AND impact_score <= 100),
  confidence NUMERIC NULL CHECK (confidence IS NULL OR (confidence >= 0 AND confidence <= 1)),
  source TEXT NULL,
  canonicalization_status VARCHAR(20) DEFAULT 'success' CHECK (canonicalization_status IN ('success', 'failed')),
  error_type VARCHAR(100),
  error_message TEXT,
  node_history JSONB,
  errors JSONB,
  created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (review_id, canonical_id)
);

    CREATE INDEX IF NOT EXISTS idx_rim_app_week ON review_statements(app_id, created_at);
    CREATE INDEX IF NOT EXISTS idx_rim_canonical ON review_statements(canonical_id);
    CREATE INDEX IF NOT EXISTS idx_rim_type ON review_statements(review_section);
    CREATE INDEX IF NOT EXISTS idx_rim_severity ON review_statements(severity);
    CREATE INDEX IF NOT EXISTS idx_rim_status ON review_statements(canonicalization_status);
    CREATE INDEX IF NOT EXISTS idx_rim_error_type ON review_statements(error_type);



-- Vector Store
-- For statement_taxonomy
ALTER TABLE statement_taxonomy 
  ADD COLUMN IF NOT EXISTS statement_embedding vector(768);  -- BERT/similar embedding

-- For canonical_aliases
ALTER TABLE canonical_aliases
  ADD COLUMN IF NOT EXISTS alias_embedding vector(768);

-- Vector indexes for similarity search
CREATE INDEX IF NOT EXISTS idx_statement_vec ON statement_taxonomy 
  USING ivfflat (statement_embedding vector_cosine_ops) WITH (lists = 100);

CREATE INDEX IF NOT EXISTS idx_alias_vec ON canonical_aliases 
  USING ivfflat (alias_embedding vector_cosine_ops) WITH (lists = 100);

