-- =============================================================================
-- Organizations, apps, and app_details_history (single migration)
-- =============================================================================
--
-- Model
--   organizations  →  apps (one org, many Play apps)
--   apps           →  app_details_history (one app, many snapshots when store data changes)
--
-- What is slug?
--   A short, URL-safe handle for the org (lowercase, hyphens, no spaces), e.g. acme-bank.
--   Use it in routes like /orgs/acme-bank or as a stable key in APIs. Must be UNIQUE.
--   Optional in app logic if you only use numeric id — column can stay NULL if you prefer.
--
-- Existing data in app_details_history?
--   This file creates app_details_history WITHOUT a foreign key first, then inserts missing
--   rows into apps from your history, then adds the FK — you do NOT delete history.
--
-- =============================================================================

-- ---------------------------------------------------------------------------
-- 1) organizations
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS public.organizations (
  id            BIGSERIAL PRIMARY KEY,
  name          TEXT NOT NULL,
  slug          TEXT UNIQUE,
  active        BOOLEAN NOT NULL DEFAULT TRUE,

  plan_tier     TEXT NOT NULL DEFAULT 'free',
  billing_status TEXT NOT NULL DEFAULT 'none',
  billing_provider TEXT,
  external_customer_id    TEXT,
  external_subscription_id TEXT,
  billing_email TEXT,
  trial_ends_at TIMESTAMPTZ,
  current_period_end TIMESTAMPTZ,
  payment_method_summary TEXT,

  metadata      JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS organizations_active_idx
  ON public.organizations (active)
  WHERE active = TRUE;

CREATE INDEX IF NOT EXISTS organizations_plan_tier_idx
  ON public.organizations (plan_tier);

-- ---------------------------------------------------------------------------
-- 2) apps (registry: one row per Google Play app_id)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS public.apps (
  id                BIGSERIAL PRIMARY KEY,
  organization_id   BIGINT NOT NULL REFERENCES public.organizations (id) ON DELETE CASCADE,
  app_id            TEXT NOT NULL,
  display_name      TEXT,
  active            BOOLEAN NOT NULL DEFAULT TRUE,
  notes             TEXT,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT apps_app_id_unique UNIQUE (app_id)
);

CREATE INDEX IF NOT EXISTS apps_organization_id_idx
  ON public.apps (organization_id);

CREATE INDEX IF NOT EXISTS apps_active_idx
  ON public.apps (active)
  WHERE active = TRUE;

-- ---------------------------------------------------------------------------
-- 3) app_details_history (append-only; FK added in step 5 after backfill)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS public.app_details_history (
  id                      BIGSERIAL PRIMARY KEY,
  app_id                  TEXT NOT NULL,
  title                   TEXT,
  description             TEXT,
  summary                 TEXT,
  installs                TEXT,
  score                   DOUBLE PRECISION,
  ratings_count           INTEGER,
  reviews_count           INTEGER,
  price                   TEXT,
  price_currency          TEXT,
  size                    TEXT,
  minimum_android         TEXT,
  developer_id            TEXT,
  developer_email         TEXT,
  developer_website       TEXT,
  developer_address       TEXT,
  privacy_policy          TEXT,
  genre                   TEXT,
  genre_id                TEXT,
  content_rating          TEXT,
  content_rating_description TEXT,
  app_updated_at          TIMESTAMPTZ,
  icon_url                TEXT,
  version                 TEXT,
  inserted_on             TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS app_details_history_app_id_inserted_on_idx
  ON public.app_details_history (app_id, inserted_on DESC);

-- ---------------------------------------------------------------------------
-- 4) Backfill: placeholder org + one apps row per distinct app_id in history
-- ---------------------------------------------------------------------------
INSERT INTO public.organizations (name, slug, active, plan_tier, billing_status)
VALUES (
  'Legacy (migrate org assignments)',
  'legacy-unassigned',
  TRUE,
  'free',
  'none'
)
ON CONFLICT (slug) DO NOTHING;

INSERT INTO public.apps (organization_id, app_id, active)
SELECT
  o.id,
  h.app_id,
  TRUE
FROM (SELECT DISTINCT app_id FROM public.app_details_history WHERE app_id IS NOT NULL AND TRIM(app_id) <> '') AS h
CROSS JOIN LATERAL (
  SELECT id FROM public.organizations WHERE slug = 'legacy-unassigned' LIMIT 1
) AS o
ON CONFLICT (app_id) DO NOTHING;

-- ---------------------------------------------------------------------------
-- 5) Enforce: history rows must reference a registered app
-- ---------------------------------------------------------------------------
ALTER TABLE public.app_details_history
  DROP CONSTRAINT IF EXISTS app_details_history_app_id_fkey;

ALTER TABLE public.app_details_history
  ADD CONSTRAINT app_details_history_app_id_fkey
  FOREIGN KEY (app_id) REFERENCES public.apps (app_id) ON DELETE CASCADE;

-- ---------------------------------------------------------------------------
-- Comments
-- ---------------------------------------------------------------------------
COMMENT ON COLUMN public.organizations.slug IS
  'URL-safe org handle (e.g. acme-bank); unique; optional if you only use id in URLs.';

COMMENT ON TABLE public.organizations IS 'Tenant / customer; billing fields for future charging.';
COMMENT ON TABLE public.apps IS 'Registered Play app per org; app_id matches app_details_history and reviews.';
COMMENT ON COLUMN public.apps.app_id IS 'Google Play package id (e.g. com.example.app).';
COMMENT ON TABLE public.app_details_history IS
  'Append-only store snapshots; latest per app: ORDER BY inserted_on DESC.';
