-- =============================================================================
-- AI commentary snapshots (Overview KPI footers + extensible slots)
-- =============================================================================
--
-- Purpose
--   Store short AI-generated lines that replace or augment the bottom detail text
--   on analytics cards (e.g. Overview secondary KPIs in xp-chex-feb
--   components/overview/overview-secondary-kpis.tsx → footerSub).
--
-- Slot keys (overview /dashboard/analytics/overview — 4 cards)
--   overview_kpi_positive_rate_footer
--   overview_kpi_critical_issues_footer
--   overview_kpi_delight_mentions_footer
--   overview_kpi_recommendations_footer
--
-- Time model (no period_key)
--   time_range_preset  — which UI preset was used (last_7_days, last_6_months, …).
--   window_start, window_end — DATE, inclusive bounds of the **same** review window
--     your analytics APIs use for that preset (map preset → dates in app code).
--   Match commentary to the UI by comparing these dates to the current request’s
--     resolved date_range; then take latest row by generated_at.
--
--   Example: preset last_6_months → API returns start=2025-09-17, end=2026-03-17.
--     Store those as window_start/window_end. When the user loads the dashboard,
--     if their resolved range equals those dates, use this commentary; else fall
--     back to frontend default until a snapshot exists for the new range.
--
-- History
--   Append-only. Same (app, slot, preset, window_start, window_end) may have many
--   rows if you regenerate; pick ORDER BY generated_at DESC LIMIT 1.
--
-- Suggested max length for footer lines: 80–160 characters (default max_chars 120).
-- =============================================================================

CREATE TABLE IF NOT EXISTS public.analytics_commentary_snapshots (
  id                   BIGSERIAL PRIMARY KEY,

  organization_id      BIGINT REFERENCES public.organizations (id) ON DELETE CASCADE,
  app_id               TEXT REFERENCES public.apps (app_id) ON DELETE CASCADE,

  slot_key             TEXT NOT NULL,
  /*
    Known keys for Overview secondary KPI footers:
      overview_kpi_positive_rate_footer
      overview_kpi_critical_issues_footer
      overview_kpi_delight_mentions_footer
      overview_kpi_recommendations_footer
  */

  time_range_preset    TEXT NOT NULL,
  -- last_7_days | last_30_days | last_3_months | last_6_months | last_12_months | this_year | all_time

  window_start         DATE NOT NULL,
  window_end           DATE NOT NULL,

  CONSTRAINT analytics_commentary_window_order_chk
    CHECK (window_end >= window_start),

  commentary_text      TEXT NOT NULL,
  max_chars            INTEGER NOT NULL DEFAULT 120,

  source_metrics_json  JSONB NOT NULL DEFAULT '{}'::jsonb,

  model_id             TEXT,
  prompt_version       TEXT,
  generated_at         TIMESTAMPTZ NOT NULL DEFAULT now(),

  CONSTRAINT analytics_commentary_slot_key_nonempty
    CHECK (length(trim(slot_key)) > 0),
  CONSTRAINT analytics_commentary_text_nonempty
    CHECK (length(trim(commentary_text)) > 0)
);

CREATE INDEX IF NOT EXISTS analytics_commentary_lookup_latest_idx
  ON public.analytics_commentary_snapshots (
    app_id,
    slot_key,
    time_range_preset,
    window_start,
    window_end,
    generated_at DESC
  );

CREATE INDEX IF NOT EXISTS analytics_commentary_org_lookup_idx
  ON public.analytics_commentary_snapshots (
    organization_id,
    slot_key,
    time_range_preset,
    window_start,
    window_end,
    generated_at DESC
  )
  WHERE organization_id IS NOT NULL;

COMMENT ON TABLE public.analytics_commentary_snapshots IS
  'Append-only AI commentary; match UI by app_id + slot_key + time_range_preset + window_start/end (same as analytics date_range); latest generated_at wins.';
