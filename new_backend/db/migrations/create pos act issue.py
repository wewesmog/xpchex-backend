
-- First, create the canonical statement details VIEW, as it's a shared lookup.
-- This CTE is defined once and can be referenced by other views.
CREATE OR REPLACE VIEW canonical_statement_details_vw AS
SELECT
    A.canonical_id,
    A.statement AS canonical_statement_text, -- Use statement column from canonical_statements
    B.category,
    B.subcategory,
    B.display_label,
    REPLACE(B.description, 'Auto-generated canonical ID for:', '') AS taxonomy_description -- Cleaned description
FROM
    canonical_statements A
LEFT OUTER JOIN
    statement_taxonomy B
ON (A.canonical_id = B.canonical_id);

---
--- 1. View for Issues Dashboard Data
---
CREATE OR REPLACE VIEW issues_dashboard_data_vw AS
WITH ISSUES_FLATTENED AS (
    SELECT
        pr.review_id,
        pr.review_created_at,
        TRIM(TRAILING '.', issue_data->>'description') AS original_issue_description, -- Trimmed for joining
        issue_data->>'type' AS issue_type,
        issue_data->>'snippet' AS snippet,
        issue_data->>'severity' AS severity,
        issue_data->>'key_words' AS key_words,
        (issue_data->>'impact_score')::numeric AS impact_score
    FROM
        processed_app_reviews pr,
        jsonb_array_elements(pr.latest_analysis->'issues'->'issues') AS issue_data
    WHERE
        issue_data->>'description' IS NOT NULL -- Ensure a description exists
)
SELECT
    A.*, -- All original columns from ISSUES_FLATTENED
    B.canonical_id,
    COALESCE(B.canonical_statement_text, A.original_issue_description) AS final_issue_statement, -- Canonical text or original
    B.category,
    B.subcategory,
    B.display_label,
    B.taxonomy_description
FROM ISSUES_FLATTENED A
LEFT OUTER JOIN canonical_statement_details_vw B -- Reference the view here
ON (A.original_issue_description = TRIM(TRAILING '.', B.canonical_statement_text));

---
--- 2. View for Actions Dashboard Data
---
CREATE OR REPLACE VIEW actions_dashboard_data_vw AS
WITH ACTIONS_FLATTENED AS (
    SELECT
        pr.review_id,
        pr.review_created_at,
        TRIM(TRAILING '.', action_data->>'description') AS original_action_description, -- Trimmed for joining
        action_data->>'type' AS action_type,
        action_data->>'estimated_effort' AS estimated_effort,
        action_data->>'suggested_timeline' AS suggested_timeline
    FROM
        processed_app_reviews pr,
        jsonb_array_elements(pr.latest_analysis->'issues'->'issues') AS issue_placeholder, -- To navigate to actions
        jsonb_array_elements(issue_placeholder->'actions') AS action_data
    WHERE
        action_data->>'description' IS NOT NULL -- Only include actions with descriptions
)
SELECT
    A.*, -- All original columns from ACTIONS_FLATTENED
    B.canonical_id,
    COALESCE(B.canonical_statement_text, A.original_action_description) AS final_action_statement, -- Canonical text or original
    B.category,
    B.subcategory,
    B.display_label,
    B.taxonomy_description
FROM ACTIONS_FLATTENED A
LEFT OUTER JOIN canonical_statement_details_vw B -- Reference the view here
ON (A.original_action_description = TRIM(TRAILING '.', B.canonical_statement_text));

---
--- 3. View for Positives Dashboard Data
---
CREATE OR REPLACE VIEW positives_dashboard_data_vw AS
WITH POSITIVES_FLATTENED AS (
    SELECT
        pr.review_id,
        pr.review_created_at,
        TRIM(TRAILING '.', positive_mentions->>'description') AS original_positive_description, -- Trimmed for joining
        positive_mentions->>'quote' AS quote,
        positive_mentions->>'metrics' AS metrics,
        positive_mentions->>'keywords' AS keywords,
        positive_mentions->>'impact_area' AS impact_area,
        (positive_mentions->>'impact_score')::numeric AS impact_score,
        positive_mentions->>'user_segments' AS user_segments
    FROM
        processed_app_reviews pr,
        jsonb_array_elements(pr.latest_analysis->'positive_feedback'->'positive_mentions') AS positive_mentions
    WHERE
        positive_mentions->>'description' IS NOT NULL
)
SELECT
    A.*, -- All original columns from POSITIVES_FLATTENED
    B.canonical_id,
    COALESCE(B.canonical_statement_text, A.original_positive_description) AS final_positive_statement, -- Canonical text or original
    B.category,
    B.subcategory,
    B.display_label,
    B.taxonomy_description
FROM POSITIVES_FLATTENED A
LEFT OUTER JOIN canonical_statement_details_vw B -- Reference the view here
ON (A.original_positive_description = TRIM(TRAILING '.', B.canonical_statement_text));