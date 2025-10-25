CREATE OR REPLACE VIEW daily_reviews_view AS
SELECT 
    DATE(review_created_at) as review_date,
    app_id,
    jsonb_agg(
        jsonb_build_object(
            'review_id', review_id,
            'content', content,
            'aspects', latest_analysis->'aspects',
            'review_created_at', review_created_at
        )
    ) as reviews
FROM reviews
WHERE analyzed = true 
AND latest_analysis IS NOT NULL
AND latest_analysis->'aspects' IS NOT NULL
GROUP BY DATE(review_created_at), app_id; 