from datetime import date
from typing import List, Dict, Any
from app.models.daily_summary_models import (
    PositiveFeedbackAnalysis, KeyValue, DelightFactor,
    SuccessStory, SuccessStoryMetrics, SuccessfulFeature,
    FeatureSuccessMetrics, RetentionDriver
)
from app.shared_services.logger_setup import setup_logger
from app.shared_services.db import get_postgres_connection

logger = setup_logger()

def get_reviews_with_positive_feedback(date: date, app_id: str) -> List[dict]:
    conn = get_postgres_connection("daily_reviews_view")
    try:
        with conn.cursor() as cur:
            query = """
            SELECT content, latest_analysis ->> 'positive_feedback' as positive_feedback FROM processed_app_reviews
            WHERE date(review_created_at) = %s::date AND app_id = %s 
            """
            logger.info(f"Executing query: {str(cur.mogrify(query, (date, app_id)))[:500]}...")
            cur.execute(query, (date, app_id))

            columns = [desc[0] for desc in cur.description]
            rows = cur.fetchall()

            reviews = [dict(zip(columns, row)) for row in rows]
            logger.info(f"Fetched {len(rows)} reviews for date {date} and app_id {app_id}")
            return reviews
    except Exception as e:
        logger.error(f"Error fetching reviews: {str(e)[:500]}...")
        raise e
    finally:
        conn.close()

def aggregate_key_values(values_list: List[Dict]) -> List[KeyValue]:
    """Aggregate key values across reviews."""
    values_map = {}
    
    for values in values_list:
        for value in values:
            quote = value.get('user_quote', '')
            if quote not in values_map:
                values_map[quote] = value
                values_map[quote]['frequency'] = 1
            else:
                existing = values_map[quote]
                existing['frequency'] += 1
                if value.get('sentiment_score', 0) > existing.get('sentiment_score', 0):
                    existing.update(value)
                    existing['frequency'] = max(existing['frequency'], value.get('frequency', 1))

    return [KeyValue(**value) for value in values_map.values()]

def aggregate_delight_factors(factors_list: List[Dict]) -> List[DelightFactor]:
    """Aggregate delight factors across reviews."""
    factors_map = {}
    
    for factors in factors_list:
        for factor in factors:
            desc = factor.get('description', '')
            if desc not in factors_map:
                factors_map[desc] = factor
            else:
                existing = factors_map[desc]
                # Update with higher scores
                for field in ['wow_factor', 'viral_potential']:
                    if factor.get(field, 0) > existing.get(field, 0):
                        existing[field] = factor[field]

    return [DelightFactor(**factor) for factor in factors_map.values()]

def aggregate_success_stories(stories_list: List[Dict]) -> List[SuccessStory]:
    """Aggregate success stories across reviews."""
    stories_map = {}
    
    for stories in stories_list:
        for story in stories:
            title = story.get('title', '')
            if title not in stories_map:
                stories_map[title] = story
            else:
                existing = stories_map[title]
                # Update metrics if they're better
                existing_metrics = existing.get('supporting_metrics', {})
                new_metrics = story.get('supporting_metrics', {})
                for key in new_metrics:
                    if key not in existing_metrics or new_metrics[key] > existing_metrics[key]:
                        existing_metrics[key] = new_metrics[key]
                # Update replication potential if higher
                if story.get('replication_potential', 0) > existing.get('replication_potential', 0):
                    existing['replication_potential'] = story['replication_potential']

    return [
        SuccessStory(
            title=story['title'],
            impact=story['impact'],
            description=story['description'],
            user_segment=story['user_segment'],
            supporting_metrics=SuccessStoryMetrics(**story['supporting_metrics']),
            replication_potential=story['replication_potential']
        )
        for story in stories_map.values()
    ]

def aggregate_successful_features(features_list: List[Dict]) -> List[SuccessfulFeature]:
    """Aggregate successful features across reviews."""
    features_map = {}
    
    for features in features_list:
        for feature in features:
            name = feature.get('feature_name', '')
            if name not in features_map:
                features_map[name] = feature
            else:
                existing = features_map[name]
                # Merge user segments
                existing_segments = set(existing.get('user_segments', []))
                new_segments = set(feature.get('user_segments', []))
                existing['user_segments'] = list(existing_segments | new_segments)
                # Update metrics if better
                existing_metrics = existing.get('success_metrics', {})
                new_metrics = feature.get('success_metrics', {})
                for key in new_metrics:
                    if key not in existing_metrics or new_metrics[key] > existing_metrics[key]:
                        existing_metrics[key] = new_metrics[key]
                # Merge positive mentions
                existing_mentions = set(existing.get('positive_mentions', []))
                new_mentions = set(feature.get('positive_mentions', []))
                existing['positive_mentions'] = list(existing_mentions | new_mentions)

    return [
        SuccessfulFeature(
            feature_name=feature['feature_name'],
            user_segments=feature['user_segments'],
            success_metrics=FeatureSuccessMetrics(**feature['success_metrics']),
            positive_mentions=feature['positive_mentions'],
            competitive_advantage=feature['competitive_advantage']
        )
        for feature in features_map.values()
    ]

def aggregate_retention_drivers(drivers_list: List[Dict]) -> List[RetentionDriver]:
    """Aggregate retention drivers across reviews."""
    drivers_map = {}
    
    for drivers in drivers_list:
        for driver in drivers:
            aspect = driver.get('feature_or_aspect', '')
            if aspect not in drivers_map:
                drivers_map[aspect] = driver
            else:
                existing = drivers_map[aspect]
                # Update with higher impact scores
                if driver.get('retention_impact', 0) > existing.get('retention_impact', 0):
                    existing['retention_impact'] = driver['retention_impact']
                # Merge testimonials
                existing_testimonials = set(existing.get('user_testimonials', []))
                new_testimonials = set(driver.get('user_testimonials', []))
                existing['user_testimonials'] = list(existing_testimonials | new_testimonials)
                # Update stickiness factor if higher
                if driver.get('user_stickiness_factor', 0) > existing.get('user_stickiness_factor', 0):
                    existing['user_stickiness_factor'] = driver['user_stickiness_factor']

    return [RetentionDriver(**driver) for driver in drivers_map.values()]

def summarize_positive_feedback_for_date(date: date, app_id: str) -> PositiveFeedbackAnalysis:
    """Summarize positive feedback analysis for a specific date and app."""
    reviews = get_reviews_with_positive_feedback(date, app_id)
    logger.info(f"Processing positive feedback analysis for {len(reviews)} reviews")

    if not reviews:
        return PositiveFeedbackAnalysis(
            analysis_error="No reviews found for the specified date",
            key_values=[],
            delight_factors=[],
            success_stories=[],
            competitive_edges={},
            retention_drivers=[],
            satisfaction_score=0,
            successful_features=[],
            growth_opportunities=[],
            brand_advocates_percentage=0
        )

    all_key_values = []
    all_delight_factors = []
    all_success_stories = []
    all_competitive_edges = {}
    all_retention_drivers = []
    all_successful_features = []
    all_growth_opportunities = set()
    total_satisfaction = 0
    total_advocates = 0
    review_count = 0

    for review in reviews:
        feedback_data = review.get('positive_feedback')
        if not feedback_data:
            continue

        # Parse JSON if needed
        if isinstance(feedback_data, str):
            try:
                import json
                feedback_data = json.loads(feedback_data)
            except json.JSONDecodeError:
                logger.error("Failed to parse positive feedback JSON")
                continue

        # Extract components
        key_values = feedback_data.get('key_values', [])
        delight_factors = feedback_data.get('delight_factors', [])
        success_stories = feedback_data.get('success_stories', [])
        competitive_edges = feedback_data.get('competitive_edges', {})
        retention_drivers = feedback_data.get('retention_drivers', [])
        successful_features = feedback_data.get('successful_features', [])
        growth_opportunities = feedback_data.get('growth_opportunities', [])

        # Aggregate components
        if key_values:
            all_key_values.append(key_values)
        if delight_factors:
            all_delight_factors.append(delight_factors)
        if success_stories:
            all_success_stories.append(success_stories)
        all_competitive_edges.update(competitive_edges)
        if retention_drivers:
            all_retention_drivers.append(retention_drivers)
        if successful_features:
            all_successful_features.append(successful_features)
        all_growth_opportunities.update(growth_opportunities)

        # Track satisfaction and advocates
        total_satisfaction += feedback_data.get('satisfaction_score', 0)
        total_advocates += feedback_data.get('brand_advocates_percentage', 0)
        review_count += 1

    # Aggregate all components
    aggregated_key_values = aggregate_key_values(all_key_values)
    aggregated_delight_factors = aggregate_delight_factors(all_delight_factors)
    aggregated_success_stories = aggregate_success_stories(all_success_stories)
    aggregated_retention_drivers = aggregate_retention_drivers(all_retention_drivers)
    aggregated_successful_features = aggregate_successful_features(all_successful_features)

    # Calculate averages
    avg_satisfaction = total_satisfaction / (review_count or 1)
    avg_advocates = total_advocates / (review_count or 1)

    # Return final analysis
    return PositiveFeedbackAnalysis(
        key_values=aggregated_key_values,
        delight_factors=aggregated_delight_factors,
        success_stories=aggregated_success_stories,
        competitive_edges=all_competitive_edges,
        retention_drivers=aggregated_retention_drivers,
        satisfaction_score=avg_satisfaction,
        successful_features=aggregated_successful_features,
        growth_opportunities=list(all_growth_opportunities),
        brand_advocates_percentage=avg_advocates
    )

if __name__ == "__main__":
    test_date = date(2025, 3, 11)
    test_app_id = "com.kcb.mobilebanking.android.mbp"
    
    feedback = summarize_positive_feedback_for_date(test_date, test_app_id)
    if feedback:
        logger.info("Positive Feedback Analysis Summary:")
        logger.info(f"Satisfaction Score: {feedback.satisfaction_score:.2f}")
        logger.info(f"Brand Advocates: {feedback.brand_advocates_percentage:.1f}%")
        
        logger.info("\nKey Values:")
        for value in feedback.key_values:
            logger.info(f"- {value.description} (mentioned {value.frequency} times)")
            logger.info(f"  Quote: '{value.user_quote}'")
        
        logger.info("\nDelight Factors:")
        for factor in feedback.delight_factors:
            logger.info(f"- {factor.description}")
            logger.info(f"  Wow Factor: {factor.wow_factor:.2f}")
            logger.info(f"  Viral Potential: {factor.viral_potential:.2f}")
        
        logger.info("\nSuccess Stories:")
        for story in feedback.success_stories:
            logger.info(f"- {story.title}")
            logger.info(f"  Impact: {story.impact}")
            logger.info(f"  User Segment: {story.user_segment}")
        
        logger.info("\nSuccessful Features:")
        for feature in feedback.successful_features:
            logger.info(f"- {feature.feature_name}")
            logger.info(f"  User Segments: {', '.join(feature.user_segments)}")
            logger.info(f"  Positive Mentions: {', '.join(feature.positive_mentions)}")
        
        logger.info("\nGrowth Opportunities:")
        for opportunity in feedback.growth_opportunities:
            logger.info(f"- {opportunity}")
    else:
        logger.warning("No positive feedback analysis available for the specified date") 