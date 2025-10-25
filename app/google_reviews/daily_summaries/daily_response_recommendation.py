from datetime import date
from typing import List, Dict, Any
from app.models.daily_summary_models import (
    ResponseRecommendation, ResponseContext, ResponseTone,
    ResponseStrategy, ResponseCommitment
)
from app.shared_services.logger_setup import setup_logger
from app.shared_services.db import get_postgres_connection

logger = setup_logger()

def get_reviews_with_response_recommendation(date: date, app_id: str) -> List[dict]:
    conn = get_postgres_connection("daily_reviews_view")
    try:
        with conn.cursor() as cur:
            query = """
            SELECT content, latest_analysis ->> 'response_recommendation' as response_recommendation FROM processed_app_reviews
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

def aggregate_response_context(contexts: List[Dict]) -> ResponseContext:
    """Aggregate response contexts across reviews."""
    all_issues = set()
    all_positives = set()
    total_sentiment = 0
    platforms = set()
    segments = set()
    
    for context in contexts:
        all_issues.update(context.get('related_issues', []))
        all_positives.update(context.get('related_positives', []))
        total_sentiment += context.get('user_sentiment', 0)
        if context.get('platform'):
            platforms.add(context['platform'])
        if context.get('user_segment'):
            segments.add(context['user_segment'])
    
    count = len(contexts) or 1
    avg_sentiment = total_sentiment / count
    
    return ResponseContext(
        platform=list(platforms)[0] if platforms else None,
        user_segment=list(segments)[0] if segments else None,
        related_issues=list(all_issues),
        user_sentiment=avg_sentiment,
        related_positives=list(all_positives)
    )

def aggregate_response_tone(tones: List[Dict]) -> ResponseTone:
    """Aggregate response tones across reviews."""
    primary_tones = {}
    secondary_tones = {}
    formality_levels = {}
    total_personalization = 0
    
    for tone in tones:
        # Count tone frequencies
        primary = tone.get('primary_tone', '')
        if primary:
            primary_tones[primary] = primary_tones.get(primary, 0) + 1
        
        secondary = tone.get('secondary_tone', '')
        if secondary:
            secondary_tones[secondary] = secondary_tones.get(secondary, 0) + 1
        
        formality = tone.get('formality_level', '')
        if formality:
            formality_levels[formality] = formality_levels.get(formality, 0) + 1
        
        total_personalization += tone.get('personalization_level', 0)
    
    count = len(tones) or 1
    
    return ResponseTone(
        primary_tone=max(primary_tones.items(), key=lambda x: x[1])[0] if primary_tones else "neutral",
        secondary_tone=max(secondary_tones.items(), key=lambda x: x[1])[0] if secondary_tones else "neutral",
        formality_level=max(formality_levels.items(), key=lambda x: x[1])[0] if formality_levels else "neutral",
        personalization_level=total_personalization / count
    )

def aggregate_commitments(commitments_list: List[Dict]) -> List[ResponseCommitment]:
    """Aggregate response commitments across reviews."""
    commitments_map = {}
    
    for commitments in commitments_list:
        for commitment in commitments:
            key = (commitment.get('timeline', ''), commitment.get('commitment_type', ''))
            if key not in commitments_map:
                commitments_map[key] = commitment
            else:
                existing = commitments_map[key]
                # Update confidence if higher
                if commitment.get('confidence_level', 0) > existing.get('confidence_level', 0):
                    existing['confidence_level'] = commitment['confidence_level']
                # Update conditions if more specific
                if commitment.get('conditions') and (not existing.get('conditions') or 
                   len(commitment['conditions']) > len(existing['conditions'])):
                    existing['conditions'] = commitment['conditions']

    return [ResponseCommitment(**commitment) for commitment in commitments_map.values()]

def aggregate_response_strategy(strategies: List[Dict]) -> ResponseStrategy:
    """Aggregate response strategies across reviews."""
    all_key_points = set()
    total_priority_score = 0
    should_respond_count = 0
    public_response_count = 0
    all_commitments = []
    
    for strategy in strategies:
        all_key_points.update(strategy.get('key_points', []))
        priority_scores = {'low': 1, 'medium': 2, 'high': 3}
        total_priority_score += priority_scores.get(strategy.get('priority', 'low'), 1)
        should_respond_count += 1 if strategy.get('should_respond', False) else 0
        public_response_count += 1 if strategy.get('public_response', False) else 0
        if strategy.get('commitments'):
            all_commitments.append(strategy['commitments'])
    
    count = len(strategies) or 1
    avg_priority_score = total_priority_score / count
    priority = 'high' if avg_priority_score > 2 else 'medium' if avg_priority_score > 1 else 'low'
    
    return ResponseStrategy(
        tone=aggregate_response_tone([s.get('tone', {}) for s in strategies if s.get('tone')]),
        priority=priority,
        key_points=list(all_key_points),
        commitments=aggregate_commitments(all_commitments),
        should_respond=should_respond_count > count/2,
        public_response=public_response_count > count/2
    )

def summarize_response_recommendation_for_date(date: date, app_id: str) -> ResponseRecommendation:
    """Summarize response recommendation analysis for a specific date and app."""
    reviews = get_reviews_with_response_recommendation(date, app_id)
    logger.info(f"Processing response recommendation analysis for {len(reviews)} reviews")

    if not reviews:
        return ResponseRecommendation(
            analysis_error="No reviews found for the specified date",
            context=ResponseContext(
                platform=None,
                user_segment=None,
                related_issues=[],
                user_sentiment=0,
                related_positives=[]
            ),
            metadata={},
            strategy=ResponseStrategy(
                tone=ResponseTone(
                    primary_tone="neutral",
                    secondary_tone="neutral",
                    formality_level="neutral",
                    personalization_level=0
                ),
                priority="low",
                key_points=[],
                commitments=[],
                should_respond=False,
                public_response=False
            ),
            response_id=None,
            follow_up_needed=False,
            response_required=False,
            follow_up_timeline="none",
            suggested_response="No response recommendation available.",
            response_guidelines=[],
            alternative_responses=[]
        )

    all_contexts = []
    all_strategies = []
    all_guidelines = set()
    all_alternatives = set()
    follow_up_needed_count = 0
    response_required_count = 0
    follow_up_timelines = []
    suggested_responses = []
    metadata = {}

    for review in reviews:
        recommendation_data = review.get('response_recommendation')
        if not recommendation_data:
            continue

        # Parse JSON if needed
        if isinstance(recommendation_data, str):
            try:
                import json
                recommendation_data = json.loads(recommendation_data)
            except json.JSONDecodeError:
                logger.error("Failed to parse response recommendation JSON")
                continue

        # Extract components
        context = recommendation_data.get('context')
        strategy = recommendation_data.get('strategy')
        guidelines = recommendation_data.get('response_guidelines', [])
        alternatives = recommendation_data.get('alternative_responses', [])
        metadata.update(recommendation_data.get('metadata', {}))

        if context:
            all_contexts.append(context)
        if strategy:
            all_strategies.append(strategy)
        all_guidelines.update(guidelines)
        all_alternatives.update(alternatives)

        follow_up_needed_count += 1 if recommendation_data.get('follow_up_needed', False) else 0
        response_required_count += 1 if recommendation_data.get('response_required', False) else 0
        if recommendation_data.get('follow_up_timeline'):
            follow_up_timelines.append(recommendation_data['follow_up_timeline'])
        if recommendation_data.get('suggested_response'):
            suggested_responses.append(recommendation_data['suggested_response'])

    # Aggregate components
    count = len(reviews)
    aggregated_context = aggregate_response_context(all_contexts)
    aggregated_strategy = aggregate_response_strategy(all_strategies)

    # Choose the most detailed suggested response
    suggested_response = max(suggested_responses, key=len) if suggested_responses else "No response suggested."
    
    # Choose the most urgent follow-up timeline
    timeline_urgency = {
        'immediate': 3,
        'within 24 hours': 2,
        'within a week': 1,
        'none': 0
    }
    follow_up_timeline = max(follow_up_timelines, key=lambda x: timeline_urgency.get(x, 0)) if follow_up_timelines else "none"

    # Return final analysis
    return ResponseRecommendation(
        context=aggregated_context,
        metadata=metadata,
        strategy=aggregated_strategy,
        response_id=None,  # This would typically come from your response tracking system
        follow_up_needed=follow_up_needed_count > count/2,
        response_required=response_required_count > count/2,
        follow_up_timeline=follow_up_timeline,
        suggested_response=suggested_response,
        response_guidelines=list(all_guidelines),
        alternative_responses=list(all_alternatives)
    )

if __name__ == "__main__":
    test_date = date(2025, 3, 11)
    test_app_id = "com.kcb.mobilebanking.android.mbp"
    
    recommendation = summarize_response_recommendation_for_date(test_date, test_app_id)
    if recommendation:
        logger.info("Response Recommendation Summary:")
        logger.info(f"Response Required: {recommendation.response_required}")
        logger.info(f"Follow-up Needed: {recommendation.follow_up_needed}")
        logger.info(f"Follow-up Timeline: {recommendation.follow_up_timeline}")
        
        logger.info("\nContext:")
        logger.info(f"User Sentiment: {recommendation.context.user_sentiment:.2f}")
        logger.info(f"Related Issues: {', '.join(recommendation.context.related_issues)}")
        logger.info(f"Related Positives: {', '.join(recommendation.context.related_positives)}")
        
        logger.info("\nStrategy:")
        logger.info(f"Priority: {recommendation.strategy.priority}")
        logger.info(f"Tone: {recommendation.strategy.tone.primary_tone}/{recommendation.strategy.tone.secondary_tone}")
        logger.info(f"Personalization Level: {recommendation.strategy.tone.personalization_level:.2f}")
        
        logger.info("\nKey Points:")
        for point in recommendation.strategy.key_points:
            logger.info(f"- {point}")
        
        logger.info("\nSuggested Response:")
        logger.info(recommendation.suggested_response)
        
        logger.info("\nAlternative Responses:")
        for response in recommendation.alternative_responses:
            logger.info(f"- {response}")
    else:
        logger.warning("No response recommendation available for the specified date") 