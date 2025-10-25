from datetime import date, datetime, timezone, timedelta
from typing import List, Optional, Dict
from collections import defaultdict
import random  # Add random for sentiment tie breaking
from app.models.daily_summary_models import AspectAnalysis, Topic, UserIntent, IdentifiedFeature, AspectSentiment, Mention, ReviewContent
from app.shared_services.logger_setup import setup_logger
from app.shared_services.db import get_postgres_connection

logger = setup_logger()

# Test data
test_date = date(2025, 3, 11)
test_app_id = "com.kcb.mobilebanking.android.mbp"

def get_random_max_sentiment(sentiment_counts: Dict[str, int]) -> tuple:
    """
    Get the sentiment with maximum count, randomly selecting if there are ties.
    Returns tuple of (sentiment_label, count)
    """
    if not sentiment_counts:
        return "neutral", 0
        
    max_count = max(sentiment_counts.values())
    # Get all sentiments with the max count
    max_sentiments = [(sentiment, count) for sentiment, count in sentiment_counts.items() if count == max_count]
    # Randomly select one if there are ties
    return random.choice(max_sentiments)

def get_reviews_with_aspects(date: date, app_id: str) -> List[dict]:
    conn = get_postgres_connection("daily_reviews_view")
    try:
        with conn.cursor() as cur:
            query = """
            SELECT content, latest_analysis ->> 'aspects' as aspects FROM processed_app_reviews
            WHERE date(review_created_at) = %s::date AND app_id = %s 
            """
            logger.info(f"Executing query: {str(cur.mogrify(query, (date, app_id)))[:500]}...")
            cur.execute(query, (date, app_id))

            # Get column names from cursor description
            columns = [desc[0] for desc in cur.description]
            rows = cur.fetchall()

            # Convert list of tuples to list of dictionaries
            reviews = [dict(zip(columns, row)) for row in rows]
            logger.info(f"Fetched {len(rows)} reviews for date {date} and app_id {app_id}")
            return reviews
    except Exception as e:
        logger.error(f"Error fetching reviews: {str(e)[:500]}...")
        raise e
    finally:
        conn.close()

def aggregate_topics(topics_list: List[List[Dict]]) -> List[Topic]:
    """Aggregate topics across all reviews."""
    topic_map = defaultdict(lambda: {
        'keywords': defaultdict(int),  # Changed to count keywords
        'sentiment_counts': defaultdict(int),
        'count': 0
    })
    
    # Combine similar topics
    for topics in topics_list:
        for topic_dict in topics:
            # Count each keyword occurrence
            for keyword in topic_dict.get('keywords', []):
                topic_map[topic_dict['name']]['keywords'][keyword] += 1
            topic_map[topic_dict['name']]['sentiment_counts'][topic_dict.get('sentiment', 'neutral')] += 1
            topic_map[topic_dict['name']]['count'] += 1
    
    # Convert back to Topic objects
    aggregated_topics = []
    for name, data in topic_map.items():
        # Get most common sentiment with random tie break
        sentiment, _ = get_random_max_sentiment(data['sentiment_counts'])
        
        # Convert keyword counts to dict
        keyword_counts = dict(data['keywords'])
        
        aggregated_topics.append(Topic(
            name=name,
            keywords=list(data['keywords'].keys()),  # Unique keywords
            sentiment=sentiment,
            occurrence_count=data['count'],
            keyword_counts=keyword_counts
        ))
    
    return aggregated_topics

def aggregate_mentions(mentions_list: List[Dict]) -> List[Mention]:
    """Aggregate mentions and count duplicates."""
    mention_counts = defaultdict(int)
    
    # Count identical mentions
    for mention_dict in mentions_list:
        mention_counts[mention_dict.get('text', '')] += 1
    
    # Create new mentions with counts
    return [Mention(text=text, count=count) 
            for text, count in mention_counts.items()]

def aggregate_features(features_list: List[List[Dict]]) -> List[IdentifiedFeature]:
    """Aggregate features across all reviews."""
    feature_map = defaultdict(lambda: {
        'mentions': [],
        'sentiment_counts': defaultdict(int),
        'importance_scores': [],
        'count': 0
    })
    
    # Combine similar features
    for features in features_list:
        for feature_dict in features:
            feature_map[feature_dict['name']]['mentions'].extend(feature_dict.get('mentions', []))
            feature_map[feature_dict['name']]['sentiment_counts'][feature_dict.get('sentiment', {}).get('label', 'neutral')] += 1
            if 'importance_score' in feature_dict:
                feature_map[feature_dict['name']]['importance_scores'].append(feature_dict['importance_score'])
            feature_map[feature_dict['name']]['count'] += 1
    
    # Convert back to IdentifiedFeature objects
    aggregated_features = []
    for name, data in feature_map.items():
        # Get most common sentiment with random tie break
        sentiment_label, sentiment_count = get_random_max_sentiment(data['sentiment_counts'])
        # Calculate average importance score
        avg_importance = sum(data['importance_scores']) / len(data['importance_scores']) if data['importance_scores'] else 0.0
        
        aggregated_features.append(IdentifiedFeature(
            name=name,
            sentiment=AspectSentiment(
                label=sentiment_label,
                count=sentiment_count
            ),
            mentions=aggregate_mentions(data['mentions']),
            importance_score=avg_importance,
            occurrence_count=data['count']
        ))
    
    return aggregated_features

def aggregate_user_intent(intents: List[Dict]) -> UserIntent:
    """Aggregate user intents across all reviews."""
    intent_counts = defaultdict(int)
    total = 0
    
    for intent_dict in intents:
        primary = intent_dict.get('primary', 'unknown')
        intent_counts[primary] += 1
        total += 1
    
    return UserIntent(
        intents=dict(intent_counts),
        total_intents=total
    )

def summarize_aspects_for_date(date: date, app_id: str) -> AspectAnalysis:
    # Get the reviews for the date
    reviews = get_reviews_with_aspects(date, app_id)
    logger.info(f"Fetched reviews response for date {date} and app_id {app_id}")

    if not reviews:
        logger.warning(f"No reviews found for date {date}")
        return AspectAnalysis(
            analysis_error="No reviews found for the specified date",
            features=[],
            topic_list=[],
            intent_data=UserIntent(),
            total_reviews_analyzed=0,
            review_contents=[],
            analysis_date=date,
            app_id=app_id
        )

    # Get aspects from the reviews
    all_topics = []
    all_features = []
    all_intents = []
    review_contents = []
    total_reviews_processed = 0
    
    for i, review in enumerate(reviews):
        # Debug log the review content
        logger.debug(f"Processing review {i}: {str(review.get('content'))[:100]}...")
        
        # Store review content
        if review.get('content'):
            review_contents.append(ReviewContent(
                content=review.get('content', '')
            ))
        
        # Get aspects directly from the review
        aspects = review.get('aspects')
        if not aspects:
            logger.debug(f"No aspects found in review {i}")
            continue

        # Parse aspects as JSON if it's a string
        if isinstance(aspects, str):
            try:
                import json
                aspects = json.loads(aspects)
            except json.JSONDecodeError:
                logger.error(f"Failed to parse aspects JSON in review {i}")
                continue

        # Log the aspects for debugging
        logger.debug(f"Found aspects in review {i}: {str(aspects)[:200]}...")

        # Extract components from aspects
        if isinstance(aspects, dict):
            topics = aspects.get('topics', [])
            features = aspects.get('features', [])
            intent = aspects.get('user_intent', {})
            
            all_topics.append(topics)
            all_features.append(features)
            all_intents.append(intent)
            total_reviews_processed += 1

    # Aggregate all components
    aggregated_topics = aggregate_topics(all_topics)
    aggregated_features = aggregate_features(all_features)
    aggregated_intent = aggregate_user_intent(all_intents)

    # Create and return the AspectAnalysis object
    return AspectAnalysis(
        features=aggregated_features,
        topic_list=aggregated_topics,
        intent_data=aggregated_intent,
        total_reviews_analyzed=total_reviews_processed,
        review_contents=review_contents,
        analysis_date=date,
        app_id=app_id
    )

if __name__ == "__main__":
    aspects = summarize_aspects_for_date(test_date, test_app_id)
    if aspects:
        logger.info(f"Successfully summarized aspects from reviews for date {test_date}")
        logger.info(f"Full AspectAnalysis object:")
        logger.info(f"{aspects.model_dump_json(indent=2)}")
        
        logger.info(f"Aggregated summary:")
        logger.info(f"- Total reviews analyzed: {aspects.total_reviews_analyzed}")
        logger.info(f"- Topics found: {len(aspects.topic_list)}")
        for topic in aspects.topic_list:
            logger.info(f"  * {topic.name} (occurred {topic.occurrence_count} times)")
            logger.info(f"    Keywords: {dict(topic.keyword_counts)}")
            logger.info(f"    Sentiment: {topic.sentiment}")
        logger.info(f"- Features found: {len(aspects.features)}")
        for feature in aspects.features:
            logger.info(f"  * {feature.name} (occurred {feature.occurrence_count} times)")
            logger.info(f"    Sentiment: {feature.sentiment.label} (mentioned {feature.sentiment.count} times)")
        logger.info(f"- User Intents:")
        for intent, count in aspects.intent_data.intents.items():
            logger.info(f"  * {intent}: {count} times ({(count/aspects.intent_data.total_intents)*100:.1f}%)")
    else:
        logger.warning(f"No aspects to summarize for date {test_date}")




 