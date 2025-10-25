from datetime import date
from typing import List, Dict, Any
from app.models.daily_summary_models import (
    SentimentAnalysis, OverallSentiment, EmotionAnalysis,
    Emotion, SentimentSegment, SentimentSpan
)
from app.shared_services.logger_setup import setup_logger
from app.shared_services.db import get_postgres_connection

logger = setup_logger()

def get_reviews_with_sentiment(date: date, app_id: str) -> List[dict]:
    conn = get_postgres_connection("daily_reviews_view")
    try:
        with conn.cursor() as cur:
            query = """
            SELECT content, latest_analysis ->> 'sentiment' as sentiment FROM processed_app_reviews
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

def aggregate_overall_sentiment(sentiments: List[Dict]) -> OverallSentiment:
    """Aggregate overall sentiment across reviews."""
    total_score = 0
    total_confidence = 0
    distribution = {'neutral': 0, 'negative': 0, 'positive': 0}
    
    for sentiment in sentiments:
        overall = sentiment.get('overall', {})
        total_score += overall.get('score', 0)
        total_confidence += overall.get('confidence', 0)
        
        # Aggregate distribution
        dist = overall.get('distribution', {})
        for key in distribution:
            distribution[key] += dist.get(key, 0)
    
    # Calculate averages
    count = len(sentiments) or 1
    avg_score = total_score / count
    avg_confidence = total_confidence / count
    
    # Normalize distribution
    total_dist = sum(distribution.values()) or 1
    distribution = {k: v/total_dist for k, v in distribution.items()}
    
    # Determine classification
    if distribution['neutral'] > 0.5:
        classification = 'neutral'
    elif distribution['positive'] > distribution['negative']:
        classification = 'positive'
    else:
        classification = 'negative'
    
    return OverallSentiment(
        score=avg_score,
        confidence=avg_confidence,
        distribution=distribution,
        classification=classification
    )

def aggregate_emotions(emotions_list: List[Dict]) -> EmotionAnalysis:
    """Aggregate emotions across reviews, focusing only on primary emotions."""
    emotion_counts = {}
    emotion_confidences = {}
    
    for emotions in emotions_list:
        primary = emotions.get('primary', {})
        emotion = primary.get('emotion')
        confidence = primary.get('confidence', 0.5)
        
        if emotion:
            if emotion not in emotion_counts:
                emotion_counts[emotion] = 1
                emotion_confidences[emotion] = confidence
            else:
                emotion_counts[emotion] += 1
                # Average the confidence scores
                emotion_confidences[emotion] = (emotion_confidences[emotion] + confidence) / 2
    
    # Sort emotions by count to get ranking
    sorted_emotions = sorted(emotion_counts.items(), key=lambda x: x[1], reverse=True)
    emotion_scores = {emotion: count/len(emotions_list) for emotion, count in sorted_emotions} if emotions_list else {}
    
    # Get the most frequent emotion as primary
    primary_emotion = sorted_emotions[0][0] if sorted_emotions else "neutral"
    
    return EmotionAnalysis(
        primary=Emotion(
            emotion=primary_emotion,
            confidence=emotion_confidences.get(primary_emotion, 0.5)
        ),
        secondary=Emotion(
            emotion=primary_emotion,  # Use same as primary since we're not tracking secondary
            confidence=emotion_confidences.get(primary_emotion, 0.5)
        ),
        emotion_scores=emotion_scores
    )

def aggregate_segments(segments_list: List[Dict]) -> List[SentimentSegment]:
    """Aggregate sentiment segments across reviews, without spans since this is a daily summary."""
    segments_map = {}
    
    for segments in segments_list:
        for segment in segments:
            text = segment.get('text', '')
            if text not in segments_map:
                # Initialize with sentiment scores
                sentiment_data = segment.get('sentiment', {})
                if 'label' in sentiment_data:
                    # Convert structured sentiment to flat dict of floats
                    score = float(sentiment_data.get('score', 0))
                    confidence = float(sentiment_data.get('confidence', 0))
                    segments_map[text] = {
                        'text': text,
                        'count': 1,
                        'sentiment': {
                            'score': score,
                            'confidence': confidence
                        }
                    }
                else:
                    segments_map[text] = {
                        'text': text,
                        'count': 1,
                        'sentiment': {k: float(v) for k, v in sentiment_data.items()}
                    }
            else:
                # Update existing segment
                segments_map[text]['count'] += 1
                existing_sentiment = segments_map[text]['sentiment']
                new_sentiment = segment.get('sentiment', {})
                
                # Convert new sentiment if needed
                if 'label' in new_sentiment:
                    score = float(new_sentiment.get('score', 0))
                    confidence = float(new_sentiment.get('confidence', 0))
                    new_sentiment = {
                        'score': score,
                        'confidence': confidence
                    }
                
                # Average the sentiment scores
                for key in new_sentiment:
                    if key != 'label':
                        if key in existing_sentiment:
                            existing_sentiment[key] = (float(existing_sentiment[key]) + float(new_sentiment[key])) / 2
                        else:
                            existing_sentiment[key] = float(new_sentiment[key])
    
    # Convert to SentimentSegment objects
    return [
        SentimentSegment(
            text=data['text'],
            sentiment=data['sentiment'],
            count=data['count']
        )
        for data in segments_map.values()
    ]

def summarize_sentiment_for_date(date: date, app_id: str) -> SentimentAnalysis:
    """Summarize sentiment analysis for a specific date and app."""
    reviews = get_reviews_with_sentiment(date, app_id)
    logger.info(f"Processing sentiment analysis for {len(reviews)} reviews")

    if not reviews:
        return SentimentAnalysis(
            analysis_error="No reviews found for the specified date",
            overall=OverallSentiment(
                score=0,
                confidence=0,
                distribution={'neutral': 1, 'negative': 0, 'positive': 0},
                classification='neutral'
            ),
            emotions=EmotionAnalysis(
                primary=Emotion(emotion='neutral', confidence=0),
                secondary=Emotion(emotion='neutral', confidence=0),
                emotion_scores={}
            ),
            segments=[]
        )

    all_sentiments = []
    all_emotions = []
    all_segments = []

    for review in reviews:
        sentiment_data = review.get('sentiment')
        if not sentiment_data:
            continue

        # Parse JSON if needed
        if isinstance(sentiment_data, str):
            try:
                import json
                sentiment_data = json.loads(sentiment_data)
            except json.JSONDecodeError:
                logger.error("Failed to parse sentiment JSON")
                continue

        # Extract components
        overall = sentiment_data.get('overall')
        emotions = sentiment_data.get('emotions')
        segments = sentiment_data.get('segments', [])

        if overall:
            all_sentiments.append(overall)
        if emotions:
            all_emotions.append(emotions)
        if segments:
            all_segments.append(segments)

    # Aggregate components
    overall_sentiment = aggregate_overall_sentiment(all_sentiments)
    emotions_analysis = aggregate_emotions(all_emotions)
    sentiment_segments = aggregate_segments(all_segments)

    # Return final analysis
    return SentimentAnalysis(
        overall=overall_sentiment,
        emotions=emotions_analysis,
        segments=sentiment_segments
    )

if __name__ == "__main__":
    test_date = date(2025, 3, 11)
    test_app_id = "com.kcb.mobilebanking.android.mbp"
    
    sentiment = summarize_sentiment_for_date(test_date, test_app_id)
    logger.info(f"Full SentimentAnalysis object:")
    logger.info(f"{sentiment.model_dump_json(indent=2)}")
    if sentiment:
        logger.info("Sentiment Analysis Summary:")
        logger.info(f"Overall Score: {sentiment.overall.score:.2f} (Confidence: {sentiment.overall.confidence:.2f})")
        logger.info(f"Classification: {sentiment.overall.classification}")
        logger.info("\nEmotion Analysis:")
        logger.info(f"Primary: {sentiment.emotions.primary.emotion} (Confidence: {sentiment.emotions.primary.confidence:.2f})")
        logger.info(f"Secondary: {sentiment.emotions.secondary.emotion} (Confidence: {sentiment.emotions.secondary.confidence:.2f})")
        logger.info("\nSegments Analysis:")
        for segment in sentiment.segments:
            logger.info(f"Text: {segment.text}")
            logger.info(f"Sentiment: {segment.sentiment}")
    else:
        logger.warning("No sentiment analysis available for the specified date") 