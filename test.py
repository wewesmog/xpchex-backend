import json
import os
from google_play_scraper import app, reviews, Sort
from datetime import datetime
import psycopg
from psycopg.rows import dict_row
import time
from dotenv import load_dotenv
import argparse

# Load environment variables
load_dotenv()

# Database connection parameters
DB_NAME = os.getenv('DB_NAME', 'postgres')
DB_USER = os.getenv('DB_USER', 'postgres')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'postgres')
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = os.getenv('DB_PORT', '5432')

def get_db_connection():
    return psycopg.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT,
        row_factory=dict_row
    )

def save_reviews_to_db(reviews_data, conn):
    with conn.cursor() as cur:
        for review in reviews_data:
            cur.execute("""
                INSERT INTO app_reviews (
                    review_id, username, user_image, content, score,
                    thumbs_up_count, review_created_at, reply_content,
                    reply_created_at, app_version
                ) VALUES (
                    %(reviewId)s, %(userName)s, %(userImage)s, %(content)s,
                    %(score)s, %(thumbsUpCount)s, %(at)s, %(replyContent)s,
                    %(repliedAt)s, %(reviewCreatedVersion)s
                )
                ON CONFLICT (review_id) DO UPDATE SET
                    thumbs_up_count = EXCLUDED.thumbs_up_count,
                    reply_content = EXCLUDED.reply_content,
                    reply_created_at = EXCLUDED.reply_created_at,
                    updated_at = CURRENT_TIMESTAMP
            """, review)
        conn.commit()

class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)

def fetch_reviews(app_id, count=0, lang='en', country='ke'):
    """
    Fetch reviews for a given app.
    
    Args:
        app_id (str): The Google Play Store app ID
        count (int): Number of reviews to fetch. 0 means fetch all available reviews
        lang (str): Language of reviews to fetch
        country (str): Country code for reviews
    
    Returns:
        list: List of reviews
        int: Total number of reviews fetched
    """
    continuation_token = None
    all_reviews = []
    total_reviews = 0
    batch_size = 100  # Max allowed by the API

    try:
        while True:
            # Fetch batch of reviews
            result, continuation_token = reviews(
                app_id,
                lang=lang,
                country=country,
                count=min(batch_size, count - total_reviews) if count > 0 else batch_size,
                sort=Sort.NEWEST,
                continuation_token=continuation_token
            )
            
            if not result:
                break
            
            # Add to our collection
            all_reviews.extend(result)
            total_reviews += len(result)
            print(f"Fetched {len(result)} reviews. Total: {total_reviews}")
            
            # Save the current batch to a JSON file
            with open(os.path.join(os.path.dirname(__file__), 'all_reviews.json'), 'w', encoding='utf-8') as f:
                json.dump(all_reviews, f, ensure_ascii=False, indent=4, cls=DateTimeEncoder)
            
            # Check if we've reached the requested count
            if count > 0 and total_reviews >= count:
                break
                
            if not continuation_token:
                break
                
            # Add a small delay to avoid hitting rate limits
            time.sleep(1)
            
    except Exception as e:
        print(f"An error occurred: {e}")
        raise

    print(f"Successfully fetched {total_reviews} reviews in total.")
    print(f"Reviews saved to 'all_reviews.json'")
    return all_reviews, total_reviews

def get_most_recent_review_date(reviews_data):
    """Get the date of the most recent review from the data"""
    if not reviews_data:
        return None
    return max(review['at'] for review in reviews_data)

def main():
    parser = argparse.ArgumentParser(description='Fetch Google Play Store reviews')
    parser.add_argument('app_id', help='Google Play Store app ID')
    parser.add_argument('--count', type=int, default=0, 
                      help='Number of reviews to fetch (0 for all reviews)')
    parser.add_argument('--lang', default='en', help='Language code (default: en)')
    parser.add_argument('--country', default='ke', help='Country code (default: ke)')
    
    args = parser.parse_args()
    
    reviews_data, total = fetch_reviews(
        app_id=args.app_id,
        count=args.count,
        lang=args.lang,
        country=args.country
    )
    
    if reviews_data:
        most_recent = get_most_recent_review_date(reviews_data)
        print(f"\nMost recent review date: {most_recent}")

if __name__ == "__main__":
    main()

# If you want to get more reviews, you can use the continuation token:
# more_reviews, _ = reviews(
#     'com.kcb.mobilebanking.android.mbp',
#     continuation_token=continuation_token  # to get next batch of reviews
# )



#https://play.google.com/store/apps/details?id=com.kcb.mobilebanking.android.mbp&pcampaignid=web_share