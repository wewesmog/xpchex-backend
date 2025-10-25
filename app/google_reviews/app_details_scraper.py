import os
import argparse
from datetime import datetime, timezone
from google_play_scraper import app
from ..shared_services.db import get_postgres_connection
from ..shared_services.logger_setup import setup_logger

logger = setup_logger()

class AppDetailsScraper:
    def __init__(self, db_name="xpchex"):
        """Initialize the AppDetailsScraper with database connection"""
        self.conn = get_postgres_connection(db_name)
        self.cursor = self.conn.cursor()

    def get_current_app_version(self, app_id: str) -> str:
        """Get the current version of the app from our database"""
        try:
            self.cursor.execute("""
                SELECT version 
                FROM current_app_details 
                WHERE app_id = %s
            """, (app_id,))
            result = self.cursor.fetchone()
            return result[0] if result else None
        except Exception as e:
            logger.error(f"Error getting current app version: {e}")
            return None

    def save_app_details(self, app_id: str, details: dict) -> bool:
        """
        Save app details to history table
        Returns: True if details were saved successfully
        """
        try:
            # Prepare the data with correct field mappings
            data = {
                'app_id': app_id,
                'title': details.get('title'),
                'description': details.get('description'),
                'summary': details.get('summary'),
                'installs': details.get('installs'),
                'score': details.get('score'),
                'ratings_count': details.get('ratings'),
                'reviews_count': details.get('reviews'),
                'price': details.get('price'),
                'price_currency': details.get('currency'),
                'size': details.get('size', 'Unknown'),  # Some apps might not have size
                'minimum_android': details.get('androidVersion'),
                'developer_id': details.get('developerId'),
                'developer_email': details.get('developerEmail'),
                'developer_website': details.get('developerWebsite'),
                'developer_address': details.get('developerAddress'),
                'privacy_policy': details.get('privacyPolicy'),
                'genre': details.get('genre'),
                'genre_id': details.get('genreId'),
                'content_rating': details.get('contentRating'),
                'content_rating_description': details.get('contentRatingDescription'),
                'app_updated_at': datetime.fromtimestamp(details.get('updated'), tz=timezone.utc) if details.get('updated') else None
            }

            # Insert new record
            self.cursor.execute("""
                INSERT INTO app_details_history (
                    app_id, title, description, summary, installs,
                    score, ratings_count, reviews_count, price, price_currency,
                    size, minimum_android, developer_id, developer_email,
                    developer_website, developer_address, privacy_policy, genre,
                    genre_id, content_rating, content_rating_description,
                    app_updated_at
                ) VALUES (
                    %(app_id)s, %(title)s, %(description)s, %(summary)s, %(installs)s,
                    %(score)s, %(ratings_count)s, %(reviews_count)s, %(price)s, %(price_currency)s,
                    %(size)s, %(minimum_android)s, %(developer_id)s, %(developer_email)s,
                    %(developer_website)s, %(developer_address)s, %(privacy_policy)s, %(genre)s,
                    %(genre_id)s, %(content_rating)s, %(content_rating_description)s,
                    %(app_updated_at)s
                )
            """, data)
            
            self.conn.commit()
            logger.info(f"Successfully saved new app details for {app_id}")
            return True
            
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error saving app details: {e}")
            raise

    def fetch_app_details(self, app_id: str, country: str = 'ke', lang: str = 'en') -> dict:
        """Fetch app details from Google Play Store"""
        try:
            details = app(
                app_id,
                lang=lang,
                country=country
            )
            return details
        except Exception as e:
            logger.error(f"Error fetching app details: {e}")
            raise

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            self.conn.rollback()
            logger.error(f"Error during AppDetailsScraper execution: {exc_val}")
        else:
            self.conn.commit()
        
        self.cursor.close()
        self.conn.close()

# def main():
#     """Command line interface for the AppDetailsScraper"""
#     parser = argparse.ArgumentParser(description='Fetch and store Google Play Store app details')
#     parser.add_argument('app_id', help='Google Play Store app ID')
#     parser.add_argument('--lang', default='en', help='Language code (default: en)')
#     parser.add_argument('--country', default='ke', help='Country code (default: ke)')
#     parser.add_argument('--force', action='store_true', 
#                       help='Force update even if version hasn\'t changed')
    
#     args = parser.parse_args()
    
#     with AppDetailsScraper() as scraper:
#         # Fetch details
#         details = scraper.fetch_app_details(
#             app_id=args.app_id,
#             country=args.country,
#             lang=args.lang
#         )
        
#         # Save if forced or if details have changed
#         if args.force:
#             scraper.save_app_details(args.app_id, details)
#             print("Forced update of app details")
#         elif scraper.save_app_details(args.app_id, details):
#             print("New app details saved")
#         else:
#             print("No changes in app details")
        
#         # Print current version info
#         print(f"\nCurrent app version: {details.get('version')}")
#         print(f"Last updated: {details.get('updated')}")

if __name__ == "__main__":
    app_id = 'com.kcb.mobilebanking.android.mbp'
    fetch_app_details(app_id)
        
        # Save if forced or if details have changed
   