from app.google_reviews.app_details_scraper import AppDetailsScraper
from app.google_reviews.reviews_scraper import ReviewScraper
from app.google_reviews.review_analyzer import perform_review_analysis
from datetime import datetime, timezone, timedelta
import asyncio

def test_app_details_scraper():
    with AppDetailsScraper() as scraper:
        details = scraper.fetch_app_details(
            app_id="com.kcb.mobilebanking.android.mbp",
            country="ke",
            lang="en"
        )
        print("\nApp Details:")
        print(f"Total reviews count in Play Store: {details.get('reviews', 0)}")
        print(f"Total ratings count: {details.get('ratings', 0)}")
        print(details)

def test_fetch_app_reviews():
    # Try to fetch all available reviews
    with ReviewScraper() as scraper:
        total_reviews, total_processed = scraper.fetch_reviews(
            app_id="com.kcb.mobilebanking.android.mbp",
            lang="en",
            count=1000,  # Try to fetch more reviews
            incremental=False,  # Don't use incremental to get all reviews
            start_date=datetime(1900, 1, 1, tzinfo=timezone.utc)  # Start from very old date
        )
        print(f"\nReviews Fetched:")
        print(f"Total reviews fetched: {total_reviews}")
        print(f"Total reviews processed: {total_processed}")

async def test_perform_review_analysis():
    print("\nTesting review analysis...")
    sample_review_content = "This app is great! I love the new features, but the UI could be improved."
    analysis_result = await perform_review_analysis(sample_review_content)
    assert analysis_result is not None
    assert analysis_result.review_id is not None
    print(f"Review analysis successful. Review ID: {analysis_result.review_id}")
    print(f"Sentiment: {analysis_result.sentiment.overall.classification}")
    print(f"Aspects: {analysis_result.aspects}")

if __name__ == "__main__":
    test_fetch_app_reviews()
    test_app_details_scraper()
    asyncio.run(test_perform_review_analysis())