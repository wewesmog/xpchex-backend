from typing import List, Tuple, Optional
from google_play_scraper import search
import logging

logger = logging.getLogger(__name__)

def search_app_id(
    query: str, 
    country: str = 'ke', 
    lang: str = 'en', 
    n_hits: int = 5
) -> List[Tuple[str, str, str, str]]:
    """
    Search for apps on Google Play Store
    
    Args:
        query: Search query (e.g., "absa app", "kcb mobile banking")
        country: Country code (e.g., 'ke' for Kenya)
        lang: Language code (e.g., 'en' for English)
        n_hits: Number of results to return
        
    Returns:
        List of tuples containing (app_id, title, developer, description)
    """
    try:
        results = search(query, country=country, lang=lang, n_hits=n_hits)
        return [(
            app['appId'],
            app['title'],
            app['developer'],
            app.get('description', 'No description available')
        ) for app in results]
    except Exception as e:
        logger.error(f"Error searching for apps: {e}")
        return []

def format_search_results(results: List[Tuple[str, str, str, str]]) -> str:
    """
    Format search results in a human-readable way
    """
    if not results:
        return "No apps found"
        
    output = []
    for i, (app_id, title, developer, description) in enumerate(results, 1):
        output.append(f"{i}. {title}")
        output.append(f"   Developer: {developer}")
        output.append(f"   App ID: {app_id}")
        output.append(f"   Description: {description[:200]}...")  # First 200 chars
        output.append("")
    
    return "\n".join(output)

def main():
    """Command line interface for app search"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Search for apps on Google Play Store')
    parser.add_argument('query', help='Search query (e.g., "absa app")')
    parser.add_argument('--country', default='ke', help='Country code (default: ke)')
    parser.add_argument('--lang', default='en', help='Language code (default: en)')
    parser.add_argument('--n-hits', type=int, default=5, help='Number of results (default: 5)')
    
    args = parser.parse_args()
    
    results = search_app_id(
        query=args.query,
        country=args.country,
        lang=args.lang,
        n_hits=args.n_hits
    )
    
    print("\nSearch Results:")
    print(format_search_results(results))

if __name__ == "__main__":
    main() 