from typing import List, Dict, Optional
import math
import re
import time
from datetime import datetime
from google_play_scraper import search, app as gp_app
import logging
from urllib.parse import urlparse, parse_qs

logger = logging.getLogger(__name__)

def extract_app_id_from_url(url: str) -> Optional[str]:
    """
    Extract app_id from Google Play Store URL.
    Handles formats like:
    - https://play.google.com/store/apps/details?id=com.example.app
    - https://play.google.com/store/apps/details?id=com.example.app&hl=en
    """
    if not url:
        return None
    try:
        parsed = urlparse(url)
        # Check query parameters
        if parsed.query:
            params = parse_qs(parsed.query)
            if 'id' in params:
                return params['id'][0]
        # Check path segments (sometimes id is in path)
        path_parts = parsed.path.split('/')
        if 'details' in path_parts:
            idx = path_parts.index('details')
            if idx + 1 < len(path_parts):
                return path_parts[idx + 1]
    except Exception as e:
        logger.debug(f"Failed to parse URL {url}: {e}")
    return None

def get_app_id_robust(app_summary: Dict) -> Optional[str]:
    """
    Robustly extract app_id from search result using multiple strategies.
    """
    # Strategy 1: Direct field access (check multiple variations)
    app_id = (app_summary.get('appId') or 
              app_summary.get('app_id') or 
              app_summary.get('packageId') or
              app_summary.get('package_id'))
    if app_id:
        return str(app_id).strip()
    
    # Strategy 2: Extract from URL if present
    url_fields = ['url', 'link', 'playStoreUrl', 'storeUrl', 'href', 'permalink']
    for field in url_fields:
        url = app_summary.get(field)
        if url:
            app_id = extract_app_id_from_url(str(url))
            if app_id:
                return app_id
    
    # Strategy 3: Extract from package name (sometimes package name IS app_id)
    package_name = (app_summary.get('packageName') or 
                    app_summary.get('package_name') or
                    app_summary.get('package'))
    if package_name:
        return str(package_name).strip()
    
    # Strategy 4: Check all keys for appId-like values (case-insensitive)
    for key, value in app_summary.items():
        if isinstance(value, str) and value.strip():
            # Check if key name suggests it's an app_id
            if any(keyword in key.lower() for keyword in ['appid', 'app_id', 'package', 'id']):
                if value.startswith('com.') or '.' in value:
                    return value.strip()
    
    # Strategy 5: Try to extract from all string values (last resort)
    for key, value in app_summary.items():
        if isinstance(value, str) and value.strip():
            # Looks like a package name (com.xxx.xxx format)
            if value.startswith('com.') and '.' in value:
                parts = value.split('.')
                if len(parts) >= 2 and all(part for part in parts):
                    return value.strip()
            # Or check if it's a valid app_id format (contains dots and lowercase)
            elif '.' in value and value.replace('.', '').replace('_', '').isalnum():
                # Additional validation: typical app_id format
                if len(value.split('.')) >= 2:
                    return value.strip()
    
    return None

def search_app_by_title_fallback(title: str, developer: str, country: str = 'ke', lang: str = 'en') -> Optional[str]:
    """
    Fallback: Try to find app_id by searching with title + developer combination.
    Returns app_id if found, None otherwise.
    """
    if not title or not developer:
        return None
    
    try:
        # Try multiple search query variations
        queries = [
            f"{title} {developer}",
            f"{title}",
            f"{developer} {title}",
        ]
        
        for query in queries:
            try:
                results = search(query, country=country, lang=lang, n_hits=5)
                for result in results:
                    # Match by title (case-insensitive)
                    result_title = result.get('title', '').lower()
                    result_dev = result.get('developer', '').lower()
                    
                    if (title.lower() in result_title or result_title in title.lower()) and \
                       (developer.lower() in result_dev or result_dev in developer.lower()):
                        # Found a match, try to extract app_id
                        app_id = get_app_id_robust(result)
                        if app_id:
                            logger.info(f"Found app_id via fallback search: {app_id} for '{title}' by '{developer}'")
                            return app_id
            except Exception as e:
                logger.debug(f"Fallback search failed for query '{query}': {e}")
                continue
    except Exception as e:
        logger.warning(f"Fallback search failed for '{title}': {e}")
    
    return None

def fetch_app_details_with_retry(
    app_id: str, 
    lang: str = 'en', 
    country: str = 'ke',
    max_retries: int = 3,
    retry_delay: float = 1.0
) -> Optional[Dict]:
    """
    Fetch app details with retry logic and exponential backoff.
    """
    for attempt in range(max_retries):
        try:
            details = gp_app(app_id, lang=lang, country=country)
            # Verify app_id is in details
            if details:
                verified_id = details.get('appId') or details.get('app_id') or app_id
                if verified_id != app_id:
                    logger.info(f"App ID mismatch: requested={app_id}, returned={verified_id}")
                return details
        except Exception as e:
            if attempt < max_retries - 1:
                delay = retry_delay * (2 ** attempt)  # Exponential backoff
                logger.warning(f"Attempt {attempt + 1} failed for app_id={app_id}: {e}. Retrying in {delay}s...")
                time.sleep(delay)
            else:
                logger.error(f"All {max_retries} attempts failed for app_id={app_id}: {e}")
    return None

def search_app_id(
    query: str,
    country: str = 'ke',
    lang: str = 'en',
    n_hits: int = 5,
    min_rating: float = 0.0,
    min_total_ratings: int = 0,
    sort_by: str = 'updated',  # 'updated' | 'downloads' | 'significance' | 'rating' | 'ratings'
    max_search_retries: int = 2,
) -> List[Dict]:
    """
    Search for apps on Google Play Store and return enriched details
    matching the frontend AppDetails shape.

    Returns a list of dicts with keys:
    app_id, icon_url, name, developer, rating, total_ratings, category,
    content_rating, version, size, installs, last_updated
    """
    results = []
    search_attempts = 0
    
    # Retry search if it fails or returns no results
    while search_attempts < max_search_retries and not results:
        try:
            search_attempts += 1
            logger.info(f"Search attempt {search_attempts} for query: {query}")
            results = search(query, country=country, lang=lang, n_hits=n_hits)
            if results:
                break
            elif search_attempts < max_search_retries:
                logger.warning(f"Search returned no results. Retrying in 1s...")
                time.sleep(1)
        except Exception as e:
            logger.error(f"Search attempt {search_attempts} failed: {e}")
            if search_attempts < max_search_retries:
                time.sleep(1)
            else:
                logger.error(f"All search attempts failed. Returning empty list.")
                return []
    
    if not results:
        logger.warning(f"No search results found for query: {query}")
        return []
    
    items: List[Dict] = []
    for app_summary in results:
        # Use robust app_id extraction
        app_id = get_app_id_robust(app_summary)
        # Guard: reject URLs and non-package-looking ids
        if app_id:
            if app_id.startswith(("http://", "https://")):
                # try to extract from URL if it's a Play link, otherwise drop
                app_id = extract_app_id_from_url(app_id)
            # basic sanity: must look like a package (contain at least one dot, no spaces, no scheme)
            if app_id and (" " in app_id or "/" in app_id or app_id.startswith(("http", "www")) or "." not in app_id):
                app_id = None
        
        # CRITICAL: If app_id is still missing, try fallback search
        if not app_id:
            title = app_summary.get('title') or app_summary.get('name', '')
            developer = app_summary.get('developer') or app_summary.get('developerId', '')
            
            logger.warning(
                "Could not extract app_id for: title=%r, developer=%r, available_keys=%s",
                title, developer, list(app_summary.keys())
            )
            
            # Log full summary for debugging (excluding large fields)
            logger.debug("Full app_summary (missing app_id): %s", 
                        {k: v for k, v in app_summary.items() 
                         if k not in ['screenshots', 'icon', 'description', 'summary']})
            
            # Try fallback search by title + developer
            if title and developer:
                logger.info(f"Attempting fallback search for '{title}' by '{developer}'")
                app_id = search_app_by_title_fallback(title, developer, country=country, lang=lang)
            
            # If still no app_id after all strategies, log error but keep the app
            if not app_id:
                logger.error(
                    f"CRITICAL: Failed to extract app_id after ALL strategies (including fallback search) for: "
                    f"title='{title}', developer='{developer}'. Available keys: {list(app_summary.keys())}. "
                    f"App will be returned without app_id - frontend can match by name+developer."
                )
        
        # Fetch full app details if we have an app_id
        details = {}
        if app_id:
            details = fetch_app_details_with_retry(app_id, lang=lang, country=country) or {}
            # If details fetch succeeded but app_id is different, use the one from details
            if details:
                details_app_id = details.get('appId') or details.get('app_id')
                if details_app_id and details_app_id != app_id:
                    logger.info(f"App ID corrected: {app_id} -> {details_app_id}")
                    app_id = details_app_id
                # Verify app_id is still present after fetching details
                if not app_id:
                    logger.warning(f"App ID lost after fetching details for: {app_summary.get('title')}. Will return without app_id.")

        # Build app details dictionary (app_id may be None if all extraction strategies failed)
        rating = details.get('score') or app_summary.get('score') or 0
        total_ratings = details.get('ratings') or details.get('reviews') or 0
        name = (details.get('title') or app_summary.get('title') or '')
        developer = (details.get('developer') or app_summary.get('developer') or '')
        # simple significance score: rating * log10(total_ratings+1) boosted by name/developer match
        boost = 0.0
        q = query.lower()
        if q and name.lower().startswith(q):
            boost += 2.0
        elif q and q in name.lower():
            boost += 1.0
        if q and q in developer.lower():
            boost += 0.5
        significance = float(rating) * math.log10(float(total_ratings) + 1.0) + boost

        # parse installs to int if possible
        installs_text = details.get('installs') or app_summary.get('installs') or ''
        installs_num = 0
        if isinstance(installs_text, str):
            digits = re.sub(r"[^0-9]", "", installs_text)
            installs_num = int(digits) if digits else 0

        # updated timestamp
        updated_raw = details.get('updated')
        updated_ts = 0
        if isinstance(updated_raw, (int, float)):
            updated_ts = int(updated_raw)
        elif isinstance(updated_raw, str):
            try:
                # try common formats
                updated_ts = int(datetime.fromisoformat(updated_raw).timestamp())
            except Exception:
                updated_ts = 0

        # Ensure app_id is None (not empty string) if missing
        if app_id and not app_id.strip():
            app_id = None
        
        items.append({
            'app_id': app_id,  # Can be None if extraction failed - frontend will handle via name+developer matching
            'icon_url': details.get('icon') or app_summary.get('icon'),
            'name': name,
            'developer': developer,
            'rating': rating,
            'total_ratings': total_ratings,
            'category': details.get('genre') or '',
            'content_rating': details.get('contentRating') or '',
            'version': details.get('version') or '',
            'size': details.get('size') or '',
            'installs': installs_text,
            'downloads': installs_num,
            'last_updated': updated_raw or '',
            'updated_ts': updated_ts,
            'significance': significance,
        })
    
    # filters
    filtered = [i for i in items if (i['rating'] or 0) >= min_rating and int(i['total_ratings'] or 0) >= min_total_ratings]
    # sorting
    if sort_by and sort_by.lower() not in ['default', 'original', 'none']:
        key_map = {
            'updated': lambda x: x.get('updated_ts', 0),
            'downloads': lambda x: x.get('downloads', 0),
            'significance': lambda x: x.get('significance', 0.0),
            'rating': lambda x: x.get('rating', 0.0),
            'ratings': lambda x: x.get('total_ratings', 0),
        }
        key_fn = key_map.get(sort_by, lambda x: x.get('significance', 0.0))
        filtered.sort(key=key_fn, reverse=True)
    # else: keep original order (default)
    # trim to n_hits
    return filtered[:n_hits]

def format_search_results(results: List[Dict]) -> str:
    """
    Format search results in a human-readable way
    """
    if not results:
        return "No apps found"
        
    output = []
    for i, item in enumerate(results, 1):
        output.append(f"{i}. {item.get('name')}")
        output.append(f"   Developer: {item.get('developer')}")
        output.append(f"   App ID: {item.get('app_id')}")
        output.append(f"   Installs: {item.get('installs')}")
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