from fastapi import APIRouter, Query, HTTPException
from typing import Optional
from app.google_reviews.app_search import search_app_id
from app.google_reviews.app_details_scraper import AppDetailsScraper
from app.shared_services.db import get_postgres_connection, pooled_connection
from psycopg2.extras import RealDictCursor
import logging

router = APIRouter(prefix="/apps", tags=["apps"])
logger = logging.getLogger(__name__)


@router.get("/search")
def search_apps(
    query: str = Query(..., min_length=2, description="Search term, e.g. 'kcb mobile'"),
    country: str = Query("ke", min_length=2, max_length=2, description="ISO country code"),
    lang: str = Query("en", min_length=2, max_length=5, description="Language code"),
    limit: int = Query(6, ge=1, le=24, description="Max results to return"),
    offset: int = Query(0, ge=0, description="Offset for simulated pagination"),
    min_rating: float = Query(0.0, ge=0.0, le=5.0, description="Minimum average rating"),
    min_total_ratings: int = Query(0, ge=0, description="Minimum ratings count"),
    sort_by: str = Query("default", description="default|updated|downloads|significance|rating|ratings"),
):
    """
    Search Google Play for apps matching the query.
    Returns basic app details including icon URL for logos.
    """
    # Overfetch up to offset+limit to simulate pagination without offset support downstream
    overfetch = offset + limit
    if overfetch > 50:
        overfetch = 50

    items_full = search_app_id(
        query=query,
        country=country,
        lang=lang,
        n_hits=overfetch,
        min_rating=min_rating,
        min_total_ratings=min_total_ratings,
        sort_by=sort_by,
    )
    items = items_full[offset:offset+limit]
    return {"query": query, "count": len(items_full), "items": items}



@router.get("/exists")
def check_apps_exist(
    ids: Optional[str] = Query(None, description="Comma-separated app_ids"),
    names: Optional[str] = Query(None, description="Comma-separated app names (for fallback)"),
    developers: Optional[str] = Query(None, description="Comma-separated developers (for fallback, must match names order)")
):
    """
    Check which app_ids exist in app_details_history.
    Supports both app_id matching and name+developer fallback matching.
    
    Returns: { existing_ids: [app_id, ...], matched_by_name: {name: app_id, ...} }
    """
    try:
        existing_ids_set = set()
        matched_by_name = {}
        
        with pooled_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                # Check by app_id first - get latest record per app using inserted_on
                if ids:
                    id_list = [i.strip() for i in ids.split(",") if i.strip()]
                    if id_list:
                        query_sql = (
                            """
                            SELECT DISTINCT ON (app_id) app_id
                            FROM app_details_history
                            WHERE app_id = ANY(%s)
                            ORDER BY app_id, inserted_on DESC
                            """
                        )
                        try:
                            logged_sql = cur.mogrify(query_sql, (id_list,)).decode()
                            logger.info("/apps/exists app_id query: %s", logged_sql)
                        except Exception:
                            logger.info("/apps/exists app_id params: %s", id_list)
                        cur.execute(query_sql, (id_list,))
                        rows = cur.fetchall()
                        existing_ids_set.update(r["app_id"] for r in rows if r["app_id"])
                
                # Fallback: match by name + developer for apps without app_id
                if names and developers:
                    name_list = [n.strip() for n in names.split(",") if n.strip()]
                    dev_list = [d.strip() for d in developers.split(",") if d.strip()]
                    
                    if name_list and dev_list and len(name_list) == len(dev_list):
                        # Match each name+developer pair
                        for name, developer in zip(name_list, dev_list):
                            if not name or not developer:
                                continue
                            # Build a developer_id-like candidate from provided developer
                            # Table stores developer_id as "KCB+BANK+GROUP" format (spaces to +, uppercase)
                            dev_id_candidate = developer.replace(" ", "+").upper()
                            fallback_sql = (
                                """
                                SELECT DISTINCT ON (app_id) 
                                    app_id, title, developer_id, app_updated_at
                                FROM app_details_history
                                WHERE LOWER(TRIM(title)) = LOWER(TRIM(%s))
                                  AND (
                                       LOWER(TRIM(developer_id)) = LOWER(TRIM(%s))
                                       OR LOWER(REPLACE(TRIM(developer_id), '+', ' ')) = LOWER(TRIM(%s))
                                  )
                                ORDER BY app_id, inserted_on DESC
                                LIMIT 1
                                """
                            )
                            try:
                                logged_sql = cur.mogrify(fallback_sql, (name, dev_id_candidate, developer)).decode()
                                logger.info("/apps/exists fallback query: %s", logged_sql)
                            except Exception:
                                logger.info("/apps/exists fallback params: name=%s, dev_id_candidate=%s, developer=%s", name, dev_id_candidate, developer)
                            cur.execute(fallback_sql, (name, dev_id_candidate, developer))
                            row = cur.fetchone()
                            if row and row["app_id"]:
                                existing_ids_set.add(row["app_id"])
                                matched_by_name[name] = row["app_id"]
        
        return {
            "existing_ids": list(existing_ids_set),
            "matched_by_name": matched_by_name
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"exists check failed: {e}")


@router.get("/featured")
def get_featured_apps(
    limit: int = Query(6, ge=1, le=50, description="Max items per page (default: 6, max: 50)"),
    offset: int = Query(0, ge=0, description="Offset for pagination")
):
    """
    Get featured apps from app_details_history with simple pagination.
    - limit: up to 4 items per page
    - offset: starting offset
    Returns latest app details per app_id ordered by app_updated_at desc.
    """
    try:
        with pooled_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                # Total distinct apps for pagination metadata
                cur.execute("""
                    SELECT COUNT(DISTINCT app_id) AS total_apps
                    FROM app_details_history
                    WHERE app_id IS NOT NULL
                """)
                total_row = cur.fetchone()
                total_count = int(total_row["total_apps"] or 0) if total_row else 0

                # Get unique apps ordered by most recent inserted_on (latest record per app)
                cur.execute("""
                    WITH ranked_apps AS (
                        SELECT DISTINCT ON (app_id)
                            app_id,
                            title,
                            developer_id,
                            score,
                            ratings_count,
                            genre,
                            content_rating,
                            size,
                            installs,
                            app_updated_at,
                            icon_url,
                            version
                        FROM app_details_history
                        WHERE app_id IS NOT NULL
                        ORDER BY app_id, inserted_on DESC
                    )
                    SELECT 
                        app_id,
                        title AS name,
                        developer_id AS developer,
                        score AS rating,
                        ratings_count AS total_ratings,
                        genre AS category,
                        content_rating,
                        COALESCE(version, '') AS version,
                        COALESCE(size, '') AS size,
                        COALESCE(installs, '') AS installs,
                        COALESCE(app_updated_at::text, '') AS last_updated,
                        COALESCE(icon_url, '') AS icon_url
                    FROM ranked_apps
                    ORDER BY app_updated_at DESC NULLS LAST
                    LIMIT %s OFFSET %s
                """, (limit, offset))
                
                rows = cur.fetchall()
                
                items = []
                for row in rows:
                    items.append({
                        'app_id': row.get('app_id'),
                        'name': row.get('name') or '',
                        'developer': row.get('developer') or '',
                        'rating': float(row.get('rating') or 0),
                        'total_ratings': int(row.get('total_ratings') or 0),
                        'category': row.get('category') or '',
                        'content_rating': row.get('content_rating') or '',
                        'version': row.get('version') or '',
                        'size': row.get('size') or '',
                        'installs': row.get('installs') or '',
                        'last_updated': row.get('last_updated') or '',
                        'icon_url': row.get('icon_url') or '',
                    })
                
                return {
                    "status": "success",
                    "count": total_count,
                    "items": items
                }
    except Exception as e:
        logger.error(f"Error getting featured apps: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error getting featured apps: {str(e)}")


@router.get("/by-org/{org_slug}")
def get_apps_by_org(
    org_slug: str,
    limit: int = Query(50, ge=1, le=200, description="Max items per page"),
    offset: int = Query(0, ge=0, description="Offset for pagination"),
):
    """
    Get registered apps for a single organization slug.
    Uses organizations/apps registry, then enriches with latest history snapshot when available.
    """
    try:
        with pooled_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT COUNT(*) AS total_apps
                    FROM apps a
                    INNER JOIN organizations o ON o.id = a.organization_id
                    WHERE o.slug = %s
                      AND o.active = TRUE
                      AND a.active = TRUE
                    """,
                    (org_slug,),
                )
                total_row = cur.fetchone()
                total_count = int(total_row["total_apps"] or 0) if total_row else 0

                cur.execute(
                    """
                    WITH org_apps AS (
                        SELECT
                            a.app_id,
                            a.display_name
                        FROM apps a
                        INNER JOIN organizations o ON o.id = a.organization_id
                        WHERE o.slug = %s
                          AND o.active = TRUE
                          AND a.active = TRUE
                    ),
                    latest_history AS (
                        SELECT DISTINCT ON (h.app_id)
                            h.app_id,
                            h.title,
                            h.icon_url,
                            h.score,
                            h.ratings_count,
                            h.app_updated_at
                        FROM app_details_history h
                        INNER JOIN org_apps oa ON oa.app_id = h.app_id
                        ORDER BY h.app_id, h.inserted_on DESC
                    )
                    SELECT
                        oa.app_id,
                        COALESCE(NULLIF(oa.display_name, ''), NULLIF(lh.title, ''), oa.app_id) AS name,
                        COALESCE(lh.icon_url, '') AS icon_url,
                        COALESCE(lh.score, 0) AS rating,
                        COALESCE(lh.ratings_count, 0) AS total_ratings,
                        COALESCE(lh.app_updated_at::text, '') AS last_updated
                    FROM org_apps oa
                    LEFT JOIN latest_history lh ON lh.app_id = oa.app_id
                    ORDER BY name ASC
                    LIMIT %s OFFSET %s
                    """,
                    (org_slug, limit, offset),
                )
                rows = cur.fetchall() or []

                items = []
                for row in rows:
                    items.append(
                        {
                            "app_id": row.get("app_id"),
                            "name": row.get("name") or "",
                            "icon_url": row.get("icon_url") or "",
                            "rating": float(row.get("rating") or 0),
                            "total_ratings": int(row.get("total_ratings") or 0),
                            "last_updated": row.get("last_updated") or "",
                        }
                    )

                return {"status": "success", "count": total_count, "items": items}
    except Exception as e:
        logger.error("Error getting org apps for slug=%s: %s", org_slug, str(e), exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error getting org apps: {e}")


@router.get("/{app_id}")
def get_app_details(app_id: str):
    """
    Get app details by app_id from app_details_history.
    Returns the latest record for the app (based on inserted_on).
    """
    try:
        with pooled_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                # Get latest record for this app_id
                cur.execute("""
                    SELECT DISTINCT ON (app_id)
                        app_id,
                        title,
                        developer_id,
                        score,
                        ratings_count,
                        genre,
                        content_rating,
                        size,
                        installs,
                        app_updated_at,
                        icon_url,
                        version,
                        description,
                        summary,
                        price,
                        price_currency,
                        minimum_android,
                        developer_email,
                        developer_website,
                        developer_address,
                        privacy_policy,
                        genre_id,
                        content_rating_description
                    FROM app_details_history
                    WHERE app_id = %s
                    ORDER BY app_id, inserted_on DESC
                    LIMIT 1
                """, (app_id,))
                
                row = cur.fetchone()
                
                if not row:
                    raise HTTPException(status_code=404, detail=f"App with app_id '{app_id}' not found")
                
                return {
                    "status": "success",
                    "app_id": row.get('app_id'),
                    "name": row.get('title') or '',
                    "developer": row.get('developer_id') or '',
                    "rating": float(row.get('score') or 0),
                    "total_ratings": int(row.get('ratings_count') or 0),
                    "category": row.get('genre') or '',
                    "content_rating": row.get('content_rating') or '',
                    "version": row.get('version') or '',
                    "size": row.get('size') or '',
                    "installs": row.get('installs') or '',
                    "last_updated": row.get('app_updated_at').isoformat() if row.get('app_updated_at') else '',
                    "icon_url": row.get('icon_url') or '',
                    "description": row.get('description') or '',
                    "summary": row.get('summary') or '',
                    "price": row.get('price') or '',
                    "price_currency": row.get('price_currency') or '',
                    "minimum_android": row.get('minimum_android') or '',
                    "developer_email": row.get('developer_email') or '',
                    "developer_website": row.get('developer_website') or '',
                    "developer_address": row.get('developer_address') or '',
                    "privacy_policy": row.get('privacy_policy') or '',
                    "genre_id": row.get('genre_id') or '',
                    "content_rating_description": row.get('content_rating_description') or '',
                }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting app details: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error getting app details: {str(e)}")

