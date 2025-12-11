"""
Health check endpoint for Docker healthchecks
"""
from fastapi import APIRouter
from app.shared_services.db import get_postgres_connection

router = APIRouter()

@router.get("/health")
async def health_check():
    """Health check endpoint"""
    try:
        # Test database connection
        conn = get_postgres_connection()
        if conn:
            conn.close()
            return {"status": "healthy", "database": "connected"}
        return {"status": "unhealthy", "database": "disconnected"}
    except Exception as e:
        return {"status": "unhealthy", "error": str(e)}



