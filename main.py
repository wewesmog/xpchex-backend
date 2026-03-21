# main.py - Simplified for human-in-the-loop conversation handling

from fastapi import FastAPI, APIRouter
import logging
from dotenv import load_dotenv
from app.routers import reviews
from app.routers import reviewAnalysis
from app.routers import issues_router
from app.routers import positives_router
from app.routers import actions_router
from app.routers import sentiments_router
from app.routers import app_search_router
from app.routers import general
from app.routers import file_upload_router
from app.routers import commentary_router
from app.shared_services.db import get_postgres_connection

# import CORS
from fastapi.middleware.cors import CORSMiddleware

# Configure Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)



# Load environment variables
load_dotenv()

# Import routers


# FastAPI App Instance


app = FastAPI()

origins = [
    "http://localhost:3000",
    "http://localhost:8000",
    "http://localhost:5173",
    "http://localhost:8081",
    "http://localhost:8082",
   
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(reviews.router)
app.include_router(reviewAnalysis.router)
app.include_router(issues_router.router)
app.include_router(positives_router.router)
app.include_router(actions_router.router)
app.include_router(sentiments_router.router)
app.include_router(app_search_router.router)
app.include_router(general.router)
app.include_router(file_upload_router.router)
app.include_router(commentary_router.router)
@app.get("/")
async def read_root():
    return {"message": "Reviews Service is running!"}

@app.get("/health")
async def health_check():
    """Health check endpoint for Docker healthchecks"""
    try:
        # Test database connection
        conn = get_postgres_connection()
        if conn:
            conn.close()
            return {"status": "healthy", "database": "connected"}
        return {"status": "unhealthy", "database": "disconnected"}
    except Exception as e:
        return {"status": "unhealthy", "error": str(e)}


if __name__ == "__main__":
    import uvicorn
    # Use import string so uvicorn can enable reload correctly
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)