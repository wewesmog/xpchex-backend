# main.py - Simplified for human-in-the-loop conversation handling

from fastapi import FastAPI, APIRouter
import logging
from dotenv import load_dotenv

# Logfire must be configured before routers are imported so that OpenAI /
# psycopg2 patches are in place before any client is first used.
load_dotenv()
from app.shared_services.logfire_setup import configure as _configure_logfire  # noqa: E402
_configure_logfire(service_name="xpchex-api")

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
from app.routers import overview_router
from app.shared_services.db import non_pooled_connection
from app.shared_services.logger_setup import setup_logger

# import CORS
from fastapi.middleware.cors import CORSMiddleware

# Configure logging: reuse shared file + console handlers
_shared_logger = setup_logger("xpchex-api")
root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)
for handler in _shared_logger.handlers:
    has_same_file = (
        hasattr(handler, "baseFilename")
        and any(
            hasattr(existing, "baseFilename")
            and existing.baseFilename == handler.baseFilename
            for existing in root_logger.handlers
        )
    )
    has_same_type_stream = (
        not hasattr(handler, "baseFilename")
        and any(type(existing) is type(handler) for existing in root_logger.handlers)
    )
    if not has_same_file and not has_same_type_stream:
        root_logger.addHandler(handler)

logger = logging.getLogger(__name__)



# load_dotenv() already called above before logfire bootstrap

# Import routers


# FastAPI App Instance


app = FastAPI()

# Instrument all FastAPI routes — must be called after app is created.
# This adds request/response spans to every endpoint in every router.
try:
    import logfire
    logfire.instrument_fastapi(app)
except Exception:
    pass

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
app.include_router(overview_router.router)
@app.get("/")
async def read_root():
    return {"message": "Reviews Service is running!"}

@app.get("/health")
async def health_check():
    """Health check endpoint for Docker healthchecks"""
    try:
        # Avoid consuming pool slots for frequent health probes.
        with non_pooled_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1")
                cursor.fetchone()
        return {"status": "healthy", "database": "connected"}
    except Exception as e:
        return {"status": "unhealthy", "error": str(e)}


if __name__ == "__main__":
    import uvicorn
    # Use import string so uvicorn can enable reload correctly
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)