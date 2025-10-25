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

@app.get("/")
async def read_root():
    return {"message": "Reviews Service is running!"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)