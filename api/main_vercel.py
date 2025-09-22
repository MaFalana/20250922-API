import os
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
from datetime import datetime, timezone

from routes import health, photos, exports

# Load environment variables
load_dotenv('.env')

# Create FastAPI app without lifespan (not supported in Vercel)
app = FastAPI(
    title="Photo Log Map API",
    description="Web API for managing photos uploaded by HWC employees.",
    version="1.0.0"
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(health.router)
app.include_router(photos.router)
app.include_router(exports.router)

@app.get('/')
async def root():
    data = {
        "Message": "Connected to HWC Engineering Photo Log Map API",
        "Framework": "FastApi",
        "Version": "1.0.0",
        "Status": "Running",
        "Timestamp": datetime.now(timezone.utc)
    }
    return data

@app.get("/health")
async def health_check():
    return {"status": "healthy", "timestamp": datetime.now(timezone.utc)}