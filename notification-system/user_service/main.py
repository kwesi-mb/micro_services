"""User Service - manages user data, perferences, auth, and push tokens."""
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from app.core.database import create_tables
from app.core.redis_client import redis_client
from app.api.v1.router import api_router

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting User Service...")
    await create_tables()
    await redis_client.connect()
    logger.info("User Service started.")
    yield
    await redis_client.disconnect()

app = FastAPI(
    title="Notification System - User Service",
    description="Manages user contact info, push tokens, preferences, and authentication.",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    logger.error(f"Unhandled exception: {exc}", exc_info=True)
    return JSONResponse(
        status_code=500,
        content={"success": False, "error": str(exc), "message":"Internal server error",
                "data": None, "meta": {"total": 0, "limit": 0, "page": 1, "total_pages": 0,
                                        "has_next": False, "has_previous": False}},
    )

@app.get("/health", tags=["Health"])
async  def health_check():
    redis_ok = await redis_client.ping()
    return {
        "service": "user-service",
        "status": "healthy" if redis_ok else "degraded",
        "checks": {"redis": "up" if redis_ok else "down"}
    }

app.include_router(api_router, prefix="/api/v1")