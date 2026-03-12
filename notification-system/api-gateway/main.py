"""
API Gateway Service - Entry point for all notification requests. Validates, authenticates, routes messages to correct queues, and tracks notification status.
"""

import uuid
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from app.core.config import settings
from app.core.database import create_tables
from app.core.rabbitmq import rabbitmq_client
from app.core.redis_client import redis_client
from app.api.v1.router import api_router

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting API Gateway...")
    await create_tables()
    await rabbitmq_client.connect()
    await rabbitmq_client.setup_exchange_and_queues()
    await redis_client.connect()
    logger.info("API Gateway started successfully.")
    yield
    logger.info("Shutting down API Gateway...")
    await rabbitmq_client.disconnect()
    await redis_client.disconnect()


app = FastAPI(
    title="Notification System - API Gateway",
    description="Entry point for all notification requests. Routes to email/push queues."
    version="1.0.0",
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.middleware("http")
async def correlation_id_middleware(request: Request, call_next):
    correlation_id = request.headers.get("X-Correlation-ID", str(uuid.uuid4()))
    request.state.correlation_id = correlation_id
    response = await call_next(request)
    response.headers["X-Correlation-ID"] = correlation_id
    return response

@app.middleware("http")
async def log_requests(request: Request, call_next):
    logger.info(f" {request.method} {request.url.path} | corr_id={getattr(request.state, 'correlation_id', '-')}")
    response = await call_next(request)
    logger.info(f" {response.status_code} {request.url.path}")
    return response

@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    logger.error(f"Unhandled exception: {exc}", exc_info=True)
    return JSONResponse(
        status_code=500,
        content={"success": False, "error": str(exc), "message": "Internal server error",
        "data": None, "meta": {"total": 0, "limit": 0, "page": 1, "total_pages": 0,
                                "has_next": False, "has_previous": False}},
    )

@app.get("/health", tags=["Health"])
async def health_check():
    rabbit_ok = rabbitmq_client.is_connected
    redis_ok = await redis_client.ping()
    status = "healthy" if (rabbit_ok and redis_ok) else "degraded"
    return {
        "service": "api-gateway",
        "status": status,
        "checks": {"rabbitmq": "up" if rabbit_ok else "down", "redis": "up" if redis_ok else "down"},
    }

app.include_router(api_router, prefix="/api/v1")
