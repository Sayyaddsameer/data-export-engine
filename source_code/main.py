"""
main.py — FastAPI application entry point.

Manages the asyncpg connection pool lifecycle and mounts all routers.
"""
from __future__ import annotations

import logging

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from config import get_settings
from database import init_pool, close_pool
from routers.exports import router as exports_router

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
settings = get_settings()
logging.basicConfig(
    level=getattr(logging, settings.log_level.upper(), logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Application
# ---------------------------------------------------------------------------
app = FastAPI(
    title="Data Export Engine",
    description=(
        "High-performance, memory-efficient data export API that streams "
        "large PostgreSQL datasets into CSV, JSON, XML, and Parquet formats."
    ),
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------------------------------------------------------
# Startup / shutdown lifespan
# ---------------------------------------------------------------------------

@app.on_event("startup")
async def on_startup() -> None:
    logger.info("Application starting …")
    await init_pool()
    logger.info("Application ready.")


@app.on_event("shutdown")
async def on_shutdown() -> None:
    logger.info("Application shutting down …")
    await close_pool()


# ---------------------------------------------------------------------------
# Routers
# ---------------------------------------------------------------------------
app.include_router(exports_router)


# ---------------------------------------------------------------------------
# Health check
# ---------------------------------------------------------------------------

@app.get("/health", tags=["health"], summary="Liveness probe")
async def health() -> dict:
    return {"status": "ok"}
