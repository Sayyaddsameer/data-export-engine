"""
database.py — Async PostgreSQL connection pool (asyncpg).

Provides:
- pool lifecycle helpers (init / close)
- stream_records() — async generator that yields row chunks via a
  server-side cursor so the full dataset is never loaded into RAM.
"""
from __future__ import annotations

import logging
from typing import AsyncGenerator, List, Sequence

import asyncpg

from config import get_settings

logger = logging.getLogger(__name__)

# Module-level pool; initialised during application startup.
_pool: asyncpg.Pool | None = None


# ---------------------------------------------------------------------------
# Pool lifecycle
# ---------------------------------------------------------------------------

async def init_pool() -> None:
    """Create the asyncpg connection pool. Called once at app startup."""
    global _pool
    settings = get_settings()
    _pool = await asyncpg.create_pool(
        dsn=settings.database_url,
        min_size=settings.db_pool_min_size,
        max_size=settings.db_pool_max_size,
        command_timeout=None,           # allow long-running export queries
    )
    logger.info(
        "DB pool ready (min=%d, max=%d)",
        settings.db_pool_min_size,
        settings.db_pool_max_size,
    )


async def close_pool() -> None:
    """Gracefully close the connection pool. Called on app shutdown."""
    global _pool
    if _pool:
        await _pool.close()
        _pool = None
        logger.info("DB pool closed.")


def get_pool() -> asyncpg.Pool:
    """Return the active connection pool (raises if not initialised)."""
    if _pool is None:
        raise RuntimeError("Database pool has not been initialised.")
    return _pool


# ---------------------------------------------------------------------------
# Streaming helper
# ---------------------------------------------------------------------------

async def stream_records(
    source_columns: Sequence[str],
    chunk_size: int | None = None,
) -> AsyncGenerator[List[asyncpg.Record], None]:
    """
    Async generator that yields lists of asyncpg.Record using a server-side
    cursor so memory usage is O(chunk_size), not O(total rows).

    :param source_columns: Column names to SELECT (validated safe list).
    :param chunk_size:      Rows per fetch; defaults to settings.db_chunk_size.
    :yields:                Lists of asyncpg.Record of length ≤ chunk_size.
    """
    settings = get_settings()
    if chunk_size is None:
        chunk_size = settings.db_chunk_size

    # Quote each identifier to handle any column name safely
    quoted_cols = ", ".join(f'"{col}"' for col in source_columns)
    query = f"SELECT {quoted_cols} FROM public.records ORDER BY id"  # noqa: S608

    pool = get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            # Declare a server-side cursor; fetch in fixed-size batches
            cur = await conn.cursor(query)
            while True:
                rows = await cur.fetch(chunk_size)
                if not rows:
                    break
                yield list(rows)


async def get_total_record_count() -> int:
    """Return the total number of rows in public.records."""
    pool = get_pool()
    async with pool.acquire() as conn:
        return await conn.fetchval("SELECT COUNT(*) FROM public.records")
