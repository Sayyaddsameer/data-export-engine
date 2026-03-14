"""
tests/conftest.py — Shared pytest fixtures for the Data Export Engine.
"""
from __future__ import annotations

import asyncio
import io
from typing import AsyncGenerator, List
from unittest.mock import AsyncMock, patch

import pytest
import asyncpg


# ---------------------------------------------------------------------------
# Synthetic dataset (3 rows of all types)
# ---------------------------------------------------------------------------

SAMPLE_COLUMNS = ["id", "created_at", "name", "value", "metadata"]

SAMPLE_ROWS = [
    {
        "id": 1,
        "created_at": "2024-01-01T00:00:00+00:00",
        "name": "Record_1",
        "value": 12345.6789,
        "metadata": {
            "index": 1,
            "category": "alpha",
            "active": True,
            "tags": ["tag_1", "tag_1"],
            "address": {"city": "City_1", "zip": "00001", "country": "US"},
        },
    },
    {
        "id": 2,
        "created_at": "2024-06-15T12:00:00+00:00",
        "name": "Record_2",
        "value": 99.0001,
        "metadata": {
            "index": 2,
            "category": "beta",
            "active": False,
            "tags": ["tag_2", "tag_2"],
            "address": {"city": "City_2", "zip": "00002", "country": "UK"},
        },
    },
    {
        "id": 3,
        "created_at": "2024-12-31T23:59:59+00:00",
        "name": "Record_3",
        "value": 0.0001,
        "metadata": {
            "index": 3,
            "category": "gamma",
            "active": True,
            "tags": ["tag_3", "tag_3"],
            "address": {"city": "City_3", "zip": "00003", "country": "DE"},
        },
    },
]


async def _mock_stream(*_args, **_kwargs) -> AsyncGenerator[List[dict], None]:
    """Async generator yielding a single batch of 3 fake records."""
    yield SAMPLE_ROWS  # type: ignore[misc]


@pytest.fixture
def sample_source_columns() -> List[str]:
    return list(SAMPLE_COLUMNS)


@pytest.fixture
def sample_target_columns() -> List[str]:
    return [c.upper() for c in SAMPLE_COLUMNS]
