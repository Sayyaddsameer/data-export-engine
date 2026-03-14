"""
models.py — Pydantic request / response schemas.
"""
from __future__ import annotations

from enum import Enum
from typing import List, Optional
from uuid import UUID

from pydantic import BaseModel, Field, field_validator


# ---------------------------------------------------------------------------
# Enumerations
# ---------------------------------------------------------------------------

class ExportFormat(str, Enum):
    csv = "csv"
    json = "json"
    xml = "xml"
    parquet = "parquet"


class Compression(str, Enum):
    gzip = "gzip"


class JobStatus(str, Enum):
    pending = "pending"
    running = "running"
    completed = "completed"
    failed = "failed"


# ---------------------------------------------------------------------------
# Request schemas
# ---------------------------------------------------------------------------

class ColumnMapping(BaseModel):
    """Maps a source database column name to a target export column name."""
    source: str = Field(..., description="Column name as it appears in the database")
    target: str = Field(..., description="Column name as it will appear in the export file")


class ExportRequest(BaseModel):
    format: ExportFormat = Field(..., description="Output format: csv | json | xml | parquet")
    columns: List[ColumnMapping] = Field(
        ...,
        min_length=1,
        description="Column selection and renaming mapping",
    )
    compression: Optional[Compression] = Field(
        None,
        description="Optional compression (gzip). Not applicable to parquet.",
    )

    @field_validator("columns")
    @classmethod
    def unique_sources(cls, v: List[ColumnMapping]) -> List[ColumnMapping]:
        sources = [c.source for c in v]
        if len(sources) != len(set(sources)):
            raise ValueError("Duplicate source column names are not allowed.")
        return v


# ---------------------------------------------------------------------------
# Response schemas
# ---------------------------------------------------------------------------

class ExportResponse(BaseModel):
    exportId: UUID
    status: JobStatus


class BenchmarkResult(BaseModel):
    format: ExportFormat
    durationSeconds: float
    fileSizeBytes: int
    peakMemoryMB: float


class BenchmarkResponse(BaseModel):
    datasetRowCount: int
    results: List[BenchmarkResult]
