"""
routers/exports.py — All /exports API routes.

Routes:
  POST /exports                       → Create a new export job
  GET  /exports/benchmark             → Run full benchmark across all formats
  GET  /exports/{exportId}/download   → Stream the export file

IMPORTANT: /exports/benchmark must be declared BEFORE /exports/{exportId}/...
so FastAPI does not mistake "benchmark" for a UUID path parameter.
"""
from __future__ import annotations

import logging
import uuid
from typing import List

from fastapi import APIRouter, HTTPException, status
from fastapi.responses import StreamingResponse

from exporters.factory import get_exporter
from models import (
    BenchmarkResponse,
    ColumnMapping,
    Compression,
    ExportFormat,
    ExportRequest,
    ExportResponse,
)
from services.export_service import (
    build_stream,
    create_job,
    get_job,
    run_benchmark,
)

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/exports", tags=["exports"])


# ---------------------------------------------------------------------------
# POST /exports — Create export job
# ---------------------------------------------------------------------------

@router.post(
    "",
    status_code=status.HTTP_201_CREATED,
    response_model=ExportResponse,
    summary="Create a new export job",
)
async def create_export(request: ExportRequest) -> ExportResponse:
    """
    Validates the export request, registers a new job, and returns its UUID.

    The *format* field selects the output serialisation format.
    The *columns* list controls which DB columns are included and their
    names in the output file.
    The optional *compression* field applies gzip to text-based formats.
    """
    try:
        return create_job(request)
    except Exception as exc:
        logger.error("Failed to create export job: %s", exc, exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to create export job.",
        ) from exc


# ---------------------------------------------------------------------------
# GET /exports/benchmark — Run performance benchmark
# ---------------------------------------------------------------------------

@router.get(
    "/benchmark",
    response_model=BenchmarkResponse,
    summary="Benchmark all export formats against the full dataset",
)
async def benchmark() -> BenchmarkResponse:
    """
    Exports the full 10 million row dataset in all four formats sequentially,
    measuring duration, file size, and peak memory for each.

     This endpoint may take several minutes on the first run.
    """
    # Use all table columns for the benchmark
    all_columns: List[ColumnMapping] = [
        ColumnMapping(source="id", target="id"),
        ColumnMapping(source="created_at", target="created_at"),
        ColumnMapping(source="name", target="name"),
        ColumnMapping(source="value", target="value"),
        ColumnMapping(source="metadata", target="metadata"),
    ]
    try:
        return await run_benchmark(all_columns)
    except Exception as exc:
        logger.error("Benchmark failed: %s", exc, exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Benchmark failed: {exc}",
        ) from exc


# ---------------------------------------------------------------------------
# GET /exports/{exportId}/download — Stream export file
# ---------------------------------------------------------------------------

@router.get(
    "/{export_id}/download",
    summary="Download the export file for a given job ID",
)
async def download_export(export_id: uuid.UUID) -> StreamingResponse:
    """
    Streams the export data for the specified job.

    Sets appropriate Content-Type and Content-Disposition headers.
    For gzip-compressed exports, adds Content-Encoding: gzip.
    """
    try:
        job = get_job(export_id)
    except KeyError:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Export job '{export_id}' not found.",
        )

    exporter = get_exporter(job.format.value)
    filename = f"export_{export_id}.{exporter.file_extension}"

    headers = {
        "Content-Disposition": f'attachment; filename="{filename}"',
    }

    if job.compression == Compression.gzip:
        headers["Content-Encoding"] = "gzip"
        filename = f"{filename}.gz"
        headers["Content-Disposition"] = f'attachment; filename="{filename}"'

    content_type = exporter.content_type

    return StreamingResponse(
        content=build_stream(job),
        media_type=content_type,
        headers=headers,
    )
