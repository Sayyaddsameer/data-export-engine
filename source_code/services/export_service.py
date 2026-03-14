"""
services/export_service.py — Business logic for export jobs.

Responsibilities:
- Maintain an in-memory registry of export jobs (UUID → ExportJob).
- Create new jobs and store their parameters.
- Produce the streaming response generator for a given job, applying
  optional gzip compression transparently.
"""
from __future__ import annotations

import asyncio
import gzip
import io
import logging
import resource
import time
import uuid
from dataclasses import dataclass
from typing import AsyncGenerator, Dict, List, Optional

from database import stream_records, get_total_record_count
from exporters.factory import get_exporter, EXPORTER_REGISTRY
from models import (
    BenchmarkResponse,
    BenchmarkResult,
    ColumnMapping,
    Compression,
    ExportFormat,
    ExportRequest,
    ExportResponse,
    JobStatus,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Internal job dataclass
# ---------------------------------------------------------------------------

@dataclass
class ExportJob:
    job_id: uuid.UUID
    format: ExportFormat
    columns: List[ColumnMapping]
    compression: Optional[Compression]
    status: JobStatus = JobStatus.pending


# ---------------------------------------------------------------------------
# In-memory job store
# ---------------------------------------------------------------------------

_jobs: Dict[uuid.UUID, ExportJob] = {}


def create_job(request: ExportRequest) -> ExportResponse:
    """Create a new export job and return its ID and initial status."""
    job_id = uuid.uuid4()
    job = ExportJob(
        job_id=job_id,
        format=request.format,
        columns=request.columns,
        compression=request.compression,
    )
    _jobs[job_id] = job
    logger.info("Created export job %s (format=%s)", job_id, request.format)
    return ExportResponse(exportId=job_id, status=JobStatus.pending)


def get_job(job_id: uuid.UUID) -> ExportJob:
    """Retrieve an existing job. Raises KeyError if not found."""
    job = _jobs.get(job_id)
    if job is None:
        raise KeyError(f"Export job {job_id} not found.")
    return job


async def build_stream(job: ExportJob) -> AsyncGenerator[bytes, None]:
    """
    Return an async generator that streams the export data for *job*.
    Applies gzip compression when job.compression == Compression.gzip.
    """
    job.status = JobStatus.running
    exporter = get_exporter(job.format.value)

    source_cols = [c.source for c in job.columns]
    target_cols = [c.target for c in job.columns]

    try:
        if job.compression == Compression.gzip:
            async for chunk in _gzip_wrap(
                exporter.stream(stream_records(source_cols), source_cols, target_cols)
            ):
                yield chunk
        else:
            async for chunk in exporter.stream(
                stream_records(source_cols), source_cols, target_cols
            ):
                yield chunk

        job.status = JobStatus.completed
        logger.info("Export job %s completed.", job.job_id)

    except Exception as exc:  # noqa: BLE001
        job.status = JobStatus.failed
        logger.error("Export job %s failed: %s", job.job_id, exc, exc_info=True)
        raise


async def _gzip_wrap(
    source: AsyncGenerator[bytes, None],
) -> AsyncGenerator[bytes, None]:
    """
    Transparently gzip-compress an async byte stream without buffering
    the whole content — uses a temporary BytesIO per chunk.
    """
    gz_buf = io.BytesIO()
    gz_file = gzip.GzipFile(fileobj=gz_buf, mode="wb", compresslevel=6)

    async for chunk in source:
        gz_file.write(chunk)
        gz_file.flush()
        data = gz_buf.getvalue()
        if data:
            yield data
            gz_buf.seek(0)
            gz_buf.truncate(0)

    gz_file.close()
    leftover = gz_buf.getvalue()
    if leftover:
        yield leftover
    gz_buf.close()


# ---------------------------------------------------------------------------
# Benchmark helper
# ---------------------------------------------------------------------------

def _get_peak_memory_mb() -> float:
    """
    Return peak RSS memory usage in MB.
    On Linux (inside Docker) resource.RUSAGE_SELF is available.
    Falls back to 0.0 on platforms where it is unavailable.
    """
    try:
        usage = resource.getrusage(resource.RUSAGE_SELF)
        # On Linux, ru_maxrss is in kilobytes
        return usage.ru_maxrss / 1024.0
    except Exception:  # noqa: BLE001
        return 0.0


async def run_benchmark(
    all_columns: List[ColumnMapping],
) -> BenchmarkResponse:
    """
    Stream the full dataset for each registered format, measuring
    duration, output size, and peak memory usage.

    :param all_columns: Column mapping to use for all benchmark exports.
    :returns: BenchmarkResponse with one result per format.
    """
    total_rows = await get_total_record_count()
    results: List[BenchmarkResult] = []

    source_cols = [c.source for c in all_columns]
    target_cols = [c.target for c in all_columns]

    for fmt_name in EXPORTER_REGISTRY.keys():
        logger.info("Benchmark: starting format=%s", fmt_name)
        exporter = get_exporter(fmt_name)

        t_start = time.perf_counter()
        size_bytes = 0

        async for chunk in exporter.stream(
            stream_records(source_cols), source_cols, target_cols
        ):
            size_bytes += len(chunk)
            # Yield control so the event loop stays responsive
            await asyncio.sleep(0)

        duration = time.perf_counter() - t_start
        peak_mem = _get_peak_memory_mb()

        results.append(
            BenchmarkResult(
                format=ExportFormat(fmt_name),
                durationSeconds=round(duration, 3),
                fileSizeBytes=size_bytes,
                peakMemoryMB=round(peak_mem, 2),
            )
        )
        logger.info(
            "Benchmark: format=%s done in %.1fs, size=%d bytes, mem=%.1fMB",
            fmt_name, duration, size_bytes, peak_mem,
        )

    return BenchmarkResponse(datasetRowCount=total_rows, results=results)
