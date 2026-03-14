"""
tests/test_api.py — Integration tests for the /exports API endpoints.

Uses FastAPI's TestClient with a mocked database layer so no real
PostgreSQL connection is needed during CI.
"""
from __future__ import annotations

import csv
import gzip
import io
import json
import uuid
import xml.etree.ElementTree as ET
from typing import AsyncGenerator, List
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi.testclient import TestClient

# ---------------------------------------------------------------------------
# Synthetic data shared across tests
# ---------------------------------------------------------------------------

SOURCE_COLS = ["id", "created_at", "name", "value", "metadata"]
FAKE_ROWS = [
    {
        "id": i,
        "created_at": "2024-01-01T00:00:00",
        "name": f"Record_{i}",
        "value": float(i) * 1.5,
        "metadata": {"index": i, "city": f"City_{i}"},
    }
    for i in range(1, 6)
]


async def _fake_stream(*_args, **_kwargs) -> AsyncGenerator[List[dict], None]:
    yield FAKE_ROWS  # type: ignore[misc]


# ---------------------------------------------------------------------------
# Fixture: FastAPI TestClient with mocked DB pool
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def mock_db(monkeypatch):
    """Patch asyncpg pool so tests run without a real database."""
    monkeypatch.setattr("database.stream_records", _fake_stream)
    monkeypatch.setattr(
        "database.get_total_record_count",
        AsyncMock(return_value=10_000_000),
    )
    # Prevent pool init from touching the network
    monkeypatch.setattr("database.init_pool", AsyncMock())
    monkeypatch.setattr("database.close_pool", AsyncMock())


@pytest.fixture
def client():
    # Import here so monkeypatching above takes effect before app loads db
    from main import app
    with TestClient(app) as c:
        yield c


# ---------------------------------------------------------------------------
# POST /exports
# ---------------------------------------------------------------------------

VALID_PAYLOAD = {
    "format": "csv",
    "columns": [
        {"source": "id", "target": "ID"},
        {"source": "name", "target": "Name"},
        {"source": "value", "target": "Value"},
        {"source": "metadata", "target": "Metadata"},
    ],
}


def test_create_export_returns_201(client):
    resp = client.post("/exports", json=VALID_PAYLOAD)
    assert resp.status_code == 201


def test_create_export_response_schema(client):
    resp = client.post("/exports", json=VALID_PAYLOAD)
    body = resp.json()
    assert "exportId" in body
    assert "status" in body
    # exportId must be a valid UUID
    uuid.UUID(body["exportId"])


def test_create_export_all_formats(client):
    for fmt in ("csv", "json", "xml", "parquet"):
        payload = {**VALID_PAYLOAD, "format": fmt}
        resp = client.post("/exports", json=payload)
        assert resp.status_code == 201, f"Failed for format: {fmt}"


def test_create_export_invalid_format(client):
    payload = {**VALID_PAYLOAD, "format": "avro"}
    resp = client.post("/exports", json=payload)
    assert resp.status_code == 422     # Pydantic validation error


def test_create_export_missing_columns(client):
    payload = {"format": "csv", "columns": []}
    resp = client.post("/exports", json=payload)
    assert resp.status_code == 422


# ---------------------------------------------------------------------------
# GET /exports/{exportId}/download — CSV
# ---------------------------------------------------------------------------

def test_csv_download_content_type(client):
    job_id = client.post("/exports", json=VALID_PAYLOAD).json()["exportId"]
    resp = client.get(f"/exports/{job_id}/download")
    assert resp.status_code == 200
    assert "text/csv" in resp.headers["content-type"]


def test_csv_download_content_disposition(client):
    job_id = client.post("/exports", json=VALID_PAYLOAD).json()["exportId"]
    resp = client.get(f"/exports/{job_id}/download")
    assert "attachment" in resp.headers["content-disposition"]


def test_csv_download_valid_content(client):
    job_id = client.post("/exports", json=VALID_PAYLOAD).json()["exportId"]
    resp = client.get(f"/exports/{job_id}/download")
    rows = list(csv.reader(io.StringIO(resp.text)))
    assert rows[0] == ["ID", "Name", "Value", "Metadata"]
    assert len(rows) == 1 + len(FAKE_ROWS)


# ---------------------------------------------------------------------------
# GET /exports/{exportId}/download — JSON
# ---------------------------------------------------------------------------

def test_json_download_content_type(client):
    payload = {**VALID_PAYLOAD, "format": "json"}
    job_id = client.post("/exports", json=payload).json()["exportId"]
    resp = client.get(f"/exports/{job_id}/download")
    assert resp.status_code == 200
    assert "application/json" in resp.headers["content-type"]


def test_json_download_valid_array(client):
    payload = {**VALID_PAYLOAD, "format": "json"}
    job_id = client.post("/exports", json=payload).json()["exportId"]
    resp = client.get(f"/exports/{job_id}/download")
    data = json.loads(resp.text)
    assert isinstance(data, list)
    assert len(data) == len(FAKE_ROWS)
    assert data[0]["Name"] == "Record_1"


# ---------------------------------------------------------------------------
# GET /exports/{exportId}/download — XML
# ---------------------------------------------------------------------------

def test_xml_download_content_type(client):
    payload = {**VALID_PAYLOAD, "format": "xml"}
    job_id = client.post("/exports", json=payload).json()["exportId"]
    resp = client.get(f"/exports/{job_id}/download")
    assert resp.status_code == 200
    assert "application/xml" in resp.headers["content-type"]


def test_xml_download_valid_structure(client):
    payload = {**VALID_PAYLOAD, "format": "xml"}
    job_id = client.post("/exports", json=payload).json()["exportId"]
    resp = client.get(f"/exports/{job_id}/download")
    root = ET.fromstring(resp.text)
    assert root.tag == "records"
    records = root.findall("record")
    assert len(records) == len(FAKE_ROWS)


# ---------------------------------------------------------------------------
# GET /exports/{exportId}/download — Parquet
# ---------------------------------------------------------------------------

def test_parquet_download_content_type(client):
    payload = {**VALID_PAYLOAD, "format": "parquet"}
    job_id = client.post("/exports", json=payload).json()["exportId"]
    resp = client.get(f"/exports/{job_id}/download")
    assert resp.status_code == 200
    assert "parquet" in resp.headers["content-type"] or \
           "octet-stream" in resp.headers["content-type"]


# ---------------------------------------------------------------------------
# Gzip compression
# ---------------------------------------------------------------------------

def test_csv_gzip_compression_header(client):
    payload = {**VALID_PAYLOAD, "compression": "gzip"}
    job_id = client.post("/exports", json=payload).json()["exportId"]
    resp = client.get(f"/exports/{job_id}/download", headers={"Accept-Encoding": "identity"})
    assert resp.status_code == 200
    assert resp.headers.get("content-encoding") == "gzip"


def test_csv_gzip_decompressible(client):
    payload = {**VALID_PAYLOAD, "compression": "gzip"}
    job_id = client.post("/exports", json=payload).json()["exportId"]
    resp = client.get(f"/exports/{job_id}/download", headers={"Accept-Encoding": "identity"})
    raw = gzip.decompress(resp.content)
    rows = list(csv.reader(io.StringIO(raw.decode("utf-8"))))
    assert rows[0] == ["ID", "Name", "Value", "Metadata"]


# ---------------------------------------------------------------------------
# Benchmark endpoint
# ---------------------------------------------------------------------------

def test_benchmark_status_200(client):
    resp = client.get("/exports/benchmark")
    assert resp.status_code == 200


def test_benchmark_response_schema(client):
    resp = client.get("/exports/benchmark")
    body = resp.json()
    assert body["datasetRowCount"] == 10_000_000
    assert "results" in body
    assert len(body["results"]) == 4  # one per format


def test_benchmark_all_formats_present(client):
    resp = client.get("/exports/benchmark")
    formats = {r["format"] for r in resp.json()["results"]}
    assert formats == {"csv", "json", "xml", "parquet"}


def test_benchmark_numeric_values_positive(client):
    resp = client.get("/exports/benchmark")
    for result in resp.json()["results"]:
        assert result["durationSeconds"] >= 0
        assert result["fileSizeBytes"] > 0


# ---------------------------------------------------------------------------
# 404 on unknown job
# ---------------------------------------------------------------------------

def test_download_unknown_job_returns_404(client):
    fake_id = str(uuid.uuid4())
    resp = client.get(f"/exports/{fake_id}/download")
    assert resp.status_code == 404
