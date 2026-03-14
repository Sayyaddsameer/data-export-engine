"""
tests/test_exporters.py — Unit tests for each format exporter.

These tests run entirely in-process with a synthetic 3-row dataset; no
real database connection is required.
"""
from __future__ import annotations

import csv
import gzip
import io
import json
import xml.etree.ElementTree as ET
from typing import AsyncGenerator, List

import pytest
import pyarrow.parquet as pq

from exporters.csv_exporter import CsvExporter
from exporters.json_exporter import JsonExporter
from exporters.parquet_exporter import ParquetExporter
from exporters.xml_exporter import XmlExporter

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

SOURCE_COLS = ["id", "created_at", "name", "value", "metadata"]
TARGET_COLS = ["ID", "CREATED_AT", "NAME", "VALUE", "METADATA"]

ROWS = [
    {
        "id": 1,
        "created_at": "2024-01-01T00:00:00",
        "name": "Alice",
        "value": 99.99,
        "metadata": {"city": "NY", "tags": ["a", "b"]},
    },
    {
        "id": 2,
        "created_at": "2024-02-01T00:00:00",
        "name": "Bob",
        "value": 12.50,
        "metadata": {"city": "LA", "tags": ["c"]},
    },
    {
        "id": 3,
        "created_at": "2024-03-01T00:00:00",
        "name": "Carol",
        "value": 0.01,
        "metadata": {"city": "SF", "tags": []},
    },
]


async def _fake_gen() -> AsyncGenerator[List[dict], None]:
    yield ROWS  # type: ignore[misc]


async def _collect(gen: AsyncGenerator) -> bytes:
    """Collect all bytes from an async generator."""
    chunks = []
    async for chunk in gen:
        chunks.append(chunk)
    return b"".join(chunks)


# ---------------------------------------------------------------------------
# CSV
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_csv_header_and_rows():
    exporter = CsvExporter()
    raw = await _collect(exporter.stream(_fake_gen(), SOURCE_COLS, TARGET_COLS))
    text = raw.decode("utf-8")
    reader = list(csv.reader(io.StringIO(text)))

    assert reader[0] == TARGET_COLS                   # header matches targets
    assert len(reader) == 1 + len(ROWS)               # header + 3 data rows
    assert reader[1][2] == "Alice"                    # name column
    # metadata cell is a JSON string, not empty
    assert reader[1][4].startswith("{")


@pytest.mark.asyncio
async def test_csv_metadata_is_json_string():
    exporter = CsvExporter()
    raw = await _collect(exporter.stream(_fake_gen(), SOURCE_COLS, TARGET_COLS))
    reader = list(csv.reader(io.StringIO(raw.decode("utf-8"))))
    meta_cell = reader[1][4]
    parsed = json.loads(meta_cell)
    assert parsed["city"] == "NY"


# ---------------------------------------------------------------------------
# JSON
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_json_valid_array():
    exporter = JsonExporter()
    raw = await _collect(exporter.stream(_fake_gen(), SOURCE_COLS, TARGET_COLS))
    parsed = json.loads(raw.decode("utf-8"))

    assert isinstance(parsed, list)
    assert len(parsed) == 3
    assert parsed[0]["NAME"] == "Alice"
    # JSONB should be native dict, not a string
    assert isinstance(parsed[0]["METADATA"], dict)
    assert parsed[0]["METADATA"]["city"] == "NY"


@pytest.mark.asyncio
async def test_json_target_keys():
    exporter = JsonExporter()
    raw = await _collect(exporter.stream(_fake_gen(), SOURCE_COLS, TARGET_COLS))
    parsed = json.loads(raw.decode("utf-8"))
    for obj in parsed:
        for key in TARGET_COLS:
            assert key in obj


# ---------------------------------------------------------------------------
# XML
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_xml_parseable_and_structure():
    exporter = XmlExporter()
    raw = await _collect(exporter.stream(_fake_gen(), SOURCE_COLS, TARGET_COLS))
    root = ET.fromstring(raw.decode("utf-8"))

    assert root.tag == "records"
    records = root.findall("record")
    assert len(records) == 3

    first = records[0]
    assert first.find("NAME").text == "Alice"
    # metadata should have nested children, not a flat string
    meta = first.find("METADATA")
    assert meta is not None
    assert meta.find("city").text == "NY"


# ---------------------------------------------------------------------------
# Parquet
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_parquet_valid_file():
    exporter = ParquetExporter()
    raw = await _collect(exporter.stream(_fake_gen(), SOURCE_COLS, TARGET_COLS))

    buf = io.BytesIO(raw)
    table = pq.read_table(buf)

    assert table.num_rows == 3
    assert "NAME" in table.schema.names
    assert "METADATA" in table.schema.names

    names = table.column("NAME").to_pylist()
    assert names == ["Alice", "Bob", "Carol"]
