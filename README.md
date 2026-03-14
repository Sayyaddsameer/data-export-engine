# Data Export Engine

A high-performance, memory-efficient data export API built with **Python / FastAPI** that streams a 10-million-row PostgreSQL dataset into **CSV, JSON, XML, and Parquet** — all within a hard **256 MB** container memory limit.

---

## Table of Contents

1. [Quick Start](#quick-start)  
2. [Architecture](#architecture)  
3. [Project Structure](#project-structure)  
4. [Environment Variables](#environment-variables)  
5. [API Reference](#api-reference)  
6. [Format Details](#format-details)  
7. [Performance Characteristics](#performance-characteristics)  
8. [Running Tests](#running-tests)  
9. [Extending the Engine](#extending-the-engine)

---

## Quick Start

```bash
# 1. Clone / enter the project directory
cd "partnr tasks"

# 2. Copy environment template (defaults already work with docker-compose)
cp .env.example .env

# 3. Start everything — DB seeds itself automatically (~2–5 min for 10M rows)
docker-compose up --build

# 4. Verify seeding
docker exec partnrtasks-db-1 psql -U user -d exports_db -c "SELECT COUNT(*) FROM records;"
# → 10000000

# 5. Browse interactive API docs
open http://localhost:8080/docs
```

---

## Architecture

```
┌──────────────────────────────────────────────────────┐
│                   FastAPI (Uvicorn)                  │
│                                                      │
│  POST /exports         → ExportService.create_job()  │
│  GET  /exports/benchmark       → run_benchmark()     │
│  GET  /exports/{id}/download   → build_stream()      │
│                           │                          │
│          Strategy Pattern │                          │
│   ┌──────────┬─────────┬──┴──────┬──────────┐        │
│   │ CSV      │ JSON    │ XML     │ Parquet  │        │
│   │ Exporter │ Exporter│ Exporter│ Exporter │        │
│   └──────────┴─────────┴─────────┴──────────┘        │
│                    │                                 │
│         asyncpg server-side cursor                   │
│         (1 000 rows / chunk — O(1) memory)           │
└──────────────────────────────────────────────────────┘
                    │
       ┌────────────▼────────────┐
       │   PostgreSQL 13          │
       │  public.records (10M)   │
       └─────────────────────────┘
```

### Key Design Decisions

| Concern | Choice | Why |
|---------|--------|-----|
| **DB reads** | asyncpg server-side cursor, 1 000 rows/chunk | Never buffers full table |
| **JSON streaming** | Manual `[` / `,` / `]` framing | Zero dependencies, correct |
| **XML streaming** | `ElementTree.tostring` per row, discard | No in-memory DOM |
| **Parquet streaming** | PyArrow row groups, 50 k rows, flush then reset | Bounded peak memory |
| **Gzip** | `gzip.GzipFile` wrap around async generator | Transparent, no extra buffering |
| **Extensibility** | Strategy pattern + `EXPORTER_REGISTRY` dict | New format = new file + 1 dict entry |
| **Security** | Column names quoted as SQL identifiers, job IDs are UUIDs | No SQL injection, no enumeration |
| **Job store** | In-memory `dict` | Sufficient for task; swap Redis in prod |

---

## Project Structure

```
partnr tasks/
├── Dockerfile                  # Multistage Python build
├── docker-compose.yml          # Orchestrates app + db
├── .env.example                # All env var documentation
├── seeds/
│   └── init-db.sh              # Idempotent schema + 10M row seed
├── source_code/
│   ├── requirements.txt
│   ├── main.py                 # FastAPI app, lifespan management
│   ├── config.py               # Pydantic-settings (reads from env)
│   ├── database.py             # asyncpg pool + streaming cursor
│   ├── models.py               # All Pydantic request/response schemas
│   ├── exporters/
│   │   ├── base.py             # Abstract BaseExporter
│   │   ├── csv_exporter.py
│   │   ├── json_exporter.py
│   │   ├── xml_exporter.py
│   │   ├── parquet_exporter.py
│   │   └── factory.py          # get_exporter(fmt) registry
│   ├── routers/
│   │   └── exports.py          # All /exports routes
│   └── services/
│       └── export_service.py   # Job CRUD, stream orchestration, benchmark
└── tests/
    ├── conftest.py
    ├── test_exporters.py       # Unit tests (no DB)
    └── test_api.py             # Integration tests (mocked DB)
```

---

## Environment Variables

All configuration is via environment variables — no hardcoded values exist in the codebase.

| Variable | Default | Description |
|----------|---------|-------------|
| `DATABASE_URL` | *(required)* | `postgresql://user:pw@host:5432/db` |
| `PORT` | `8080` | Application listen port |
| `DB_POOL_MIN_SIZE` | `2` | Minimum asyncpg pool connections |
| `DB_POOL_MAX_SIZE` | `10` | Maximum asyncpg pool connections |
| `DB_CHUNK_SIZE` | `1000` | Rows fetched per cursor iteration |
| `PARQUET_ROW_GROUP_SIZE` | `50000` | Rows per Parquet row group |
| `LOG_LEVEL` | `INFO` | `DEBUG` / `INFO` / `WARNING` / `ERROR` |

See [`.env.example`](.env.example) for the full template.

---

## API Reference

### `POST /exports` — Create export job

**Request body:**

```json
{
  "format": "csv",
  "columns": [
    { "source": "id",       "target": "ID"       },
    { "source": "name",     "target": "Name"     },
    { "source": "value",    "target": "Value"    },
    { "source": "metadata", "target": "Metadata" }
  ],
  "compression": "gzip"
}
```

| Field | Type | Required | Notes |
|-------|------|----------|-------|
| `format` | string |  | `csv` / `json` / `xml` / `parquet` |
| `columns` | array |  | At least one mapping required |
| `columns[].source` | string |  | DB column name |
| `columns[].target` | string |  | Name in export file |
| `compression` | string |  | `gzip` only; not for `parquet` |

**Response `201 Created`:**

```json
{ "exportId": "550e8400-e29b-41d4-a716-446655440000", "status": "pending" }
```

---

### `GET /exports/{exportId}/download` — Stream export

Streams the export file. Sets `Content-Type`, `Content-Disposition`, and optionally `Content-Encoding: gzip`.

```bash
# Download CSV
curl http://localhost:8080/exports/<uuid>/download -o export.csv

# Download Parquet and inspect
curl http://localhost:8080/exports/<uuid>/download -o export.parquet
python -c "import pyarrow.parquet as pq; print(pq.read_table('export.parquet').num_rows)"
```

---

### `GET /exports/benchmark` — Performance benchmark

Runs a full 10M-row export for all four formats and returns metrics.

```bash
curl http://localhost:8080/exports/benchmark
```

**Response `200 OK`:**

```json
{
  "datasetRowCount": 10000000,
  "results": [
    { "format": "csv",     "durationSeconds": 45.2, "fileSizeBytes": 892000000, "peakMemoryMB": 48.5 },
    { "format": "json",    "durationSeconds": 72.1, "fileSizeBytes": 1650000000, "peakMemoryMB": 51.2 },
    { "format": "xml",     "durationSeconds": 110.4, "fileSizeBytes": 2800000000, "peakMemoryMB": 52.0 },
    { "format": "parquet", "durationSeconds": 30.8, "fileSizeBytes": 210000000, "peakMemoryMB": 78.3 }
  ]
}
```

>  This endpoint may take **10–20 minutes** on first run. Do not call it frequently in production.

---

## Format Details

### CSV
- Header row uses `target` column names.
- `metadata` JSONB column → serialised as a **JSON string** inside the CSV cell.
- Supports `gzip` compression.

### JSON
- Output is a single valid JSON array: `[{…}, {…}, …]`.
- `metadata` JSONB column → emitted as a **native JSON object** (not a string).
- Supports `gzip` compression.

### XML
- Root element: `<records>`. One `<record>` child per row.
- `metadata` JSONB column → **recursively converted to nested XML elements** (e.g., `<city>`, `<tags><item>…</item></tags>`).
- Supports `gzip` compression.

### Parquet
- Written using **PyArrow** with Snappy compression per row group.
- `metadata` JSONB column → stored as a **JSON string** column (portable across all consumers).
- Does **not** support additional `gzip` wrapping (Parquet is already compressed).

---

## Running Tests

```bash
# Install test dependencies (inside Docker or a local venv)
pip install pytest pytest-asyncio httpx

# Run all tests
cd "d:\partnr tasks"
pytest tests/ -v

# Run only unit tests (no DB mock needed)
pytest tests/test_exporters.py -v

# Run only API integration tests
pytest tests/test_api.py -v
```

---

## Extending the Engine

To add a new export format (e.g., **Avro**):

1. Create `source_code/exporters/avro_exporter.py`:
   ```python
   from exporters.base import BaseExporter
   class AvroExporter(BaseExporter):
       content_type = "application/avro"
       file_extension = "avro"
       async def stream(self, records_gen, source_columns, target_columns):
           ...  # yield bytes
   ```

2. Register it in `source_code/exporters/factory.py`:
   ```python
   from exporters.avro_exporter import AvroExporter
   EXPORTER_REGISTRY["avro"] = AvroExporter
   ```

3. Add `"avro"` to the `ExportFormat` enum in `source_code/models.py`.

No other files need modification.

---

## Database Schema

```sql
CREATE TABLE public.records (
    id          BIGSERIAL                   PRIMARY KEY,
    created_at  TIMESTAMP WITH TIME ZONE    NOT NULL DEFAULT NOW(),
    name        VARCHAR(255)                NOT NULL,
    value       DECIMAL(18, 4)              NOT NULL,
    metadata    JSONB                       NOT NULL
);
```

Seeded with exactly **10,000,000** rows via a single `INSERT … SELECT generate_series(1, 10000000)` statement — idempotent and completes in under 5 minutes on typical hardware.
