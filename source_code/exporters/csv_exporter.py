"""
exporters/csv_exporter.py — Streaming CSV writer.

Strategy:
- Write a header row from target column names.
- For each batch of rows, serialise to CSV text in a StringIO buffer,
  encode to UTF-8 bytes, then yield. The buffer is recreated per batch
  so memory stays constant.
- JSONB (dict) columns: serialised as a JSON string inside the CSV cell.
"""
from __future__ import annotations

import csv
import io
import json
from typing import AsyncGenerator, List, Sequence

import asyncpg

from exporters.base import BaseExporter


class CsvExporter(BaseExporter):
    content_type = "text/csv"
    file_extension = "csv"

    async def stream(
        self,
        records_gen: AsyncGenerator[List[asyncpg.Record], None],
        source_columns: Sequence[str],
        target_columns: Sequence[str],
    ) -> AsyncGenerator[bytes, None]:
        # ── Header ────────────────────────────────────────────────────────────
        buf = io.StringIO()
        writer = csv.writer(buf)
        writer.writerow(target_columns)
        header_bytes = buf.getvalue().encode("utf-8")
        buf.close()
        yield header_bytes

        # ── Data rows ─────────────────────────────────────────────────────────
        async for batch in records_gen:
            buf = io.StringIO()
            writer = csv.writer(buf)
            for record in batch:
                row = []
                for col in source_columns:
                    val = record[col]
                    # datetime / date → ISO-8601 string
                    if hasattr(val, "isoformat"):
                        val = val.isoformat()
                    # JSONB (dict / list) → JSON string inside the CSV cell
                    elif isinstance(val, dict) or isinstance(val, list):
                        val = json.dumps(val, ensure_ascii=False)
                    row.append(val)
                writer.writerow(row)
            chunk = buf.getvalue().encode("utf-8")
            buf.close()
            yield chunk
