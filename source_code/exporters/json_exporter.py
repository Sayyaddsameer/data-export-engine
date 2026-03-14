"""
exporters/json_exporter.py — Streaming JSON array writer.

Strategy:
- Emit '[' first, then JSON-encoded objects separated by commas, then ']'.
- JSONB (dict) values are emitted as native JSON objects (not strings).
- No in-memory accumulation of the full array; each batch is serialised
  independently and immediately yielded.
"""
from __future__ import annotations

import json
from typing import AsyncGenerator, List, Sequence

import asyncpg

from exporters.base import BaseExporter


class JsonExporter(BaseExporter):
    content_type = "application/json"
    file_extension = "json"

    async def stream(
        self,
        records_gen: AsyncGenerator[List[asyncpg.Record], None],
        source_columns: Sequence[str],
        target_columns: Sequence[str],
    ) -> AsyncGenerator[bytes, None]:
        first_record = True

        yield b"["

        async for batch in records_gen:
            parts: List[str] = []
            for record in batch:
                obj = {}
                for src, tgt in zip(source_columns, target_columns):
                    val = record[src]
                    # asyncpg returns JSONB as dict; leave as-is for native JSON
                    if hasattr(val, "isoformat"):
                        # datetime / date → ISO string
                        val = val.isoformat()
                    obj[tgt] = val

                parts.append(json.dumps(obj, ensure_ascii=False, default=str))

            if parts:
                chunk = ""
                if first_record:
                    chunk = parts[0]
                    if len(parts) > 1:
                        chunk += "," + ",".join(parts[1:])
                    first_record = False
                else:
                    chunk = "," + ",".join(parts)
                yield chunk.encode("utf-8")

        yield b"]"
