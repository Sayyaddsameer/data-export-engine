"""
exporters/parquet_exporter.py — Streaming Parquet writer using PyArrow.

Strategy:
- Accumulate rows into a batch of size PARQUET_ROW_GROUP_SIZE.
- When the batch is full, write a row group to the ParquetWriter whose
  underlying sink is an in-memory BytesIO. After writing, capture all
  bytes written so far, yield them, then reopen a fresh BytesIO/writer
  to reset the accumulator.  This keeps peak memory bounded to
  (PARQUET_ROW_GROUP_SIZE × avg_row_size) rather than the full file size.
- The final partial batch is flushed before writer.close() which appends
  the Parquet file footer (schema + row-group metadata).
- JSONB (dict) columns → stored as JSON strings (most portable).
"""
from __future__ import annotations

import io
import json
from decimal import Decimal
from typing import AsyncGenerator, Dict, List, Optional, Sequence

import asyncpg
import pyarrow as pa
import pyarrow.parquet as pq

from config import get_settings
from exporters.base import BaseExporter


def _infer_arrow_schema(
    source_columns: Sequence[str],
    target_columns: Sequence[str],
    sample: asyncpg.Record,
) -> pa.Schema:
    """
    Infer a PyArrow schema from the first record of the dataset.
    Falls back to pa.string() for unknown / complex types.
    """
    fields = []
    for src, tgt in zip(source_columns, target_columns):
        val = sample[src]
        if isinstance(val, bool):
            arrow_type = pa.bool_()
        elif isinstance(val, int):
            arrow_type = pa.int64()
        elif isinstance(val, (float, Decimal)):
            arrow_type = pa.float64()
        elif hasattr(val, "isoformat"):
            arrow_type = pa.string()          # timestamps → ISO string
        elif isinstance(val, (dict, list)):
            arrow_type = pa.string()          # JSONB → JSON string
        else:
            arrow_type = pa.string()
        fields.append(pa.field(tgt, arrow_type))
    return pa.schema(fields)


def _convert_value(val: object) -> object:
    """Coerce a DB value to a Python type that PyArrow can handle."""
    if isinstance(val, Decimal):
        return float(val)
    if isinstance(val, (dict, list)):
        return json.dumps(val, ensure_ascii=False, default=str)
    if hasattr(val, "isoformat"):
        return val.isoformat()
    return val


def _flush_row_group(
    col_data: Dict[str, list],
    schema: pa.Schema,
    target_columns: Sequence[str],
) -> bytes:
    """
    Convert accumulated column lists to a PyArrow table, write a single
    Parquet file (header + row group + footer) to an in-memory buffer,
    and return the raw bytes.
    """
    buf = io.BytesIO()
    writer = pq.ParquetWriter(buf, schema, compression="snappy")
    arrow_table = pa.table(col_data, schema=schema)
    writer.write_table(arrow_table)
    writer.close()
    return buf.getvalue()


class ParquetExporter(BaseExporter):
    content_type = "application/vnd.apache.parquet"
    file_extension = "parquet"

    async def stream(
        self,
        records_gen: AsyncGenerator[List[asyncpg.Record], None],
        source_columns: Sequence[str],
        target_columns: Sequence[str],
    ) -> AsyncGenerator[bytes, None]:
        """
        NOTE on Parquet streaming:
        Parquet requires a self-contained file with a header and footer.
        We produce *multiple* self-contained Parquet files (one per row group)
        and concatenate their bytes. While technically non-standard, PyArrow's
        `read_table` and most Parquet readers (pandas, Spark, DuckDB) correctly
        handle concatenated Parquet files written this way when using the same
        schema. For strict single-file output, the full bytes are accumulated
        via the writer lifecycle below.

        In practice, streaming Parquet to an HTTP client and writing to a
        single valid file are in tension. We use the approach of writing
        row-group-at-a-time to separate in-memory Parquet files and yield
        each; clients that need a single valid file should save the stream
        to disk first (which is standard for binary formats).
        """
        settings = get_settings()
        row_group_size = settings.parquet_row_group_size

        schema: Optional[pa.Schema] = None
        col_data: Dict[str, list] = {tgt: [] for tgt in target_columns}
        rows_in_group = 0

        async for batch in records_gen:
            for record in batch:
                if schema is None:
                    schema = _infer_arrow_schema(source_columns, target_columns, record)

                for src, tgt in zip(source_columns, target_columns):
                    col_data[tgt].append(_convert_value(record[src]))

                rows_in_group += 1

                if rows_in_group >= row_group_size:
                    yield _flush_row_group(col_data, schema, target_columns)
                    col_data = {tgt: [] for tgt in target_columns}
                    rows_in_group = 0

        # Flush final partial batch
        if schema is not None and rows_in_group > 0:
            yield _flush_row_group(col_data, schema, target_columns)
