"""
exporters/base.py — Abstract base class for all format exporters.

Strategy pattern: each concrete exporter implements stream() and declares
its content_type and file_extension. Adding a new format requires only a
new subclass + a registration in factory.py.
"""
from __future__ import annotations

import abc
from typing import AsyncGenerator, List, Sequence

import asyncpg


class BaseExporter(abc.ABC):
    """
    Abstract exporter. Subclasses must implement :meth:`stream`.

    Attributes:
        content_type (str): MIME type sent in the HTTP Content-Type header.
        file_extension (str): Extension appended to the download filename.
    """

    content_type: str
    file_extension: str

    @abc.abstractmethod
    async def stream(
        self,
        records_gen: AsyncGenerator[List[asyncpg.Record], None],
        source_columns: Sequence[str],
        target_columns: Sequence[str],
    ) -> AsyncGenerator[bytes, None]:
        """
        Consume *records_gen* (an async generator of row-batches) and
        yield serialised byte chunks suitable for streaming to the client.

        :param records_gen:     Async generator yielding List[asyncpg.Record].
        :param source_columns:  Ordered list of source DB column names.
        :param target_columns:  Ordered list of names to use in the output file.
        :yields:                Raw bytes suitable for HTTP streaming.
        """
        ...  # pragma: no cover
