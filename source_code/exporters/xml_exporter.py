"""
exporters/xml_exporter.py — Streaming XML writer (SAX-style, row-at-a-time).

Strategy:
- Emit XML declaration + root <records> opening tag.
- For each batch, build a <record> element per row using ElementTree,
  convert to string bytes, yield, then discard — no DOM tree held in RAM.
- JSONB (dict) values: recursively converted to nested XML child elements.
- Streaming avoids the need to build the full DOM in memory.
"""
from __future__ import annotations

import json
from typing import AsyncGenerator, List, Sequence
from xml.etree import ElementTree as ET

import asyncpg

from exporters.base import BaseExporter


def _dict_to_xml(parent: ET.Element, data: dict | list | str | int | float | bool | None) -> None:
    """
    Recursively convert a Python dict/list into children of *parent* Element.

    - dict  → one child element per key, recursed.
    - list  → one <item> child per element, recursed.
    - scalars → parent.text is set to the string value.
    """
    if isinstance(data, dict):
        for key, val in data.items():
            # XML tag names must be valid; sanitise by replacing spaces/dots
            safe_key = str(key).replace(" ", "_").replace(".", "_")
            child = ET.SubElement(parent, safe_key)
            _dict_to_xml(child, val)
    elif isinstance(data, list):
        for item in data:
            item_el = ET.SubElement(parent, "item")
            _dict_to_xml(item_el, item)
    elif data is None:
        parent.text = ""
    else:
        parent.text = str(data)


class XmlExporter(BaseExporter):
    content_type = "application/xml"
    file_extension = "xml"

    async def stream(
        self,
        records_gen: AsyncGenerator[List[asyncpg.Record], None],
        source_columns: Sequence[str],
        target_columns: Sequence[str],
    ) -> AsyncGenerator[bytes, None]:
        # ── Preamble ──────────────────────────────────────────────────────────
        yield b'<?xml version="1.0" encoding="UTF-8"?>\n<records>\n'

        # ── Data rows ─────────────────────────────────────────────────────────
        async for batch in records_gen:
            chunk_parts: List[bytes] = []
            for record in batch:
                rec_el = ET.Element("record")
                for src, tgt in zip(source_columns, target_columns):
                    val = record[src]
                    safe_tgt = str(tgt).replace(" ", "_").replace(".", "_")
                    field_el = ET.SubElement(rec_el, safe_tgt)
                    if isinstance(val, dict) or isinstance(val, list):
                        _dict_to_xml(field_el, val)
                    elif hasattr(val, "isoformat"):
                        field_el.text = val.isoformat()
                    elif val is None:
                        field_el.text = ""
                    else:
                        field_el.text = str(val)

                chunk_parts.append(ET.tostring(rec_el, encoding="unicode").encode("utf-8"))
                chunk_parts.append(b"\n")

            if chunk_parts:
                yield b"".join(chunk_parts)

        # ── Closing tag ───────────────────────────────────────────────────────
        yield b"</records>\n"
