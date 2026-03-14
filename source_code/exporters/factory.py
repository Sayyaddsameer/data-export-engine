"""
exporters/factory.py — Exporter factory (registry pattern).

To add a new format:
  1. Create a new subclass of BaseExporter in a new module.
  2. Import it here and add it to EXPORTER_REGISTRY.
  3. Add the format string to the ExportFormat enum in models.py.
"""
from __future__ import annotations

from typing import Dict, Type

from exporters.base import BaseExporter
from exporters.csv_exporter import CsvExporter
from exporters.json_exporter import JsonExporter
from exporters.parquet_exporter import ParquetExporter
from exporters.xml_exporter import XmlExporter

# Registry maps format string → exporter class (not instance, so it is
# stateless and thread-safe).
EXPORTER_REGISTRY: Dict[str, Type[BaseExporter]] = {
    "csv": CsvExporter,
    "json": JsonExporter,
    "xml": XmlExporter,
    "parquet": ParquetExporter,
}


def get_exporter(fmt: str) -> BaseExporter:
    """
    Instantiate and return the exporter for *fmt*.

    :raises ValueError: if the format is not registered.
    """
    exporter_cls = EXPORTER_REGISTRY.get(fmt.lower())
    if exporter_cls is None:
        supported = ", ".join(sorted(EXPORTER_REGISTRY.keys()))
        raise ValueError(f"Unsupported export format '{fmt}'. Supported: {supported}")
    return exporter_cls()
