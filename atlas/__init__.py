"""ATLAS — Antarctic Treaty Literature & Analysis System."""

from .config import ensure_data_dir, get_data_dir, get_marker_dir
from .manual_validation import export_validation_bundle
from .support_tracer_backend import (
    DATA_DIR,
    DB_PATH,
    MARKER_DIR,
    PARQUET_DIR,
    build_or_refresh_database,
    query_by_item,
    query_by_output,
    query_by_paper,
)

__all__ = [
    "DATA_DIR",
    "DB_PATH",
    "MARKER_DIR",
    "PARQUET_DIR",
    "build_or_refresh_database",
    "ensure_data_dir",
    "export_validation_bundle",
    "get_data_dir",
    "get_marker_dir",
    "query_by_item",
    "query_by_output",
    "query_by_paper",
]
