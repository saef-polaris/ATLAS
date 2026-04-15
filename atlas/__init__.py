"""ATLAS — Antarctic Treaty Literature & Analysis System."""

from .config import ensure_data_dir, get_data_dir, get_marker_dir
from .llm_item_classifier import classify_items, export_llm_validation_bundle
from .manual_validation import export_validation_bundle
from .paper_dataset import (
    DEFAULT_CLASSIFICATIONS_PATH,
    NormalizedOutputLabel,
    NormalizedPaperLabel,
    PaperDataset,
    infer_meeting_number,
    load_paper_dataset,
    normalize_output_label,
    normalize_paper_label,
    print_dataset_overview,
)
from .paper_dataset import (
    MARKER_DIR as PAPER_DATASET_MARKER_DIR,
)
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
    "DEFAULT_CLASSIFICATIONS_PATH",
    "MARKER_DIR",
    "PARQUET_DIR",
    "PAPER_DATASET_MARKER_DIR",
    "NormalizedOutputLabel",
    "NormalizedPaperLabel",
    "PaperDataset",
    "build_or_refresh_database",
    "classify_items",
    "ensure_data_dir",
    "export_llm_validation_bundle",
    "export_validation_bundle",
    "get_data_dir",
    "get_marker_dir",
    "infer_meeting_number",
    "load_paper_dataset",
    "normalize_output_label",
    "normalize_paper_label",
    "print_dataset_overview",
    "query_by_item",
    "query_by_output",
    "query_by_paper",
]
