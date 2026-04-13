from __future__ import annotations

import json
from datetime import datetime, UTC
from pathlib import Path
from typing import Any

import pandas as pd

from .config import ensure_data_dir, get_marker_dir

MARKER_DIR = get_marker_dir()
DEFAULT_BUNDLE_PATH = MARKER_DIR / "validation_bundle.json"


TABLE_FILES = {
    "items": "derived_marker_items.csv",
    "outputs": "derived_marker_outputs.csv",
    "item_output_links": "derived_marker_item_output_links.csv",
    "paper_item_links": "derived_marker_paper_item_links.csv",
    "paper_output_links": "derived_marker_paper_output_links.csv",
}


def _read_table(marker_dir: Path, name: str) -> pd.DataFrame:
    path = marker_dir / TABLE_FILES[name]
    if not path.exists():
        raise FileNotFoundError(f"Missing required table: {path}")
    return pd.read_csv(path)


def _clean_value(value: Any) -> Any:
    if pd.isna(value):
        return None
    return value.item() if hasattr(value, "item") else value


def _record_from_series(row: pd.Series, keys: list[str]) -> dict[str, Any]:
    return {key: _clean_value(row.get(key)) for key in keys}


def _join_items(marker_dir: Path) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    ensure_data_dir(marker_dir.parent)
    items = _read_table(marker_dir, "items")
    item_output = _read_table(marker_dir, "item_output_links")
    paper_item = _read_table(marker_dir, "paper_item_links")
    paper_output = _read_table(marker_dir, "paper_output_links")
    return items, item_output, paper_item, paper_output


def build_validation_cases(
    marker_dir: Path = MARKER_DIR,
    run: str | None = None,
    case_types: tuple[str, ...] = ("item_output", "paper_item", "paper_output"),
    limit: int | None = None,
) -> list[dict[str, Any]]:
    items, item_output, paper_item, paper_output = _join_items(marker_dir)
    item_cols = [
        "run",
        "sequence_id",
        "sequence_type",
        "item_num",
        "item_title",
        "heading_source",
        "start_page",
        "end_page",
        "start_line",
        "end_line",
        "text",
    ]
    item_lookup = items[item_cols].copy()

    if run is not None:
        item_lookup = item_lookup[item_lookup["run"] == run]
        item_output = item_output[item_output["run"] == run]
        paper_item = paper_item[paper_item["run"] == run]
        paper_output = paper_output[paper_output["run"] == run]

    merge_keys = ["run", "sequence_id", "sequence_type", "item_num"]
    cases: list[dict[str, Any]] = []

    if "item_output" in case_types and not item_output.empty:
        joined = item_output.merge(item_lookup, on=merge_keys, how="left", suffixes=("", "_item"))
        for idx, row in joined.reset_index(drop=True).iterrows():
            cases.append(
                {
                    "case_id": f"item_output::{idx:06d}",
                    "case_type": "item_output",
                    "predicted": _record_from_series(
                        row,
                        [
                            "run",
                            "sequence_id",
                            "sequence_type",
                            "item_num",
                            "item_title",
                            "output_type",
                            "output_number",
                            "output_year",
                            "output_label",
                            "title",
                            "link_basis",
                            "link_confidence",
                        ],
                    ),
                    "evidence": {
                        "primary_evidence": _clean_value(row.get("evidence")),
                        "item_text": _clean_value(row.get("text")),
                        "item_metadata": _record_from_series(
                            row,
                            [
                                "heading_source",
                                "start_page",
                                "end_page",
                                "start_line",
                                "end_line",
                            ],
                        ),
                    },
                    "review": {
                        "status": "unreviewed",
                        "corrected": {},
                        "notes": "",
                    },
                }
            )

    if "paper_item" in case_types and not paper_item.empty:
        joined = paper_item.merge(item_lookup, on=merge_keys, how="left", suffixes=("", "_item"))
        base = len(cases)
        for offset, row in joined.reset_index(drop=True).iterrows():
            cases.append(
                {
                    "case_id": f"paper_item::{base + offset:06d}",
                    "case_type": "paper_item",
                    "predicted": _record_from_series(
                        row,
                        [
                            "run",
                            "sequence_id",
                            "sequence_type",
                            "item_num",
                            "item_title",
                            "paper_kind",
                            "paper_number",
                            "paper_rev",
                            "paper_label",
                            "link_basis",
                            "link_confidence",
                        ],
                    ),
                    "evidence": {
                        "primary_evidence": _clean_value(row.get("link_basis")),
                        "item_text": _clean_value(row.get("text")),
                        "item_metadata": _record_from_series(
                            row,
                            [
                                "heading_source",
                                "start_page",
                                "end_page",
                                "start_line",
                                "end_line",
                            ],
                        ),
                    },
                    "review": {
                        "status": "unreviewed",
                        "corrected": {},
                        "notes": "",
                    },
                }
            )

    if "paper_output" in case_types and not paper_output.empty:
        joined = paper_output.merge(item_lookup, on=merge_keys, how="left", suffixes=("", "_item"))
        base = len(cases)
        for offset, row in joined.reset_index(drop=True).iterrows():
            cases.append(
                {
                    "case_id": f"paper_output::{base + offset:06d}",
                    "case_type": "paper_output",
                    "predicted": _record_from_series(
                        row,
                        [
                            "run",
                            "sequence_id",
                            "sequence_type",
                            "item_num",
                            "item_title",
                            "paper_kind",
                            "paper_number",
                            "paper_rev",
                            "paper_label",
                            "output_type",
                            "output_number",
                            "output_year",
                            "output_label",
                            "output_title",
                            "link_basis",
                            "link_confidence",
                        ],
                    ),
                    "evidence": {
                        "primary_evidence": _clean_value(row.get("evidence")),
                        "item_text": _clean_value(row.get("text")),
                        "item_metadata": _record_from_series(
                            row,
                            [
                                "heading_source",
                                "start_page",
                                "end_page",
                                "start_line",
                                "end_line",
                            ],
                        ),
                    },
                    "review": {
                        "status": "unreviewed",
                        "corrected": {},
                        "notes": "",
                    },
                }
            )

    if limit is not None:
        cases = cases[:limit]
    return cases


def export_validation_bundle(
    output_path: Path = DEFAULT_BUNDLE_PATH,
    marker_dir: Path = MARKER_DIR,
    run: str | None = None,
    case_types: tuple[str, ...] = ("item_output", "paper_item", "paper_output"),
    limit: int | None = None,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    bundle = {
        "generated_at": datetime.now(UTC).isoformat(),
        "marker_dir": str(marker_dir),
        "run_filter": run,
        "case_types": list(case_types),
        "case_count": 0,
        "cases": build_validation_cases(marker_dir=marker_dir, run=run, case_types=case_types, limit=limit),
    }
    bundle["case_count"] = len(bundle["cases"])
    output_path.write_text(json.dumps(bundle, ensure_ascii=False, indent=2))
    return output_path
