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

ITEM_KEY_COLS = ["run", "sequence_id", "sequence_type", "item_num"]


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


def _item_key_from_mapping(row: dict[str, Any] | pd.Series) -> tuple[str, str, str, str]:
    return tuple("" if _clean_value(row.get(col)) is None else str(_clean_value(row.get(col))) for col in ITEM_KEY_COLS)  # type: ignore[return-value]


def _item_key_to_str(key: tuple[str, str, str, str]) -> str:
    return "::".join(str(part).replace("::", "_") for part in key)


def _stable_claim_id(row: pd.Series, idx: int) -> str:
    parts = [
        _clean_value(row.get("run")),
        _clean_value(row.get("sequence_id")),
        _clean_value(row.get("sequence_type")),
        _clean_value(row.get("item_num")),
        _clean_value(row.get("paper_label")),
        _clean_value(row.get("output_label")),
        _clean_value(row.get("link_basis")),
        idx,
    ]
    clean = [str(part).replace("::", "_") if part is not None else "" for part in parts]
    return "claim::" + "::".join(clean)


def _dedupe_records(rows: list[dict[str, Any]], keys: list[str]) -> list[dict[str, Any]]:
    seen: set[tuple[Any, ...]] = set()
    out: list[dict[str, Any]] = []
    for row in rows:
        key = tuple(row.get(k) for k in keys)
        if key in seen:
            continue
        seen.add(key)
        out.append(row)
    return out


def _build_item_contexts(
    items: pd.DataFrame,
    item_output: pd.DataFrame,
    paper_item: pd.DataFrame,
    paper_output: pd.DataFrame,
) -> dict[tuple[str, str, str, str], dict[str, Any]]:
    contexts: dict[tuple[str, str, str, str], dict[str, Any]] = {}

    def ensure_context(row: pd.Series) -> dict[str, Any]:
        key = _item_key_from_mapping(row)
        if key not in contexts:
            contexts[key] = {
                "item_key": dict(zip(ITEM_KEY_COLS, key, strict=True)),
                "item_entries": [],
                "papers": [],
                "outcomes": [],
                "claims": [],
            }
        return contexts[key]

    item_entry_cols = [
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
        "has_papers",
        "has_outputs",
    ]
    for _, row in items.iterrows():
        ensure_context(row)["item_entries"].append(_record_from_series(row, item_entry_cols))

    paper_cols = [
        "paper_kind",
        "paper_number",
        "paper_rev",
        "paper_label",
        "link_basis",
        "link_confidence",
        "item_title",
    ]
    for _, row in paper_item.iterrows():
        ensure_context(row)["papers"].append(_record_from_series(row, paper_cols))

    outcome_cols = [
        "output_type",
        "output_number",
        "output_year",
        "output_label",
        "title",
        "evidence",
        "link_confidence",
        "link_basis",
    ]
    for _, row in item_output.iterrows():
        ensure_context(row)["outcomes"].append(_record_from_series(row, outcome_cols))

    claim_cols = [
        "paper_kind",
        "paper_number",
        "paper_rev",
        "paper_label",
        "item_title",
        "output_type",
        "output_number",
        "output_year",
        "output_label",
        "output_title",
        "link_basis",
        "link_confidence",
        "evidence",
    ]
    for idx, row in paper_output.reset_index(drop=True).iterrows():
        claim = _record_from_series(row, claim_cols)
        claim["claim_id"] = _stable_claim_id(row, idx)
        claim["claim_path"] = {
            "paper": claim.get("paper_label"),
            "item": row.get("item_title") or row.get("item_num"),
            "outcome": claim.get("output_label"),
        }
        ensure_context(row)["claims"].append(claim)

    for context in contexts.values():
        context["item_entries"] = _dedupe_records(
            context["item_entries"],
            ["run", "sequence_id", "sequence_type", "item_num", "item_title", "start_line", "end_line"],
        )
        context["papers"] = _dedupe_records(
            context["papers"], ["paper_label", "paper_kind", "paper_number", "paper_rev", "item_title"]
        )
        context["outcomes"] = _dedupe_records(
            context["outcomes"], ["output_label", "output_type", "output_number", "output_year", "title"]
        )
        context["claims"] = _dedupe_records(
            context["claims"], ["paper_label", "output_label", "item_title", "link_basis", "evidence"]
        )
    return contexts


def _attach_item_context(
    cases: list[dict[str, Any]],
    contexts: dict[tuple[str, str, str, str], dict[str, Any]],
) -> list[dict[str, Any]]:
    del contexts
    for case in cases:
        key = _item_key_from_mapping(case.get("predicted", {}))
        case["item_context_key"] = _item_key_to_str(key)
    return cases


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
    sample_per_case_type: int | None = None,
    random_seed: int = 0,
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

    item_contexts = _build_item_contexts(item_lookup, item_output, paper_item, paper_output)

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

    if sample_per_case_type is not None:
        sampled: list[dict[str, Any]] = []
        rng = __import__("random").Random(random_seed)
        for case_type in case_types:
            pool = [case for case in cases if case["case_type"] == case_type]
            if len(pool) <= sample_per_case_type:
                sampled.extend(pool)
            else:
                sampled.extend(rng.sample(pool, sample_per_case_type))
        cases = sampled
    if limit is not None:
        cases = cases[:limit]
    return _attach_item_context(cases, item_contexts)


def export_validation_bundle(
    output_path: Path = DEFAULT_BUNDLE_PATH,
    marker_dir: Path = MARKER_DIR,
    run: str | None = None,
    case_types: tuple[str, ...] = ("item_output", "paper_item", "paper_output"),
    limit: int | None = None,
    sample_per_case_type: int | None = None,
    random_seed: int = 0,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    cases = build_validation_cases(
        marker_dir=marker_dir,
        run=run,
        case_types=case_types,
        limit=limit,
        sample_per_case_type=sample_per_case_type,
        random_seed=random_seed,
    )
    items, item_output, paper_item, paper_output = _join_items(marker_dir)
    if run is not None:
        items = items[items["run"] == run]
        item_output = item_output[item_output["run"] == run]
        paper_item = paper_item[paper_item["run"] == run]
        paper_output = paper_output[paper_output["run"] == run]
    contexts = _build_item_contexts(items, item_output, paper_item, paper_output)
    needed_contexts = {case.get("item_context_key") for case in cases}
    bundle = {
        "generated_at": datetime.now(UTC).isoformat(),
        "marker_dir": str(marker_dir),
        "run_filter": run,
        "case_types": list(case_types),
        "case_count": len(cases),
        "item_contexts": {
            _item_key_to_str(key): value
            for key, value in contexts.items()
            if _item_key_to_str(key) in needed_contexts
        },
        "cases": cases,
    }
    output_path.write_text(json.dumps(bundle, ensure_ascii=False, indent=2))
    return output_path
