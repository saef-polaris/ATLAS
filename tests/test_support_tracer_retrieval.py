from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path
from tempfile import TemporaryDirectory

import networkx as nx
import pandas as pd
import pytest

from atlas.paper_dataset import (
    PaperDataset,
    load_paper_dataset,
    normalize_output_label,
    normalize_paper_label,
)
from atlas.support_tracer_backend import (
    build_or_refresh_database,
    query_by_item,
    query_by_output,
    query_by_paper,
)
from atlas.truth_table import (
    build_truth_table,
    build_truth_table_from_tables,
    load_truth_table,
)


# ---------------------------------------------------------------------------
# Helpers to build synthetic CSV data
# ---------------------------------------------------------------------------

def _write_items_csv(marker_dir: Path, rows: list[dict]) -> None:
    pd.DataFrame(rows).to_csv(marker_dir / "derived_marker_items.csv", index=False)


def _write_outputs_csv(marker_dir: Path, rows: list[dict]) -> None:
    pd.DataFrame(rows).to_csv(marker_dir / "derived_marker_outputs.csv", index=False)


def _write_item_output_links_csv(marker_dir: Path, rows: list[dict]) -> None:
    pd.DataFrame(rows).to_csv(
        marker_dir / "derived_marker_item_output_links.csv", index=False
    )


def _write_paper_item_links_csv(marker_dir: Path, rows: list[dict]) -> None:
    pd.DataFrame(rows).to_csv(
        marker_dir / "derived_marker_paper_item_links.csv", index=False
    )


def _write_paper_output_links_csv(marker_dir: Path, rows: list[dict]) -> None:
    pd.DataFrame(rows).to_csv(
        marker_dir / "derived_marker_paper_output_links.csv", index=False
    )


ITEM_5 = {
    "run": "ATCM99_full_pagewise",
    "sequence_id": 1,
    "sequence_type": "atcm",
    "item_num": "5",
    "item_title": "General Matters",
    "heading_source": "markdown_heading",
    "start_page": 10,
    "end_page": 12,
    "start_line": 1,
    "end_line": 40,
    "text": "Item 5 text with WP-48 and Decision 1 (2099)",
    "has_papers": True,
    "has_outputs": True,
}

ITEM_6 = {
    "run": "ATCM99_full_pagewise",
    "sequence_id": 1,
    "sequence_type": "atcm",
    "item_num": "6",
    "item_title": "Other Business",
    "heading_source": "markdown_heading",
    "start_page": 13,
    "end_page": 14,
    "start_line": 1,
    "end_line": 20,
    "text": "Item 6 text with IP-11 and Resolution 2 (2099)",
    "has_papers": True,
    "has_outputs": True,
}

OUTPUT_DECISION_1 = {
    "run": "ATCM99_full_pagewise",
    "sequence_id": None,
    "sequence_type": None,
    "item_num": None,
    "output_type": "decision",
    "output_number": 1,
    "output_year": 2099,
    "output_label": "Decision 1 (2099)",
    "title": "Synthetic decision",
    "evidence": "The Meeting adopted Decision 1 (2099).",
    "extraction_basis": "contents",
}

OUTPUT_RESOLUTION_2 = {
    "run": "ATCM99_full_pagewise",
    "sequence_id": None,
    "sequence_type": None,
    "item_num": None,
    "output_type": "resolution",
    "output_number": 2,
    "output_year": 2099,
    "output_label": "Resolution 2 (2099)",
    "title": "Synthetic resolution",
    "evidence": "The Meeting adopted Resolution 2 (2099).",
    "extraction_basis": "contents",
}

LINK_ITEM5_DECISION1 = {
    "run": "ATCM99_full_pagewise",
    "sequence_id": 1,
    "sequence_type": "atcm",
    "item_num": "5",
    "output_type": "decision",
    "output_number": 1,
    "output_year": 2099,
    "output_label": "Decision 1 (2099)",
    "title": "Synthetic decision",
    "evidence": "Paragraph adopted Decision 1 (2099).",
    "link_confidence": "high",
    "link_basis": "adoption_line",
}

LINK_ITEM6_RESOLUTION2 = {
    "run": "ATCM99_full_pagewise",
    "sequence_id": 1,
    "sequence_type": "atcm",
    "item_num": "6",
    "output_type": "resolution",
    "output_number": 2,
    "output_year": 2099,
    "output_label": "Resolution 2 (2099)",
    "title": "Synthetic resolution",
    "evidence": "Paragraph adopted Resolution 2 (2099).",
    "link_confidence": "high",
    "link_basis": "adoption_line",
}

PAPER_WP48_ITEM5 = {
    "run": "ATCM99_full_pagewise",
    "sequence_id": 1,
    "sequence_type": "atcm",
    "item_num": "5",
    "item_title": "General Matters",
    "paper_kind": "WP",
    "paper_number": 48,
    "paper_rev": None,
    "paper_label": "WP-48",
    "link_basis": "same_item_text",
    "link_confidence": "high",
}

PAPER_IP11_ITEM6 = {
    "run": "ATCM99_full_pagewise",
    "sequence_id": 1,
    "sequence_type": "atcm",
    "item_num": "6",
    "item_title": "Other Business",
    "paper_kind": "IP",
    "paper_number": 11,
    "paper_rev": None,
    "paper_label": "IP-11",
    "link_basis": "same_item_text",
    "link_confidence": "high",
}

PAPER_WP48_DECISION1 = {
    "run": "ATCM99_full_pagewise",
    "sequence_id": 1,
    "sequence_type": "atcm",
    "item_num": "5",
    "item_title": "General Matters",
    "paper_kind": "WP",
    "paper_number": 48,
    "paper_rev": None,
    "paper_label": "WP-48",
    "output_type": "decision",
    "output_number": 1,
    "output_year": 2099,
    "output_label": "Decision 1 (2099)",
    "output_title": "Synthetic decision",
    "link_basis": "paper_to_item_plus_item_to_output",
    "link_confidence": "medium",
    "evidence": "Paragraph adopted Decision 1 (2099).",
}


def _write_classifications_jsonl(path: Path, entries: list[dict]) -> None:
    path.write_text(
        "\n".join(json.dumps(e, ensure_ascii=False) for e in entries) + "\n"
    )


CLASSIFICATION_DUAL_LINK = {
    "item_id": "ATCM99_full_pagewise::1::atcm::5::10::1",
    "item": {
        "run": "ATCM99_full_pagewise",
        "sequence_id": 1,
        "sequence_type": "atcm",
        "item_num": "5",
        "item_title": "General Matters",
        "text": "WP-48 directly supported Decision 1 (2099).",
    },
    "llm_result": {
        "item_reason": "Synthetic direct link",
        "paper_output_links": [
            {
                "paper_label": "WP-48",
                "output_label": "Decision 1 (2099)",
                "relation_type": "supports",
                "evidence_basis": "explicit_direct",
                "confidence": "high",
                "reason": "Direct support statement",
                "evidence": "WP-48 directly supported Decision 1 (2099).",
            },
            {
                "paper_label": "WP-49",
                "output_label": "Decision 1 (2099)",
                "relation_type": "discusses",
                "evidence_basis": "joint_discussion",
                "confidence": "low",
                "reason": "Weak co-discussion only",
                "evidence": "WP-49 was discussed jointly.",
            },
        ],
    },
    "classified_at": "2026-01-01T00:00:00+00:00",
    "model": "test-model",
}

CLASSIFICATION_SINGLE_LINK = {
    "item_id": "ATCM99_full_pagewise::1::atcm::5::10::1",
    "item": {
        "run": "ATCM99_full_pagewise",
        "sequence_id": 1,
        "sequence_type": "atcm",
        "item_num": "5",
        "item_title": "General Matters",
        "text": "WP-48 directly supported Decision 1 (2099).",
    },
    "llm_result": {
        "item_reason": "Synthetic direct link",
        "paper_output_links": [
            {
                "paper_label": "WP-48",
                "output_label": "Decision 1 (2099)",
                "relation_type": "supports",
                "evidence_basis": "explicit_direct",
                "confidence": "high",
                "reason": "Direct support statement",
                "evidence": "WP-48 directly supported Decision 1 (2099).",
            }
        ],
    },
    "classified_at": "2026-01-01T00:00:00+00:00",
    "model": "test-model",
}


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def support_tracer_db(tmp_path):
    marker_dir = tmp_path / "marker_runs"
    parquet_dir = marker_dir / "support_tracer_parquet"
    db_path = marker_dir / "support_tracer.duckdb"
    marker_dir.mkdir(parents=True)

    _write_items_csv(marker_dir, [ITEM_5, ITEM_6])
    _write_outputs_csv(marker_dir, [OUTPUT_DECISION_1, OUTPUT_RESOLUTION_2])
    _write_item_output_links_csv(marker_dir, [LINK_ITEM5_DECISION1, LINK_ITEM6_RESOLUTION2])
    _write_paper_item_links_csv(marker_dir, [PAPER_WP48_ITEM5, PAPER_IP11_ITEM6])
    _write_paper_output_links_csv(marker_dir, [PAPER_WP48_DECISION1])
    pd.DataFrame(
        [
            {
                "run": "ATCM99_full_pagewise",
                "pages": 2,
                "nonempty_pages": 2,
                "nonempty_rate": 1.0,
                "items": 2,
                "outputs": 2,
                "papers": 2,
                "sequence_summary": "[]",
                "magic_and_assumptions": "{}",
            }
        ]
    ).to_csv(marker_dir / "derived_marker_run_audit.csv", index=False)

    build_or_refresh_database(
        marker_dir=marker_dir, parquet_dir=parquet_dir, db_path=db_path
    )
    return db_path


@pytest.fixture()
def truth_table_env(tmp_path):
    marker_dir = tmp_path / "marker_runs"
    marker_dir.mkdir(parents=True)
    classifications_path = marker_dir / "item_llm_links.jsonl"

    _write_items_csv(marker_dir, [ITEM_5])
    _write_outputs_csv(marker_dir, [OUTPUT_DECISION_1])
    _write_item_output_links_csv(marker_dir, [LINK_ITEM5_DECISION1])
    _write_paper_item_links_csv(marker_dir, [PAPER_WP48_ITEM5])
    _write_paper_output_links_csv(marker_dir, [PAPER_WP48_DECISION1])
    _write_classifications_jsonl(classifications_path, [CLASSIFICATION_DUAL_LINK])

    return marker_dir, classifications_path


@pytest.fixture()
def pipeline_env(tmp_path):
    repo_root = tmp_path / "apparent_consensus"
    atlas_dir = repo_root / "ATLAS"
    marker_dir = repo_root / "marker_runs"
    pipelines_dir = repo_root / "pipelines"
    pipeline_path = pipelines_dir / "build_truth_table.py"
    classifications_path = marker_dir / "item_llm_links.jsonl"

    marker_dir.mkdir(parents=True)
    pipelines_dir.mkdir(parents=True)
    atlas_dir.symlink_to(
        Path(__file__).resolve().parents[1], target_is_directory=True
    )

    pipeline_path.write_text(
        """\
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _default_atlas_dir() -> Path:
    return _repo_root() / "ATLAS"


def _default_output_path() -> Path:
    return _repo_root() / "marker_runs" / "truth_table.parquet"


def _default_manifest_path() -> Path:
    return _repo_root() / "marker_runs" / "truth_table_manifest.json"


def _default_csv_output_path() -> Path:
    return _repo_root() / "marker_runs" / "truth_table.csv"


def _default_marker_dir() -> Path:
    return _repo_root() / "marker_runs"


def _default_classifications_path() -> Path:
    return _default_marker_dir() / "item_llm_links.jsonl"


def _ensure_atlas_on_path(atlas_dir: Path) -> None:
    atlas_str = str(atlas_dir)
    if atlas_str not in sys.path:
        sys.path.insert(0, atlas_str)


def _load_atlas_truth_table_module(atlas_dir: Path):
    _ensure_atlas_on_path(atlas_dir)
    try:
        from atlas import truth_table as tt  # type: ignore
    except Exception as exc:
        raise RuntimeError(
            "Failed to import `atlas.truth_table`. Ensure `ATLAS` is present and dependencies are installed."
        ) from exc
    return tt


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build the canonical truth table through apparent_consensus orchestration."
    )
    parser.add_argument("--atlas-dir", type=Path, default=_default_atlas_dir())
    parser.add_argument("--marker-dir", type=Path, default=_default_marker_dir())
    parser.add_argument(
        "--classifications-path", type=Path, default=_default_classifications_path()
    )
    parser.add_argument("--output-path", type=Path, default=_default_output_path())
    parser.add_argument("--manifest-path", type=Path, default=_default_manifest_path())
    parser.add_argument("--csv-output-path", type=Path, default=_default_csv_output_path())
    parser.add_argument("--exclude-llm-links", action="store_true")
    parser.add_argument("--debug-output-dir", type=Path, default=None)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    tt = _load_atlas_truth_table_module(args.atlas_dir)
    result = tt.build_truth_table(
        marker_dir=args.marker_dir,
        classifications_path=args.classifications_path,
        output_path=args.output_path,
        manifest_path=args.manifest_path,
        csv_output_path=args.csv_output_path,
        include_llm_links=not args.exclude_llm_links,
        debug_output_dir=args.debug_output_dir,
    )
    print(json.dumps(result.to_dict(), ensure_ascii=False))


if __name__ == "__main__":
    main()
"""
    )

    _write_items_csv(marker_dir, [ITEM_5])
    _write_outputs_csv(marker_dir, [OUTPUT_DECISION_1])
    _write_item_output_links_csv(marker_dir, [LINK_ITEM5_DECISION1])
    _write_paper_item_links_csv(marker_dir, [PAPER_WP48_ITEM5])
    _write_paper_output_links_csv(marker_dir, [PAPER_WP48_DECISION1])
    _write_classifications_jsonl(classifications_path, [CLASSIFICATION_SINGLE_LINK])

    return {
        "atlas_dir": atlas_dir,
        "marker_dir": marker_dir,
        "pipeline_path": pipeline_path,
        "classifications_path": classifications_path,
        "truth_table_path": marker_dir / "truth_table.parquet",
        "truth_manifest_path": marker_dir / "truth_table_manifest.json",
        "truth_csv_path": marker_dir / "truth_table.csv",
    }


@pytest.fixture()
def paper_dataset_jsonl(tmp_path):
    jsonl_path = tmp_path / "item_llm_links.jsonl"
    rows = [
        {
            "item_id": "ATCM27_full_pagewise::1::atcm::5::10::1",
            "item": {
                "run": "ATCM27_full_pagewise",
                "sequence_id": 1,
                "sequence_type": "atcm",
                "item_num": "5",
                "item_title": "General Matters",
                "text": "Discussion of WP-01 and ATCM XXVIII WP-02 leading to Decision 1 (2004).",
            },
            "llm_result": {
                "item_reason": "Synthetic test row",
                "paper_output_links": [
                    {
                        "paper_label": "WP-01",
                        "output_label": "decision 1 (2004)",
                        "relation_type": "supports",
                        "evidence_basis": "explicit_direct",
                        "confidence": "high",
                        "reason": "Directly linked",
                        "evidence": "WP-01 supported Decision 1 (2004).",
                    },
                    {
                        "paper_label": "ATCM XXVIII WP-02",
                        "output_label": "Decision 1 (2004)",
                        "relation_type": "discusses",
                        "evidence_basis": "local_episode",
                        "confidence": "medium",
                        "reason": "Cross-meeting reference",
                        "evidence": "ATCM XXVIII WP-02 was referenced.",
                    },
                    {
                        "paper_label": "WP-99",
                        "output_label": "Decision 1 (2004)",
                        "relation_type": "unclear",
                        "evidence_basis": "joint_discussion",
                        "confidence": "low",
                        "reason": "Should be skipped unless explicitly included",
                        "evidence": "Joint discussion only.",
                    },
                ],
            },
            "classified_at": "2026-01-01T00:00:00+00:00",
            "model": "test-model",
        },
        {
            "item_id": "ATCM28_full_pagewise::1::atcm::5::20::1",
            "item": {
                "run": "ATCM28_full_pagewise",
                "sequence_id": 1,
                "sequence_type": "atcm",
                "item_num": "5",
                "item_title": "General Matters",
                "text": "WP-1 informed Resolution 2 (2005).",
            },
            "llm_result": {
                "item_reason": "Synthetic second row",
                "paper_output_links": [
                    {
                        "paper_label": "WP-1",
                        "output_label": "Resolution 2 (2005)",
                        "relation_type": "informs",
                        "evidence_basis": "explicit_direct",
                        "confidence": "high",
                        "reason": "Directly linked",
                        "evidence": "WP-1 informed Resolution 2 (2005).",
                    }
                ],
            },
            "classified_at": "2026-01-02T00:00:00+00:00",
            "model": "test-model",
        },
    ]
    jsonl_path.write_text(
        "\n".join(json.dumps(row, ensure_ascii=False) for row in rows) + "\n"
    )
    return jsonl_path


# ---------------------------------------------------------------------------
# Support tracer retrieval tests
# ---------------------------------------------------------------------------

def test_query_by_output(support_tracer_db):
    result = query_by_output("Decision 1 (2099)", db_path=support_tracer_db)
    assert len(result["outputs"]) == 1
    assert len(result["supporting_items"]) == 1
    assert result["supporting_items"][0]["item_num"] == "5"
    assert len(result["supporting_papers"]) == 1
    assert result["supporting_papers"][0]["paper_label"] == "WP-48"


def test_query_by_item(support_tracer_db):
    result = query_by_item(
        "ATCM99_full_pagewise", "6", sequence_type="atcm", db_path=support_tracer_db
    )
    assert len(result["items"]) == 1
    assert result["items"][0]["item_title"] == "Other Business"
    assert len(result["outputs"]) == 1
    assert result["outputs"][0]["output_label"] == "Resolution 2 (2099)"
    assert len(result["papers"]) == 1
    assert result["papers"][0]["paper_label"] == "IP-11"


def test_query_by_paper(support_tracer_db):
    result = query_by_paper("WP-48", db_path=support_tracer_db)
    assert len(result["paper_items"]) == 1
    assert result["paper_items"][0]["item_num"] == "5"
    assert len(result["paper_outputs"]) == 1
    assert result["paper_outputs"][0]["output_label"] == "Decision 1 (2099)"


# ---------------------------------------------------------------------------
# Truth table plan tests
# ---------------------------------------------------------------------------

def test_plan_exists():
    plan_path = Path(__file__).resolve().parents[2] / "ATLAS" / "singular_truth_table_plan.md"
    assert plan_path.exists()


def test_plan_has_checkbox_section():
    plan_path = Path(__file__).resolve().parents[2] / "ATLAS" / "singular_truth_table_plan.md"
    text = plan_path.read_text()
    assert "## Immediate implementation tasks" in text
    assert "- [x] Create a canonical truth-table schema note" in text
    assert "- [x] Add dedicated truth-table builder module in `ATLAS`" in text
    assert "- [x] Add `build-truth-table` CLI entry point" in text
    assert "- [x] Refactor backend queries to read from truth-table-backed views" in text
    assert "- [x] Add in-memory-first builder API entry points (`build_truth_table_from_tables` etc.)" in text
    assert "- [x] Make intermediate artifact writes debug-only and opt-in" in text
    assert "- [x] Mark old direct-analysis linkage tables as deprecated" in text


# ---------------------------------------------------------------------------
# Truth table build tests
# ---------------------------------------------------------------------------

def test_build_truth_table_writes_outputs(truth_table_env):
    marker_dir, classifications_path = truth_table_env
    truth_table_path = marker_dir / "truth_table.parquet"
    manifest_path = marker_dir / "truth_table_manifest.json"
    csv_path = marker_dir / "truth_table.csv"

    result = build_truth_table(
        marker_dir=marker_dir,
        classifications_path=classifications_path,
        output_path=truth_table_path,
        manifest_path=manifest_path,
        csv_output_path=csv_path,
    )
    assert result.truth_table_path == truth_table_path
    assert result.manifest_path == manifest_path
    assert truth_table_path.exists()
    assert manifest_path.exists()
    assert csv_path.exists()


def test_truth_table_contains_deterministic_and_llm_rows(truth_table_env):
    marker_dir, classifications_path = truth_table_env
    build_truth_table(
        marker_dir=marker_dir,
        classifications_path=classifications_path,
        output_path=marker_dir / "truth_table.parquet",
        manifest_path=marker_dir / "truth_table_manifest.json",
        csv_output_path=marker_dir / "truth_table.csv",
    )
    truth_df = load_truth_table(marker_dir / "truth_table.parquet")
    link_types = set(truth_df["link_path_type"])
    parser_sources = set(truth_df["parser_source"])
    assert "paper_item" in link_types
    assert "item_output" in link_types
    assert "paper_item_output" in link_types
    assert "deterministic_parser" in parser_sources
    assert "llm_item_classifier" in parser_sources


def test_truth_table_flags_joint_discussion_as_non_default(truth_table_env):
    marker_dir, classifications_path = truth_table_env
    build_truth_table(
        marker_dir=marker_dir,
        classifications_path=classifications_path,
        output_path=marker_dir / "truth_table.parquet",
        manifest_path=marker_dir / "truth_table_manifest.json",
        csv_output_path=marker_dir / "truth_table.csv",
    )
    truth_df = load_truth_table(marker_dir / "truth_table.parquet")
    joint_rows = truth_df.loc[truth_df["paper_label"] == "WP-49"]
    assert len(joint_rows) == 1
    assert bool(joint_rows.iloc[0]["is_joint_discussion"])
    assert not bool(joint_rows.iloc[0]["is_default_analysis_row"])


def test_truth_table_normalizes_keys(truth_table_env):
    marker_dir, classifications_path = truth_table_env
    build_truth_table(
        marker_dir=marker_dir,
        classifications_path=classifications_path,
        output_path=marker_dir / "truth_table.parquet",
        manifest_path=marker_dir / "truth_table_manifest.json",
        csv_output_path=marker_dir / "truth_table.csv",
    )
    truth_df = load_truth_table(marker_dir / "truth_table.parquet")
    direct_rows = truth_df.loc[
        (truth_df["paper_label"] == "WP-48")
        & (truth_df["parser_source"] == "llm_item_classifier")
    ]
    assert len(direct_rows) == 1
    row = direct_rows.iloc[0]
    assert row["meeting_key"] == "ATCM99"
    assert row["item_key"] == "ATCM99:item5"
    assert row["paper_key"] == "ATCM99:WP-48"
    assert row["output_key"] == "ATCM99:Decision 1 (2099)"
    assert row["paper_output_link_type"] == "direct"


def test_build_truth_table_from_tables_matches_file_backed_build(truth_table_env):
    marker_dir, classifications_path = truth_table_env
    tables = {
        "items": pd.read_csv(marker_dir / "derived_marker_items.csv"),
        "outputs": pd.read_csv(marker_dir / "derived_marker_outputs.csv"),
        "item_output_links": pd.read_csv(
            marker_dir / "derived_marker_item_output_links.csv"
        ),
        "paper_item_links": pd.read_csv(
            marker_dir / "derived_marker_paper_item_links.csv"
        ),
        "paper_output_links": pd.read_csv(
            marker_dir / "derived_marker_paper_output_links.csv"
        ),
    }
    out_path = marker_dir / "truth_table_in_memory.parquet"
    manifest_path = marker_dir / "truth_table_in_memory_manifest.json"
    csv_path = marker_dir / "truth_table_in_memory.csv"

    result = build_truth_table_from_tables(
        tables=tables,
        marker_dir=marker_dir,
        classifications_path=classifications_path,
        output_path=out_path,
        manifest_path=manifest_path,
        csv_output_path=csv_path,
    )
    assert result.truth_table_path == out_path
    assert result.manifest_path == manifest_path
    assert out_path.exists()
    assert manifest_path.exists()
    assert csv_path.exists()

    truth_df = load_truth_table(out_path)
    link_types = set(truth_df["link_path_type"])
    parser_sources = set(truth_df["parser_source"])
    assert "paper_item" in link_types
    assert "item_output" in link_types
    assert "paper_item_output" in link_types
    assert "deterministic_parser" in parser_sources
    assert "llm_item_classifier" in parser_sources


def test_build_truth_table_debug_outputs_are_opt_in(truth_table_env):
    marker_dir, classifications_path = truth_table_env
    debug_dir = marker_dir / "debug_truth_inputs"

    result = build_truth_table(
        marker_dir=marker_dir,
        classifications_path=classifications_path,
        output_path=marker_dir / "truth_table.parquet",
        manifest_path=marker_dir / "truth_table_manifest.json",
        csv_output_path=marker_dir / "truth_table.csv",
    )
    assert not debug_dir.exists()
    assert result.debug_artifact_paths == {}

    debug_result = build_truth_table(
        marker_dir=marker_dir,
        classifications_path=classifications_path,
        output_path=marker_dir / "truth_table_debug.parquet",
        manifest_path=marker_dir / "truth_table_debug_manifest.json",
        csv_output_path=marker_dir / "truth_table_debug.csv",
        debug_output_dir=debug_dir,
    )
    assert debug_result.debug_artifact_paths is not None
    debug_artifact_paths = debug_result.debug_artifact_paths or {}
    assert debug_artifact_paths["items"] == str(debug_dir / "derived_marker_items.csv")
    assert (debug_dir / "derived_marker_items.csv").exists()
    assert (debug_dir / "derived_marker_outputs.csv").exists()
    assert (debug_dir / "derived_marker_item_output_links.csv").exists()
    assert (debug_dir / "derived_marker_paper_item_links.csv").exists()
    assert (debug_dir / "derived_marker_paper_output_links.csv").exists()


# ---------------------------------------------------------------------------
# Pipeline integration tests
# ---------------------------------------------------------------------------

def _run_pipeline(env, extra_args=None):
    cmd = [
        sys.executable,
        str(env["pipeline_path"]),
        "--atlas-dir", str(env["atlas_dir"]),
        "--marker-dir", str(env["marker_dir"]),
        "--classifications-path", str(env["classifications_path"]),
        "--output-path", str(env["truth_table_path"]),
        "--manifest-path", str(env["truth_manifest_path"]),
        "--csv-output-path", str(env["truth_csv_path"]),
    ]
    if extra_args:
        cmd.extend(extra_args)
    return subprocess.run(cmd, check=True, capture_output=True, text=True)


def test_pipeline_builds_truth_table_via_atlas_library(pipeline_env):
    proc = _run_pipeline(pipeline_env)
    payload = json.loads(proc.stdout)
    assert payload["truth_table_path"] == str(pipeline_env["truth_table_path"])
    assert payload["manifest_path"] == str(pipeline_env["truth_manifest_path"])
    assert pipeline_env["truth_table_path"].exists()
    assert pipeline_env["truth_manifest_path"].exists()
    assert pipeline_env["truth_csv_path"].exists()

    truth_df = load_truth_table(pipeline_env["truth_table_path"])
    assert "paper_item_output" in set(truth_df["link_path_type"])
    assert "llm_item_classifier" in set(truth_df["parser_source"])


def test_pipeline_debug_output_is_opt_in(pipeline_env):
    debug_dir = pipeline_env["marker_dir"] / "debug_truth_inputs"

    _run_pipeline(pipeline_env)
    assert not debug_dir.exists()

    proc = _run_pipeline(pipeline_env, ["--debug-output-dir", str(debug_dir)])
    payload = json.loads(proc.stdout)
    assert payload["debug_artifact_paths"]["items"] == str(
        debug_dir / "derived_marker_items.csv"
    )
    assert (debug_dir / "derived_marker_items.csv").exists()
    assert (debug_dir / "derived_marker_outputs.csv").exists()
    assert (debug_dir / "derived_marker_item_output_links.csv").exists()
    assert (debug_dir / "derived_marker_paper_item_links.csv").exists()
    assert (debug_dir / "derived_marker_paper_output_links.csv").exists()


# ---------------------------------------------------------------------------
# Paper dataset tests
# ---------------------------------------------------------------------------

def test_normalize_paper_label_variants():
    norm = normalize_paper_label("WP-1")
    assert norm is not None
    assert norm.canonical_label == "WP-1"
    assert norm.meeting_aware_label == "WP-1"
    assert norm.paper_kind == "WP"
    assert norm.paper_number == 1
    assert norm.meeting_number is None

    norm = normalize_paper_label("WP-01")
    assert norm is not None
    assert norm.canonical_label == "WP-1"

    norm = normalize_paper_label("wp 001")
    assert norm is not None
    assert norm.canonical_label == "WP-1"

    norm = normalize_paper_label("ATCMXXVIIWP-01")
    assert norm is not None
    assert norm.canonical_label == "WP-1"
    assert norm.meeting_number == 27
    assert norm.meeting_aware_label == "ATCM27:WP-1"

    norm = normalize_paper_label("ATCM XXVII WP 01")
    assert norm is not None
    assert norm.meeting_number == 27
    assert norm.meeting_aware_label == "ATCM27:WP-1"

    norm = normalize_paper_label("ATCM27/WP-01")
    assert norm is not None
    assert norm.meeting_number == 27
    assert norm.meeting_aware_label == "ATCM27:WP-1"

    norm = normalize_paper_label("WP-01", default_meeting_number=28)
    assert norm is not None
    assert norm.meeting_number == 28
    assert norm.meeting_aware_label == "ATCM28:WP-1"


def test_normalize_output_label():
    norm = normalize_output_label("decision 1 (2004)")
    assert norm is not None
    assert norm.canonical_label == "Decision 1 (2004)"
    assert norm.meeting_aware_label == "Decision 1 (2004)"
    assert norm.output_type == "Decision"
    assert norm.output_number == 1
    assert norm.output_year == 2004

    norm = normalize_output_label("Resolution 02 (2005)", default_meeting_number=28)
    assert norm is not None
    assert norm.canonical_label == "Resolution 2 (2005)"
    assert norm.meeting_aware_label == "ATCM28:Resolution 2 (2005)"
    assert norm.output_type == "Resolution"
    assert norm.output_number == 2
    assert norm.output_year == 2005

    norm = normalize_output_label("Measure 7 (2011)")
    assert norm is not None
    assert norm.canonical_label == "Measure 7 (2011)"
    assert norm.meeting_aware_label == "Measure 7 (2011)"


def test_load_paper_dataset(paper_dataset_jsonl):
    dataset = load_paper_dataset(paper_dataset_jsonl)
    assert isinstance(dataset, PaperDataset)
    assert dataset.source_path == paper_dataset_jsonl
    assert len(dataset.links) == 3


def test_joint_discussion_is_excluded_by_default(paper_dataset_jsonl):
    dataset = load_paper_dataset(paper_dataset_jsonl)
    assert "WP-99" not in set(dataset.links["paper_label"])


def test_joint_discussion_can_be_included_with_flag(paper_dataset_jsonl):
    dataset = PaperDataset.from_jsonl(paper_dataset_jsonl, include_joint_discussion=True)
    assert len(dataset.links) == 4
    assert "WP-99" in set(dataset.links["paper_label"])
    joint_rows = dataset.links.loc[dataset.links["paper_label"] == "WP-99"]
    assert len(joint_rows) == 1
    assert joint_rows.iloc[0]["evidence_basis"] == "joint_discussion"
    assert joint_rows.iloc[0]["output_key"] == "ATCM27:Decision 1 (2004)"


def test_papers_property(paper_dataset_jsonl):
    dataset = load_paper_dataset(paper_dataset_jsonl)
    papers = dataset.papers
    assert len(papers) == 3
    assert set(papers["paper_key"]) == {"ATCM27:WP-1", "ATCM28:WP-1", "ATCM28:WP-2"}


def test_meetings_property(paper_dataset_jsonl):
    dataset = load_paper_dataset(paper_dataset_jsonl)
    meetings = dataset.meetings
    assert list(meetings["meeting_number"]) == [27, 28]
    assert meetings.set_index("meeting_number").loc[27, "paper_count"] == 2
    assert meetings.set_index("meeting_number").loc[28, "paper_count"] == 1


def test_links_for_meeting(paper_dataset_jsonl):
    dataset = load_paper_dataset(paper_dataset_jsonl)
    meeting_links = dataset.links_for_meeting(28)
    assert len(meeting_links) == 1
    assert set(meeting_links["paper_key"]) == {"ATCM28:WP-1"}


def test_links_for_paper_without_meeting(paper_dataset_jsonl):
    dataset = load_paper_dataset(paper_dataset_jsonl)
    paper_links = dataset.links_for_paper("WP-1")
    assert len(paper_links) == 2
    assert set(paper_links["meeting_number"]) == {27, 28}


def test_links_for_paper_with_meeting(paper_dataset_jsonl):
    dataset = load_paper_dataset(paper_dataset_jsonl)
    paper_links = dataset.links_for_paper("WP-01", meeting_number=27)
    assert len(paper_links) == 1
    assert paper_links.iloc[0]["paper_key"] == "ATCM27:WP-1"
    assert paper_links.iloc[0]["output_label"] == "Decision 1 (2004)"
    assert paper_links.iloc[0]["output_key"] == "ATCM27:Decision 1 (2004)"


def test_outputs_for_paper(paper_dataset_jsonl):
    dataset = load_paper_dataset(paper_dataset_jsonl)
    outputs = dataset.outputs_for_paper("WP-1", meeting_number=28)
    assert len(outputs) == 1
    assert outputs.iloc[0]["paper_key"] == "ATCM28:WP-1"
    assert outputs.iloc[0]["output_label"] == "Resolution 2 (2005)"
    assert outputs.iloc[0]["output_key"] == "ATCM28:Resolution 2 (2005)"


def test_papers_for_output(paper_dataset_jsonl):
    dataset = load_paper_dataset(paper_dataset_jsonl)
    papers = dataset.papers_for_output("decision 1 (2004)")
    assert len(papers) == 2
    assert set(papers["paper_key"]) == {"ATCM27:WP-1", "ATCM28:WP-2"}
    assert set(papers["output_label"]) == {"Decision 1 (2004)"}
    assert set(papers["output_key"]) == {"ATCM27:Decision 1 (2004)"}


def test_adjacency(paper_dataset_jsonl):
    dataset = load_paper_dataset(paper_dataset_jsonl)
    adjacency = dataset.adjacency()
    assert len(adjacency) == 3
    assert set(adjacency["source"]) == {"ATCM27:WP-1", "ATCM28:WP-1", "ATCM28:WP-2"}
    assert set(adjacency["target"]) == {
        "ATCM27:Decision 1 (2004)",
        "ATCM28:Resolution 2 (2005)",
    }


def test_to_dict_summary(paper_dataset_jsonl):
    dataset = load_paper_dataset(paper_dataset_jsonl)
    summary = dataset.to_dict()
    assert summary["source_path"] == str(paper_dataset_jsonl)
    assert summary["meeting_count"] == 2
    assert summary["paper_count"] == 3
    assert summary["link_count"] == 3
    assert len(summary["meetings"]) == 2


def test_print_overview(paper_dataset_jsonl):
    dataset = load_paper_dataset(paper_dataset_jsonl)
    overview = str(dataset)
    assert isinstance(overview, str)
    assert "PaperDataset overview" in overview
    assert "source_path:" in overview
    assert "meetings: 2" in overview
    assert "papers: 3" in overview
    assert "links: 3" in overview
    assert "ATCM27: papers=2 outputs=1 links=2" in overview
    assert "ATCM28: papers=1 outputs=1 links=1" in overview


def test_to_graph_all_meetings(paper_dataset_jsonl):
    dataset = load_paper_dataset(paper_dataset_jsonl)
    graph = dataset.to_graph()
    assert isinstance(graph, nx.Graph)
    assert set(graph.nodes) == {
        "ATCM27:WP-1",
        "ATCM28:WP-1",
        "ATCM28:WP-2",
        "ATCM27:Decision 1 (2004)",
        "ATCM28:Resolution 2 (2005)",
    }
    assert graph.has_edge("ATCM27:WP-1", "ATCM27:Decision 1 (2004)")
    assert graph.has_edge("ATCM28:WP-1", "ATCM28:Resolution 2 (2005)")
    assert graph.has_edge("ATCM28:WP-2", "ATCM27:Decision 1 (2004)")


def test_to_graph_single_meeting_subset(paper_dataset_jsonl):
    dataset = load_paper_dataset(paper_dataset_jsonl)
    graph = dataset.to_graph(meeting_number=27)
    assert isinstance(graph, nx.Graph)
    assert set(graph.nodes) == {
        "ATCM27:WP-1",
        "ATCM28:WP-2",
        "ATCM27:Decision 1 (2004)",
    }
    assert graph.has_edge("ATCM27:WP-1", "ATCM27:Decision 1 (2004)")
    assert graph.has_edge("ATCM28:WP-2", "ATCM27:Decision 1 (2004)")
    assert not graph.has_node("ATCM28:WP-1")
    assert not graph.has_node("ATCM28:Resolution 2 (2005)")


def test_to_graph_multiple_meeting_subset(paper_dataset_jsonl):
    dataset = load_paper_dataset(paper_dataset_jsonl)
    graph = dataset.to_graph(meeting_number=[27, 28])
    assert isinstance(graph, nx.Graph)
    assert set(graph.nodes) == {
        "ATCM27:WP-1",
        "ATCM28:WP-1",
        "ATCM28:WP-2",
        "ATCM27:Decision 1 (2004)",
        "ATCM28:Resolution 2 (2005)",
    }
    assert graph.number_of_edges() == 3


def test_to_item_graph_all_meetings(paper_dataset_jsonl):
    dataset = load_paper_dataset(paper_dataset_jsonl)
    graph = dataset.to_item_graph()
    assert isinstance(graph, nx.DiGraph)
    assert set(graph.nodes) == {
        "ATCM27:item5",
        "ATCM28:item5",
        "ATCM27:WP-1",
        "ATCM28:WP-1",
        "ATCM28:WP-2",
        "ATCM27:Decision 1 (2004)",
        "ATCM28:Resolution 2 (2005)",
    }
    assert graph.has_edge("ATCM27:WP-1", "ATCM27:item5")
    assert graph.has_edge("ATCM28:WP-2", "ATCM27:item5")
    assert graph.has_edge("ATCM27:item5", "ATCM27:Decision 1 (2004)")
    assert graph.has_edge("ATCM28:WP-1", "ATCM28:item5")
    assert graph.has_edge("ATCM28:item5", "ATCM28:Resolution 2 (2005)")


def test_to_item_graph_single_meeting_subset(paper_dataset_jsonl):
    dataset = load_paper_dataset(paper_dataset_jsonl)
    graph = dataset.to_item_graph(meeting_number=27)
    assert isinstance(graph, nx.DiGraph)
    assert set(graph.nodes) == {
        "ATCM27:item5",
        "ATCM27:WP-1",
        "ATCM28:WP-2",
        "ATCM27:Decision 1 (2004)",
    }
    assert graph.has_edge("ATCM27:WP-1", "ATCM27:item5")
    assert graph.has_edge("ATCM28:WP-2", "ATCM27:item5")
    assert graph.has_edge("ATCM27:item5", "ATCM27:Decision 1 (2004)")
    assert not graph.has_node("ATCM28:item5")
    assert not graph.has_node("ATCM28:WP-1")


def test_to_item_graph_cross_meeting_flow(paper_dataset_jsonl):
    dataset = load_paper_dataset(paper_dataset_jsonl)
    graph = dataset.to_item_graph()
    assert graph.has_edge("ATCM28:WP-2", "ATCM27:item5")
    edge_data = graph.get_edge_data("ATCM28:WP-2", "ATCM27:item5")
    assert edge_data is not None
    assert edge_data["edge_type"] == "paper_to_item"
    assert edge_data["meeting_number"] == 27
    assert edge_data["paper_meeting_number"] == 28
    assert edge_data["item_meeting_number"] == 27
    assert not graph.has_edge("ATCM27:item5", "ATCM28:item5")


def test_to_item_graph_multiple_meeting_subset(paper_dataset_jsonl):
    dataset = load_paper_dataset(paper_dataset_jsonl)
    graph = dataset.to_item_graph(meeting_number=[27, 28])
    assert isinstance(graph, nx.DiGraph)
    assert set(graph.nodes) == {
        "ATCM27:item5",
        "ATCM28:item5",
        "ATCM27:WP-1",
        "ATCM28:WP-1",
        "ATCM28:WP-2",
        "ATCM27:Decision 1 (2004)",
        "ATCM28:Resolution 2 (2005)",
    }
    assert graph.number_of_edges() == 5
