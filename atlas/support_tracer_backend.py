#!/usr/bin/env python3
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

from .config import ensure_data_dir, get_data_dir, get_marker_dir
from .truth_table import DEFAULT_TRUTH_TABLE_PATH, build_truth_table

DATA_DIR = get_data_dir()
MARKER_DIR = get_marker_dir()
PARQUET_DIR = MARKER_DIR / "support_tracer_parquet"
DB_PATH = MARKER_DIR / "support_tracer.duckdb"
TRUTH_TABLE_PATH = DEFAULT_TRUTH_TABLE_PATH

TABLE_SPECS = {
    "items": "derived_marker_items.csv",
    "outputs": "derived_marker_outputs.csv",
    "item_output_links": "derived_marker_item_output_links.csv",
    "paper_item_links": "derived_marker_paper_item_links.csv",
    "paper_output_links": "derived_marker_paper_output_links.csv",
    "audit": "derived_marker_run_audit.csv",
}


def ensure_parquet_tables(
    marker_dir: Path = MARKER_DIR, parquet_dir: Path = PARQUET_DIR
) -> dict[str, Path]:
    ensure_data_dir(marker_dir.parent)
    parquet_dir.mkdir(parents=True, exist_ok=True)
    out: dict[str, Path] = {}
    for table_name, csv_name in TABLE_SPECS.items():
        csv_path = marker_dir / csv_name
        if not csv_path.exists():
            raise FileNotFoundError(f"Missing source table: {csv_path}")
        parquet_path = parquet_dir / f"{table_name}.parquet"
        df = pd.read_csv(csv_path)
        df.to_parquet(parquet_path, index=False)
        out[table_name] = parquet_path
    return out


def build_truth_table_for_backend(
    marker_dir: Path = MARKER_DIR,
    truth_table_path: Path = TRUTH_TABLE_PATH,
) -> Path:
    result = build_truth_table(
        marker_dir=marker_dir,
        output_path=truth_table_path,
    )
    return result.truth_table_path


def connect(db_path: Path = DB_PATH) -> duckdb.DuckDBPyConnection:
    ensure_data_dir(db_path.parent)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(db_path))


def build_or_refresh_database(
    marker_dir: Path = MARKER_DIR,
    parquet_dir: Path = PARQUET_DIR,
    db_path: Path = DB_PATH,
) -> Path:
    parquet_paths = ensure_parquet_tables(
        marker_dir=marker_dir, parquet_dir=parquet_dir
    )
    truth_table_path = build_truth_table_for_backend(marker_dir=marker_dir)
    con = connect(db_path)
    try:
        for table_name, parquet_path in parquet_paths.items():
            con.execute(f"DROP VIEW IF EXISTS {table_name}")
            con.execute(
                f"CREATE VIEW {table_name} AS SELECT * FROM read_parquet('{parquet_path.as_posix()}')"
            )

        con.execute("DROP VIEW IF EXISTS truth_table")
        con.execute(
            f"CREATE VIEW truth_table AS SELECT * FROM read_parquet('{truth_table_path.as_posix()}')"
        )

        con.execute("DROP VIEW IF EXISTS output_support")
        con.execute("""
            CREATE VIEW output_support AS
            SELECT DISTINCT
                run,
                sequence_id,
                sequence_type,
                item_num,
                item_title,
                output_type,
                output_number,
                output_year,
                output_label,
                output_title,
                evidence AS item_output_evidence,
                paper_output_confidence AS item_output_confidence
            FROM truth_table
            WHERE item_output_link_exists = TRUE
              AND paper_item_link_exists = FALSE
              AND output_label IS NOT NULL
            """)

        con.execute("DROP VIEW IF EXISTS paper_support")
        con.execute("""
            CREATE VIEW paper_support AS
            SELECT DISTINCT
                run,
                sequence_id,
                sequence_type,
                item_num,
                item_title,
                paper_kind,
                paper_number,
                paper_rev,
                paper_label,
                output_type,
                output_number,
                output_year,
                output_label,
                output_title,
                paper_output_confidence,
                evidence AS paper_output_evidence
            FROM truth_table
            WHERE paper_label IS NOT NULL
            """)
    finally:
        con.close()
    return db_path


def fetch_df(
    con: duckdb.DuckDBPyConnection, query: str, params: list[Any] | None = None
) -> pd.DataFrame:
    if params is None:
        params = []
    df = con.execute(query, params).df()
    for col in ["item_num", "sequence_type", "run", "paper_label", "output_label"]:
        if col in df.columns:
            df[col] = df[col].map(lambda x: None if pd.isna(x) else str(x))
    return df


def query_by_output(output_label: str, db_path: Path = DB_PATH) -> dict[str, Any]:
    con = connect(db_path)
    try:
        output_df = fetch_df(
            con,
            "SELECT * FROM outputs WHERE output_label = ? ORDER BY run, output_label",
            [output_label],
        )
        support_df = fetch_df(
            con,
            "SELECT * FROM output_support WHERE output_label = ? ORDER BY run, sequence_id, item_num",
            [output_label],
        )
        paper_df = fetch_df(
            con,
            "SELECT * FROM paper_support WHERE output_label = ? ORDER BY run, sequence_id, item_num, paper_kind, paper_number",
            [output_label],
        )
        return {
            "query_type": "output",
            "query": output_label,
            "outputs": output_df.to_dict(orient="records"),
            "supporting_items": support_df.to_dict(orient="records"),
            "supporting_papers": paper_df.to_dict(orient="records"),
        }
    finally:
        con.close()


def query_by_item(
    run: str, item_num: str, sequence_type: str | None = None, db_path: Path = DB_PATH
) -> dict[str, Any]:
    con = connect(db_path)
    try:
        if sequence_type is None:
            item_df = fetch_df(
                con,
                "SELECT * FROM items WHERE run = ? AND item_num = ? ORDER BY sequence_id",
                [run, item_num],
            )
            output_df = fetch_df(
                con,
                "SELECT * FROM item_output_links WHERE run = ? AND item_num = ? ORDER BY sequence_id, output_label",
                [run, item_num],
            )
            paper_df = fetch_df(
                con,
                "SELECT * FROM paper_item_links WHERE run = ? AND item_num = ? ORDER BY sequence_id, paper_kind, paper_number",
                [run, item_num],
            )
        else:
            item_df = fetch_df(
                con,
                "SELECT * FROM items WHERE run = ? AND item_num = ? AND sequence_type = ? ORDER BY sequence_id",
                [run, item_num, sequence_type],
            )
            output_df = fetch_df(
                con,
                "SELECT * FROM item_output_links WHERE run = ? AND item_num = ? AND sequence_type = ? ORDER BY sequence_id, output_label",
                [run, item_num, sequence_type],
            )
            paper_df = fetch_df(
                con,
                "SELECT * FROM paper_item_links WHERE run = ? AND item_num = ? AND sequence_type = ? ORDER BY sequence_id, paper_kind, paper_number",
                [run, item_num, sequence_type],
            )
        return {
            "query_type": "item",
            "query": {"run": run, "item_num": item_num, "sequence_type": sequence_type},
            "items": item_df.to_dict(orient="records"),
            "outputs": output_df.to_dict(orient="records"),
            "papers": paper_df.to_dict(orient="records"),
        }
    finally:
        con.close()


def query_by_paper(paper_label: str, db_path: Path = DB_PATH) -> dict[str, Any]:
    con = connect(db_path)
    try:
        item_df = fetch_df(
            con,
            "SELECT * FROM paper_item_links WHERE paper_label = ? ORDER BY run, sequence_id, item_num",
            [paper_label],
        )
        output_df = fetch_df(
            con,
            "SELECT * FROM paper_output_links WHERE paper_label = ? ORDER BY run, sequence_id, item_num, output_label",
            [paper_label],
        )
        return {
            "query_type": "paper",
            "query": paper_label,
            "paper_items": item_df.to_dict(orient="records"),
            "paper_outputs": output_df.to_dict(orient="records"),
        }
    finally:
        con.close()


def query_to_text(result: dict[str, Any]) -> str:
    lines = [
        json.dumps(
            {"query_type": result["query_type"], "query": result["query"]},
            ensure_ascii=False,
        )
    ]
    if result["query_type"] == "output":
        lines.append(
            f"outputs={len(result['outputs'])} items={len(result['supporting_items'])} papers={len(result['supporting_papers'])}"
        )
    elif result["query_type"] == "item":
        lines.append(
            f"items={len(result['items'])} outputs={len(result['outputs'])} papers={len(result['papers'])}"
        )
    else:
        lines.append(
            f"paper_items={len(result['paper_items'])} paper_outputs={len(result['paper_outputs'])}"
        )
    return "\n".join(lines)
