#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

from .config import DEFAULT_DATA_REPO, ensure_data_dir, get_data_dir
from .manual_validation import DEFAULT_BUNDLE_PATH, export_validation_bundle
from .support_tracer_backend import (
    DB_PATH,
    MARKER_DIR,
    PARQUET_DIR,
    build_or_refresh_database,
    query_by_item,
    query_by_output,
    query_by_paper,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="CLI for tracing ATCM outputs, items, and papers.")
    sub = parser.add_subparsers(dest="command", required=True)

    init_data = sub.add_parser("init-data", help="Clone the ATLAS-data repo if the data directory is missing.")
    init_data.add_argument("--data-dir", type=Path, default=get_data_dir())
    init_data.add_argument("--repo-url", default=DEFAULT_DATA_REPO)

    build = sub.add_parser("build-db", help="Build / refresh Parquet tables and DuckDB views.")
    build.add_argument("--marker-dir", type=Path, default=MARKER_DIR)
    build.add_argument("--parquet-dir", type=Path, default=PARQUET_DIR)
    build.add_argument("--db-path", type=Path, default=DB_PATH)

    validation = sub.add_parser("export-validation", help="Export a JSON bundle for the manual validation HTML interface.")
    validation.add_argument("--output-path", type=Path, default=DEFAULT_BUNDLE_PATH)
    validation.add_argument("--marker-dir", type=Path, default=MARKER_DIR)
    validation.add_argument("--run", default=None)
    validation.add_argument(
        "--case-type",
        action="append",
        choices=["item_output", "paper_item", "paper_output"],
        dest="case_types",
        help="Repeat to include multiple case types. Defaults to all.",
    )
    validation.add_argument("--limit", type=int, default=None)

    q_out = sub.add_parser("output", help="Trace a formal output to items and papers.")
    q_out.add_argument("label")
    q_out.add_argument("--db-path", type=Path, default=DB_PATH)
    q_out.add_argument("--json", action="store_true")

    q_item = sub.add_parser("item", help="Trace an item to outputs and papers.")
    q_item.add_argument("run")
    q_item.add_argument("item_num")
    q_item.add_argument("--sequence-type", default=None)
    q_item.add_argument("--db-path", type=Path, default=DB_PATH)
    q_item.add_argument("--json", action="store_true")

    q_paper = sub.add_parser("paper", help="Trace a paper to items and outputs.")
    q_paper.add_argument("label")
    q_paper.add_argument("--db-path", type=Path, default=DB_PATH)
    q_paper.add_argument("--json", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.command == "init-data":
        data_dir = ensure_data_dir(args.data_dir, repo_url=args.repo_url, clone_if_missing=True)
        print(data_dir)
        return
    if args.command == "build-db":
        db_path = build_or_refresh_database(marker_dir=args.marker_dir, parquet_dir=args.parquet_dir, db_path=args.db_path)
        print(db_path)
        return
    if args.command == "export-validation":
        bundle_path = export_validation_bundle(
            output_path=args.output_path,
            marker_dir=args.marker_dir,
            run=args.run,
            case_types=tuple(args.case_types or ["item_output", "paper_item", "paper_output"]),
            limit=args.limit,
        )
        print(bundle_path)
        return
    if args.command == "output":
        result = query_by_output(args.label, db_path=args.db_path)
    elif args.command == "item":
        result = query_by_item(args.run, args.item_num, sequence_type=args.sequence_type, db_path=args.db_path)
    else:
        result = query_by_paper(args.label, db_path=args.db_path)

    print(json.dumps(result, ensure_ascii=False, indent=2) if args.json else json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
