from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

import pandas as pd

from atlas.support_tracer_backend import (
    build_or_refresh_database,
    query_by_item,
    query_by_output,
    query_by_paper,
)


class SupportTracerRetrievalTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.marker_dir = self.root / "marker_runs"
        self.parquet_dir = self.marker_dir / "support_tracer_parquet"
        self.db_path = self.marker_dir / "support_tracer.duckdb"
        self.marker_dir.mkdir(parents=True, exist_ok=True)

        pd.DataFrame(
            [
                {
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
                },
                {
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
                },
            ]
        ).to_csv(self.marker_dir / "derived_marker_items.csv", index=False)

        pd.DataFrame(
            [
                {
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
                },
                {
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
                },
            ]
        ).to_csv(self.marker_dir / "derived_marker_outputs.csv", index=False)

        pd.DataFrame(
            [
                {
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
                },
                {
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
                },
            ]
        ).to_csv(self.marker_dir / "derived_marker_item_output_links.csv", index=False)

        pd.DataFrame(
            [
                {
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
                },
                {
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
                },
            ]
        ).to_csv(self.marker_dir / "derived_marker_paper_item_links.csv", index=False)

        pd.DataFrame(
            [
                {
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
            ]
        ).to_csv(self.marker_dir / "derived_marker_paper_output_links.csv", index=False)

        pd.DataFrame(
            [{
                "run": "ATCM99_full_pagewise",
                "pages": 2,
                "nonempty_pages": 2,
                "nonempty_rate": 1.0,
                "items": 2,
                "outputs": 2,
                "papers": 2,
                "sequence_summary": "[]",
                "magic_and_assumptions": "{}",
            }]
        ).to_csv(self.marker_dir / "derived_marker_run_audit.csv", index=False)

        build_or_refresh_database(marker_dir=self.marker_dir, parquet_dir=self.parquet_dir, db_path=self.db_path)

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def test_query_by_output(self) -> None:
        result = query_by_output("Decision 1 (2099)", db_path=self.db_path)
        self.assertEqual(len(result["outputs"]), 1)
        self.assertEqual(len(result["supporting_items"]), 1)
        self.assertEqual(result["supporting_items"][0]["item_num"], "5")
        self.assertEqual(len(result["supporting_papers"]), 1)
        self.assertEqual(result["supporting_papers"][0]["paper_label"], "WP-48")

    def test_query_by_item(self) -> None:
        result = query_by_item("ATCM99_full_pagewise", "6", sequence_type="atcm", db_path=self.db_path)
        self.assertEqual(len(result["items"]), 1)
        self.assertEqual(result["items"][0]["item_title"], "Other Business")
        self.assertEqual(len(result["outputs"]), 1)
        self.assertEqual(result["outputs"][0]["output_label"], "Resolution 2 (2099)")
        self.assertEqual(len(result["papers"]), 1)
        self.assertEqual(result["papers"][0]["paper_label"], "IP-11")

    def test_query_by_paper(self) -> None:
        result = query_by_paper("WP-48", db_path=self.db_path)
        self.assertEqual(len(result["paper_items"]), 1)
        self.assertEqual(result["paper_items"][0]["item_num"], "5")
        self.assertEqual(len(result["paper_outputs"]), 1)
        self.assertEqual(result["paper_outputs"][0]["output_label"], "Decision 1 (2099)")


if __name__ == "__main__":
    unittest.main()
