from __future__ import annotations

import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

import networkx as nx
import pandas as pd

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
        ).to_csv(self.marker_dir / "derived_marker_run_audit.csv", index=False)

        build_or_refresh_database(
            marker_dir=self.marker_dir,
            parquet_dir=self.parquet_dir,
            db_path=self.db_path,
        )

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
        result = query_by_item(
            "ATCM99_full_pagewise",
            "6",
            sequence_type="atcm",
            db_path=self.db_path,
        )
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
        self.assertEqual(
            result["paper_outputs"][0]["output_label"], "Decision 1 (2099)"
        )


class PaperDatasetTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.jsonl_path = self.root / "item_llm_links.jsonl"

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
        self.jsonl_path.write_text(
            "\n".join(json.dumps(row, ensure_ascii=False) for row in rows) + "\n"
        )
        self.expected_output_keys = {
            "ATCM27:Decision 1 (2004)",
            "ATCM28:Resolution 2 (2005)",
        }

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def test_normalize_paper_label_variants(self) -> None:
        norm = normalize_paper_label("WP-1")
        self.assertIsNotNone(norm)
        assert norm is not None
        self.assertEqual(norm.canonical_label, "WP-1")
        self.assertEqual(norm.meeting_aware_label, "WP-1")
        self.assertEqual(norm.paper_kind, "WP")
        self.assertEqual(norm.paper_number, 1)
        self.assertIsNone(norm.meeting_number)

        norm = normalize_paper_label("WP-01")
        self.assertIsNotNone(norm)
        assert norm is not None
        self.assertEqual(norm.canonical_label, "WP-1")

        norm = normalize_paper_label("wp 001")
        self.assertIsNotNone(norm)
        assert norm is not None
        self.assertEqual(norm.canonical_label, "WP-1")

        norm = normalize_paper_label("ATCMXXVIIWP-01")
        self.assertIsNotNone(norm)
        assert norm is not None
        self.assertEqual(norm.canonical_label, "WP-1")
        self.assertEqual(norm.meeting_number, 27)
        self.assertEqual(norm.meeting_aware_label, "ATCM27:WP-1")

        norm = normalize_paper_label("ATCM XXVII WP 01")
        self.assertIsNotNone(norm)
        assert norm is not None
        self.assertEqual(norm.meeting_number, 27)
        self.assertEqual(norm.meeting_aware_label, "ATCM27:WP-1")

        norm = normalize_paper_label("ATCM27/WP-01")
        self.assertIsNotNone(norm)
        assert norm is not None
        self.assertEqual(norm.meeting_number, 27)
        self.assertEqual(norm.meeting_aware_label, "ATCM27:WP-1")

        norm = normalize_paper_label("WP-01", default_meeting_number=28)
        self.assertIsNotNone(norm)
        assert norm is not None
        self.assertEqual(norm.meeting_number, 28)
        self.assertEqual(norm.meeting_aware_label, "ATCM28:WP-1")

    def test_normalize_output_label(self) -> None:
        norm = normalize_output_label("decision 1 (2004)")
        self.assertIsNotNone(norm)
        assert norm is not None
        self.assertEqual(norm.canonical_label, "Decision 1 (2004)")
        self.assertEqual(norm.meeting_aware_label, "Decision 1 (2004)")
        self.assertEqual(norm.output_type, "Decision")
        self.assertEqual(norm.output_number, 1)
        self.assertEqual(norm.output_year, 2004)

        norm = normalize_output_label("Resolution 02 (2005)", default_meeting_number=28)
        self.assertIsNotNone(norm)
        assert norm is not None
        self.assertEqual(norm.canonical_label, "Resolution 2 (2005)")
        self.assertEqual(norm.meeting_aware_label, "ATCM28:Resolution 2 (2005)")
        self.assertEqual(norm.output_type, "Resolution")
        self.assertEqual(norm.output_number, 2)
        self.assertEqual(norm.output_year, 2005)

        norm = normalize_output_label("Measure 7 (2011)")
        self.assertIsNotNone(norm)
        assert norm is not None
        self.assertEqual(norm.canonical_label, "Measure 7 (2011)")
        self.assertEqual(norm.meeting_aware_label, "Measure 7 (2011)")

    def test_load_paper_dataset(self) -> None:
        dataset = load_paper_dataset(self.jsonl_path)
        self.assertIsInstance(dataset, PaperDataset)
        self.assertEqual(dataset.source_path, self.jsonl_path)
        self.assertEqual(len(dataset.links), 3)

    def test_joint_discussion_is_excluded_by_default(self) -> None:
        dataset = load_paper_dataset(self.jsonl_path)
        self.assertNotIn("WP-99", set(dataset.links["paper_label"]))

    def test_joint_discussion_can_be_included_with_flag(self) -> None:
        dataset = PaperDataset.from_jsonl(
            self.jsonl_path, include_joint_discussion=True
        )
        self.assertEqual(len(dataset.links), 4)
        self.assertIn("WP-99", set(dataset.links["paper_label"]))
        joint_rows = dataset.links.loc[dataset.links["paper_label"] == "WP-99"]
        self.assertEqual(len(joint_rows), 1)
        self.assertEqual(joint_rows.iloc[0]["evidence_basis"], "joint_discussion")
        self.assertEqual(joint_rows.iloc[0]["output_key"], "ATCM27:Decision 1 (2004)")

    def test_papers_property(self) -> None:
        dataset = load_paper_dataset(self.jsonl_path)
        papers = dataset.papers
        self.assertEqual(len(papers), 3)
        self.assertEqual(
            set(papers["paper_key"]),
            {"ATCM27:WP-1", "ATCM28:WP-1", "ATCM28:WP-2"},
        )

    def test_meetings_property(self) -> None:
        dataset = load_paper_dataset(self.jsonl_path)
        meetings = dataset.meetings
        self.assertEqual(list(meetings["meeting_number"]), [27, 28])
        self.assertEqual(
            meetings.set_index("meeting_number").loc[27, "paper_count"],
            2,
        )
        self.assertEqual(
            meetings.set_index("meeting_number").loc[28, "paper_count"],
            1,
        )

    def test_links_for_meeting(self) -> None:
        dataset = load_paper_dataset(self.jsonl_path)
        meeting_links = dataset.links_for_meeting(28)
        self.assertEqual(len(meeting_links), 1)
        self.assertEqual(
            set(meeting_links["paper_key"]),
            {"ATCM28:WP-1"},
        )

    def test_links_for_paper_without_meeting(self) -> None:
        dataset = load_paper_dataset(self.jsonl_path)
        paper_links = dataset.links_for_paper("WP-1")
        self.assertEqual(len(paper_links), 2)
        self.assertEqual(set(paper_links["meeting_number"]), {27, 28})

    def test_links_for_paper_with_meeting(self) -> None:
        dataset = load_paper_dataset(self.jsonl_path)
        paper_links = dataset.links_for_paper("WP-01", meeting_number=27)
        self.assertEqual(len(paper_links), 1)
        self.assertEqual(paper_links.iloc[0]["paper_key"], "ATCM27:WP-1")
        self.assertEqual(paper_links.iloc[0]["output_label"], "Decision 1 (2004)")
        self.assertEqual(paper_links.iloc[0]["output_key"], "ATCM27:Decision 1 (2004)")

    def test_outputs_for_paper(self) -> None:
        dataset = load_paper_dataset(self.jsonl_path)
        outputs = dataset.outputs_for_paper("WP-1", meeting_number=28)
        self.assertEqual(len(outputs), 1)
        self.assertEqual(outputs.iloc[0]["paper_key"], "ATCM28:WP-1")
        self.assertEqual(outputs.iloc[0]["output_label"], "Resolution 2 (2005)")
        self.assertEqual(outputs.iloc[0]["output_key"], "ATCM28:Resolution 2 (2005)")

    def test_papers_for_output(self) -> None:
        dataset = load_paper_dataset(self.jsonl_path)
        papers = dataset.papers_for_output("decision 1 (2004)")
        self.assertEqual(len(papers), 2)
        self.assertEqual(
            set(papers["paper_key"]),
            {"ATCM27:WP-1", "ATCM28:WP-2"},
        )
        self.assertEqual(
            set(papers["output_label"]),
            {"Decision 1 (2004)"},
        )
        self.assertEqual(
            set(papers["output_key"]),
            {"ATCM27:Decision 1 (2004)"},
        )

    def test_adjacency(self) -> None:
        dataset = load_paper_dataset(self.jsonl_path)
        adjacency = dataset.adjacency()
        self.assertEqual(len(adjacency), 3)
        self.assertEqual(
            set(adjacency["source"]),
            {"ATCM27:WP-1", "ATCM28:WP-1", "ATCM28:WP-2"},
        )
        self.assertEqual(set(adjacency["target"]), self.expected_output_keys)

    def test_to_dict_summary(self) -> None:
        dataset = load_paper_dataset(self.jsonl_path)
        summary = dataset.to_dict()
        self.assertEqual(summary["source_path"], str(self.jsonl_path))
        self.assertEqual(summary["meeting_count"], 2)
        self.assertEqual(summary["paper_count"], 3)
        self.assertEqual(summary["link_count"], 3)
        self.assertEqual(len(summary["meetings"]), 2)

    def test_print_overview(self) -> None:
        dataset = load_paper_dataset(self.jsonl_path)
        overview = str(dataset)
        self.assertIsInstance(overview, str)
        self.assertIn("PaperDataset overview", overview)
        self.assertIn("source_path:", overview)
        self.assertIn("meetings: 2", overview)
        self.assertIn("papers: 3", overview)
        self.assertIn("links: 3", overview)
        self.assertIn("ATCM27: papers=2 outputs=1 links=2", overview)
        self.assertIn("ATCM28: papers=1 outputs=1 links=1", overview)

    def test_to_graph_all_meetings(self) -> None:
        dataset = load_paper_dataset(self.jsonl_path)
        graph = dataset.to_graph()
        self.assertIsInstance(graph, nx.Graph)
        self.assertEqual(
            set(graph.nodes),
            {
                "ATCM27:WP-1",
                "ATCM28:WP-1",
                "ATCM28:WP-2",
                "ATCM27:Decision 1 (2004)",
                "ATCM28:Resolution 2 (2005)",
            },
        )
        self.assertTrue(graph.has_edge("ATCM27:WP-1", "ATCM27:Decision 1 (2004)"))
        self.assertTrue(graph.has_edge("ATCM28:WP-1", "ATCM28:Resolution 2 (2005)"))
        self.assertTrue(graph.has_edge("ATCM28:WP-2", "ATCM27:Decision 1 (2004)"))

    def test_to_graph_single_meeting_subset(self) -> None:
        dataset = load_paper_dataset(self.jsonl_path)
        graph = dataset.to_graph(meeting_number=27)
        self.assertIsInstance(graph, nx.Graph)
        self.assertEqual(
            set(graph.nodes),
            {"ATCM27:WP-1", "ATCM28:WP-2", "ATCM27:Decision 1 (2004)"},
        )
        self.assertTrue(graph.has_edge("ATCM27:WP-1", "ATCM27:Decision 1 (2004)"))
        self.assertTrue(graph.has_edge("ATCM28:WP-2", "ATCM27:Decision 1 (2004)"))
        self.assertFalse(graph.has_node("ATCM28:WP-1"))
        self.assertFalse(graph.has_node("ATCM28:Resolution 2 (2005)"))

    def test_to_graph_multiple_meeting_subset(self) -> None:
        dataset = load_paper_dataset(self.jsonl_path)
        graph = dataset.to_graph(meeting_number=[27, 28])
        self.assertIsInstance(graph, nx.Graph)
        self.assertEqual(
            set(graph.nodes),
            {
                "ATCM27:WP-1",
                "ATCM28:WP-1",
                "ATCM28:WP-2",
                "ATCM27:Decision 1 (2004)",
                "ATCM28:Resolution 2 (2005)",
            },
        )
        self.assertEqual(graph.number_of_edges(), 3)

    def test_to_item_graph_all_meetings(self) -> None:
        dataset = load_paper_dataset(self.jsonl_path)
        graph = dataset.to_item_graph()
        self.assertIsInstance(graph, nx.DiGraph)
        self.assertEqual(
            set(graph.nodes),
            {
                "ATCM27:item5",
                "ATCM28:item5",
                "ATCM27:WP-1",
                "ATCM28:WP-1",
                "ATCM28:WP-2",
                "ATCM27:Decision 1 (2004)",
                "ATCM28:Resolution 2 (2005)",
            },
        )
        self.assertTrue(graph.has_edge("ATCM27:WP-1", "ATCM27:item5"))
        self.assertTrue(graph.has_edge("ATCM28:WP-2", "ATCM27:item5"))
        self.assertTrue(graph.has_edge("ATCM27:item5", "ATCM27:Decision 1 (2004)"))
        self.assertTrue(graph.has_edge("ATCM28:WP-1", "ATCM28:item5"))
        self.assertTrue(graph.has_edge("ATCM28:item5", "ATCM28:Resolution 2 (2005)"))

    def test_to_item_graph_single_meeting_subset(self) -> None:
        dataset = load_paper_dataset(self.jsonl_path)
        graph = dataset.to_item_graph(meeting_number=27)
        self.assertIsInstance(graph, nx.DiGraph)
        self.assertEqual(
            set(graph.nodes),
            {
                "ATCM27:item5",
                "ATCM27:WP-1",
                "ATCM28:WP-2",
                "ATCM27:Decision 1 (2004)",
            },
        )
        self.assertTrue(graph.has_edge("ATCM27:WP-1", "ATCM27:item5"))
        self.assertTrue(graph.has_edge("ATCM28:WP-2", "ATCM27:item5"))
        self.assertTrue(graph.has_edge("ATCM27:item5", "ATCM27:Decision 1 (2004)"))
        self.assertFalse(graph.has_node("ATCM28:item5"))
        self.assertFalse(graph.has_node("ATCM28:WP-1"))

    def test_to_item_graph_cross_meeting_flow(self) -> None:
        dataset = load_paper_dataset(self.jsonl_path)
        graph = dataset.to_item_graph()
        self.assertTrue(graph.has_edge("ATCM28:WP-2", "ATCM27:item5"))
        edge_data = graph.get_edge_data("ATCM28:WP-2", "ATCM27:item5")
        self.assertIsNotNone(edge_data)
        assert edge_data is not None
        self.assertEqual(edge_data["edge_type"], "paper_to_item")
        self.assertEqual(edge_data["meeting_number"], 27)
        self.assertEqual(edge_data["paper_meeting_number"], 28)
        self.assertEqual(edge_data["item_meeting_number"], 27)
        self.assertFalse(graph.has_edge("ATCM27:item5", "ATCM28:item5"))

    def test_to_item_graph_multiple_meeting_subset(self) -> None:
        dataset = load_paper_dataset(self.jsonl_path)
        graph = dataset.to_item_graph(meeting_number=[27, 28])
        self.assertIsInstance(graph, nx.DiGraph)
        self.assertEqual(
            set(graph.nodes),
            {
                "ATCM27:item5",
                "ATCM28:item5",
                "ATCM27:WP-1",
                "ATCM28:WP-1",
                "ATCM28:WP-2",
                "ATCM27:Decision 1 (2004)",
                "ATCM28:Resolution 2 (2005)",
            },
        )
        self.assertEqual(graph.number_of_edges(), 5)


if __name__ == "__main__":
    unittest.main()
