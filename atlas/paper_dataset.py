from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import networkx as nx
import pandas as pd

from .config import get_marker_dir

ATCM_MEETING_YEAR = {
    1: 1961,
    2: 1962,
    3: 1964,
    4: 1966,
    5: 1968,
    6: 1970,
    7: 1972,
    8: 1975,
    9: 1977,
    10: 1979,
    11: 1981,
    12: 1983,
    13: 1985,
    14: 1987,
    15: 1989,
    16: 1991,
    17: 1992,
    18: 1994,
    19: 1995,
    20: 1996,
    21: 1997,
    22: 1998,
    23: 1999,
    24: 2000,
    25: 2002,
    26: 2003,
    27: 2004,
    28: 2005,
    29: 2006,
    30: 2007,
    31: 2008,
    32: 2009,
    33: 2010,
    34: 2011,
    35: 2012,
    36: 2013,
    37: 2014,
    38: 2015,
    39: 2016,
    40: 2017,
    41: 2018,
    42: 2019,
    43: 2021,
    44: 2022,
    45: 2023,
    46: 2024,
    47: 2025,
}

MARKER_DIR = get_marker_dir()
DEFAULT_CLASSIFICATIONS_PATH = MARKER_DIR / "item_llm_links.jsonl"

ROMAN_RE = re.compile(r"^[IVXLCDM]+$", re.IGNORECASE)
MEETING_TOKEN_RE = re.compile(r"ATCM\s*([0-9]+|[IVXLCDM]+)", re.IGNORECASE)
PAPER_TOKEN_RE = re.compile(
    r"""
    (?:
        (?P<meeting>ATCM\s*(?:[0-9]+|[IVXLCDM]+))?
        [\s\-_/]*
    )?
    (?P<kind>WP|IP|SP|INF)
    [\s\-_/]*
    (?P<number>\d{1,3})
    (?:
        [\s,;:/\-]*
        (?:
            rev(?:ision)?\.?
            [\s\-]*
            (?P<rev>\d+)
        )
    )?
    """,
    re.IGNORECASE | re.VERBOSE,
)


def _clean_str(value: Any) -> str | None:
    if value is None:
        return None
    if pd.isna(value):
        return None
    text = str(value).strip()
    return text or None


def _roman_to_int(text: str) -> int:
    values = {"I": 1, "V": 5, "X": 10, "L": 50, "C": 100, "D": 500, "M": 1000}
    total = 0
    prev = 0
    for char in reversed(text.upper()):
        value = values[char]
        if value < prev:
            total -= value
        else:
            total += value
            prev = value
    return total


def _parse_meeting_number(value: Any) -> int | None:
    text = _clean_str(value)
    if text is None:
        return None
    match = MEETING_TOKEN_RE.search(text)
    if match:
        token = match.group(1)
    else:
        token = text
    token = token.strip().upper()
    if token.isdigit():
        return int(token)
    if ROMAN_RE.fullmatch(token):
        return _roman_to_int(token)
    return None


def _extract_meeting_number_from_run(run: Any) -> int | None:
    text = _clean_str(run)
    if text is None:
        return None
    match = re.search(r"ATCM\s*([0-9]+|[IVXLCDM]+)", text, flags=re.IGNORECASE)
    if not match:
        return None
    return _parse_meeting_number(match.group(1))


def _extract_meeting_number_from_text(text: Any) -> int | None:
    value = _clean_str(text)
    if value is None:
        return None
    match = MEETING_TOKEN_RE.search(value)
    if not match:
        return None
    return _parse_meeting_number(match.group(1))


@dataclass(frozen=True)
class NormalizedPaperLabel:
    raw_label: str
    canonical_label: str
    paper_kind: str
    paper_number: int
    paper_rev: int | None = None
    meeting_number: int | None = None
    meeting_token: str | None = None

    @property
    def meeting_aware_label(self) -> str:
        if self.meeting_number is None:
            return self.canonical_label
        return f"ATCM{self.meeting_number}:{self.canonical_label}"

    def to_dict(self) -> dict[str, Any]:
        return {
            "raw_label": self.raw_label,
            "canonical_label": self.canonical_label,
            "meeting_aware_label": self.meeting_aware_label,
            "paper_kind": self.paper_kind,
            "paper_number": self.paper_number,
            "paper_rev": self.paper_rev,
            "meeting_number": self.meeting_number,
            "meeting_token": self.meeting_token,
        }


@dataclass(frozen=True)
class NormalizedOutputLabel:
    raw_label: str
    canonical_label: str
    output_type: str
    output_number: int
    output_year: int
    meeting_number: int | None = None

    @property
    def meeting_aware_label(self) -> str:
        if self.meeting_number is None:
            return self.canonical_label
        return f"ATCM{self.meeting_number}:{self.canonical_label}"

    def to_dict(self) -> dict[str, Any]:
        return {
            "raw_label": self.raw_label,
            "canonical_label": self.canonical_label,
            "meeting_aware_label": self.meeting_aware_label,
            "output_type": self.output_type,
            "output_number": self.output_number,
            "output_year": self.output_year,
            "meeting_number": self.meeting_number,
        }


def normalize_paper_label(
    label: Any, default_meeting_number: int | None = None
) -> NormalizedPaperLabel | None:
    raw = _clean_str(label)
    if raw is None:
        return None
    compact = re.sub(r"\s+", "", raw)
    match = PAPER_TOKEN_RE.search(compact)
    if not match:
        return None

    kind = match.group("kind").upper()
    number = int(match.group("number"))
    rev = int(match.group("rev")) if match.group("rev") else None
    meeting_token = match.group("meeting")
    meeting_number = (
        _parse_meeting_number(meeting_token)
        if meeting_token
        else default_meeting_number
    )

    canonical_label = f"{kind}-{number}"
    if rev is not None:
        canonical_label = f"{canonical_label} rev. {rev}"

    return NormalizedPaperLabel(
        raw_label=raw,
        canonical_label=canonical_label,
        paper_kind=kind,
        paper_number=number,
        paper_rev=rev,
        meeting_number=meeting_number,
        meeting_token=meeting_token,
    )


def normalize_output_label(
    label: Any, default_meeting_number: int | None = None
) -> NormalizedOutputLabel | None:
    text = _clean_str(label)
    if text is None:
        return None
    match = re.fullmatch(
        r"\s*(decision|measure|resolution)\s+(\d+)\s*\((\d{4})\)\s*",
        text,
        flags=re.IGNORECASE,
    )
    if not match:
        return None
    out_type = match.group(1).title()
    out_number = int(match.group(2))
    out_year = int(match.group(3))
    canonical_label = f"{out_type} {out_number} ({out_year})"
    return NormalizedOutputLabel(
        raw_label=text,
        canonical_label=canonical_label,
        output_type=out_type,
        output_number=out_number,
        output_year=out_year,
        meeting_number=default_meeting_number,
    )


def infer_meeting_number(item: dict[str, Any]) -> int | None:
    for key in ("meeting_number", "run", "sequence_id", "item_id"):
        value = item.get(key)
        meeting_number = _extract_meeting_number_from_run(value)
        if meeting_number is not None:
            return meeting_number
    for key in ("item_title", "text"):
        meeting_number = _extract_meeting_number_from_text(item.get(key))
        if meeting_number is not None:
            return meeting_number
    return None


def meeting_year_from_number(meeting_number: Any) -> int | None:
    parsed = _parse_meeting_number(meeting_number)
    if parsed is None:
        return None
    return ATCM_MEETING_YEAR.get(parsed)


def meeting_period_from_number(meeting_number: Any) -> str | None:
    year = meeting_year_from_number(meeting_number)
    if year is None:
        return None
    if 2000 <= year <= 2011:
        return "2000--2011"
    if 2012 <= year <= 2025:
        return "2012--2025"
    return str(year)


def _iter_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for line in path.read_text().splitlines():
        if line.strip():
            rows.append(json.loads(line))
    return rows


def _normalize_link_record(
    row: dict[str, Any], include_joint_discussion: bool = False
) -> list[dict[str, Any]]:
    item = row.get("item") or {}
    llm = row.get("llm_result") or {}
    links = llm.get("paper_output_links") or []
    meeting_number = infer_meeting_number(item)
    item_key = None
    if meeting_number is not None and _clean_str(item.get("item_num")) is not None:
        item_key = f"ATCM{meeting_number}:item{_clean_str(item.get('item_num'))}"

    normalized_rows: list[dict[str, Any]] = []
    for link in links:
        if (
            not include_joint_discussion
            and (link or {}).get("evidence_basis") == "joint_discussion"
        ):
            continue

        normalized_paper = normalize_paper_label(
            (link or {}).get("paper_label"),
            default_meeting_number=meeting_number,
        )
        if normalized_paper is None:
            continue

        normalized_output = normalize_output_label(
            (link or {}).get("output_label"),
            default_meeting_number=meeting_number,
        )
        if normalized_output is None:
            continue

        normalized_rows.append(
            {
                "item_id": row.get("item_id"),
                "item_key": item_key,
                "run": item.get("run"),
                "sequence_id": item.get("sequence_id"),
                "sequence_type": item.get("sequence_type"),
                "item_num": item.get("item_num"),
                "item_title": item.get("item_title"),
                "item_text": item.get("text"),
                "meeting_number": meeting_number,
                "meeting_year": meeting_year_from_number(meeting_number),
                "meeting_period": meeting_period_from_number(meeting_number),
                "item_meeting_number": meeting_number,
                "paper_meeting_number": normalized_paper.meeting_number,
                "paper_kind": normalized_paper.paper_kind,
                "paper_number": normalized_paper.paper_number,
                "paper_rev": normalized_paper.paper_rev,
                "paper_label_raw": normalized_paper.raw_label,
                "paper_label": normalized_paper.canonical_label,
                "paper_key": normalized_paper.meeting_aware_label,
                "output_type": normalized_output.output_type,
                "output_number": normalized_output.output_number,
                "output_year": normalized_output.output_year,
                "output_label_raw": normalized_output.raw_label,
                "output_label": normalized_output.canonical_label,
                "output_key": normalized_output.meeting_aware_label,
                "relation_type": (link or {}).get("relation_type"),
                "evidence_basis": (link or {}).get("evidence_basis"),
                "confidence": (link or {}).get("confidence"),
                "reason": (link or {}).get("reason"),
                "evidence": (link or {}).get("evidence"),
                "classified_at": row.get("classified_at"),
                "model": row.get("model"),
            }
        )
    return normalized_rows


class PaperDataset:
    def __init__(
        self,
        links: pd.DataFrame,
        source_path: Path | None = None,
        include_joint_discussion: bool = False,
    ):
        self.links = links.copy()
        self.source_path = source_path
        self.include_joint_discussion = include_joint_discussion

    @classmethod
    def from_jsonl(
        cls,
        path: str | Path = DEFAULT_CLASSIFICATIONS_PATH,
        include_joint_discussion: bool = False,
    ) -> PaperDataset:
        source_path = Path(path)
        rows = _iter_jsonl(source_path)
        normalized_rows: list[dict[str, Any]] = []
        for row in rows:
            normalized_rows.extend(
                _normalize_link_record(
                    row, include_joint_discussion=include_joint_discussion
                )
            )
        links = pd.DataFrame(normalized_rows)
        if links.empty:
            links = pd.DataFrame(
                columns=[
                    "item_id",
                    "item_key",
                    "run",
                    "sequence_id",
                    "sequence_type",
                    "item_num",
                    "item_title",
                    "item_text",
                    "meeting_number",
                    "item_meeting_number",
                    "paper_meeting_number",
                    "paper_kind",
                    "paper_number",
                    "paper_rev",
                    "paper_label_raw",
                    "paper_label",
                    "paper_key",
                    "output_type",
                    "output_number",
                    "output_year",
                    "output_label_raw",
                    "output_label",
                    "output_key",
                    "relation_type",
                    "evidence_basis",
                    "confidence",
                    "reason",
                    "evidence",
                    "classified_at",
                    "model",
                ]
            )
        return cls(
            links=links,
            source_path=source_path,
            include_joint_discussion=include_joint_discussion,
        )

    @property
    def papers(self) -> pd.DataFrame:
        if self.links.empty:
            return self.links.loc[
                :,
                [
                    "meeting_number",
                    "paper_kind",
                    "paper_number",
                    "paper_rev",
                    "paper_label",
                    "paper_key",
                ],
            ].drop_duplicates()
        return (
            self.links.loc[
                :,
                [
                    "meeting_number",
                    "paper_kind",
                    "paper_number",
                    "paper_rev",
                    "paper_label",
                    "paper_key",
                ],
            ]
            .drop_duplicates()
            .sort_values(
                by=["meeting_number", "paper_kind", "paper_number", "paper_rev"],
                na_position="last",
            )
            .reset_index(drop=True)
        )

    @property
    def meetings(self) -> pd.DataFrame:
        if self.links.empty:
            return pd.DataFrame(
                columns=["meeting_number", "paper_count", "output_count", "link_count"]
            )
        grouped = (
            self.links.groupby("meeting_number", dropna=False)
            .agg(
                paper_count=("paper_key", "nunique"),
                output_count=("output_label", "nunique"),
                link_count=("paper_key", "size"),
            )
            .reset_index()
            .sort_values(by="meeting_number", na_position="last")
            .reset_index(drop=True)
        )
        return grouped

    def links_for_meeting(self, meeting_number: int) -> pd.DataFrame:
        return (
            self.links.loc[self.links["meeting_number"] == meeting_number]
            .sort_values(
                by=["paper_kind", "paper_number", "paper_rev", "output_label"],
                na_position="last",
            )
            .reset_index(drop=True)
        )

    def links_for_paper(
        self, paper_label: str, meeting_number: int | None = None
    ) -> pd.DataFrame:
        normalized = normalize_paper_label(
            paper_label, default_meeting_number=meeting_number
        )
        if normalized is None:
            return self.links.iloc[0:0].copy()
        if normalized.meeting_number is None:
            mask = self.links["paper_label"] == normalized.canonical_label
        else:
            mask = self.links["paper_key"] == normalized.meeting_aware_label
        return (
            self.links.loc[mask]
            .sort_values(by=["meeting_number", "output_label"])
            .reset_index(drop=True)
        )

    def outputs_for_paper(
        self, paper_label: str, meeting_number: int | None = None
    ) -> pd.DataFrame:
        df = self.links_for_paper(paper_label, meeting_number=meeting_number)
        if df.empty:
            return pd.DataFrame(
                columns=[
                    "meeting_number",
                    "paper_label",
                    "paper_key",
                    "output_label",
                    "output_key",
                ]
            )
        return (
            df.loc[
                :,
                [
                    "meeting_number",
                    "paper_label",
                    "paper_key",
                    "output_label",
                    "output_key",
                ],
            ]
            .drop_duplicates()
            .sort_values(by=["meeting_number", "output_label"])
            .reset_index(drop=True)
        )

    def papers_for_output(
        self, output_label: str, meeting_number: int | None = None
    ) -> pd.DataFrame:
        normalized_output = normalize_output_label(
            output_label, default_meeting_number=meeting_number
        )
        if normalized_output is None:
            return pd.DataFrame(
                columns=[
                    "meeting_number",
                    "paper_label",
                    "paper_key",
                    "output_label",
                    "output_key",
                ]
            )
        if normalized_output.meeting_number is None:
            df = self.links.loc[
                self.links["output_label"] == normalized_output.canonical_label
            ]
        else:
            df = self.links.loc[
                self.links["output_key"] == normalized_output.meeting_aware_label
            ]
        if df.empty:
            return pd.DataFrame(
                columns=[
                    "meeting_number",
                    "paper_label",
                    "paper_key",
                    "output_label",
                    "output_key",
                ]
            )
        return (
            df.loc[
                :,
                [
                    "meeting_number",
                    "paper_label",
                    "paper_key",
                    "output_label",
                    "output_key",
                ],
            ]
            .drop_duplicates()
            .sort_values(by=["meeting_number", "paper_label"])
            .reset_index(drop=True)
        )

    def adjacency(
        self, meeting_number: int | list[int] | tuple[int, ...] | set[int] | None = None
    ) -> pd.DataFrame:
        if self.links.empty:
            return pd.DataFrame(
                columns=[
                    "source",
                    "target",
                    "meeting_number",
                    "paper_label",
                    "output_label",
                ]
            )
        if meeting_number is None:
            df = self.links.copy()
        elif isinstance(meeting_number, int):
            df = self.links.loc[self.links["meeting_number"] == meeting_number].copy()
        else:
            meeting_numbers = list(meeting_number)
            df = self.links.loc[
                self.links["meeting_number"].isin(meeting_numbers)
            ].copy()
        if df.empty:
            return pd.DataFrame(
                columns=[
                    "source",
                    "target",
                    "meeting_number",
                    "paper_label",
                    "output_label",
                    "output_key",
                ]
            )
        df.loc[:, "source"] = df["paper_key"]
        df.loc[:, "target"] = df["output_key"]
        return (
            df.loc[
                :,
                [
                    "source",
                    "target",
                    "meeting_number",
                    "paper_label",
                    "output_label",
                    "output_key",
                ],
            ]
            .drop_duplicates()
            .sort_values(by=["meeting_number", "paper_label", "output_label"])
            .reset_index(drop=True)
        )

    def to_graph(
        self, meeting_number: int | list[int] | tuple[int, ...] | set[int] | None = None
    ) -> nx.Graph:
        graph = nx.Graph()
        edge_rows = self.adjacency(meeting_number=meeting_number)
        if edge_rows.empty:
            return graph

        for row in edge_rows.to_dict(orient="records"):
            source = row["source"]
            target = row["target"]
            graph.add_node(
                source,
                node_type="paper",
                label=row["paper_label"],
                meeting_number=row["meeting_number"],
            )
            graph.add_node(
                target,
                node_type="output",
                label=row["output_label"],
                output_key=row["output_key"],
                meeting_number=row["meeting_number"],
            )
            graph.add_edge(
                source,
                target,
                meeting_number=row["meeting_number"],
                paper_label=row["paper_label"],
                output_label=row["output_label"],
                output_key=row["output_key"],
            )
        return graph

    def graph_for_meeting(self, meeting_number: int) -> nx.Graph:
        return self.to_graph(meeting_number=meeting_number)

    def item_nodes(self) -> pd.DataFrame:
        if self.links.empty:
            return pd.DataFrame(
                columns=[
                    "item_id",
                    "item_key",
                    "meeting_number",
                    "run",
                    "sequence_id",
                    "sequence_type",
                    "item_num",
                    "item_title",
                    "item_text",
                ]
            )
        return (
            self.links.loc[
                :,
                [
                    "item_id",
                    "item_key",
                    "meeting_number",
                    "run",
                    "sequence_id",
                    "sequence_type",
                    "item_num",
                    "item_title",
                    "item_text",
                ],
            ]
            .dropna(subset=["item_key"])
            .drop_duplicates()
            .sort_values(
                by=["meeting_number", "item_num", "item_id"], na_position="last"
            )
            .reset_index(drop=True)
        )

    def item_adjacency(
        self, meeting_number: int | list[int] | tuple[int, ...] | set[int] | None = None
    ) -> pd.DataFrame:
        item_nodes = self.item_nodes()
        if item_nodes.empty:
            return pd.DataFrame(
                columns=[
                    "source",
                    "target",
                    "edge_type",
                    "meeting_number",
                    "source_meeting_number",
                    "target_meeting_number",
                    "paper_meeting_number",
                    "item_meeting_number",
                    "paper_key",
                    "output_key",
                    "item_key",
                    "paper_label",
                    "output_label",
                    "item_title",
                ]
            )

        if meeting_number is None:
            filtered_links = self.links.copy()
            filtered_items = item_nodes.copy()
        elif isinstance(meeting_number, int):
            filtered_links = self.links.loc[
                self.links["item_meeting_number"] == meeting_number
            ].copy()
            filtered_items = item_nodes.loc[
                item_nodes["meeting_number"] == meeting_number
            ].copy()
        else:
            meeting_numbers = list(meeting_number)
            filtered_links = self.links.loc[
                self.links["item_meeting_number"].isin(meeting_numbers)
            ].copy()
            filtered_items = item_nodes.loc[
                item_nodes["meeting_number"].isin(meeting_numbers)
            ].copy()

        edges: list[dict[str, Any]] = []

        for row in filtered_links.to_dict(orient="records"):
            item_key = row.get("item_key")
            if item_key is None:
                continue
            edges.append(
                {
                    "source": row["paper_key"],
                    "target": item_key,
                    "edge_type": "paper_to_item",
                    "meeting_number": row["item_meeting_number"],
                    "source_meeting_number": row["paper_meeting_number"],
                    "target_meeting_number": row["item_meeting_number"],
                    "paper_meeting_number": row["paper_meeting_number"],
                    "item_meeting_number": row["item_meeting_number"],
                    "paper_key": row["paper_key"],
                    "output_key": None,
                    "item_key": item_key,
                    "paper_label": row["paper_label"],
                    "output_label": None,
                    "item_title": row["item_title"],
                }
            )
            edges.append(
                {
                    "source": item_key,
                    "target": row["output_key"],
                    "edge_type": "item_to_output",
                    "meeting_number": row["item_meeting_number"],
                    "source_meeting_number": row["item_meeting_number"],
                    "target_meeting_number": row["item_meeting_number"],
                    "paper_meeting_number": row["paper_meeting_number"],
                    "item_meeting_number": row["item_meeting_number"],
                    "paper_key": None,
                    "output_key": row["output_key"],
                    "item_key": item_key,
                    "paper_label": None,
                    "output_label": row["output_label"],
                    "item_title": row["item_title"],
                }
            )

        if not edges:
            return pd.DataFrame(
                columns=[
                    "source",
                    "target",
                    "edge_type",
                    "meeting_number",
                    "source_meeting_number",
                    "target_meeting_number",
                    "paper_meeting_number",
                    "item_meeting_number",
                    "paper_key",
                    "output_key",
                    "item_key",
                    "paper_label",
                    "output_label",
                    "item_title",
                ]
            )

        return (
            pd.DataFrame(edges)
            .drop_duplicates()
            .sort_values(
                by=[
                    "edge_type",
                    "source_meeting_number",
                    "target_meeting_number",
                    "source",
                    "target",
                ],
                na_position="last",
            )
            .reset_index(drop=True)
        )

    def to_item_graph(
        self, meeting_number: int | list[int] | tuple[int, ...] | set[int] | None = None
    ) -> nx.DiGraph:
        graph = nx.DiGraph()
        item_nodes = self.item_nodes()
        if meeting_number is None:
            filtered_items = item_nodes.copy()
        elif isinstance(meeting_number, int):
            filtered_items = item_nodes.loc[
                item_nodes["meeting_number"] == meeting_number
            ].copy()
        else:
            meeting_numbers = list(meeting_number)
            filtered_items = item_nodes.loc[
                item_nodes["meeting_number"].isin(meeting_numbers)
            ].copy()

        for row in filtered_items.to_dict(orient="records"):
            item_key = row.get("item_key")
            if item_key is None:
                continue
            graph.add_node(
                item_key,
                node_type="item",
                label=row.get("item_title"),
                meeting_number=row.get("meeting_number"),
                item_id=row.get("item_id"),
                item_num=row.get("item_num"),
                run=row.get("run"),
                sequence_id=row.get("sequence_id"),
                sequence_type=row.get("sequence_type"),
                item_title=row.get("item_title"),
                item_text=row.get("item_text"),
            )

        edge_rows = self.item_adjacency(meeting_number=meeting_number)
        if edge_rows.empty:
            return graph

        for row in edge_rows.to_dict(orient="records"):
            source = row["source"]
            target = row["target"]
            edge_type = row["edge_type"]

            if edge_type == "paper_to_item":
                if source not in graph:
                    graph.add_node(
                        source,
                        node_type="paper",
                        label=row.get("paper_label"),
                        meeting_number=row.get("source_meeting_number"),
                    )
                if target not in graph:
                    graph.add_node(
                        target,
                        node_type="item",
                        label=row.get("item_title"),
                        meeting_number=row.get("target_meeting_number"),
                    )
            elif edge_type == "item_to_output":
                if source not in graph:
                    graph.add_node(
                        source,
                        node_type="item",
                        label=row.get("item_title"),
                        meeting_number=row.get("source_meeting_number"),
                    )
                if target not in graph:
                    graph.add_node(
                        target,
                        node_type="output",
                        label=row.get("output_label"),
                        meeting_number=row.get("target_meeting_number"),
                        output_key=row.get("output_key"),
                    )
            elif edge_type == "item_flow":
                if source not in graph:
                    graph.add_node(
                        source,
                        node_type="item",
                        meeting_number=row.get("source_meeting_number"),
                    )
                if target not in graph:
                    graph.add_node(
                        target,
                        node_type="item",
                        meeting_number=row.get("target_meeting_number"),
                    )

            graph.add_edge(
                source,
                target,
                edge_type=edge_type,
                meeting_number=row.get("meeting_number"),
                source_meeting_number=row.get("source_meeting_number"),
                target_meeting_number=row.get("target_meeting_number"),
                paper_meeting_number=row.get("paper_meeting_number"),
                item_meeting_number=row.get("item_meeting_number"),
                paper_key=row.get("paper_key"),
                output_key=row.get("output_key"),
                item_key=row.get("item_key"),
                paper_label=row.get("paper_label"),
                output_label=row.get("output_label"),
                item_title=row.get("item_title"),
            )

        return graph

    def to_dict(self) -> dict[str, Any]:
        return {
            "source_path": None if self.source_path is None else str(self.source_path),
            "include_joint_discussion": self.include_joint_discussion,
            "meeting_count": int(self.meetings.shape[0]),
            "paper_count": int(self.papers.shape[0]),
            "item_count": int(self.item_nodes().shape[0]),
            "link_count": int(self.links.shape[0]),
            "meetings": self.meetings.to_dict(orient="records"),
        }

    def overview_text(self, max_meetings: int = 10) -> str:
        lines = [
            "PaperDataset overview",
            f"source_path: {None if self.source_path is None else str(self.source_path)}",
            f"include_joint_discussion: {self.include_joint_discussion}",
            f"meetings: {int(self.meetings.shape[0])}",
            f"papers: {int(self.papers.shape[0])}",
            f"links: {int(self.links.shape[0])}",
        ]
        if self.links.empty:
            lines.append("meeting_summary: none")
            return "\n".join(lines)

        lines.append("meeting_summary:")
        meeting_rows = self.meetings.head(max_meetings).to_dict(orient="records")
        for row in meeting_rows:
            meeting_number = row.get("meeting_number")
            if meeting_number is None or pd.isna(meeting_number):
                meeting_label = "unknown"
            else:
                meeting_label = f"ATCM{int(meeting_number)}"
            lines.append(
                "  "
                + f"{meeting_label}: "
                + f"papers={int(row['paper_count'])} "
                + f"outputs={int(row['output_count'])} "
                + f"links={int(row['link_count'])}"
            )
        remaining = int(self.meetings.shape[0]) - len(meeting_rows)
        if remaining > 0:
            lines.append(f"  ... {remaining} more meeting(s)")
        return "\n".join(lines)

    def __str__(self) -> str:
        return self.overview_text()

    def __repr__(self) -> str:
        return self.__str__()

    def print_overview(self, max_meetings: int = 10) -> str:
        text = self.overview_text(max_meetings=max_meetings)
        print(text)
        return text


def load_paper_dataset(
    path: str | Path = DEFAULT_CLASSIFICATIONS_PATH,
    include_joint_discussion: bool = False,
) -> PaperDataset:
    return PaperDataset.from_jsonl(
        path, include_joint_discussion=include_joint_discussion
    )


def print_dataset_overview(
    path: str | Path = DEFAULT_CLASSIFICATIONS_PATH,
    max_meetings: int = 10,
    include_joint_discussion: bool = False,
) -> str:
    dataset = load_paper_dataset(
        path, include_joint_discussion=include_joint_discussion
    )
    return dataset.print_overview(max_meetings=max_meetings)


__all__ = [
    "DEFAULT_CLASSIFICATIONS_PATH",
    "MARKER_DIR",
    "NormalizedOutputLabel",
    "NormalizedPaperLabel",
    "PaperDataset",
    "infer_meeting_number",
    "load_paper_dataset",
    "normalize_output_label",
    "normalize_paper_label",
    "print_dataset_overview",
]
