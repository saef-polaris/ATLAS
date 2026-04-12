#!/usr/bin/env python3
# %%
"""Parse Marker full-pagewise ATCM runs into items, outputs, and link tables.

Inputs
------
- marker_runs/*_full_pagewise/chunk_XXXX_XXXX/.../*.md

Outputs
-------
- marker_runs/derived_marker_items.csv
- marker_runs/derived_marker_outputs.csv
- marker_runs/derived_marker_item_output_links.csv
- marker_runs/derived_marker_paper_item_links.csv
- marker_runs/derived_marker_paper_output_links.csv
- marker_runs/derived_marker_run_audit.csv

Design
------
This parser is intentionally position-aware rather than number-aware:
- pagewise markdown is merged in page order
- item blocks are assigned by the nearest preceding item heading
- repeated item numbers are allowed across separate sequences
- sequences are classified heuristically (ATCM main vs CEP vs other)

Notes on assumptions / hard-coding
----------------------------------
See ``MAGIC_AND_ASSUMPTIONS`` below for all explicit parser heuristics.
"""

from __future__ import annotations

# %%
import json
import re
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any

import pandas as pd

from .config import ensure_data_dir, get_marker_dir


# %%
MARKER_DIR = get_marker_dir()
OUT_ITEMS = MARKER_DIR / "derived_marker_items.csv"
OUT_OUTPUTS = MARKER_DIR / "derived_marker_outputs.csv"
OUT_ITEM_OUTPUT = MARKER_DIR / "derived_marker_item_output_links.csv"
OUT_PAPER_ITEM = MARKER_DIR / "derived_marker_paper_item_links.csv"
OUT_PAPER_OUTPUT = MARKER_DIR / "derived_marker_paper_output_links.csv"
OUT_AUDIT = MARKER_DIR / "derived_marker_run_audit.csv"

# Explicit record of parser heuristics.
MAGIC_AND_ASSUMPTIONS = {
    "sequence_reset_threshold": "Start a new sequence when item numbering resets to <=2 after a prior item >=10.",
    "sequence_type_cep_signals": [
        "CEP Agenda Item",
        "Committee for Environmental Protection",
        "CEP Chair",
        "Chair of the CEP",
    ],
    "item_heading_patterns": [
        r"^#+\s+\*{0,2}Item\s+\d+[a-z]?\s*:",
        r"^\*{1,2}Item\s+\d+[a-z]?\s*:",
        r"^Item\s+\d+[a-z]?\s*:",
    ],
    "formal_output_types": ["Decision", "Measure", "Resolution", "Recommendation"],
    "paper_types": ["WP", "IP", "SP", "BP"],
    "line_cleanup": [
        "drop HTML chunk comments",
        "collapse repeated spaces",
        "retain page boundaries",
    ],
    "output_link_rule": "Assign outputs to the item block that contains the adoption sentence or output list.",
    "paper_item_rule": "Assign papers to the nearest preceding item block containing the paper mention.",
}

ITEM_HEADING_PATTERNS = [
    re.compile(r"^#+\s+\*{0,2}Item\s+(\d+)([a-z]?)\s*:\s*(.+)$", re.I),
    re.compile(r"^\*{1,2}Item\s+(\d+)([a-z]?)\s*:\s*(.+)\*{0,2}$", re.I),
    re.compile(r"^Item\s+(\d+)([a-z]?)\s*:\s*(.+)$", re.I),
]
SUBITEM_RE = re.compile(r"^#+\s+\*{0,2}(\d+)([a-z])\)\s+(.+)$", re.I)
PAPER_RE = re.compile(r"\b(?P<kind>WP|IP|SP|BP)\s*-?\s*(?P<number>\d+)(?:\s*rev\.?\s*(?P<rev>\d+))?\b", re.I)
OUTPUT_RE = re.compile(
    r"\b(?P<type>Decision|Measure|Resolution|Recommendation)\s+"
    r"(?P<number>\d{1,3})\s*\((?P<year>\d{4})\)"
    r"(?:\s*[:\-]\s*(?P<title>[^\n]{3,220}))?",
    re.I,
)
ADOPTION_RE = re.compile(
    r"\b(?:The\s+(?:Meeting|ATCM|Committee)|Parties)\b.{0,200}?"
    r"(?:adopt(?:ed)?|approve(?:d)?|agreed\s+to\s+adopt|endors(?:ed)?)\b",
    re.I,
)
LIST_INTRO_RE = re.compile(
    r"\b(?:The\s+Meeting|ATCM|Committee)\b.{0,120}?adopted\s+the\s+following\s+"
    r"(?:Measures|Decisions|Resolutions|Recommendations)",
    re.I,
)
CEP_SIGNAL_RE = re.compile(
    r"CEP Agenda Item|Committee for Environmental Protection|Chair of the CEP|CEP Chair",
    re.I,
)
PAGE_MARKER_RE = re.compile(r"<!--\s*page\s*:\s*(\d+)\s*-->", re.I)
CHUNK_COMMENT_RE = re.compile(r"<!--\s*chunk\s+\d+-\d+\s*-->")


# %%
@dataclass
class PageRecord:
    run: str
    page: int
    text: str
    nonempty: bool


@dataclass
class ItemRecord:
    run: str
    sequence_id: int
    sequence_type: str
    item_num: str
    item_title: str
    heading_source: str
    start_page: int
    end_page: int
    start_line: int
    end_line: int
    text: str
    has_papers: bool
    has_outputs: bool


@dataclass
class OutputRecord:
    run: str
    sequence_id: int | None
    sequence_type: str | None
    item_num: str | None
    output_type: str
    output_number: int
    output_year: int
    output_label: str
    title: str | None
    evidence: str
    extraction_basis: str


# %%
def normalise_space(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


# %%
def clean_markdown(text: str) -> str:
    text = CHUNK_COMMENT_RE.sub("", text)
    text = text.replace("\r", "\n")
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


# %%
def parse_page_number(md_path: Path) -> int:
    chunk = md_path.parts[-3]
    return int(chunk.split("_")[1])


# %%
def iter_runs() -> list[Path]:
    ensure_data_dir(MARKER_DIR.parent)
    return sorted(p for p in MARKER_DIR.glob("*_full_pagewise") if p.is_dir())


# %%
def load_pages(run_dir: Path) -> list[PageRecord]:
    mds = sorted(run_dir.glob("chunk_*/ATCM*_fr001_e/ATCM*_fr001_e.md"))
    pages: list[PageRecord] = []
    for md in mds:
        page = parse_page_number(md)
        text = clean_markdown(md.read_text(errors="replace"))
        pages.append(PageRecord(run=run_dir.name, page=page, text=text, nonempty=bool(text.strip())))
    return pages


# %%
def merge_pages(pages: list[PageRecord]) -> list[tuple[int, str]]:
    merged: list[tuple[int, str]] = []
    for page in pages:
        merged.append((page.page, f"<!-- page: {page.page} -->\n{page.text}" if page.text else f"<!-- page: {page.page} -->"))
    return merged


# %%
def detect_item_heading(line: str) -> tuple[str, str, str] | None:
    stripped = line.strip().replace("**", "").replace("*", "")
    for pattern in ITEM_HEADING_PATTERNS:
        match = pattern.match(stripped)
        if match:
            num = f"{match.group(1)}{match.group(2).lower()}"
            title = normalise_space(match.group(3))
            src = "markdown_heading" if line.strip().startswith("#") else "bold_or_plain"
            return num, title, src
    return None


# %%
def classify_sequence_type_from_titles(items: list[dict[str, Any]]) -> str:
    titles = " | ".join(str(item.get("title") or "") for item in items[:8])
    title_l = titles.lower()
    if any(
        key in title_l
        for key in [
            "operation of the cep",
            "future work of the cep",
            "cooperation with other organisations",
            "environmental impact assessment",
            "area protection and management plans",
            "repair and remediation of environment damage",
            "climate change implications for the environment",
            "adoption of the report",
            "preparation for the next meeting",
        ]
    ):
        return "cep"
    if any(
        key in title_l
        for key in [
            "election of officers and creation of working groups",
            "allocation of items to working groups",
            "exchange of information",
            "safety and operations in antarctica",
            "tourism and non-governmental activities",
            "adoption of the final report",
            "close of the meeting",
        ]
    ):
        return "atcm"
    if items and items[0].get("item_num") == "1":
        first_title = str(items[0].get("title") or "").lower()
        if "opening of the meeting" in first_title:
            return "atcm"
    return "other"


# %%
def extract_items(run: str, pages: list[PageRecord]) -> list[ItemRecord]:
    merged_lines: list[tuple[int, int, str]] = []
    for page in pages:
        for idx, line in enumerate(page.text.splitlines(), start=1):
            merged_lines.append((page.page, idx, line))

    headings: list[dict[str, Any]] = []
    for i, (page, line_no, line) in enumerate(merged_lines):
        hit = detect_item_heading(line)
        if not hit:
            continue
        item_num, title, src = hit
        context_window = "\n".join(x[2] for x in merged_lines[max(0, i - 6): min(len(merged_lines), i + 6)])
        headings.append(
            {
                "page": page,
                "line_no": line_no,
                "item_num": item_num,
                "title": title,
                "heading_source": src,
                "context_window": context_window,
                "global_idx": i,
            }
        )

    # sequence ids by numbering reset only
    sequence_id = 0
    prev_num = None
    for heading in headings:
        num_main = int(re.match(r"\d+", heading["item_num"]).group(0))
        if prev_num is None or (num_main <= 2 and prev_num >= 10):
            sequence_id += 1
        heading["sequence_id"] = sequence_id
        prev_num = num_main

    seq_type_map: dict[int, str] = {}
    for seq_id in sorted({h["sequence_id"] for h in headings}):
        seq_heads = [h for h in headings if h["sequence_id"] == seq_id]
        seq_type_map[seq_id] = classify_sequence_type_from_titles(seq_heads)

    items: list[ItemRecord] = []
    for idx, heading in enumerate(headings):
        start = heading["global_idx"]
        end = headings[idx + 1]["global_idx"] if idx + 1 < len(headings) else len(merged_lines)
        block_lines = merged_lines[start:end]
        start_page = block_lines[0][0]
        end_page = block_lines[-1][0]
        text = "\n".join(line for _, _, line in block_lines).strip()
        items.append(
            ItemRecord(
                run=run,
                sequence_id=int(heading["sequence_id"]),
                sequence_type=str(seq_type_map.get(heading["sequence_id"], "other")),
                item_num=str(heading["item_num"]),
                item_title=str(heading["title"]),
                heading_source=str(heading["heading_source"]),
                start_page=int(start_page),
                end_page=int(end_page),
                start_line=int(heading["line_no"]),
                end_line=int(block_lines[-1][1]),
                text=text,
                has_papers=bool(PAPER_RE.search(text)),
                has_outputs=bool(OUTPUT_RE.search(text)),
            )
        )
    return items


# %%
def extract_item_outputs(item: ItemRecord) -> list[OutputRecord]:
    outputs: list[OutputRecord] = []
    lines = item.text.splitlines()

    # Direct adoption lines.
    for line in lines:
        if not ADOPTION_RE.search(line) and not LIST_INTRO_RE.search(line):
            continue
        for match in OUTPUT_RE.finditer(line):
            out_type = match.group("type").lower()
            out_number = int(match.group("number"))
            out_year = int(match.group("year"))
            title = normalise_space(match.group("title") or "") or None
            outputs.append(
                OutputRecord(
                    run=item.run,
                    sequence_id=item.sequence_id,
                    sequence_type=item.sequence_type,
                    item_num=item.item_num,
                    output_type=out_type,
                    output_number=out_number,
                    output_year=out_year,
                    output_label=f"{out_type.title()} {out_number} ({out_year})",
                    title=title,
                    evidence=normalise_space(line),
                    extraction_basis="adoption_line",
                )
            )

    # Output lists under a list-intro line.
    for idx, line in enumerate(lines):
        if not LIST_INTRO_RE.search(line):
            continue
        for next_line in lines[idx + 1: idx + 20]:
            if next_line.strip().startswith("##"):
                break
            for match in OUTPUT_RE.finditer(next_line):
                out_type = match.group("type").lower()
                out_number = int(match.group("number"))
                out_year = int(match.group("year"))
                title = normalise_space(match.group("title") or "") or None
                outputs.append(
                    OutputRecord(
                        run=item.run,
                        sequence_id=item.sequence_id,
                        sequence_type=item.sequence_type,
                        item_num=item.item_num,
                        output_type=out_type,
                        output_number=out_number,
                        output_year=out_year,
                        output_label=f"{out_type.title()} {out_number} ({out_year})",
                        title=title,
                        evidence=normalise_space(next_line),
                        extraction_basis="adoption_list",
                    )
                )

    # Deduplicate within item.
    dedup: dict[tuple[str, int, int], OutputRecord] = {}
    for output in outputs:
        key = (output.output_type, output.output_number, output.output_year)
        prev = dedup.get(key)
        if prev is None or len(output.evidence) > len(prev.evidence):
            dedup[key] = output
    return list(dedup.values())


# %%
def extract_meeting_outputs_from_contents(run: str, pages: list[PageRecord]) -> list[OutputRecord]:
    outputs: list[OutputRecord] = []
    content_pages = [p for p in pages[:10] if p.text]
    for page in content_pages:
        for line in page.text.splitlines():
            for match in OUTPUT_RE.finditer(line):
                out_type = match.group("type").lower()
                out_number = int(match.group("number"))
                out_year = int(match.group("year"))
                title = normalise_space(match.group("title") or "") or None
                outputs.append(
                    OutputRecord(
                        run=run,
                        sequence_id=None,
                        sequence_type=None,
                        item_num=None,
                        output_type=out_type,
                        output_number=out_number,
                        output_year=out_year,
                        output_label=f"{out_type.title()} {out_number} ({out_year})",
                        title=title,
                        evidence=normalise_space(line),
                        extraction_basis="contents",
                    )
                )
    dedup: dict[tuple[str, int, int], OutputRecord] = {}
    for output in outputs:
        key = (output.output_type, output.output_number, output.output_year)
        prev = dedup.get(key)
        if prev is None or len(output.evidence) > len(prev.evidence):
            dedup[key] = output
    return list(dedup.values())


# %%
def extract_paper_item_links(item: ItemRecord) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    seen: set[tuple[str, int, int | None]] = set()
    for match in PAPER_RE.finditer(item.text):
        kind = match.group("kind").upper()
        number = int(match.group("number"))
        rev = int(match.group("rev")) if match.group("rev") else None
        key = (kind, number, rev)
        if key in seen:
            continue
        seen.add(key)
        rows.append(
            {
                "run": item.run,
                "sequence_id": item.sequence_id,
                "sequence_type": item.sequence_type,
                "item_num": item.item_num,
                "item_title": item.item_title,
                "paper_kind": kind,
                "paper_number": number,
                "paper_rev": rev,
                "paper_label": f"{kind}-{number}" if rev is None else f"{kind}-{number} rev. {rev}",
                "link_basis": "same_item_text",
                "link_confidence": "high",
            }
        )
    return rows


# %%
def derive_paper_output_links(paper_item_df: pd.DataFrame, item_output_df: pd.DataFrame) -> pd.DataFrame:
    if paper_item_df.empty or item_output_df.empty:
        return pd.DataFrame()
    merged = paper_item_df.merge(
        item_output_df,
        on=["run", "sequence_id", "sequence_type", "item_num"],
        how="inner",
        suffixes=("_paper", "_output"),
    )
    if merged.empty:
        return merged
    merged["link_basis"] = "paper_to_item_plus_item_to_output"
    merged["link_confidence"] = "medium"
    item_title_col = "item_title"
    return merged[
        [
            "run",
            "sequence_id",
            "sequence_type",
            "item_num",
            item_title_col,
            "paper_kind",
            "paper_number",
            "paper_rev",
            "paper_label",
            "output_type",
            "output_number",
            "output_year",
            "output_label",
            "title",
            "link_basis",
            "link_confidence",
            "evidence",
        ]
    ].rename(columns={"title": "output_title"})


# %%
def run_audit_row(run: Path, pages: list[PageRecord], items: list[ItemRecord], outputs: list[OutputRecord]) -> dict[str, Any]:
    seq_summary = []
    for seq_id in sorted({item.sequence_id for item in items}):
        seq_items = [item for item in items if item.sequence_id == seq_id]
        nums = [int(re.match(r"\d+", item.item_num).group(0)) for item in seq_items]
        missing = [n for n in range(min(nums), max(nums) + 1) if n not in nums] if nums else []
        seq_summary.append(
            {
                "sequence_id": seq_id,
                "sequence_type": seq_items[0].sequence_type if seq_items else None,
                "min_item": min(nums) if nums else None,
                "max_item": max(nums) if nums else None,
                "missing": missing,
            }
        )
    return {
        "run": run.name,
        "pages": len(pages),
        "nonempty_pages": sum(page.nonempty for page in pages),
        "nonempty_rate": round(sum(page.nonempty for page in pages) / max(len(pages), 1), 3),
        "items": len(items),
        "outputs": len(outputs),
        "papers": sum(item.has_papers for item in items),
        "sequence_summary": json.dumps(seq_summary, ensure_ascii=False),
        "magic_and_assumptions": json.dumps(MAGIC_AND_ASSUMPTIONS, ensure_ascii=False),
    }


# %%
def main() -> None:
    item_rows: list[dict[str, Any]] = []
    output_rows: list[dict[str, Any]] = []
    item_output_rows: list[dict[str, Any]] = []
    paper_item_rows: list[dict[str, Any]] = []
    audit_rows: list[dict[str, Any]] = []

    for run in iter_runs():
        pages = load_pages(run)
        items = extract_items(run.name, pages)
        meeting_outputs = extract_meeting_outputs_from_contents(run.name, pages)
        item_outputs: list[OutputRecord] = []
        for item in items:
            item_outputs.extend(extract_item_outputs(item))

        all_outputs = meeting_outputs + item_outputs

        for item in items:
            item_rows.append(asdict(item))
            paper_item_rows.extend(extract_paper_item_links(item))

        for output in all_outputs:
            output_rows.append(asdict(output))
            if output.item_num is not None:
                item_output_rows.append(
                    {
                        "run": output.run,
                        "sequence_id": output.sequence_id,
                        "sequence_type": output.sequence_type,
                        "item_num": output.item_num,
                        "output_type": output.output_type,
                        "output_number": output.output_number,
                        "output_year": output.output_year,
                        "output_label": output.output_label,
                        "title": output.title,
                        "evidence": output.evidence,
                        "link_confidence": "high",
                        "link_basis": output.extraction_basis,
                    }
                )

        audit_rows.append(run_audit_row(run, pages, items, all_outputs))

    items_df = pd.DataFrame(item_rows)
    outputs_df = pd.DataFrame(output_rows).drop_duplicates()
    item_output_df = pd.DataFrame(item_output_rows).drop_duplicates()
    paper_item_df = pd.DataFrame(paper_item_rows).drop_duplicates()
    paper_output_df = derive_paper_output_links(paper_item_df, item_output_df)
    audit_df = pd.DataFrame(audit_rows)

    items_df.to_csv(OUT_ITEMS, index=False)
    outputs_df.to_csv(OUT_OUTPUTS, index=False)
    item_output_df.to_csv(OUT_ITEM_OUTPUT, index=False)
    paper_item_df.to_csv(OUT_PAPER_ITEM, index=False)
    paper_output_df.to_csv(OUT_PAPER_OUTPUT, index=False)
    audit_df.to_csv(OUT_AUDIT, index=False)

    print(f"Wrote items -> {OUT_ITEMS}")
    print(f"Wrote outputs -> {OUT_OUTPUTS}")
    print(f"Wrote item-output links -> {OUT_ITEM_OUTPUT}")
    print(f"Wrote paper-item links -> {OUT_PAPER_ITEM}")
    print(f"Wrote paper-output links -> {OUT_PAPER_OUTPUT}")
    print(f"Wrote audit -> {OUT_AUDIT}")


# %%
if __name__ == "__main__":
    main()
