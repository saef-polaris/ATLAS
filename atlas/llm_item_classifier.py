from __future__ import annotations

import json
import shutil
import subprocess
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import sys

import pandas as pd

from .config import ensure_data_dir, get_marker_dir

ROOT = Path(__file__).resolve().parent.parent
MARKER_DIR = get_marker_dir()
DEFAULT_PROMPT_PATH = ROOT / "prompt_check.md"
DEFAULT_CLASSIFICATIONS_PATH = MARKER_DIR / "item_llm_links.jsonl"
DEFAULT_VALIDATION_BUNDLE_PATH = MARKER_DIR / "item_llm_link_validation_bundle.json"
DEFAULT_MODEL = "gpt-5.4-mini"

SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "item_summary": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "adoption_context": {"type": "boolean"},
                "substantive_discussion": {"type": "boolean"},
                "confidence": {"type": "string", "enum": ["high", "medium", "low"]},
            },
            "required": ["adoption_context", "substantive_discussion", "confidence"],
        },
        "outputs": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "output_label": {"type": "string"},
                    "output_type": {"type": "string", "enum": ["Measure", "Resolution", "Decision", "Recommendation"]},
                    "output_number": {"type": "integer"},
                    "output_year": {"type": "integer"},
                    "adoption_context": {"type": "boolean"},
                    "substantive_discussion": {"type": "boolean"},
                    "confidence": {"type": "string", "enum": ["high", "medium", "low"]},
                    "evidence": {"type": "string"},
                },
                "required": [
                    "output_label",
                    "output_type",
                    "output_number",
                    "output_year",
                    "adoption_context",
                    "substantive_discussion",
                    "confidence",
                    "evidence",
                ],
            },
        },
        "papers": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "paper_label": {"type": "string"},
                    "paper_type": {"type": "string", "enum": ["WP", "IP", "SP", "BP"]},
                    "paper_number": {"type": "integer"},
                    "paper_rev": {"type": ["integer", "null"]},
                    "confidence": {"type": "string", "enum": ["high", "medium", "low"]},
                    "evidence": {"type": "string"},
                },
                "required": ["paper_label", "paper_type", "paper_number", "paper_rev", "confidence", "evidence"],
            },
        },
        "paper_output_links": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "paper_label": {"type": "string"},
                    "output_label": {"type": "string"},
                    "relation_type": {"type": "string", "enum": ["supports", "discusses", "proposes", "informs", "unclear"]},
                    "confidence": {"type": "string", "enum": ["high", "medium", "low"]},
                    "reason": {"type": "string"},
                    "evidence": {"type": "string"},
                },
                "required": ["paper_label", "output_label", "relation_type", "confidence", "reason", "evidence"],
            },
        },
    },
    "required": ["item_summary", "outputs", "papers", "paper_output_links"],
}


def _clean_value(value: Any) -> Any:
    if pd.isna(value):
        return None
    return value.item() if hasattr(value, "item") else value


def _item_id(row: pd.Series) -> str:
    return "::".join(
        str(_clean_value(row.get(key)) or "")
        for key in ["run", "sequence_id", "sequence_type", "item_num", "start_page", "start_line"]
    )


def load_items(marker_dir: Path = MARKER_DIR, run: str | None = None, limit: int | None = None) -> list[dict[str, Any]]:
    ensure_data_dir(marker_dir.parent)
    path = marker_dir / "derived_marker_items.csv"
    if not path.exists():
        raise FileNotFoundError(f"Missing items table: {path}")
    df = pd.read_csv(path)
    if run is not None:
        df = df[df["run"] == run]
    if limit is not None:
        df = df.head(limit)
    records: list[dict[str, Any]] = []
    for _, row in df.iterrows():
        rec = {col: _clean_value(row.get(col)) for col in df.columns}
        rec["item_id"] = _item_id(row)
        records.append(rec)
    return records


def _build_prompt(prompt_text: str, item: dict[str, Any]) -> str:
    payload = {
        "run": item.get("run"),
        "sequence_id": item.get("sequence_id"),
        "sequence_type": item.get("sequence_type"),
        "item_num": item.get("item_num"),
        "item_title": item.get("item_title"),
        "start_page": item.get("start_page"),
        "end_page": item.get("end_page"),
        "heading_source": item.get("heading_source"),
        "item_text": item.get("text"),
    }
    return f"{prompt_text.strip()}\n\nReturn only JSON matching the schema.\n\nClassify this agenda item:\n{json.dumps(payload, ensure_ascii=False, indent=2)}\n"


def _parse_response(text: str) -> dict[str, Any]:
    text = text.strip()
    if text.startswith("```"):
        lines = text.splitlines()
        if lines and lines[0].startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].startswith("```"):
            lines = lines[:-1]
        text = "\n".join(lines).strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        start = text.find("{")
        end = text.rfind("}")
        if start != -1 and end != -1 and end > start:
            return json.loads(text[start : end + 1])
        raise


def classify_item(item: dict[str, Any], prompt_path: Path = DEFAULT_PROMPT_PATH, model: str = DEFAULT_MODEL) -> dict[str, Any]:
    if shutil.which("codex") is None:
        raise RuntimeError("`codex` CLI is not installed or not on PATH.")
    prompt_text = prompt_path.read_text()
    prompt = _build_prompt(prompt_text, item)
    with tempfile.TemporaryDirectory() as tmp:
        tmpdir = Path(tmp)
        schema_path = tmpdir / "schema.json"
        output_path = tmpdir / "response.json"
        schema_path.write_text(json.dumps(SCHEMA, indent=2))
        cmd = [
            "codex",
            "exec",
            "--skip-git-repo-check",
            "--model",
            model,
            "--output-schema",
            str(schema_path),
            "-o",
            str(output_path),
            "-C",
            str(ROOT),
            "-",
        ]
        proc = subprocess.run(cmd, input=prompt, text=True, capture_output=True)
        if proc.returncode != 0:
            raise RuntimeError(f"Codex failed for {item['item_id']}: {proc.stderr.strip() or proc.stdout.strip()}")
        raw = output_path.read_text().strip() if output_path.exists() else proc.stdout.strip()
        parsed = _parse_response(raw)
    return {
        "item_id": item["item_id"],
        "item": item,
        "llm_result": parsed,
        "classified_at": datetime.now(UTC).isoformat(),
        "model": model,
    }


def classify_items(
    marker_dir: Path = MARKER_DIR,
    prompt_path: Path = DEFAULT_PROMPT_PATH,
    output_path: Path = DEFAULT_CLASSIFICATIONS_PATH,
    model: str = DEFAULT_MODEL,
    run: str | None = None,
    limit: int | None = None,
    workers: int = 4,
    overwrite: bool = False,
    show_progress: bool = True,
) -> Path:
    items = load_items(marker_dir=marker_dir, run=run, limit=limit)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    existing: dict[str, dict[str, Any]] = {}
    if output_path.exists() and not overwrite:
        for line in output_path.read_text().splitlines():
            if line.strip():
                rec = json.loads(line)
                existing[rec["item_id"]] = rec

    pending = [item for item in items if overwrite or item["item_id"] not in existing]
    results = list(existing.values())

    total = len(items)
    skipped = len(existing) if not overwrite else 0
    if show_progress:
        print(
            f"[llm] loaded={total} pending={len(pending)} skipped={skipped} workers={max(1, workers)} model={model}",
            file=sys.stderr,
            flush=True,
        )

    completed = 0
    with ThreadPoolExecutor(max_workers=max(1, workers)) as ex:
        futures = {ex.submit(classify_item, item, prompt_path, model): item for item in pending}
        for fut in as_completed(futures):
            item = futures[fut]
            try:
                results.append(fut.result())
            except Exception as exc:
                if show_progress:
                    print(f"[llm] error item_id={item['item_id']} error={exc}", file=sys.stderr, flush=True)
                raise
            completed += 1
            output_path.write_text(
                "\n".join(json.dumps(rec, ensure_ascii=False) for rec in sorted(results, key=lambda r: r["item_id"])) + "\n"
            )
            if show_progress:
                print(
                    f"[llm] completed={completed}/{len(pending)} item_id={item['item_id']}",
                    file=sys.stderr,
                    flush=True,
                )
    if not results:
        output_path.write_text("")
    elif show_progress:
        print(f"[llm] wrote {output_path}", file=sys.stderr, flush=True)
    return output_path


def export_llm_validation_bundle(classifications_path: Path = DEFAULT_CLASSIFICATIONS_PATH, output_path: Path = DEFAULT_VALIDATION_BUNDLE_PATH, limit: int | None = None) -> Path:
    rows = [json.loads(line) for line in classifications_path.read_text().splitlines() if line.strip()]
    rows = sorted(rows, key=lambda r: r["item_id"])
    cases: list[dict[str, Any]] = []
    case_idx = 0
    for row in rows:
        item = row["item"]
        llm = row["llm_result"]
        base_pred = {
            "run": item.get("run"),
            "sequence_id": item.get("sequence_id"),
            "sequence_type": item.get("sequence_type"),
            "item_num": item.get("item_num"),
            "item_title": item.get("item_title"),
            "model": row.get("model"),
            "item_confidence": llm.get("item_summary", {}).get("confidence"),
            "adoption_context": llm.get("item_summary", {}).get("adoption_context"),
            "substantive_discussion": llm.get("item_summary", {}).get("substantive_discussion"),
        }

        for output in llm.get("outputs", []):
            cases.append(
                {
                    "case_id": f"llm_output::{case_idx:06d}",
                    "case_type": "llm_output",
                    "predicted": {**base_pred, **output},
                    "evidence": {
                        "primary_evidence": output.get("evidence"),
                        "item_text": item.get("text"),
                        "item_metadata": {
                            "heading_source": item.get("heading_source"),
                            "start_page": item.get("start_page"),
                            "end_page": item.get("end_page"),
                            "start_line": item.get("start_line"),
                            "end_line": item.get("end_line"),
                        },
                    },
                    "review": {"status": "unreviewed", "corrected": {}, "notes": ""},
                }
            )
            case_idx += 1

        for paper in llm.get("papers", []):
            cases.append(
                {
                    "case_id": f"llm_paper::{case_idx:06d}",
                    "case_type": "llm_paper",
                    "predicted": {**base_pred, **paper},
                    "evidence": {
                        "primary_evidence": paper.get("evidence"),
                        "item_text": item.get("text"),
                        "item_metadata": {
                            "heading_source": item.get("heading_source"),
                            "start_page": item.get("start_page"),
                            "end_page": item.get("end_page"),
                            "start_line": item.get("start_line"),
                            "end_line": item.get("end_line"),
                        },
                    },
                    "review": {"status": "unreviewed", "corrected": {}, "notes": ""},
                }
            )
            case_idx += 1

        for link in llm.get("paper_output_links", []):
            cases.append(
                {
                    "case_id": f"llm_link::{case_idx:06d}",
                    "case_type": "llm_link",
                    "predicted": {**base_pred, **link},
                    "evidence": {
                        "primary_evidence": link.get("evidence"),
                        "item_text": item.get("text"),
                        "item_metadata": {
                            "heading_source": item.get("heading_source"),
                            "start_page": item.get("start_page"),
                            "end_page": item.get("end_page"),
                            "start_line": item.get("start_line"),
                            "end_line": item.get("end_line"),
                        },
                    },
                    "review": {"status": "unreviewed", "corrected": {}, "notes": ""},
                }
            )
            case_idx += 1

    if limit is not None:
        cases = cases[:limit]
    bundle = {
        "generated_at": datetime.now(UTC).isoformat(),
        "classifications_path": str(classifications_path),
        "case_types": ["llm_output", "llm_paper", "llm_link"],
        "case_count": len(cases),
        "cases": cases,
    }
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(bundle, ensure_ascii=False, indent=2))
    return output_path
