from __future__ import annotations

import json
import shutil
import subprocess
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from .config import ensure_data_dir, get_marker_dir

ROOT = Path(__file__).resolve().parent.parent
MARKER_DIR = get_marker_dir()
DEFAULT_PROMPT_PATH = ROOT / "prompt_check.md"
DEFAULT_CLASSIFICATIONS_PATH = MARKER_DIR / "item_llm_classifications.jsonl"
DEFAULT_VALIDATION_BUNDLE_PATH = MARKER_DIR / "item_llm_validation_bundle.json"
DEFAULT_MODEL = "gpt-5.4-mini"

SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "formal_output_mentioned": {"type": "boolean"},
        "formal_output_types": {
            "type": "array",
            "items": {"type": "string", "enum": ["Measure", "Resolution", "Decision", "Recommendation"]},
        },
        "adoption_context": {"type": "boolean"},
        "substantive_discussion": {"type": "boolean"},
        "input_documents_mentioned": {"type": "boolean"},
        "input_document_types": {
            "type": "array",
            "items": {"type": "string", "enum": ["WP", "IP", "SP", "BP"]},
        },
        "confidence": {"type": "string", "enum": ["high", "medium", "low"]},
        "reason": {"type": "string"},
        "evidence_spans": {"type": "array", "items": {"type": "string"}},
    },
    "required": [
        "formal_output_mentioned",
        "formal_output_types",
        "adoption_context",
        "substantive_discussion",
        "input_documents_mentioned",
        "input_document_types",
        "confidence",
        "reason",
        "evidence_spans",
    ],
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
    return (
        f"{prompt_text.strip()}\n\n"
        "Important: more than one formal output type may be present in the same item, and none may be present. "
        "Return only JSON matching the schema.\n\n"
        "Classify this ATCM item:\n"
        f"{json.dumps(payload, ensure_ascii=False, indent=2)}\n"
    )


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


def classify_item(
    item: dict[str, Any],
    prompt_path: Path = DEFAULT_PROMPT_PATH,
    model: str = DEFAULT_MODEL,
) -> dict[str, Any]:
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
            raise RuntimeError(
                f"Codex failed for {item['item_id']}: {proc.stderr.strip() or proc.stdout.strip()}"
            )
        raw = output_path.read_text().strip() if output_path.exists() else proc.stdout.strip()
        parsed = _parse_response(raw)
    return {
        "item_id": item["item_id"],
        "item": item,
        "llm_classification": parsed,
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

    with ThreadPoolExecutor(max_workers=max(1, workers)) as ex:
        futures = {ex.submit(classify_item, item, prompt_path, model): item for item in pending}
        for fut in as_completed(futures):
            results.append(fut.result())
            output_path.write_text(
                "\n".join(json.dumps(rec, ensure_ascii=False) for rec in sorted(results, key=lambda r: r["item_id"])) + "\n"
            )
    if not results:
        output_path.write_text("")
    return output_path


def export_llm_validation_bundle(
    classifications_path: Path = DEFAULT_CLASSIFICATIONS_PATH,
    output_path: Path = DEFAULT_VALIDATION_BUNDLE_PATH,
    limit: int | None = None,
) -> Path:
    rows = [json.loads(line) for line in classifications_path.read_text().splitlines() if line.strip()]
    rows = sorted(rows, key=lambda r: r["item_id"])
    if limit is not None:
        rows = rows[:limit]
    cases = []
    for idx, row in enumerate(rows):
        item = row["item"]
        llm = row["llm_classification"]
        cases.append(
            {
                "case_id": f"llm_item::{idx:06d}",
                "case_type": "llm_item",
                "predicted": {
                    "run": item.get("run"),
                    "sequence_id": item.get("sequence_id"),
                    "sequence_type": item.get("sequence_type"),
                    "item_num": item.get("item_num"),
                    "item_title": item.get("item_title"),
                    "model": row.get("model"),
                    **llm,
                },
                "evidence": {
                    "primary_evidence": "\n".join(llm.get("evidence_spans", [])),
                    "item_text": item.get("text"),
                    "item_metadata": {
                        "heading_source": item.get("heading_source"),
                        "start_page": item.get("start_page"),
                        "end_page": item.get("end_page"),
                        "start_line": item.get("start_line"),
                        "end_line": item.get("end_line"),
                    },
                },
                "review": {
                    "status": "unreviewed",
                    "corrected": {},
                    "notes": "",
                },
            }
        )
    bundle = {
        "generated_at": datetime.now(UTC).isoformat(),
        "classifications_path": str(classifications_path),
        "case_types": ["llm_item"],
        "case_count": len(cases),
        "cases": cases,
    }
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(bundle, ensure_ascii=False, indent=2))
    return output_path
