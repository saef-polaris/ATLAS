from __future__ import annotations

import json
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd
from google import genai

from .config import ensure_data_dir, get_marker_dir

ROOT = Path(__file__).resolve().parent.parent
MARKER_DIR = get_marker_dir()
DEFAULT_PROMPT_PATH = ROOT / "prompt_check.md"
DEFAULT_CLASSIFICATIONS_PATH = MARKER_DIR / "item_llm_links.jsonl"
DEFAULT_VALIDATION_BUNDLE_PATH = MARKER_DIR / "item_llm_link_validation_bundle.json"
DEFAULT_MODEL = "gemma-4-26b-a4b-it"


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
    return f"{prompt_text.strip()}\n\nReturn valid JSON only.\n\nClassify this agenda item:\n{json.dumps(payload, ensure_ascii=False, indent=2)}\n"


def _strip_response_wrappers(text: str) -> str:
    text = text.strip()
    if text.startswith("```"):
        lines = text.splitlines()
        if lines and lines[0].startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].startswith("```"):
            lines = lines[:-1]
        text = "\n".join(lines).strip()
    return text


def _parse_response(text: str) -> dict[str, Any]:
    text = _strip_response_wrappers(text)
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        start = text.find("{")
        end = text.rfind("}")
        if start != -1 and end != -1 and end > start:
            return json.loads(text[start : end + 1])
        raise


def _retry_delay_seconds(exc: Exception) -> int:
    text = str(exc)
    for pattern in (r"retryDelay['\"]?: ['\"]?(\d+)s", r"retry in ([\d.]+)s"):
        match = re.search(pattern, text)
        if match:
            return max(1, int(float(match.group(1))) + 2)
    return 30


def _generate_content_with_retries(client: genai.Client, model: str, prompt: str, attempts: int = 3) -> str:
    last_exc: Exception | None = None
    for attempt in range(attempts):
        try:
            response = client.models.generate_content(model=model, contents=prompt)
            return response.text or ""
        except Exception as exc:
            last_exc = exc
            text = str(exc)
            if attempt + 1 >= attempts or ("429" not in text and "RESOURCE_EXHAUSTED" not in text):
                raise
            time.sleep(_retry_delay_seconds(exc))
    if last_exc is not None:
        raise last_exc
    raise RuntimeError("content generation failed without an exception")


def _repair_response_prompt(raw: str, parse_error: Exception) -> str:
    return f"""The following model response was intended to be valid JSON but failed to parse.

Return valid JSON only, using exactly this top-level schema:
{{
  "item_reason": "...",
  "paper_output_links": [
    {{
      "paper_label": "...",
      "output_label": "Measure N (YYYY)",
      "relation_type": "supports|discusses|proposes|informs|unclear",
      "evidence_basis": "explicit_direct|local_episode|joint_discussion",
      "confidence": "high|medium|low",
      "reason": "...",
      "evidence": "..."
    }}
  ],
  "paper_outcomes": [
    {{
      "paper_label": "...",
      "outcome": "approved|approved_with_reservations|blocked|rejected|deferred|noted|not_determined",
      "confidence": "high|medium|low",
      "reason": "...",
      "evidence": "..."
    }}
  ],
  "consensus_signals": [
    {{
      "party": "...",
      "signal_type": "reservation|statement_for_record|objection|block|conditional_support|withdrawal|chair_intervention|amendment|support",
      "paper_label": "...",
      "output_label": "...",
      "reason": "...",
      "evidence": "..."
    }}
  ]
}}

Preserve the intended links, outcomes, and consensus signals from the malformed
response where possible. If an entry is ambiguous or cannot be repaired
confidently, omit it.

Parse error: {parse_error}

Malformed response:
{raw}
"""


def _retry_classification_prompt(prompt: str, parse_error: Exception) -> str:
    return (
        f"{prompt.rstrip()}\n\n"
        "Your previous response was not valid JSON and could not be parsed. "
        f"Parser error: {parse_error}\n\n"
        "Retry the same classification task from the original item text. "
        "Return valid JSON only. Escape all quotation marks inside string values."
    )


def _parse_or_repair_response(client: genai.Client, model: str, raw: str) -> tuple[dict[str, Any], bool, str | None]:
    try:
        return _parse_response(raw), False, None
    except json.JSONDecodeError as exc:
        repaired_raw = _generate_content_with_retries(client, model, _repair_response_prompt(raw, exc), attempts=2)
        return _parse_response(repaired_raw), True, str(exc)


def classify_item(item: dict[str, Any], prompt_path: Path = DEFAULT_PROMPT_PATH, model: str = DEFAULT_MODEL) -> dict[str, Any]:
    prompt_text = prompt_path.read_text()
    prompt = _build_prompt(prompt_text, item)
    client = genai.Client()
    parse_repaired = False
    parse_error: str | None = None
    json_retry_count = 0
    raw = ""

    for attempt in range(3):
        request_prompt = prompt if attempt == 0 else _retry_classification_prompt(prompt, parse_error or "invalid JSON")
        raw = _generate_content_with_retries(client, model, request_prompt)
        try:
            parsed = _parse_response(raw)
            json_retry_count = attempt
            parse_error = None if attempt == 0 else parse_error
            break
        except json.JSONDecodeError as exc:
            parse_error = str(exc)
    else:
        json_retry_count = 3
        parsed, parse_repaired, repair_error = _parse_or_repair_response(client, model, raw)
        parse_error = parse_error or repair_error
    return {
        "item_id": item["item_id"],
        "item": item,
        "llm_result": parsed,
        "classified_at": datetime.now(UTC).isoformat(),
        "model": model,
        "parse_repaired": parse_repaired,
        "parse_error": parse_error,
        "json_retry_count": json_retry_count,
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
    for case_idx, row in enumerate(rows):
        item = row["item"]
        llm = row["llm_result"]
        links = llm.get("paper_output_links", []) or []
        outcomes = llm.get("paper_outcomes", []) or []
        signals = llm.get("consensus_signals", []) or []
        predicted = {
            "run": item.get("run"),
            "sequence_id": item.get("sequence_id"),
            "sequence_type": item.get("sequence_type"),
            "item_num": item.get("item_num"),
            "item_title": item.get("item_title"),
            "model": row.get("model"),
            "item_reason": llm.get("item_reason"),
            "paper_output_links": links,
            "paper_outcomes": outcomes,
            "consensus_signals": signals,
            "paper_labels": sorted({link.get("paper_label") for link in links if link.get("paper_label")}),
            "output_labels": sorted({link.get("output_label") for link in links if link.get("output_label")}),
            "link_count": len(links),
            "outcome_count": len(outcomes),
            "signal_count": len(signals),
        }
        primary_evidence = "\n\n".join(
            f"{link.get('paper_label', '')} -> {link.get('output_label', '')}: {link.get('evidence', '')}" for link in links
        )
        cases.append(
            {
                "case_id": f"llm_item::{case_idx:06d}",
                "case_type": "llm_item",
                "predicted": predicted,
                "evidence": {
                    "primary_evidence": primary_evidence,
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

    if limit is not None:
        cases = cases[:limit]
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
