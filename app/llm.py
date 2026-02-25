"""LLM-based metadata enrichment via KoboldCpp for filenames that heuristics could not parse."""
from __future__ import annotations

import json
import logging
from typing import Callable, Optional

import requests

from .parser import Parsed, make_work_key, normalize_author_display
from .util import normalize_text

logger = logging.getLogger(__name__)

# KoboldCpp endpoint — adjust host/port if needed
KOBOLD_URL = "http://localhost:5001/api/v1/generate"

# Confidence level assigned to fields extracted by the LLM
LLM_CONFIDENCE = 0.72

# Filenames per request.  14B Q4 handles 20 reliably; reduce if you see ordering errors.
LLM_BATCH_SIZE = 20

# Seconds to wait for a full batch response
LLM_TIMEOUT_S = 120

# GBNF grammar: constrains output to a valid JSON array of objects.
# Key names are enforced through the prompt, not the grammar.
_GRAMMAR = r"""
root    ::= "[" ws object ("," ws object)* ws "]"
object  ::= "{" ws pair ("," ws pair)* ws "}"
pair    ::= string ws ":" ws value
value   ::= string | number | "null" | array | object
string  ::= "\"" char* "\""
char    ::= [^"\\] | "\\" ["\\/bfnrt]
number  ::= "-"? [0-9]+ ("." [0-9]+)?
array   ::= "[" ws (value ("," ws value)*)? ws "]"
ws      ::= [ \t\n\r]*
"""

_PROMPT_HEADER = (
    "### Instruction:\n"
    "You are a book metadata extractor. Automated filename parsing has already been attempted "
    "and failed or produced low-confidence results for each filename below.\n\n"
    "Rules:\n"
    "- Each filename may contain some or none of: author name, series name, series number, "
    "title, format/quality tags.\n"
    "- There may be NO author at all — do not invent one. Return null for author if unsure.\n"
    "- Tags are format or quality markers such as: epub, retail, ocr, scan, mobi, pdf, "
    "fixed, v2, v3.\n"
    "- Series index is a number (may be decimal, e.g. 1.5).\n"
    "- The separator between fields is usually \" - \" (space dash space) but may be absent "
    "or different.\n"
    "- Return null for any field you cannot confidently identify.\n\n"
    "For each filename return exactly one JSON object with these keys:\n"
    '  "author": string or null\n'
    '  "series": string or null\n'
    '  "series_index": number or null\n'
    '  "title": string or null\n'
    '  "tags": array of strings (empty array if none)\n\n'
    "Return a JSON array with one object per filename, in the same order as the input.\n\n"
    "Filenames:\n"
)


def _build_prompt(stems: list[str]) -> str:
    numbered = "\n".join(f"{i + 1}. {s}" for i, s in enumerate(stems))
    return _PROMPT_HEADER + numbered + "\n\n### Response:\n"


def _call_llm(stems: list[str]) -> list[dict] | None:
    """Send one batch to KoboldCpp. Returns a list of result dicts or None on failure."""
    payload = {
        "prompt": _build_prompt(stems),
        "max_length": 80 * len(stems),  # ~80 tokens per result covers all four fields
        "temperature": 0,
        "grammar": _GRAMMAR,
    }
    try:
        r = requests.post(KOBOLD_URL, json=payload, timeout=LLM_TIMEOUT_S)
        r.raise_for_status()
        text = r.json()["results"][0]["text"].strip()
        results = json.loads(text)
        if not isinstance(results, list):
            logger.warning("LLM returned non-list for %d stems", len(stems))
            return None
        if len(results) != len(stems):
            logger.warning(
                "LLM count mismatch: expected %d, got %d", len(stems), len(results)
            )
            return None
        return results
    except Exception as exc:
        logger.warning("LLM call failed: %s", exc)
        return None


def _series_index_norm(series_index: float | None) -> str:
    if series_index is None:
        return ""
    s = f"{series_index:05.1f}".lstrip("0")
    return "0" + s if s.startswith(".") else s


def _apply_result(parsed: Parsed, result: dict) -> None:
    """Apply a single LLM result dict to a Parsed object in-place."""
    changed = False

    author = result.get("author")
    if author and isinstance(author, str) and author.strip():
        parsed.author = normalize_author_display(author.strip())
        parsed.author_norm = normalize_text(parsed.author)
        parsed.author_confidence = LLM_CONFIDENCE
        parsed.author_reason = "llm_extracted"
        changed = True

    series = result.get("series")
    if series and isinstance(series, str) and series.strip():
        parsed.series = series.strip()
        parsed.series_norm = normalize_text(parsed.series)
        changed = True

    series_index = result.get("series_index")
    if series_index is not None:
        try:
            parsed.series_index = float(series_index)
            changed = True
        except (TypeError, ValueError):
            pass

    title = result.get("title")
    if title and isinstance(title, str) and title.strip():
        parsed.title = title.strip()
        parsed.title_norm = normalize_text(parsed.title)
        changed = True

    tags = result.get("tags")
    if isinstance(tags, list) and tags:
        parsed.tags = [str(t).strip() for t in tags if t]
        changed = True

    if changed:
        parsed.work_key = make_work_key(
            parsed.author_norm,
            parsed.series_norm,
            parsed.title_norm,
            _series_index_norm(parsed.series_index),
        )
        parsed.needs_llm = False


def enrich_with_llm(
    pending: list[tuple[str, str, Parsed]],
    progress_cb: Optional[Callable[[str], None]] = None,
) -> None:
    """
    Enrich a list of (path, stem, Parsed) triples using the LLM.

    Parsed objects are updated in-place.  needs_llm is cleared on success.
    On batch failure, each item in the failed batch is retried individually.
    """
    total = len(pending)
    for i in range(0, total, LLM_BATCH_SIZE):
        chunk = pending[i: i + LLM_BATCH_SIZE]
        done = min(i + LLM_BATCH_SIZE, total)
        if progress_cb:
            progress_cb(f"LLM enrichment: {done}/{total} files")

        stems = [stem for _, stem, _ in chunk]
        results = _call_llm(stems)

        if results is None:
            # Batch failed — retry each file individually to recover partial results
            for _, stem, parsed in chunk:
                single = _call_llm([stem])
                if single:
                    _apply_result(parsed, single[0])
            continue

        for (_, _, parsed), result in zip(chunk, results):
            _apply_result(parsed, result)
