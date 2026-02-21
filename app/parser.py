from __future__ import annotations
import os
import re
from dataclasses import dataclass
from typing import Optional, Tuple, List
from .util import normalize_text

TAG_RE = re.compile(r"\(([^)]+)\)$")
BRACKET_TAG_RE = re.compile(r"\[([^\]]+)\]$")

STRICT_RE = re.compile(
    r"""
    ^\s*
    (?P<author>.+?)
    (?:\s*-\s*\[(?P<series>.+?)\s+(?P<series_index>\d+(?:\.\d+)?)\])?
    \s*-\s*
    (?P<title>.+?)
    \s*$
    """,
    re.VERBOSE
)

def strip_trailing_tags(stem: str) -> Tuple[str, List[str]]:
    tags: List[str] = []
    s = stem.strip()
    while True:
        m = TAG_RE.search(s)
        if m:
            tags.append(m.group(1).strip())
            s = s[:m.start()].rstrip()
            continue
        m2 = BRACKET_TAG_RE.search(s)
        if m2:
            tags.append(m2.group(1).strip())
            s = s[:m2.start()].rstrip()
            continue
        break
    return s, tags

@dataclass
class Parsed:
    author: str
    series: Optional[str]
    series_index: Optional[float]
    title: str
    tags: List[str]

    author_norm: str
    series_norm: str
    title_norm: str
    work_key: str

def make_work_key(author_norm: str, series_norm: str, title_norm: str, series_index_norm: str) -> str:
    return f"{author_norm}||{series_norm}||{series_index_norm}||{title_norm}"

def parse_filename(name: str) -> Parsed:
    base = os.path.basename(name)
    stem = base
    if "." in stem:
        stem = stem.rsplit(".", 1)[0]

    stem, tags = strip_trailing_tags(stem)

    author = "Unknown"
    series = None
    series_index = None
    title = stem.strip()

    m = STRICT_RE.match(stem)
    if m:
        author = (m.group("author") or "").strip()
        title = (m.group("title") or "").strip()
        if m.group("series"):
            series = m.group("series").strip()
        if m.group("series_index"):
            try:
                series_index = float(m.group("series_index"))
            except Exception:
                series_index = None
    else:
        parts = [p.strip() for p in stem.split(" - ") if p.strip()]
        if len(parts) >= 2:
            author = parts[0]
            title = parts[-1]
            mid = " - ".join(parts[1:-1]).strip() if len(parts) > 2 else ""
            sm = re.search(r"\[(.+?)\s+(\d+(?:\.\d+)?)\]", mid)
            if sm:
                series = sm.group(1).strip()
                try:
                    series_index = float(sm.group(2))
                except Exception:
                    series_index = None
            else:
                sm2 = re.search(r"(.+?)\s+(\d+(?:\.\d+)?)$", mid)
                if sm2:
                    series = sm2.group(1).strip()
                    try:
                        series_index = float(sm2.group(2))
                    except Exception:
                        series_index = None

    author_norm = normalize_text(author)
    series_norm = normalize_text(series or "")
    title_norm = normalize_text(title)

    series_index_norm = ""
    if series_index is not None:
        series_index_norm = f"{series_index:05.1f}".lstrip("0")
        if series_index_norm.startswith("."):
            series_index_norm = "0" + series_index_norm

    work_key = make_work_key(author_norm, series_norm, title_norm, series_index_norm)

    return Parsed(
        author=author,
        series=series,
        series_index=series_index,
        title=title,
        tags=tags,
        author_norm=author_norm,
        series_norm=series_norm,
        title_norm=title_norm,
        work_key=work_key,
    )

def detect_quality_tags(tags: List[str]) -> List[str]:
    return [t.strip().lower() for t in tags if t.strip()]
