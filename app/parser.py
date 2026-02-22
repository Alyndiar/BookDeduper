from __future__ import annotations
import os
import re
import unicodedata
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

_SUFFIX_CANON = {
    "jr": "Jr",
    "sr": "Sr",
    "junior": "Junior",
    "senior": "Senior",
    "aine": "Aîné",
    "ainee": "Aînée",
}


def _strip_accents(s: str) -> str:
    nkfd = unicodedata.normalize("NFKD", s)
    return "".join(ch for ch in nkfd if not unicodedata.combining(ch))


def _capitalize_word(word: str) -> str:
    if not word:
        return ""
    return word[:1].upper() + word[1:].lower()


def _normalize_suffix(word: str) -> str:
    key = _strip_accents(word).lower().rstrip(".")
    return _SUFFIX_CANON.get(key, _capitalize_word(word))


def _normalize_author_part(raw: str) -> str:
    author = " ".join((raw or "").strip().split())
    if not author:
        return ""

    if "," in author:
        left, right = [x.strip() for x in author.split(",", 1)]
        if left and right:
            author = f"{right} {left}"

    words = author.split()
    if not words:
        return ""

    normalized_words: List[str] = []
    for i, word in enumerate(words):
        is_last = (i == len(words) - 1)
        if is_last:
            normalized_words.append(_normalize_suffix(word))
        else:
            normalized_words.append(_capitalize_word(word))

    return " ".join(normalized_words)


def normalize_author_display(author: str) -> str:
    parts = [p.strip() for p in re.split(r"\s*&\s*", author or "") if p.strip()]
    if not parts:
        return "Unknown"
    normalized = [_normalize_author_part(p) for p in parts]
    normalized = [n for n in normalized if n]
    return " & ".join(normalized) if normalized else "Unknown"


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

    author = normalize_author_display(author)
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
