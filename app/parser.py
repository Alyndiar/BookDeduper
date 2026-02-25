from __future__ import annotations
import os
import re
import unicodedata
import logging
import time
from dataclasses import dataclass
from difflib import SequenceMatcher
from typing import Optional, Tuple, List, Dict, Any, Callable
from .util import normalize_text

logger = logging.getLogger(__name__)

TAG_RE = re.compile(r"\(([^)]+)\)$")
BRACKET_TAG_RE = re.compile(r"\[([^\]]+)\]$")

STRICT_RE = re.compile(
    r"""
    ^\s*
    (?P<author>.+?)
    (?:[ ]+-[ ]+\[(?P<series>.+?)\s+(?P<series_index>\d+(?:\.\d+)?)\])?
    [ ]+-[ ]+
    (?P<title>.+?)
    \s*$
    """,
    re.VERBOSE
)

LLM_CONFIDENCE_THRESHOLD = 0.65  # files below this confidence are queued for LLM enrichment

# Multi-part split thresholds (Author - Series - Title case)
# Derived from _is_name_like scoring:
#   clean 2-token name:            0.5 + 2×0.02          = 0.54
#   article-led string ("The …"): 0.5 − 0.12 + n×0.02  ≤ 0.44
# 0.45 sits cleanly between those two bands.
MULTI_PART_AUTHOR_THRESHOLD = 0.45
# Last segment must beat first by this margin to override the conventional Author-first order.
MULTI_PART_FIRST_MARGIN = 0.10

STOPWORDS = {
    "volume", "vol", "book", "series", "edition", "part", "tome", "integrale", "complete", "collection"
}
TITLE_LEAD_WORDS = {"the", "a", "an"}
TAG_WORDS = {"retail", "ocr", "scan", "fixed", "v2", "v3", "v4", "v5"}
# Words that appear in sentences but never in author names; any hit signals a sentence fragment.
SENTENCE_WORDS = {
    "is", "are", "was", "were", "be", "been", "being",
    "has", "have", "had", "do", "does", "did",
    "will", "would", "could", "should", "may", "might", "shall", "can",
    "this", "that", "these", "those",
    "he", "she", "they", "we", "you", "it",
    "his", "her", "their", "our", "your", "its",
}
DASH_SEPS = [" - ", " – ", " — "]

_SUFFIX_CANON = {
    "jr": "Jr",
    "sr": "Sr",
    "junior": "Junior",
    "senior": "Senior",
    "aine": "Aîné",
    "ainee": "Aînée",
}


@dataclass
class AuthorCandidate:
    display: str
    normalized: str
    score: float
    reason: str


@dataclass
class MergeSuggestion:
    left_name: str
    right_name: str
    similarity: float
    reason: str
    auto_merge_safe: bool


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

    author_confidence: float = 0.0
    author_reason: str = ""
    author_trace: str = ""
    needs_llm: bool = False


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


def _is_name_like(text: str) -> Tuple[bool, float, List[str]]:
    cleaned = text.strip()
    if not cleaned:
        return False, 0.0, ["empty"]

    norm = normalize_text(cleaned)
    tokens = [t for t in norm.split() if t]
    reasons: List[str] = []

    if tokens and tokens[0][0].isdigit():
        return False, 0.0, ["leading_number"]

    if not (2 <= len(tokens) <= 5):
        reasons.append("token_count")
    score = 0.5

    if any(t in SENTENCE_WORDS for t in tokens):
        score -= 0.4
        reasons.append("sentence_word")

    if any(t in STOPWORDS for t in tokens):
        score -= 0.25
        reasons.append("stopword_penalty")
    if any(t in TAG_WORDS for t in tokens):
        score -= 0.25
        reasons.append("tag_penalty")
    if tokens and tokens[0] in TITLE_LEAD_WORDS:
        score -= 0.12
        reasons.append("title_lead_penalty")

    digit_chars = sum(ch.isdigit() for ch in cleaned)
    if digit_chars >= max(3, len(cleaned) // 4):
        score -= 0.3
        reasons.append("digit_heavy")

    if re.search(r"\b97[89]\d{10}\b", _strip_accents(cleaned).replace("-", "")):
        score -= 0.4
        reasons.append("isbn_like")

    if re.search(r"\b([A-Za-z]\.){1,4}\b", cleaned):
        score += 0.08
        reasons.append("initials_pattern")

    if "," in cleaned:
        score += 0.05
        reasons.append("comma_order")

    for t in tokens:
        if len(t) > 1 and t[0].isalpha():
            score += 0.02

    valid = score >= 0.25 and len(tokens) >= 2
    return valid, max(0.0, min(score, 1.0)), reasons


def _add_candidate(out: List[AuthorCandidate], raw_author: str, base_score: float, reason: str):
    display = normalize_author_display(raw_author)
    normalized = normalize_text(display)
    if not normalized or normalized == "unknown":
        return
    valid, name_score, penalties = _is_name_like(raw_author)
    score = max(0.0, min(1.0, base_score + (name_score - 0.5) * 0.4))
    if not valid:
        score *= 0.5
    full_reason = f"{reason};heur={','.join(penalties) if penalties else 'ok'}"
    out.append(AuthorCandidate(display=display, normalized=normalized, score=score, reason=full_reason))


def extract_author_candidates(stem: str, known_authors: Optional[Dict[str, Dict[str, Any]]] = None, invalid_authors: Optional[set[str]] = None) -> List[AuthorCandidate]:
    known_authors = known_authors or {}
    invalid_authors = invalid_authors or set()
    candidates: List[AuthorCandidate] = []
    s = stem.strip()

    m = re.match(r"^\s*[\[(](.+?)[\])]\s+", s)
    if m:
        _add_candidate(candidates, m.group(1), 0.95, "bracketed_prefix")

    for sep in DASH_SEPS:
        if sep in s:
            left, right = s.split(sep, 1)
            _add_candidate(candidates, left.strip(), 0.84, "prefix_split")
            if "," in left:
                _add_candidate(candidates, left.strip(), 0.96, "prefix_comma_order")
            _add_candidate(candidates, right.strip(), 0.84, "suffix_split")
            break

    for sep in DASH_SEPS:
        if sep in s:
            left, right = s.rsplit(sep, 1)
            _add_candidate(candidates, right.strip(), 0.88, "suffix_split_last")
            break

    if len(candidates) == 0:
        # "Something by Author Name" — common informal naming when no " - " separator exists
        m_by = re.search(r"(?<!\w)by(?!\w)", s, re.IGNORECASE)
        if m_by:
            right = s[m_by.end():].strip()
            if right:
                _add_candidate(candidates, right, 0.78, "by_separator")

        for part in re.split(r"[\-–—]", s):
            part = part.strip()
            if part:
                _add_candidate(candidates, part, 0.45, "infix_scan")

    boosted: List[AuthorCandidate] = []
    for c in candidates:
        if c.normalized in invalid_authors:
            continue
        freq = 0
        if c.normalized in known_authors:
            freq = int(known_authors[c.normalized].get("frequency", 0))
        boost = 0.0
        if freq > 0:
            boost = min(0.22, 0.05 + (freq / 200.0))
        score = max(0.0, min(1.0, c.score + boost))
        reason = c.reason + (f";known_boost={boost:.2f}" if boost > 0 else "")
        boosted.append(AuthorCandidate(display=c.display, normalized=c.normalized, score=score, reason=reason))

    boosted.sort(key=lambda c: (c.score, known_authors.get(c.normalized, {}).get("frequency", 0), c.display), reverse=True)
    return boosted


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


def make_work_key(author_norm: str, series_norm: str, title_norm: str, series_index_norm: str) -> str:
    return f"{author_norm}||{series_norm}||{series_index_norm}||{title_norm}"


def parse_filename(name: str, known_authors: Optional[Dict[str, Dict[str, Any]]] = None, invalid_authors: Optional[set[str]] = None) -> Parsed:
    base = os.path.basename(name)
    stem = base
    if "." in stem:
        stem = stem.rsplit(".", 1)[0]

    stem, tags = strip_trailing_tags(stem)

    author = "Unknown"
    series = None
    series_index = None
    title = stem.strip()
    trace: List[str] = []

    parts = [p.strip() for p in stem.split(" - ") if p.strip()]

    if len(parts) == 1:
        # No " - " separator — whole stem is the title, author unknown
        title = parts[0]
        trace.append("no_separator")

    elif len(parts) == 2:
        # Single " - " separator: conventionally Author - Title, but check both ends.
        valid_first, score_first, _ = _is_name_like(parts[0])
        valid_last, score_last, _ = _is_name_like(parts[1])
        first_qualifies = valid_first and score_first >= MULTI_PART_AUTHOR_THRESHOLD
        last_qualifies = valid_last and score_last >= MULTI_PART_AUTHOR_THRESHOLD

        if last_qualifies and score_last > score_first + MULTI_PART_FIRST_MARGIN:
            # Last part is notably more name-like — reversed ordering (Title - Author)
            author = parts[1]
            title = parts[0]
            trace.append("two_part_reversed")
        elif first_qualifies:
            # Conventional ordering, or both qualify with similar scores → prefer first
            author = parts[0]
            title = parts[1]
            trace.append("two_part")
        elif last_qualifies:
            # First doesn't reach threshold but last does (within the margin)
            author = parts[1]
            title = parts[0]
            trace.append("two_part_reversed")
        else:
            # Neither end is confidently a name — leave author Unknown
            title = parts[1]
            trace.append("two_part_uncertain")

    else:
        # Two or more " - " separators: Author - Series[/index] - Title
        # Most probable format regardless of whether series has brackets or a number.
        # Try STRICT_RE first — only matches when series is [bracketed with number].
        m = STRICT_RE.match(stem)
        if m and m.group("series"):
            author = (m.group("author") or "").strip()
            title = (m.group("title") or "").strip()
            series = m.group("series").strip()
            if m.group("series_index"):
                try:
                    series_index = float(m.group("series_index"))
                except Exception:
                    series_index = None
            trace.append("strict_bracketed_series")
        else:
            # Plain format: Author - Series[/index] - Title.
            # Score both ends and prefer whichever looks more like a name, with a
            # bias toward the first part to respect the conventional Author-first order.
            series_raw = " - ".join(parts[1:-1])
            valid_first, score_first, _ = _is_name_like(parts[0])
            valid_last, score_last, _ = _is_name_like(parts[-1])
            first_qualifies = valid_first and score_first >= MULTI_PART_AUTHOR_THRESHOLD
            last_qualifies = valid_last and score_last >= MULTI_PART_AUTHOR_THRESHOLD

            if last_qualifies and score_last > score_first + MULTI_PART_FIRST_MARGIN:
                # Last part is notably more name-like — reversed ordering (Title - Series - Author)
                author = parts[-1]
                title = parts[0]
                trace.append("multi_part_series_reversed")
            elif first_qualifies:
                # Conventional ordering, or both qualify with similar scores → prefer first
                author = parts[0]
                title = parts[-1]
                trace.append("multi_part_series")
            elif last_qualifies:
                # First doesn't reach threshold but last does (within the margin)
                author = parts[-1]
                title = parts[0]
                trace.append("multi_part_series_reversed")
            else:
                # Neither end is confidently a name — uncertain ordering
                author = "Unknown"
                title = parts[-1]
                series = series_raw if series_raw else None
                trace.append("multi_part_uncertain")

            # Extract series name and optional index from the middle segment
            if author != "Unknown":
                sm = re.search(r"\[(.+?)\s+(\d+(?:\.\d+)?)\]", series_raw)
                if sm:
                    series = sm.group(1).strip()
                    try:
                        series_index = float(sm.group(2))
                    except Exception:
                        series_index = None
                else:
                    sm2 = re.search(r"^(.+?)\s+(\d+(?:\.\d+)?)$", series_raw)
                    if sm2:
                        series = sm2.group(1).strip()
                        try:
                            series_index = float(sm2.group(2))
                        except Exception:
                            series_index = None
                    else:
                        series = series_raw  # plain series name, no number

    candidates = extract_author_candidates(stem, known_authors=known_authors, invalid_authors=invalid_authors)
    chosen_conf = 0.35
    chosen_reason = "fallback_unknown"
    if candidates:
        top = candidates[0]
        known_hit = bool(known_authors and top.normalized in known_authors)
        if top.reason.startswith("infix_scan") and not known_hit and top.score < 0.93:
            trace.append("infix_rejected_low_confidence")
        else:
            author = top.display
            chosen_conf = top.score
            chosen_reason = top.reason
        trace.extend([f"candidate:{c.display}:{c.score:.2f}:{c.reason}" for c in candidates[:6]])

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

    trace_text = " | ".join(trace)
    needs_llm = author_norm == "unknown" or chosen_conf < LLM_CONFIDENCE_THRESHOLD
    logger.debug("parse_filename name=%s author=%s conf=%.2f reason=%s needs_llm=%s trace=%s", name, author, chosen_conf, chosen_reason, needs_llm, trace_text)

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
        author_confidence=chosen_conf,
        author_reason=chosen_reason,
        author_trace=trace_text,
        needs_llm=needs_llm,
    )


def detect_quality_tags(tags: List[str]) -> List[str]:
    return [t.strip().lower() for t in tags if t.strip()]


def _token_jaccard(a: str, b: str) -> float:
    sa = set(a.split())
    sb = set(b.split())
    if not sa or not sb:
        return 0.0
    return len(sa & sb) / len(sa | sb)


def _punctuation_only_variant(a: str, b: str) -> bool:
    pa = re.sub(r"[\W_]+", "", a.lower())
    pb = re.sub(r"[\W_]+", "", b.lower())
    return pa == pb


def build_merge_suggestions(
    known_authors: List[Tuple[str, str, int]],
    threshold: float = 0.92,
    progress_cb: Optional[Callable[[int, int], None]] = None,
    progress_every: int = 50000,
    progress_interval_s: float = 10.0,
) -> List[MergeSuggestion]:
    suggestions: List[MergeSuggestion] = []
    n = len(known_authors)
    if n < 2:
        if progress_cb:
            progress_cb(0, 0)
        return suggestions

    # Keep progress semantics stable for UI/tests: total reflects logical all-pairs upper bound.
    upper_total = (n * (n - 1)) // 2
    now = time.monotonic()
    next_emit_ts = now + max(float(progress_interval_s), 0.1)

    def _emit(done: int):
        nonlocal next_emit_ts
        if not progress_cb:
            return
        d = max(0, min(int(done), upper_total))
        now2 = time.monotonic()
        if d in (0, upper_total) or now2 >= next_emit_ts:
            progress_cb(d, upper_total)
            next_emit_ts = now2 + max(float(progress_interval_s), 0.1)

    _emit(0)

    # Build cheap candidate blocks to avoid O(n^2) full-pair scans.
    token_index: Dict[str, List[int]] = {}
    punct_index: Dict[str, List[int]] = {}
    token_sets: List[set[str]] = []
    token_sorts: List[List[str]] = []

    for i, (ln, ld, _freq) in enumerate(known_authors):
        toks = set(t for t in ln.split() if t)
        token_sets.append(toks)
        token_sorts.append(sorted(toks))
        for t in toks:
            token_index.setdefault(t, []).append(i)
        punct = re.sub(r"[\W_]+", "", str(ld).lower())
        if punct:
            punct_index.setdefault(punct, []).append(i)
        if i and i % 1000 == 0:
            _emit(int(upper_total * 0.08 * (i / max(n, 1))))

    candidate_pairs: set[Tuple[int, int]] = set()

    # Shared-token blocking (skip very common tokens to limit fanout blowups).
    token_items = list(token_index.items())
    for ti, (_t, ids) in enumerate(token_items):
        if len(ids) > 1200:
            continue
        for a in range(len(ids)):
            ia = ids[a]
            for b in range(a + 1, len(ids)):
                ib = ids[b]
                if ia < ib:
                    candidate_pairs.add((ia, ib))
                else:
                    candidate_pairs.add((ib, ia))
        if ti and ti % 500 == 0:
            _emit(int(upper_total * (0.08 + 0.22 * (ti / max(len(token_items), 1)))))

    # Punctuation-only key blocking catches punctuation/spacing variants quickly.
    punct_items = list(punct_index.items())
    for pi, (_k, ids) in enumerate(punct_items):
        if len(ids) < 2:
            continue
        for a in range(len(ids)):
            ia = ids[a]
            for b in range(a + 1, len(ids)):
                ib = ids[b]
                if ia < ib:
                    candidate_pairs.add((ia, ib))
                else:
                    candidate_pairs.add((ib, ia))
        if pi and pi % 500 == 0:
            _emit(int(upper_total * (0.30 + 0.10 * (pi / max(len(punct_items), 1)))))

    total_candidates = len(candidate_pairs)
    checked_pairs = 0
    check_every = max(int(progress_every), 1)

    for i, j in candidate_pairs:
        ln, ld, _lf = known_authors[i]
        rn, rd, _rf = known_authors[j]
        checked_pairs += 1

        shared = len(token_sets[i] & token_sets[j])
        punct_only = _punctuation_only_variant(ld, rd)
        if shared == 0 and not punct_only:
            if checked_pairs % check_every == 0 or checked_pairs == total_candidates:
                # 40%-100% reserved for candidate evaluation phase.
                frac = (checked_pairs / max(total_candidates, 1))
                _emit(int(upper_total * (0.40 + 0.60 * frac)))
            continue

        jacc = _token_jaccard(ln, rn)
        if jacc < 0.15 and not punct_only:
            seq = 0.0
        else:
            seq = SequenceMatcher(None, ln, rn).ratio()

        sim = 0.62 * jacc + 0.38 * seq
        reason = "token+string similarity"
        if token_sorts[i] == token_sorts[j]:
            sim = max(sim, 0.97)
            reason = "comma-order variant"
        elif "," in ld or "," in rd:
            reason = "comma-order variant"
        elif punct_only:
            reason = "punctuation-only difference"

        if sim >= threshold:
            auto_merge_safe = sim >= 0.98 and punct_only
            suggestions.append(MergeSuggestion(ld, rd, sim, reason, auto_merge_safe))

        if checked_pairs % check_every == 0 or checked_pairs == total_candidates:
            frac = (checked_pairs / max(total_candidates, 1))
            _emit(int(upper_total * (0.40 + 0.60 * frac)))

    _emit(upper_total)

    suggestions.sort(key=lambda s: (s.similarity, s.left_name, s.right_name), reverse=True)
    logger.debug(
        "merge_suggestions count=%s candidate_pairs=%s upper_total_pairs=%s",
        len(suggestions), total_candidates, upper_total,
    )
    return suggestions
