from __future__ import annotations
import os
import json
import time
import unicodedata
import re
from dataclasses import dataclass
from typing import Any, Optional

def now_ts() -> int:
    return int(time.time())

def norm_path(p: str) -> str:
    p = os.path.abspath(p)
    return os.path.normpath(p)

def to_long_path(p: str) -> str:
    p = norm_path(p)
    if p.startswith("\\\\?\\"):
        return p
    if p.startswith("\\\\"):
        return "\\\\?\\UNC\\" + p.lstrip("\\")
    return "\\\\?\\" + p

def dumps(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False)

def loads(s: str, default: Any):
    try:
        return json.loads(s)
    except Exception:
        return default

def normalize_text(s: str) -> str:
    if s is None:
        return ""
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.lower()
    s = re.sub(r"[^\w\s]", " ", s, flags=re.UNICODE)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def safe_int(x, default=0) -> int:
    try:
        return int(x)
    except Exception:
        return default

@dataclass(frozen=True)
class FileSig:
    size: int
    mtime_ns: int

def file_sig_from_stat(st: os.stat_result) -> FileSig:
    size = int(getattr(st, "st_size", 0))
    mtime_ns = int(getattr(st, "st_mtime_ns", int(st.st_mtime * 1e9)))
    return FileSig(size=size, mtime_ns=mtime_ns)

def is_probably_archive(ext: str) -> bool:
    ext = (ext or "").lower().lstrip(".")
    return ext in {
        "zip","7z","rar","tar","gz","bz2","xz","tgz","tbz","tbz2","txz","cbz","cbr","cb7"
    }

def ext_of(name: str) -> str:
    base = os.path.basename(name)
    if "." not in base:
        return ""
    return base.rsplit(".", 1)[-1].lower()


DUMP_PATTERN = re.compile(r"^ol_dump_authors_(\d{4}-\d{2}-\d{2})\.txt$")


def discover_latest_author_dump_file(folder: str) -> tuple[str | None, str | None]:
    try:
        names = os.listdir(folder)
    except Exception:
        return None, None
    best_name = None
    best_date = None
    for name in names:
        m = DUMP_PATTERN.match(name)
        if not m:
            continue
        d = m.group(1)
        if best_date is None or d > best_date:
            best_date = d
            best_name = name
    if not best_name:
        return None, None
    return os.path.join(folder, best_name), best_date


def extract_author_name_from_dump_line(line: str) -> str:
    parts = line.rstrip("\n").split("\t", 4)
    if len(parts) < 5:
        return ""
    try:
        payload = json.loads(parts[4])
    except Exception:
        return ""
    return str(payload.get("name") or "").strip()


def is_single_token_name(norm: str) -> bool:
    tokens = [t for t in norm.split() if t]
    return len(tokens) <= 1
