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


def parse_author_dump_line(line: str) -> Optional[dict[str, Any]]:
    parts = line.rstrip("\n").split("\t", 4)
    if len(parts) < 5:
        return None
    try:
        payload = json.loads(parts[4])
    except Exception:
        return None
    if not isinstance(payload, dict):
        return None

    ol_key = str(payload.get("key") or "").strip()
    canonical_name = str(payload.get("name") or "").strip()
    if not ol_key or not canonical_name:
        return None

    last_modified = ""
    lm = payload.get("last_modified")
    if isinstance(lm, dict):
        last_modified = str(lm.get("value") or "").strip()
    if not last_modified:
        last_modified = str(parts[3] or "").strip()

    canonical_norm = normalize_text(canonical_name)
    token_count = len([t for t in canonical_norm.split() if t])

    aliases_norm: list[str] = []
    seen: set[str] = set()

    def _add_alias(raw_name: Any):
        txt = str(raw_name or "").strip()
        if not txt:
            return
        n = normalize_text(txt)
        if not n or n in seen:
            return
        seen.add(n)
        aliases_norm.append(n)

    _add_alias(canonical_name)
    _add_alias(payload.get("personal_name"))
    _add_alias(payload.get("fuller_name"))
    alternates = payload.get("alternate_names")
    if isinstance(alternates, list):
        for a in alternates:
            _add_alias(a)

    return {
        "ol_key": ol_key,
        "last_modified": last_modified,
        "canonical_name": canonical_name,
        "canonical_norm": canonical_norm,
        "aliases_norm": aliases_norm,
        "token_count": token_count,
    }


def extract_author_name_from_dump_line(line: str) -> str:
    rec = parse_author_dump_line(line)
    return str(rec.get("canonical_name") or "") if rec else ""


def is_single_token_name(norm: str) -> bool:
    tokens = [t for t in norm.split() if t]
    return len(tokens) <= 1


def read_epub_metadata(path: str) -> dict:
    """Read author and title from an EPUB's OPF metadata. Returns {} on any error."""
    import zipfile
    import xml.etree.ElementTree as ET
    try:
        with zipfile.ZipFile(path, "r") as zf:
            try:
                container_data = zf.read("META-INF/container.xml")
            except KeyError:
                return {}
            root = ET.fromstring(container_data)
            ns = {"c": "urn:oasis:names:tc:opendocument:xmlns:container"}
            rootfile_el = root.find(".//c:rootfile", ns)
            if rootfile_el is None:
                return {}
            opf_path = rootfile_el.get("full-path")
            if not opf_path:
                return {}
            try:
                opf_data = zf.read(opf_path)
            except KeyError:
                return {}
            opf_root = ET.fromstring(opf_data)
            DC = "http://purl.org/dc/elements/1.1/"
            creator_el = opf_root.find(f".//{{{DC}}}creator")
            title_el = opf_root.find(f".//{{{DC}}}title")
            result = {}
            if creator_el is not None and creator_el.text:
                result["author"] = creator_el.text.strip()
            if title_el is not None and title_el.text:
                result["title"] = title_el.text.strip()
            return result
    except Exception:
        return {}
