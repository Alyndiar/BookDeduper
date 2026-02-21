from __future__ import annotations
from typing import List, Tuple

FORMAT_RANK = {
    "epub": 1000,
    "azw3": 900,
    "azw": 850,
    "mobi": 800,
}

OTHER_RANK = {
    "pdf": 500,
    "html": 450, "htm": 450,
    "docx": 420,
    "doc": 400,
    "rtf": 380,
    "txt": 300,
    "fb2": 340,
    "djvu": 320,
    "chm": 310,
}

QUALITY_RANK = {
    "retail": 100,
    "v5": 90,
    "v4": 80,
    "v3": 70,
    "v2": 60,
    "v1": 50,
}

WORSE_THAN_NONE = {"raw", "unproofed", "un-proofed", "un proofed"}

def format_score(ext: str) -> int:
    ext = (ext or "").lower()
    if ext in FORMAT_RANK:
        return FORMAT_RANK[ext]
    return OTHER_RANK.get(ext, 100)

def quality_score(tags_lower: List[str]) -> int:
    if not tags_lower:
        return 0
    best = 0
    for t in tags_lower:
        if t in QUALITY_RANK:
            best = max(best, QUALITY_RANK[t])
    if any(t in WORSE_THAN_NONE for t in tags_lower):
        best -= 10
    return best

def is_named_format(ext: str) -> bool:
    ext = (ext or "").lower()
    return ext in ("epub", "azw3", "azw", "mobi")

def pick_best(files: List[dict]) -> Tuple[dict, List[int]]:
    named_present = any(is_named_format(f["ext_effective"]) for f in files)
    pdf_files = [f for f in files if (f["ext_effective"] or "").lower() == "pdf"]
    keep_extra_ids: List[int] = []

    def key(f: dict):
        fs = format_score(f["ext_effective"])
        qs = quality_score(f["tags_lower"])
        return (fs, qs, int(f["mtime_ns"]), int(f["size"]))

    best = max(files, key=key)

    if not named_present and pdf_files:
        best_pdf = max(pdf_files, key=key)
        if best_pdf["id"] != best["id"]:
            keep_extra_ids.append(best_pdf["id"])

    return best, keep_extra_ids
