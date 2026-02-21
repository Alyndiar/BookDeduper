from __future__ import annotations
import os
import subprocess
from typing import Dict, Optional
from .util import norm_path

def detect_7z(existing: Optional[str]) -> Optional[str]:
    candidates = []
    if existing and os.path.isfile(existing):
        candidates.append(existing)

    candidates += [
        r"C:\Program Files\7-Zip\7z.exe",
        r"C:\Program Files (x86)\7-Zip\7z.exe",
    ]
    for c in candidates:
        if c and os.path.isfile(c):
            return c

    try:
        cp = subprocess.run(["where", "7z"], capture_output=True, text=True)
        if cp.returncode == 0:
            first = cp.stdout.strip().splitlines()[0].strip()
            if first and os.path.isfile(first):
                return first
    except Exception:
        pass
    return None

IGNORE_EXTS = {
    "jpg","jpeg","png","gif","webp","bmp","tif","tiff",
    "opf","xml","nfo","css","js","ini","db","ds_store","thumbs",
}

def list_archive_exts(sevenzip_path: str, archive_path: str, timeout_s: int = 60) -> Dict[str, int]:
    archive_path = norm_path(archive_path)
    cmd = [sevenzip_path, "l", "-slt", "--", archive_path]
    cp = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout_s, errors="replace")
    if cp.returncode != 0:
        return {}
    text = cp.stdout
    hist: Dict[str, int] = {}

    for line in text.splitlines():
        line = line.strip()
        if not line.startswith("Path ="):
            continue
        inner = line.split("=", 1)[1].strip()
        if inner.endswith("/") or inner.endswith("\\"):
            continue
        base = os.path.basename(inner)
        if "." not in base:
            continue
        ext = base.rsplit(".", 1)[-1].lower()
        if not ext or ext in IGNORE_EXTS:
            continue
        hist[ext] = hist.get(ext, 0) + 1

    return hist
