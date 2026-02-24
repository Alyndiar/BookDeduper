#!/usr/bin/env python3
from __future__ import annotations
import argparse
import json
from collections import Counter
from pathlib import Path


def analyze(path: Path, limit: int | None = None, progress_every: int = 500000):
    top_level_keys = Counter()
    watched_fields = ("key", "last_modified", "name", "personal_name", "alternate_names", "fuller_name")
    watched_present = Counter()
    missing_type_key = 0
    non_author_type = Counter()
    bad_json = 0
    short_lines = 0
    lines = 0

    with path.open("r", encoding="utf-8", errors="replace") as fh:
        for raw in fh:
            lines += 1
            if limit and lines > limit:
                break

            parts = raw.rstrip("\n").split("\t", 4)
            if len(parts) < 5:
                short_lines += 1
                continue

            payload_txt = parts[4]
            try:
                payload = json.loads(payload_txt)
            except Exception:
                bad_json += 1
                continue

            if not isinstance(payload, dict):
                bad_json += 1
                continue

            for k in payload.keys():
                top_level_keys[k] += 1
            for f in watched_fields:
                if f in payload and payload.get(f) not in (None, "", []):
                    watched_present[f] += 1

            t = payload.get("type")
            if isinstance(t, dict):
                tkey = t.get("key")
                if tkey != "/type/author":
                    non_author_type[str(tkey)] += 1
            else:
                missing_type_key += 1

            if progress_every and lines % progress_every == 0:
                print(f"processed {lines:,} lines...", flush=True)

    print("\n=== OpenLibrary dump field profile ===")
    print(f"file: {path}")
    print(f"lines_seen: {lines}")
    print(f"short_lines(<5 tab fields): {short_lines}")
    print(f"bad_json_payloads: {bad_json}")
    print(f"records_missing_type_key_obj: {missing_type_key}")
    if non_author_type:
        print("non_/type/author type.key values:")
        for key, count in non_author_type.most_common(10):
            print(f"  {key!r}: {count}")

    print("\nWatched field presence:")
    for f in watched_fields:
        print(f"  {f}: {watched_present.get(f, 0)}")

    print("\nTop payload keys:")
    for key, count in top_level_keys.most_common(50):
        print(f"  {key}: {count}")


def main():
    ap = argparse.ArgumentParser(description="Profile JSON fields present in an OpenLibrary author dump file.")
    ap.add_argument("dump_file", help="Path to ol_dump_authors_YYYY-MM-DD.txt")
    ap.add_argument("--limit", type=int, default=0, help="Optional max lines to inspect (0=all)")
    ap.add_argument("--progress-every", type=int, default=500000, help="Print progress every N lines")
    args = ap.parse_args()

    path = Path(args.dump_file)
    if not path.exists():
        raise SystemExit(f"Not found: {path}")

    analyze(path, limit=(args.limit or None), progress_every=args.progress_every)


if __name__ == "__main__":
    main()
