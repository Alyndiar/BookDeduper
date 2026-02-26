#!/usr/bin/env python3
r"""
OpenLibrary -> Parquet export using DuckDB (phased, disk-safe)

Reads OL dumps (e.g. from K:\) and writes Parquet outputs to another drive
(e.g. W:\Dany\BookDeduper).

The pipeline is split into 5 phases, each using its own DuckDB connection.
Spill files are cleaned between phases so disk usage never accumulates
across the whole pipeline.  Phases whose output Parquet already exists
are skipped, making the run resumable after a crash (delete the incomplete
intermediate file to re-run that phase).

Phases:
  1. Author redirect resolution                    (tiny  – redirects dump)
  2. Fiction work-key classification by subjects    (medium – works dump)
  3. Filter to works with >= 1 English edition      (large – editions dump)
  4. Extract & canonicalize author keys             (medium – works dump)
  5. Export author details, aliases, redirects       (small – authors dump)

Expected input filenames in --dumps-dir:
  ol_dump_authors_YYYY-MM-DD.txt.gz
  ol_dump_works_YYYY-MM-DD.txt.gz
  ol_dump_editions_YYYY-MM-DD.txt.gz
  ol_dump_redirects_YYYY-MM-DD.txt.gz

Usage (same CLI as before):
  py ol_duckdb_export_fiction_eng.py \
      --dumps-dir "K:\" --out-dir "W:\Dany\BookDeduper" \
      --temp-dir "W:\Dany\BookDeduper\duckdb_tmp" \
      --dump-date "2026-01-31" --memory-limit 40GB --threads 2
"""

from __future__ import annotations

import argparse
import shutil
import time
from pathlib import Path

import duckdb


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def qpath(p: Path) -> str:
    """Convert a Path to a forward-slash string for DuckDB SQL literals."""
    return str(p).replace("\\", "/")


def clean_spill(temp_dir: Path) -> None:
    """Delete everything inside the DuckDB spill directory."""
    if not temp_dir.exists():
        return
    for item in temp_dir.iterdir():
        try:
            if item.is_dir():
                shutil.rmtree(item)
            else:
                item.unlink()
        except OSError:
            pass


def run_phase(
    phase_num: int,
    name: str,
    sql: str,
    temp_dir: Path,
    memory_limit: str,
    threads: int,
    total_phases: int = 5,
) -> duckdb.DuckDBPyResult | None:
    """Run *sql* in a fresh in-memory DuckDB connection.

    Returns the result-set of the **last** statement (usually a SELECT
    with counts for reporting).
    """
    print(f"\n{'=' * 60}")
    print(f"  Phase {phase_num}/{total_phases}: {name}")
    print(f"{'=' * 60}")

    clean_spill(temp_dir)
    t0 = time.time()

    con = duckdb.connect(database=":memory:")
    result = None
    try:
        # Pragmas --------------------------------------------------------
        con.execute(f"PRAGMA threads = {threads}")
        con.execute(f"PRAGMA memory_limit = '{memory_limit}'")
        con.execute(f"PRAGMA temp_directory = '{qpath(temp_dir)}'")
        con.execute("PRAGMA preserve_insertion_order = false")
        con.execute("PRAGMA enable_progress_bar = true")

        # Execute the phase SQL (may be multi-statement) ----------------
        result = con.execute(sql)
    finally:
        con.close()
        clean_spill(temp_dir)

    elapsed = time.time() - t0
    print(f"  Phase {phase_num} finished in {elapsed / 60:.1f} min")
    return result


def csv_view(view_name: str, gz_path: Path, key_col: str, max_line_size: int) -> str:
    """Return SQL to create a raw-dump view over a .txt.gz file."""
    p = qpath(gz_path)
    return f"""
CREATE OR REPLACE VIEW {view_name} AS
SELECT
  column0 AS rec_type,
  column1 AS {key_col},
  column2 AS revision,
  column3 AS last_modified,
  column4 AS json
FROM read_csv(
  '{p}',
  delim = '\\t', header = false, quote = '', escape = '',
  strict_mode = false,
  max_line_size = {max_line_size},
  columns = {{
    'column0': 'VARCHAR', 'column1': 'VARCHAR',
    'column2': 'VARCHAR', 'column3': 'VARCHAR',
    'column4': 'VARCHAR'
  }}
);
"""


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    ap = argparse.ArgumentParser(
        description="Export English-fiction authors from OpenLibrary dumps to Parquet (phased)."
    )
    ap.add_argument("--dumps-dir", default=r"K:",
                    help=r"Directory with OL dump .txt.gz files (e.g. K:\)")
    ap.add_argument("--out-dir", default=r"W:\Dany\BookDeduper",
                    help="Directory for final Parquet outputs")
    ap.add_argument("--dump-date", default="2026-01-31",
                    help="Dump date string in filenames")
    ap.add_argument("--threads", type=int, default=2,
                    help="DuckDB threads (2-4 recommended for spill-heavy jobs)")
    ap.add_argument("--max-line-size", type=int, default=50_000_000,
                    help="Max line size in bytes for DuckDB read_csv")
    ap.add_argument("--memory-limit", default="40GB",
                    help="DuckDB memory limit (e.g. 24GB, 40GB)")
    ap.add_argument("--temp-dir",
                    default=r"W:\Dany\BookDeduper\duckdb_tmp",
                    help="Directory for DuckDB spill files (fast SSD, lots of space)")
    args = ap.parse_args()

    dumps_dir = Path(args.dumps_dir)
    out_dir   = Path(args.out_dir)
    temp_dir  = Path(args.temp_dir)
    inter_dir = out_dir / "_intermediate"

    out_dir.mkdir(parents=True, exist_ok=True)
    temp_dir.mkdir(parents=True, exist_ok=True)
    inter_dir.mkdir(parents=True, exist_ok=True)

    dd  = args.dump_date
    mls = int(args.max_line_size)
    mem = args.memory_limit
    thr = int(args.threads)

    # Input dump paths ---------------------------------------------------
    authors_gz   = dumps_dir / f"ol_dump_authors_{dd}.txt.gz"
    works_gz     = dumps_dir / f"ol_dump_works_{dd}.txt.gz"
    editions_gz  = dumps_dir / f"ol_dump_editions_{dd}.txt.gz"
    redirects_gz = dumps_dir / f"ol_dump_redirects_{dd}.txt.gz"

    missing = [p for p in [authors_gz, works_gz, editions_gz, redirects_gz]
               if not p.exists()]
    if missing:
        print("ERROR – missing dump files:")
        for p in missing:
            print(f"  - {p}")
        return 2

    # Intermediate Parquet paths (between phases) ------------------------
    p1_redirects    = inter_dir / "author_redirects_resolved.parquet"
    p2_fiction_keys  = inter_dir / "fiction_work_keys.parquet"
    p3_eng_keys     = inter_dir / "fiction_eng_work_keys.parquet"
    p4_canon_authors = inter_dir / "canon_author_keys.parquet"

    # Final output paths -------------------------------------------------
    out_authors   = out_dir / f"fiction_eng_authors_{dd}.parquet"
    out_aliases   = out_dir / f"fiction_eng_author_aliases_{dd}.parquet"
    out_redirects = out_dir / f"author_redirects_resolved_{dd}.parquet"

    print("OpenLibrary fiction-eng Parquet export (phased)")
    print(f"  dumps_dir  : {dumps_dir}")
    print(f"  out_dir    : {out_dir}")
    print(f"  temp_dir   : {temp_dir}")
    print(f"  memory     : {mem}")
    print(f"  threads    : {thr}")
    t_total = time.time()

    # ===================================================================
    # Phase 1 – Author redirects  (redirects dump, ~66 MB gz)
    # ===================================================================
    if p1_redirects.exists():
        print(f"\n  Phase 1 skipped (exists): {p1_redirects}")
    else:
        view = csv_view("redirects_raw", redirects_gz, "from_key", mls)
        sql = view + f"""
-- Keep only author-to-author redirects
CREATE TEMP TABLE author_redirects AS
SELECT
  from_key,
  json_extract_string(json, '$.location') AS to_key,
  TRY_CAST(revision AS BIGINT) AS revision,
  last_modified
FROM redirects_raw
WHERE rec_type = '/type/redirect'
  AND starts_with(from_key, '/authors/')
  AND starts_with(json_extract_string(json, '$.location'), '/authors/');

-- Resolve chains (cap at 50 hops to avoid loops)
CREATE TEMP TABLE author_redirects_resolved AS
WITH RECURSIVE chain AS (
  SELECT from_key, to_key AS cur_key, 1 AS depth
  FROM author_redirects
  UNION ALL
  SELECT c.from_key, r.to_key AS cur_key, c.depth + 1
  FROM chain c
  JOIN author_redirects r ON r.from_key = c.cur_key
  WHERE c.depth < 50
),
final AS (
  SELECT from_key, arg_max(cur_key, depth) AS to_key_resolved
  FROM chain
  GROUP BY from_key
)
SELECT r.from_key, r.to_key, f.to_key_resolved, r.revision, r.last_modified
FROM author_redirects r
JOIN final f USING (from_key);

COPY author_redirects_resolved
TO '{qpath(p1_redirects)}' (FORMAT 'parquet');

SELECT count(*) AS redirects FROM author_redirects_resolved;
"""
        run_phase(1, "Author redirects", sql, temp_dir, mem, thr)

    # ===================================================================
    # Phase 2 – Fiction work keys  (works dump, ~3.7 GB gz)
    # ===================================================================
    if p2_fiction_keys.exists():
        print(f"\n  Phase 2 skipped (exists): {p2_fiction_keys}")
    else:
        view = csv_view("works_raw", works_gz, "work_key", mls)
        sql = view + f"""
-- Classify works by subjects:
--   KEEP if NOT poetry AND (strong_genre OR (has_fiction AND NOT academic))
--   ALSO EXCLUDE (has_literature AND has_academic) to drop criticism/study works
CREATE TEMP TABLE fiction_works AS
WITH params AS (
  SELECT
    [
      'fantasy','science fiction','sci-fi','sf','horror','thriller','suspense',
      'mystery','detective','crime','noir','romance','romantic','erotic',
      'adventure','western','dystopia','post-apocalyptic','supernatural','paranormal',
      'urban fantasy','gothic','steampunk','cyberpunk','space opera',
      'short stories','short story','novel','novels','fairy tale','fairy tales',
      'juvenile fiction','children''s fiction','young adult','ya','fan fiction'
    ] AS strong_terms,
    [
      'fiction','literary fiction','genre fiction','speculative fiction'
    ] AS fiction_terms,
    [
      'poetry','poem','poems','poet','poets','verse','sonnet','sonnets',
      'haiku','ballad','ballads','epic poetry','lyric poetry'
    ] AS poetry_terms,
    [
      'history and criticism','criticism and interpretation','criticism',
      'analysis','analytical','theory','philosophy','aesthetics','themes, motives',
      'in literature','in fiction',
      'bibliography','bibliographies','catalog','catalogs','catalogue','catalogues',
      'index','indexes','handbook','handbooks','guide','guides','study guide','study guides',
      'study and teaching','textbooks','reference','references',
      'dictionary','dictionaries','encyclopedia','encyclopedias','encyclopaedia','encyclopaedias',
      'annotated bibliography','concordance','concordances','classification',
      'collections -- catalogs','collections -- catalogues',
      'historiography',
      'biography'
    ] AS academic_terms,
    ['literature'] AS literature_terms
),
work_subjects AS (
  SELECT
    wr.work_key,
    lower(trim(json_extract_string(s.value, '$'))) AS subject
  FROM works_raw wr
  JOIN json_each(wr.json, '$.subjects') AS s ON TRUE
  WHERE wr.rec_type = '/type/work'
    AND json_extract_string(s.value, '$') IS NOT NULL
),
flags AS (
  SELECT
    ws.work_key,
    bool_or(EXISTS (SELECT 1 FROM params p, unnest(p.strong_terms)   t(term) WHERE ws.subject LIKE '%' || term || '%')) AS has_strong,
    bool_or(EXISTS (SELECT 1 FROM params p, unnest(p.fiction_terms)  t(term) WHERE ws.subject LIKE '%' || term || '%')) AS has_fiction,
    bool_or(EXISTS (SELECT 1 FROM params p, unnest(p.poetry_terms)  t(term) WHERE ws.subject LIKE '%' || term || '%')) AS has_poetry,
    bool_or(EXISTS (SELECT 1 FROM params p, unnest(p.academic_terms) t(term) WHERE ws.subject LIKE '%' || term || '%')) AS has_academic,
    bool_or(EXISTS (SELECT 1 FROM params p, unnest(p.literature_terms) t(term) WHERE ws.subject LIKE '%' || term || '%')) AS has_literature
  FROM work_subjects ws
  GROUP BY ws.work_key
)
SELECT work_key
FROM flags
WHERE has_poetry = FALSE
  AND (has_strong = TRUE OR (has_fiction = TRUE AND has_academic = FALSE))
  AND NOT (has_literature = TRUE AND has_academic = TRUE);

COPY fiction_works
TO '{qpath(p2_fiction_keys)}' (FORMAT 'parquet');

SELECT count(*) AS fiction_works FROM fiction_works;
"""
        run_phase(2, "Fiction work-key classification", sql, temp_dir, mem, thr)

    # ===================================================================
    # Phase 3 – English-edition fiction works  (editions dump, ~12 GB gz)
    #
    # This is the heaviest phase.  Two key optimisations vs. the original:
    #   1) Pre-filter editions with a cheap string LIKE before json_each
    #   2) Use EXISTS for the language check instead of JOIN json_each,
    #      eliminating the languages x works cross-product
    # ===================================================================
    if p3_eng_keys.exists():
        print(f"\n  Phase 3 skipped (exists): {p3_eng_keys}")
    else:
        view = csv_view("editions_raw", editions_gz, "edition_key", mls)
        sql = view + f"""
-- Load the fiction work keys from Phase 2 (small Parquet)
CREATE TEMP TABLE fiction_works AS
SELECT * FROM read_parquet('{qpath(p2_fiction_keys)}');

-- Scan editions for English-language editions of fiction works.
-- EXISTS avoids the cross-product that JOIN json_each(languages) would cause.
-- The LIKE pre-filter skips non-English editions before any JSON parsing.
CREATE TEMP TABLE fiction_eng_works AS
SELECT DISTINCT fw.work_key
FROM editions_raw e
JOIN json_each(e.json, '$.works') AS w ON TRUE
JOIN fiction_works fw
  ON fw.work_key = json_extract_string(w.value, '$.key')
WHERE e.rec_type = '/type/edition'
  AND starts_with(json_extract_string(w.value, '$.key'), '/works/')
  AND e.json LIKE '%/languages/eng%'
  AND EXISTS (
    SELECT 1 FROM json_each(e.json, '$.languages') AS lang
    WHERE json_extract_string(lang.value, '$.key') = '/languages/eng'
  );

COPY fiction_eng_works
TO '{qpath(p3_eng_keys)}' (FORMAT 'parquet');

SELECT count(*) AS fiction_eng_works FROM fiction_eng_works;
"""
        run_phase(3, "English-edition fiction works", sql, temp_dir, mem, thr)

    # ===================================================================
    # Phase 4 – Author key extraction & canonicalization  (works dump)
    # ===================================================================
    if p4_canon_authors.exists():
        print(f"\n  Phase 4 skipped (exists): {p4_canon_authors}")
    else:
        view = csv_view("works_raw", works_gz, "work_key", mls)
        sql = view + f"""
-- Load small intermediates from previous phases
CREATE TEMP TABLE fiction_eng_works AS
SELECT * FROM read_parquet('{qpath(p3_eng_keys)}');

CREATE TEMP TABLE author_redirects_resolved AS
SELECT * FROM read_parquet('{qpath(p1_redirects)}');

-- Pull author keys from qualifying fiction works
CREATE TEMP TABLE fiction_eng_author_keys AS
SELECT DISTINCT
  json_extract_string(a.value, '$.author.key') AS author_key
FROM works_raw wr
JOIN fiction_eng_works few USING (work_key)
JOIN json_each(wr.json, '$.authors') AS a ON TRUE
WHERE wr.rec_type = '/type/work'
  AND starts_with(json_extract_string(a.value, '$.author.key'), '/authors/');

-- Resolve to canonical keys via redirect map
CREATE TEMP TABLE fiction_eng_author_keys_canon AS
SELECT DISTINCT
  COALESCE(r.to_key_resolved, k.author_key) AS canonical_author_key
FROM fiction_eng_author_keys k
LEFT JOIN author_redirects_resolved r
  ON r.from_key = k.author_key;

COPY fiction_eng_author_keys_canon
TO '{qpath(p4_canon_authors)}' (FORMAT 'parquet');

SELECT count(*) AS canon_authors FROM fiction_eng_author_keys_canon;
"""
        run_phase(4, "Author key extraction", sql, temp_dir, mem, thr)

    # ===================================================================
    # Phase 5 – Author detail export  (authors dump, ~717 MB gz)
    # ===================================================================
    view = csv_view("authors_raw", authors_gz, "author_key", mls)
    sql = view + f"""
-- Load intermediates
CREATE TEMP TABLE fiction_eng_author_keys_canon AS
SELECT * FROM read_parquet('{qpath(p4_canon_authors)}');

CREATE TEMP TABLE author_redirects_resolved AS
SELECT * FROM read_parquet('{qpath(p1_redirects)}');

-- Join to author records (canonical keys only)
CREATE TEMP TABLE fiction_eng_authors_export AS
SELECT
  a.author_key AS canonical_author_key,
  a.last_modified AS author_last_modified,
  json_extract_string(a.json, '$.name')          AS name,
  json_extract_string(a.json, '$.personal_name') AS personal_name
FROM authors_raw a
JOIN fiction_eng_author_keys_canon k
  ON k.canonical_author_key = a.author_key
WHERE a.rec_type = '/type/author';

-- Explode alternate_names into one row per alias
CREATE TEMP TABLE fiction_eng_author_aliases_export AS
SELECT
  e.canonical_author_key,
  e.name AS author_display,
  json_extract_string(alt.value, '$') AS alias
FROM fiction_eng_authors_export e
JOIN authors_raw a
  ON a.author_key = e.canonical_author_key
JOIN json_each(a.json, '$.alternate_names') alt ON TRUE
WHERE a.rec_type = '/type/author'
  AND json_extract_string(alt.value, '$') IS NOT NULL
  AND length(trim(json_extract_string(alt.value, '$'))) > 0;

-- Write final Parquet outputs
COPY fiction_eng_authors_export
TO '{qpath(out_authors)}' (FORMAT 'parquet');

COPY fiction_eng_author_aliases_export
TO '{qpath(out_aliases)}' (FORMAT 'parquet');

COPY author_redirects_resolved
TO '{qpath(out_redirects)}' (FORMAT 'parquet');

SELECT
  (SELECT count(*) FROM fiction_eng_authors_export) AS authors,
  (SELECT count(*) FROM fiction_eng_author_aliases_export) AS aliases,
  (SELECT count(*) FROM author_redirects_resolved) AS redirects;
"""
    run_phase(5, "Author detail export", sql, temp_dir, mem, thr)

    # Final summary ------------------------------------------------------
    elapsed_total = time.time() - t_total
    print(f"\n{'=' * 60}")
    print(f"  ALL DONE  ({elapsed_total / 60:.1f} min total)")
    print(f"{'=' * 60}")
    print(f"  Authors  : {out_authors}")
    print(f"  Aliases  : {out_aliases}")
    print(f"  Redirects: {out_redirects}")
    print(f"\n  Intermediate files in {inter_dir}")
    print(f"  (safe to delete once you've verified the outputs)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
