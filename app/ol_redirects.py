from __future__ import annotations
import os
import json
import logging
from typing import Dict

from .author_db import AuthorDB

logger = logging.getLogger(__name__)


def discover_latest_redirect_dump_file(folder: str) -> tuple[str | None, str | None]:
    best_name = None
    best_date = None
    try:
        names = os.listdir(folder)
    except Exception:
        return None, None
    for name in names:
        if not (name.startswith("ol_dump_redirects_") and name.endswith(".txt")):
            continue
        date = name[len("ol_dump_redirects_"):-len(".txt")]
        if len(date) != 10:
            continue
        if best_date is None or date > best_date:
            best_date = date
            best_name = name
    if not best_name:
        return None, None
    return os.path.join(folder, best_name), best_date


def parse_redirect_dump_line(line: str) -> tuple[str, str, int, str] | None:
    parts = line.rstrip("\n").split("\t", 4)
    if len(parts) < 5:
        return None
    typ, key, rev_s, last_modified, payload_txt = parts
    if typ != "/type/redirect":
        return None
    if not key.startswith("/authors/"):
        return None
    try:
        payload = json.loads(payload_txt)
    except Exception:
        return None
    if not isinstance(payload, dict):
        return None
    location = str(payload.get("location") or "").strip()
    if not location.startswith("/authors/"):
        return None
    try:
        rev = int(rev_s or "0")
    except Exception:
        rev = 0
    return key, location, rev, str(last_modified or "")


def resolve_redirect_target(redirect_map: Dict[str, str], key: str, max_depth: int = 50) -> tuple[str, bool]:
    cur = key
    visited = set()
    for _ in range(max_depth):
        nxt = redirect_map.get(cur)
        if not nxt:
            return cur, False
        if nxt in visited:
            return cur, True
        visited.add(cur)
        cur = nxt
    return cur, True


def _start_or_resume_run(db: AuthorDB, dump_date: str, dump_file: str) -> tuple[int, int, bool]:
    db.execute(
        "UPDATE ol_import_runs SET status='obsolete', updated_at=strftime('%s','now') WHERE import_type='ol_redirects' AND dump_date<>? AND status IN ('running','failed','completed')",
        (dump_date,),
    )
    row = db.query_one(
        "SELECT id,status,progress_line FROM ol_import_runs WHERE import_type='ol_redirects' AND dump_date=? ORDER BY id DESC LIMIT 1",
        (dump_date,),
    )
    if row and str(row["status"]) == "completed":
        return int(row["id"]), int(row["progress_line"] or 0), True
    if row and str(row["status"]) in ("running", "failed"):
        rid = int(row["id"])
        pl = int(row["progress_line"] or 0)
        db.execute("UPDATE ol_import_runs SET status='running', updated_at=strftime('%s','now'), last_error=NULL WHERE id=?", (rid,))
        return rid, pl, False

    db.execute(
        "INSERT INTO ol_import_runs(import_type,dump_date,dump_filename,status,started_at,updated_at,progress_line,rows_processed,redirects_stored,aliases_added,errors_count) VALUES('ol_redirects',?,?, 'running',strftime('%s','now'),strftime('%s','now'),0,0,0,0,0)",
        (dump_date, os.path.basename(dump_file)),
    )
    rid = int(db.query_one("SELECT last_insert_rowid() AS id")["id"])
    return rid, 0, False


def import_latest_redirect_dump(
    db: AuthorDB,
    folder: str,
    checkpoint_every: int = 5000,
    progress_cb=None,   # callable(msg: str, pct: float) — pct in [0, 100]
    stop_fn=None,       # callable() -> bool
) -> dict:
    dump_file, dump_date = discover_latest_redirect_dump_file(folder)
    if not dump_file or not dump_date:
        return {"ok": False, "message": "No redirect dump found."}

    db.begin()
    try:
        run_id, start_line, already_done = _start_or_resume_run(db, dump_date, dump_file)
        db.commit()
    except Exception:
        db.rollback()
        raise

    if already_done:
        return {"ok": True, "message": f"Redirect dump {dump_date} already completed.", "run_id": run_id}

    file_size = max(1, os.path.getsize(dump_file))
    processed = start_line
    stored = 0
    errors = 0
    try:
        db.begin()
        line_no = 0
        with open(dump_file, "r", encoding="utf-8", errors="replace") as fh:
            while True:
                line = fh.readline()
                if not line:
                    break
                line_no += 1
                if line_no <= start_line:
                    continue
                rec = parse_redirect_dump_line(line)
                if not rec:
                    errors += 1
                    continue
                from_key, to_key, rev, last_modified = rec
                db.execute(
                    "INSERT INTO ol_author_redirects(from_key,to_key,to_key_resolved,dump_date,last_modified,revision,updated_at) VALUES(?,?,?,?,?,?,strftime('%s','now')) ON CONFLICT(from_key) DO UPDATE SET to_key=excluded.to_key,dump_date=excluded.dump_date,last_modified=excluded.last_modified,revision=excluded.revision,updated_at=excluded.updated_at",
                    (from_key, to_key, None, dump_date, last_modified, rev),
                )
                stored += 1
                processed = line_no
                if line_no % checkpoint_every == 0:
                    db.execute(
                        "UPDATE ol_import_runs SET progress_line=?,rows_processed=?,redirects_stored=?,errors_count=?,updated_at=strftime('%s','now') WHERE id=?",
                        (processed, processed, stored, errors, run_id),
                    )
                    db.commit()
                    db.begin()
                    if stop_fn and stop_fn():
                        db.execute(
                            "UPDATE ol_import_runs SET status='failed',last_error='Cancelled',updated_at=strftime('%s','now') WHERE id=?",
                            (run_id,),
                        )
                        db.commit()
                        return {"ok": False, "message": f"Cancelled at line {line_no}.", "run_id": run_id}
                    if progress_cb:
                        pct = min(58.0, 60.0 * fh.tell() / file_size)
                        progress_cb(f"Loading: line {line_no}, stored {stored}", pct)
        db.execute(
            "UPDATE ol_import_runs SET progress_line=?,rows_processed=?,redirects_stored=?,errors_count=?,status='completed',completed_at=strftime('%s','now'),updated_at=strftime('%s','now') WHERE id=?",
            (processed, processed, stored, errors, run_id),
        )
        db.commit()
    except Exception as e:
        db.rollback()
        db.execute(
            "UPDATE ol_import_runs SET status='failed',last_error=?,updated_at=strftime('%s','now') WHERE id=?",
            (repr(e), run_id),
        )
        return {"ok": False, "message": f"Redirect import failed: {e!r}", "run_id": run_id}

    if progress_cb:
        progress_cb("Migrating redirect aliases…", 60.0)

    def _mig_progress(msg: str, pct: float):
        if progress_cb:
            progress_cb(msg, 60.0 + pct * 0.40)

    mig = migrate_redirect_aliases(db, dump_date, progress_cb=_mig_progress, stop_fn=stop_fn)

    if mig.get("cancelled"):
        # Reset status so the next run skips loading (progress_line = EOF) and re-runs migration.
        db.execute(
            "UPDATE ol_import_runs SET status='failed',last_error='Migration cancelled',updated_at=strftime('%s','now') WHERE id=?",
            (run_id,),
        )
        db.commit()
        return {"ok": False, "message": "Migration cancelled.", "run_id": run_id}

    db.execute("UPDATE ol_import_runs SET aliases_added=?,updated_at=strftime('%s','now') WHERE id=?", (int(mig.get("aliases_added", 0)), run_id))
    return {"ok": True, "message": f"Redirect import completed for {dump_date}.", "run_id": run_id, **mig}


def migrate_redirect_aliases(
    db: AuthorDB,
    dump_date: str,
    progress_cb=None,           # callable(msg: str, pct: float) — pct in [0, 100]
    stop_fn=None,               # callable() -> bool
    checkpoint_every: int = 2000,
) -> dict:
    rows = db.query_all("SELECT from_key,to_key FROM ol_author_redirects WHERE dump_date=?", (dump_date,))
    redirect_map = {str(r["from_key"]): str(r["to_key"]) for r in rows}
    total = max(1, len(redirect_map))
    aliases_added = 0
    loops = 0
    unresolved = 0
    processed = 0

    db.begin()
    try:
        for from_key, to_key in list(redirect_map.items()):
            processed += 1

            canon_key, loop = resolve_redirect_target(redirect_map, from_key)
            if loop:
                loops += 1
                logger.warning("redirect_loop_detected from=%s", from_key)
            if canon_key == from_key:
                # Self-redirect — nothing to merge, but still counts toward progress.
                if processed % checkpoint_every == 0:
                    db.commit()
                    db.begin()
                    if stop_fn and stop_fn():
                        return {"aliases_added": aliases_added, "loops": loops, "unresolved": unresolved, "cancelled": True}
                    if progress_cb:
                        progress_cb(f"Migrating {processed}/{total}: {aliases_added} aliases added", min(99.0, 100.0 * processed / total))
                continue

            from_rec = db.query_one("SELECT author_norm, canonical_name FROM author_dump_records WHERE ol_key=?", (from_key,))
            canon_rec = db.query_one("SELECT author_norm FROM author_dump_records WHERE ol_key=?", (canon_key,))
            if not canon_rec:
                unresolved += 1
            else:
                canon_norm = str(canon_rec["author_norm"])
                alias_norms = set()
                if from_rec:
                    if str(from_rec["author_norm"] or ""):
                        alias_norms.add(str(from_rec["author_norm"]))
                    for ar in db.query_all("SELECT alias_norm FROM author_aliases WHERE author_norm=?", (str(from_rec["author_norm"] or ""),)):
                        alias_norms.add(str(ar["alias_norm"] or ""))

                if not alias_norms:
                    unresolved += 1

                for alias_norm in [a for a in alias_norms if a]:
                    db.execute(
                        "INSERT INTO author_aliases(alias_norm,author_norm,author_display,confidence,source,source_key,updated_at) VALUES(?,?,?,?,?,?,strftime('%s','now')) ON CONFLICT(alias_norm) DO UPDATE SET author_norm=excluded.author_norm,author_display=excluded.author_display,confidence=excluded.confidence,source='openlibrary_redirect',source_key=excluded.source_key,updated_at=excluded.updated_at",
                        (alias_norm, canon_norm, alias_norm, 1.0, "openlibrary_redirect", from_key),
                    )
                    aliases_added += 1

                # Remove the superseded author from known_authors; the canonical record remains.
                if from_rec and str(from_rec["author_norm"] or "") != canon_norm:
                    db.execute("DELETE FROM known_authors WHERE normalized_name=?", (str(from_rec["author_norm"]),))

                db.execute(
                    "UPDATE ol_author_redirects SET to_key_resolved=?,updated_at=strftime('%s','now') WHERE from_key=?",
                    (canon_key, from_key),
                )

            if processed % checkpoint_every == 0:
                db.commit()
                db.begin()
                if stop_fn and stop_fn():
                    return {"aliases_added": aliases_added, "loops": loops, "unresolved": unresolved, "cancelled": True}
                if progress_cb:
                    progress_cb(f"Migrating {processed}/{total}: {aliases_added} aliases added", min(99.0, 100.0 * processed / total))

        db.commit()
    except Exception:
        db.rollback()
        raise

    if progress_cb:
        progress_cb(f"Migration done: {total} redirects, {aliases_added} aliases", 100.0)
    return {"aliases_added": aliases_added, "loops": loops, "unresolved": unresolved}


def purge_redirect_table(db: AuthorDB, dump_date: str):
    """Delete all redirect rows for dump_date after migration is complete.

    Call this after migrate_redirect_aliases() to reclaim space. The redirect
    chains are fully encoded in author_aliases after migration, so this data
    is no longer needed.
    """
    db.execute("DELETE FROM ol_author_redirects WHERE dump_date=?", (dump_date,))
    logger.info("purge_redirect_table dump_date=%s", dump_date)
