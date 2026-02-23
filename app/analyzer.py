from __future__ import annotations
import time
import logging
from typing import Dict, List, Tuple
from PySide6.QtCore import QObject, Signal
from .db import DB
from .util import now_ts
from .parser import detect_quality_tags, parse_filename, build_merge_suggestions
from .ranker import pick_best

logger = logging.getLogger(__name__)


class AnalyzeWorker(QObject):
    progress = Signal(str)
    progress_percent = Signal(float, str)
    stats = Signal(dict)
    finished = Signal(bool, str)

    def __init__(self, db: DB, phase: str = "duplicates", commit_every: int = 2000):
        super().__init__()
        self.db = db
        self.phase = phase
        self.commit_every = commit_every
        self._pause = False
        self._stop = False

    def request_pause(self):
        self._pause = True

    def request_resume(self):
        self._pause = False

    def request_stop(self):
        self._stop = True

    def _maybe_pause(self):
        while self._pause and not self._stop:
            time.sleep(0.15)

    def run(self):
        try:
            self._maybe_backup_before_phase(self.phase)
            if self.phase == "authors":
                ok, msg = self._run_analyze_authors()
            else:
                ok, msg = self._run_analyze_duplicates()
            self.finished.emit(ok, msg)
        except Exception as e:
            self.finished.emit(False, f"Analyze error: {e!r}")

    def _maybe_backup_before_phase(self, phase: str):
        enabled = (self.db.get_state("backup_before_analyze", "1") == "1")
        if not enabled:
            return
        path = self.db.create_timestamped_backup(label=f"analyze_{phase}")
        logger.debug("analyze_backup_created phase=%s path=%s", phase, path)

    def _load_invalid_set(self) -> set[str]:
        rows = self.db.query_all("SELECT normalized_name FROM invalid_authors")
        return {str(r["normalized_name"] or "").strip() for r in rows if str(r["normalized_name"] or "").strip()}

    def _authors_chunk_size(self) -> int:
        p = (self.db.get_state("memory_profile", "balanced") or "balanced").lower()
        if p == "safe":
            return 3000
        if p == "extreme":
            return 50000
        if p == "extreme+":
            return 1000000
        return 15000

    def _flush_author_deltas(self, known_delta: Dict[str, Dict[str, object]], variant_delta: Dict[Tuple[str, str], int], ts: int):
        if not known_delta and not variant_delta:
            return
        known_rows = []
        for norm, info in known_delta.items():
            known_rows.append((norm, str(info["canonical"]), int(info["frequency"]), ts, ts))
        if known_rows:
            self.db.executemany(
                """
                INSERT INTO known_authors(normalized_name,canonical_name,frequency,created_at,updated_at)
                VALUES(?,?,?,?,?)
                ON CONFLICT(normalized_name) DO UPDATE SET
                  canonical_name=CASE WHEN length(excluded.canonical_name) > length(known_authors.canonical_name)
                    THEN excluded.canonical_name ELSE known_authors.canonical_name END,
                  frequency=known_authors.frequency + excluded.frequency,
                  updated_at=excluded.updated_at
                """,
                known_rows,
            )

        variant_rows = []
        for (norm, variant), freq in variant_delta.items():
            variant_rows.append((norm, variant, int(freq), ts))
        if variant_rows:
            self.db.executemany(
                """
                INSERT INTO author_variants(normalized_name,variant_text,frequency,updated_at)
                VALUES(?,?,?,?)
                ON CONFLICT(normalized_name,variant_text) DO UPDATE SET
                  frequency=author_variants.frequency + excluded.frequency,
                  updated_at=excluded.updated_at
                """,
                variant_rows,
            )

    def _run_analyze_authors(self) -> Tuple[bool, str]:
        if self.db.get_state("scan_completed", "0") != "1":
            return False, "Scan must complete before Analyze Authors."

        self.db.set_state("last_action", "analyze_authors")
        self.db.set_state("analyze_authors_completed", "0")
        self.progress_percent.emit(0.0, "authors_suggestions")
        invalid_set = self._load_invalid_set()
        dirty = (self.db.get_state("author_db_dirty", "0") == "1")
        last_id = int(self.db.get_state("analyze_authors_last_file_id", "0") or "0")

        if dirty:
            self.db.begin()
            try:
                self.db.execute("DELETE FROM known_authors")
                self.db.execute("DELETE FROM author_variants")
                self.db.set_state("analyze_authors_last_file_id", "0")
                self.db.set_state("analyze_authors_completed", "0")
                self.db.commit()
            except Exception:
                self.db.rollback()
                raise
            last_id = 0

        files = self.db.query_all("SELECT id,name FROM files WHERE id > ? ORDER BY id", (last_id,))
        if not files:
            self.db.set_state("analyze_authors_completed", "1")
            self.db.set_state("author_db_dirty", "0")
            return True, "Analyze Authors completed (no pending files)."

        chunk_size = self._authors_chunk_size()
        known_delta: Dict[str, Dict[str, object]] = {}
        variant_delta: Dict[Tuple[str, str], int] = {}
        processed = 0
        ts = now_ts()

        self.db.begin()
        try:
            for r in files:
                if self._stop:
                    break
                self._maybe_pause()
                fid = int(r["id"])
                parsed = parse_filename(str(r["name"] or ""), invalid_authors=invalid_set)
                if parsed.author_confidence >= 0.70 and parsed.author_norm and parsed.author_norm not in invalid_set and parsed.author_norm != "unknown":
                    rec = known_delta.get(parsed.author_norm)
                    if not rec:
                        known_delta[parsed.author_norm] = {"canonical": parsed.author, "frequency": 1}
                    else:
                        rec["frequency"] = int(rec["frequency"]) + 1
                        if len(parsed.author) > len(str(rec["canonical"])):
                            rec["canonical"] = parsed.author
                    k = (parsed.author_norm, parsed.author)
                    variant_delta[k] = int(variant_delta.get(k, 0)) + 1

                processed += 1
                if processed % 500 == 0:
                    self.progress.emit(f"Analyze Authors: +{processed} (last_file_id={fid})")

                if processed % chunk_size == 0:
                    ts = now_ts()
                    self._flush_author_deltas(known_delta, variant_delta, ts)
                    known_delta.clear()
                    variant_delta.clear()
                    self.db.set_state("analyze_authors_last_file_id", str(fid))
                    self.db.commit()
                    self.db.begin()

            self.progress.emit("Analyze Authors: flushing final author deltas…")
            ts = now_ts()
            self._flush_author_deltas(known_delta, variant_delta, ts)
            if files:
                final_id = int(files[min(processed, len(files)) - 1]["id"]) if processed > 0 else last_id
                self.db.set_state("analyze_authors_last_file_id", str(final_id))
            self.db.commit()
        except Exception:
            self.db.rollback()
            raise

        # recompute suggestions from persisted known_authors only when completed.
        # This step is O(n²); emit percentage progress so long runs remain observable.
        if not self._stop:
            rows = self.db.query_all("SELECT normalized_name, canonical_name, frequency FROM known_authors")
            author_count = len(rows)
            self.progress.emit(f"Analyze Authors: finalizing merge suggestions for {author_count} authors…")

            def _on_pair_progress(done: int, total: int):
                if total <= 0:
                    pct = 100.0
                else:
                    pct = (done * 100.0) / total
                # Qt queued signal delivery keeps UI updates asynchronous.
                self.progress_percent.emit(pct, "authors_suggestions")
                if done == 0 or done == total:
                    self.progress.emit(f"Analyze Authors: merge suggestions {pct:.2f}% ({done}/{total} pairs)")
                self._maybe_pause()
                if self._stop:
                    raise InterruptedError("Analyze Authors stopped during merge suggestion pass")

            try:
                suggestions = build_merge_suggestions([
                    (str(r["normalized_name"]), str(r["canonical_name"]), int(r["frequency"] or 0))
                    for r in rows
                ], progress_cb=_on_pair_progress, progress_interval_s=10.0)
                suggestions_count = len(suggestions)
            except InterruptedError:
                return False, "Analyze Authors aborted (progress saved)."

            logger.debug("author_db_rebuilt authors=%s suggestions=%s", author_count, suggestions_count)
            self.progress_percent.emit(100.0, "authors_suggestions")
            self.db.set_state("analyze_authors_completed", "1")
            self.db.set_state("author_db_dirty", "0")
            return True, "Analyze Authors completed."

        return False, "Analyze Authors aborted (progress saved)."

    def _load_duplicate_rows_in_memory(self, last_key: str) -> Dict[str, List[dict]]:
        if last_key:
            rows = self.db.query_all(
                """
                SELECT work_key,id,path,name,ext,is_archive,inner_ext_guess,size,mtime_ns,tags
                FROM files
                WHERE work_key > ?
                ORDER BY work_key
                """,
                (last_key,),
            )
        else:
            rows = self.db.query_all(
                """
                SELECT work_key,id,path,name,ext,is_archive,inner_ext_guess,size,mtime_ns,tags
                FROM files
                ORDER BY work_key
                """
            )
        grouped: Dict[str, List[dict]] = {}
        for r in rows:
            wk = str(r["work_key"] or "")
            if not wk:
                continue
            grouped.setdefault(wk, []).append(dict(r))
        return grouped

    def _duplicates_flush_chunk(self) -> int:
        p = (self.db.get_state("memory_profile", "balanced") or "balanced").lower()
        if p == "safe":
            return 5000
        if p == "extreme":
            return 50000
        if p == "extreme+":
            return 1000000
        return 15000

    def _flush_duplicate_queue_chunk(self, queue_rows: List[Tuple[str, int, int, str, int]]):
        if not queue_rows:
            return
        self.db.executemany(
            "INSERT INTO deletion_queue(work_key,file_id,checked,reason,created_at) VALUES(?,?,?,?,?)",
            queue_rows,
        )

    def _run_analyze_duplicates(self) -> Tuple[bool, str]:
        if self.db.get_state("scan_completed", "0") != "1":
            return False, "Scan must complete before Analyze Duplicates."

        self.db.set_state("last_action", "analyze_duplicates")
        was_completed = (self.db.get_state("analyze_duplicates_completed", self.db.get_state("analyze_completed", "0")) == "1")

        last_key = self.db.get_state("analyze_last_work_key", "")
        is_resume = bool(last_key and not was_completed)

        self.db.set_state("analyze_duplicates_completed", "0")
        self.db.set_state("analyze_completed", "0")

        if is_resume:
            self.progress.emit(f"Analyze Duplicates: resuming after work_key={last_key[:60]}")
        else:
            self.db.set_state("analyze_last_work_key", "")
            last_key = ""
            self.db.execute("DELETE FROM deletion_queue")

        stats = {"works": 0, "works_with_dupes": 0, "queued": 0}
        self.stats.emit(stats.copy())

        memory_profile = (self.db.get_state("memory_profile", "balanced") or "balanced").lower()
        in_memory_queue = [] if memory_profile in ("extreme", "extreme+") else None
        grouped_rows = self._load_duplicate_rows_in_memory(last_key) if memory_profile in ("extreme", "extreme+") else None
        flush_chunk = self._duplicates_flush_chunk()

        if grouped_rows is not None:
            works = [{"work_key": wk} for wk in grouped_rows.keys()]
        elif last_key:
            works = self.db.query_all("SELECT work_key FROM works WHERE work_key > ? ORDER BY work_key", (last_key,))
        else:
            works = self.db.query_all("SELECT work_key FROM works ORDER BY work_key")

        self.db.begin()
        try:
            changed = 0
            for w in works:
                if self._stop:
                    break
                self._maybe_pause()

                work_key = w["work_key"]
                stats["works"] += 1

                if stats["works"] % 200 == 0:
                    self.progress.emit(f"Analyzing duplicates {stats['works']}: {work_key[:60]}")
                    self.stats.emit(stats.copy())

                if grouped_rows is not None:
                    rows = grouped_rows.get(work_key, [])
                else:
                    rows = self.db.query_all(
                        "SELECT id,path,name,ext,is_archive,inner_ext_guess,size,mtime_ns,tags FROM files WHERE work_key=?",
                        (work_key,),
                    )
                if len(rows) <= 1:
                    self.db.set_state("analyze_last_work_key", work_key)
                    continue

                stats["works_with_dupes"] += 1

                files: List[dict] = []
                for r in rows:
                    ext_eff = (r["inner_ext_guess"] if int(r["is_archive"] or 0) == 1 and r["inner_ext_guess"] else r["ext"]) or ""
                    tags_lower = detect_quality_tags((r["tags"] or "").split("|")) if (r["tags"] or "").strip() else []
                    files.append({
                        "id": int(r["id"]),
                        "path": r["path"],
                        "name": r["name"],
                        "ext_effective": ext_eff.lower(),
                        "tags_lower": tags_lower,
                        "mtime_ns": int(r["mtime_ns"]),
                        "size": int(r["size"]),
                    })

                best, keep_extra_ids = pick_best(files)

                created = now_ts()
                for f in files:
                    if f["id"] == best["id"] or f["id"] in keep_extra_ids:
                        checked = 0
                        reason = "KEEP (best)" if f["id"] == best["id"] else "KEEP (pdf rule)"
                    else:
                        checked = 1
                        reason = "Lower rank"
                    row = (work_key, f["id"], checked, reason, created)
                    if in_memory_queue is not None:
                        in_memory_queue.append(row)
                    else:
                        self.db.execute(
                            "INSERT INTO deletion_queue(work_key,file_id,checked,reason,created_at) VALUES(?,?,?,?,?)",
                            row,
                        )
                    stats["queued"] += 1
                    changed += 1

                self.db.set_state("analyze_last_work_key", work_key)

                # chunk-flush to reduce loss window while keeping low I/O
                if in_memory_queue is not None and len(in_memory_queue) >= flush_chunk:
                    self._flush_duplicate_queue_chunk(in_memory_queue)
                    in_memory_queue.clear()
                    self.db.commit()
                    self.db.begin()

                if in_memory_queue is None and changed >= self.commit_every:
                    self.db.commit()
                    self.db.begin()
                    changed = 0

            if in_memory_queue is not None and in_memory_queue:
                self._flush_duplicate_queue_chunk(in_memory_queue)
            self.db.commit()
        except Exception:
            self.db.rollback()
            raise

        if self._stop:
            return False, "Analyze Duplicates aborted (progress saved for resume)."
        self.db.set_state("analyze_completed", "1")
        self.db.set_state("analyze_duplicates_completed", "1")
        return True, "Analyze Duplicates completed."
