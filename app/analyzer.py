from __future__ import annotations
import time
import logging
import os
from typing import Dict, List, Tuple
from PySide6.QtCore import QObject, Signal
from .db import DB
from .util import now_ts
from .parser import detect_quality_tags, parse_filename, build_merge_suggestions
from .ranker import pick_best

logger = logging.getLogger(__name__)


class AnalyzeWorker(QObject):
    progress = Signal(str)
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

    def _rebuild_known_authors(self):
        invalid_set = self._load_invalid_set()
        files = self.db.query_all("SELECT id,name FROM files")
        temp_counts: Dict[str, Dict[str, object]] = {}

        for i, r in enumerate(files, start=1):
            if self._stop:
                break
            self._maybe_pause()
            if i % 500 == 0:
                self.progress.emit(f"Analyze Authors: {i}/{len(files)}")
            parsed = parse_filename(str(r["name"] or ""), invalid_authors=invalid_set)
            if parsed.author_confidence < 0.70:
                continue
            if not parsed.author_norm or parsed.author_norm in invalid_set or parsed.author_norm == "unknown":
                continue
            row = temp_counts.get(parsed.author_norm)
            if not row:
                temp_counts[parsed.author_norm] = {"canonical": parsed.author, "frequency": 1}
            else:
                row["frequency"] = int(row["frequency"]) + 1
                if len(parsed.author) > len(str(row["canonical"])):
                    row["canonical"] = parsed.author

        ts = now_ts()
        self.db.execute("DELETE FROM known_authors")
        self.db.execute("DELETE FROM author_variants")
        known_rows = []
        variant_rows = []
        for norm, info in temp_counts.items():
            canonical = str(info["canonical"])
            freq = int(info["frequency"])
            known_rows.append((norm, canonical, freq, ts, ts))
            variant_rows.append((norm, canonical, freq, ts))
        if known_rows:
            self.db.executemany(
                "INSERT INTO known_authors(normalized_name,canonical_name,frequency,created_at,updated_at) VALUES(?,?,?,?,?)",
                known_rows,
            )
        if variant_rows:
            self.db.executemany(
                "INSERT INTO author_variants(normalized_name,variant_text,frequency,updated_at) VALUES(?,?,?,?)",
                variant_rows,
            )

        known_for_suggest = [(k, str(v["canonical"]), int(v["frequency"])) for k, v in temp_counts.items()]
        suggestions = build_merge_suggestions(known_for_suggest)
        logger.debug("author_db_rebuilt authors=%s suggestions=%s", len(temp_counts), len(suggestions))

    def _run_analyze_authors(self) -> Tuple[bool, str]:
        if self.db.get_state("scan_completed", "0") != "1":
            return False, "Scan must complete before Analyze Authors."
        self.db.begin()
        try:
            self._rebuild_known_authors()
            self.db.set_state("analyze_authors_completed", "1")
            self.db.set_state("author_db_dirty", "0")
            self.db.commit()
        except Exception:
            self.db.rollback()
            raise
        if self._stop:
            return False, "Analyze Authors aborted."
        return True, "Analyze Authors completed."


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

    def _run_analyze_duplicates(self) -> Tuple[bool, str]:
        if self.db.get_state("scan_completed", "0") != "1":
            return False, "Scan must complete before Analyze Duplicates."

        self.db.set_state("last_action", "analyze_duplicates")
        self.db.execute("DELETE FROM deletion_queue")

        last_key = self.db.get_state("analyze_last_work_key", "")
        stats = {"works": 0, "works_with_dupes": 0, "queued": 0}
        self.stats.emit(stats.copy())

        memory_profile = (self.db.get_state("memory_profile", "balanced") or "balanced").lower()
        in_memory_queue = [] if memory_profile == "extreme" else None
        grouped_rows = self._load_duplicate_rows_in_memory(last_key) if memory_profile == "extreme" else None

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

                if in_memory_queue is None and changed >= self.commit_every:
                    self.db.commit()
                    self.db.begin()
                    changed = 0

            if in_memory_queue is not None and in_memory_queue:
                self.db.executemany(
                    "INSERT INTO deletion_queue(work_key,file_id,checked,reason,created_at) VALUES(?,?,?,?,?)",
                    in_memory_queue,
                )
            self.db.commit()
        except Exception:
            self.db.rollback()
            raise

        if self._stop:
            return False, "Analyze Duplicates aborted."
        self.db.set_state("analyze_completed", "1")
        self.db.set_state("analyze_duplicates_completed", "1")
        return True, "Analyze Duplicates completed."
