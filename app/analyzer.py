from __future__ import annotations
import time
from typing import Dict, List, Tuple
from PySide6.QtCore import QObject, Signal
from .db import DB
from .util import now_ts
from .parser import detect_quality_tags
from .ranker import pick_best

class AnalyzeWorker(QObject):
    progress = Signal(str)
    stats = Signal(dict)
    finished = Signal(bool, str)

    def __init__(self, db: DB, commit_every: int = 2000):
        super().__init__()
        self.db = db
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
            ok, msg = self._run_analyze()
            self.finished.emit(ok, msg)
        except Exception as e:
            self.finished.emit(False, f"Analyze error: {e!r}")

    def _run_analyze(self) -> Tuple[bool, str]:
        if self.db.get_state("scan_completed", "0") != "1":
            return False, "Scan must complete before Analyze."

        self.db.set_state("last_action", "analyze")
        self.db.execute("DELETE FROM deletion_queue")

        last_key = self.db.get_state("analyze_last_work_key", "")
        stats = {"works": 0, "works_with_dupes": 0, "queued": 0}
        self.stats.emit(stats.copy())

        if last_key:
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
                    self.progress.emit(f"Analyzing work {stats['works']}: {work_key[:60]}")
                    self.stats.emit(stats.copy())

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
                    self.db.execute(
                        "INSERT INTO deletion_queue(work_key,file_id,checked,reason,created_at) VALUES(?,?,?,?,?)",
                        (work_key, f["id"], checked, reason, created),
                    )
                    stats["queued"] += 1
                    changed += 1

                self.db.set_state("analyze_last_work_key", work_key)

                if changed >= self.commit_every:
                    self.db.commit()
                    self.db.begin()
                    changed = 0

            self.db.commit()
        except Exception:
            self.db.rollback()
            raise

        if self._stop:
            return False, "Analyze aborted."
        self.db.set_state("analyze_completed", "1")
        return True, "Analyze completed."
