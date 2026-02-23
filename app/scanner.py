from __future__ import annotations
import os
import time
import logging
from dataclasses import dataclass
from typing import List, Optional, Dict, Tuple
from PySide6.QtCore import QObject, Signal
from .db import DB
from .util import norm_path, file_sig_from_stat, ext_of, is_probably_archive, dumps, loads, now_ts, normalize_text
from .parser import parse_filename, detect_quality_tags, make_work_key
from .sevenzip import detect_7z, list_archive_exts

logger = logging.getLogger(__name__)

@dataclass
class ScanConfig:
    folder_skip_enabled: bool
    sevenzip_path: Optional[str]
    commit_every: int = 5000
    checkpoint_every_s: float = 1.5
    archive_timeout_s: int = 60

class ScanWorker(QObject):
    progress = Signal(str)
    stats = Signal(dict)
    finished = Signal(bool, str)

    def __init__(self, db: DB, cfg: ScanConfig):
        super().__init__()
        self.db = db
        self.cfg = cfg
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
            ok, msg = self._run_scan()
            self.finished.emit(ok, msg)
        except Exception as e:
            self.finished.emit(False, f"Scan error: {e!r}")

    def _start_scan_run(self) -> int:
        ts = now_ts()
        self.db.execute(
            "INSERT INTO scan_runs(started_at,status,stats_json) VALUES(?,?,?)",
            (ts, "running", "{}"),
        )
        row = self.db.query_one("SELECT last_insert_rowid() AS id")
        return int(row["id"])

    def _set_scan_status(self, scan_id: int, status: str):
        finished = now_ts() if status in ("completed", "aborted", "paused") else None
        self.db.execute(
            "UPDATE scan_runs SET status=?, finished_at=COALESCE(?, finished_at) WHERE id=?",
            (status, finished, scan_id),
        )

    def _load_stack(self, roots: List[str]) -> List[str]:
        raw = self.db.get_state("scan_stack_json")
        if raw:
            stack = loads(raw, [])
            if stack:
                return [str(x) for x in stack]
        return [norm_path(r) for r in roots]

    def _save_checkpoint(self, scan_id: int, stack: List[str], current_dir: str, last_path: str, stats: Dict):
        self.db.set_state("last_action", "scan")
        self.db.set_state("scan_run_id", str(scan_id))
        self.db.set_state("scan_stack_json", dumps(stack))
        self.db.set_state("scan_current_folder", current_dir or "")
        self.db.set_state("scan_last_path", last_path or "")
        self.db.set_state("folder_skip_enabled", "1" if self.cfg.folder_skip_enabled else "0")
        if self.cfg.sevenzip_path:
            self.db.set_state("7z_path", self.cfg.sevenzip_path)
        self.db.execute("UPDATE scan_runs SET stats_json=? WHERE id=?", (dumps(stats), scan_id))

    def _ensure_7z(self) -> Optional[str]:
        p = detect_7z(self.cfg.sevenzip_path)
        if p:
            self.cfg.sevenzip_path = p
            self.db.set_state("7z_path", p)
        return p

    def _folder_fingerprint(self, dir_path: str) -> Tuple[int, int, int, int]:
        st = os.stat(dir_path)
        dir_mtime_ns = int(getattr(st, "st_mtime_ns", int(st.st_mtime * 1e9)))
        entry_count = 0
        sum_child_sizes = 0
        max_child_mtime = 0
        try:
            with os.scandir(dir_path) as it:
                for e in it:
                    entry_count += 1
                    try:
                        if e.is_file(follow_symlinks=False):
                            stf = e.stat(follow_symlinks=False)
                            sum_child_sizes += int(stf.st_size)
                            m = int(getattr(stf, "st_mtime_ns", int(stf.st_mtime * 1e9)))
                            if m > max_child_mtime:
                                max_child_mtime = m
                    except Exception:
                        continue
        except Exception:
            pass
        return (dir_mtime_ns, entry_count, sum_child_sizes, max_child_mtime)

    def _folder_skip_allowed(self, root_id: int, dir_path: str, scan_id: int) -> bool:
        if not self.cfg.folder_skip_enabled:
            return False
        fp = self._folder_fingerprint(dir_path)
        row = self.db.query_one("SELECT * FROM folders WHERE path=?", (dir_path,))
        if row:
            if int(row["force_rescan"] or 0) == 1:
                return False
            prev = (
                int(row["fingerprint_dir_mtime"] or -1),
                int(row["fingerprint_entry_count"] or -1),
                int(row["fingerprint_sum_child_sizes"] or -1),
                int(row["fingerprint_max_child_mtime"] or -1),
            )
            if prev == fp:
                self.db.execute("UPDATE folders SET last_seen_scan_id=? WHERE path=?", (scan_id, dir_path))
                return True

        self.db.execute(
            "INSERT INTO folders(root_id,path,fingerprint_dir_mtime,fingerprint_entry_count,fingerprint_sum_child_sizes,"
            "fingerprint_max_child_mtime,last_seen_scan_id,force_rescan) VALUES(?,?,?,?,?,?,?,0) "
            "ON CONFLICT(path) DO UPDATE SET "
            "root_id=excluded.root_id,"
            "fingerprint_dir_mtime=excluded.fingerprint_dir_mtime,"
            "fingerprint_entry_count=excluded.fingerprint_entry_count,"
            "fingerprint_sum_child_sizes=excluded.fingerprint_sum_child_sizes,"
            "fingerprint_max_child_mtime=excluded.fingerprint_max_child_mtime,"
            "last_seen_scan_id=excluded.last_seen_scan_id,"
            "force_rescan=0",
            (root_id, dir_path, fp[0], fp[1], fp[2], fp[3], scan_id),
        )
        return False

    def _root_id_for_dir(self, current_dir: str, root_pairs: List[Tuple[str, int]]) -> Optional[int]:
        for rp, rid in root_pairs:
            if current_dir == rp or current_dir.startswith(rp + os.sep):
                return rid
        return None

    def _folder_file_index(self, folder_path: str) -> Dict[str, Tuple[int, int]]:
        rows = self.db.query_all("SELECT path,size,mtime_ns FROM files WHERE folder_path=?", (folder_path,))
        return {str(r["path"]): (int(r["size"]), int(r["mtime_ns"])) for r in rows}


    def _load_author_aliases(self) -> Dict[str, Tuple[str, str]]:
        try:
            rows = self.db.query_all("SELECT alias_norm, author_norm, author_display FROM author_aliases")
        except Exception:
            return {}
        out: Dict[str, Tuple[str, str]] = {}
        for r in rows:
            alias_norm = str(r["alias_norm"] or "").strip()
            author_norm = str(r["author_norm"] or "").strip()
            author_display = str(r["author_display"] or "").strip()
            if alias_norm and author_norm and author_display:
                out[alias_norm] = (author_norm, author_display)
        return out

    def _candidate_author_aliases(self, filename: str) -> List[str]:
        stem = os.path.basename(filename)
        if "." in stem:
            stem = stem.rsplit(".", 1)[0]
        candidates: List[str] = []
        for sep in (" - ", " — ", " – ", "_"):
            if sep in stem:
                first = stem.split(sep, 1)[0].strip()
                if first:
                    candidates.append(first)
        if not candidates and stem:
            candidates.append(stem)
        out: List[str] = []
        for c in candidates:
            norm = normalize_text(c)
            if norm and norm not in out:
                out.append(norm)
        return out

    def _try_correct_author(self, parsed, filename: str, alias_map: Dict[str, Tuple[str, str]]) -> bool:
        if parsed.author_norm and parsed.author_norm != "unknown":
            return False
        for alias_norm in self._candidate_author_aliases(filename):
            hit = alias_map.get(alias_norm)
            if not hit:
                continue
            corrected_norm, corrected_display = hit
            parsed.author = corrected_display
            parsed.author_norm = corrected_norm
            parsed.work_key = make_work_key(parsed.author_norm, parsed.series_norm, parsed.title_norm, self._series_index_norm(parsed.series_index))
            return True
        return False

    def _series_index_norm(self, series_index: Optional[float]) -> str:
        series_index_norm = ""
        if series_index is not None:
            series_index_norm = f"{series_index:05.1f}".lstrip("0")
            if series_index_norm.startswith("."):
                series_index_norm = "0" + series_index_norm
        return series_index_norm

    def _learn_author_alias(self, parsed, alias_map: Dict[str, Tuple[str, str]], ts: int):
        if not parsed.author_norm or parsed.author_norm == "unknown" or not parsed.author:
            return
        alias_norm = normalize_text(parsed.author)
        if not alias_norm:
            return
        if alias_norm not in alias_map:
            alias_map[alias_norm] = (parsed.author_norm, parsed.author)
            self.db.execute(
                """
                INSERT INTO author_aliases(alias_norm,author_norm,author_display,confidence,source,updated_at)
                VALUES(?,?,?,?,?,?)
                ON CONFLICT(alias_norm) DO UPDATE SET
                  author_norm=excluded.author_norm,
                  author_display=excluded.author_display,
                  updated_at=excluded.updated_at
                """,
                (alias_norm, parsed.author_norm, parsed.author, 1.0, "derived", ts),
            )


    def _run_scan(self) -> Tuple[bool, str]:
        roots_rows = self.db.query_all("SELECT id,path FROM roots WHERE enabled=1 ORDER BY id")
        roots = [r["path"] for r in roots_rows]
        if not roots:
            return False, "No enabled roots. Add at least one folder."

        scan_id = self._start_scan_run()
        self._ensure_7z()
        author_aliases = self._load_author_aliases()

        stack = self._load_stack(roots)
        stats = {
            "dirs": 0,
            "files": 0,
            "files_changed": 0,
            "archives_listed": 0,
            "skipped_unchanged": 0,
            "skipped_folders": 0,
        }

        changed_since_commit = 0
        last_checkpoint_time = time.time()
        current_dir = ""
        last_path = ""

        root_pairs = sorted(
            ((norm_path(r["path"]), int(r["id"])) for r in roots_rows),
            key=lambda item: len(item[0]),
            reverse=True,
        )

        self.db.begin()
        try:
            while stack and not self._stop:
                self._maybe_pause()
                current_dir = stack.pop()
                current_dir = norm_path(current_dir)
                last_path = current_dir

                root_id = self._root_id_for_dir(current_dir, root_pairs)
                if root_id is None:
                    continue

                if not os.path.isdir(current_dir):
                    continue

                try:
                    if self._folder_skip_allowed(root_id, current_dir, scan_id):
                        stats["skipped_folders"] += 1
                        stats["dirs"] += 1
                        continue
                except Exception:
                    pass

                stats["dirs"] += 1
                if stats["dirs"] % 50 == 0:
                    self.progress.emit(f"Scanning: {current_dir}")

                try:
                    existing_by_path = self._folder_file_index(current_dir)
                    dir_interrupted = False
                    with os.scandir(current_dir) as it:
                        for entry in it:
                            if self._stop:
                                dir_interrupted = True
                                break
                            self._maybe_pause()

                            try:
                                if entry.is_dir(follow_symlinks=False):
                                    stack.append(entry.path)
                                    continue
                                if not entry.is_file(follow_symlinks=False):
                                    continue
                            except Exception:
                                continue

                            stats["files"] += 1
                            p = norm_path(entry.path)
                            folder_path = norm_path(os.path.dirname(p))
                            name = os.path.basename(p)
                            ext = ext_of(name)
                            is_arch = 1 if is_probably_archive(ext) else 0

                            try:
                                st = entry.stat(follow_symlinks=False)
                            except Exception:
                                continue

                            sig = file_sig_from_stat(st)
                            ctime_ns = int(getattr(st, "st_ctime_ns", int(st.st_ctime * 1e9)))

                            cached_sig = existing_by_path.get(p)
                            if cached_sig and cached_sig[0] == sig.size and cached_sig[1] == sig.mtime_ns:
                                self.db.execute("UPDATE files SET last_seen_scan_id=? WHERE path=?", (scan_id, p))
                                stats["skipped_unchanged"] += 1
                                continue

                            stats["files_changed"] += 1

                            parsed = parse_filename(name)
                            self._try_correct_author(parsed, name, author_aliases)
                            ts_now = now_ts()
                            self._learn_author_alias(parsed, author_aliases, ts_now)
                            tags_lower = detect_quality_tags(parsed.tags)

                            inner_guess = None

                            if is_arch:
                                from_tags = None
                                for t in tags_lower:
                                    t2 = t.strip().lower()
                                    if t2 in ("epub","azw3","azw","mobi","pdf","html","htm","doc","docx","rtf","txt","fb2","djvu","chm"):
                                        from_tags = t2
                                        break
                                if from_tags:
                                    inner_guess = from_tags
                                else:
                                    if self.cfg.sevenzip_path:
                                        hist = list_archive_exts(self.cfg.sevenzip_path, p, timeout_s=self.cfg.archive_timeout_s)
                                        if hist:
                                            from .ranker import format_score
                                            inner_guess = max(hist.keys(), key=lambda e: format_score(e))
                                            stats["archives_listed"] += 1

                            self.db.execute(
                                """
                                INSERT INTO files(
                                  root_id, folder_path, path, name, ext, size, mtime_ns, ctime_ns,
                                  is_archive, inner_ext_guess,
                                  author, series, series_index, title, tags,
                                  author_norm, series_norm, title_norm, work_key,
                                  last_seen_scan_id
                                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                                ON CONFLICT(path) DO UPDATE SET
                                  root_id=excluded.root_id,
                                  folder_path=excluded.folder_path,
                                  name=excluded.name,
                                  ext=excluded.ext,
                                  size=excluded.size,
                                  mtime_ns=excluded.mtime_ns,
                                  ctime_ns=excluded.ctime_ns,
                                  is_archive=excluded.is_archive,
                                  inner_ext_guess=excluded.inner_ext_guess,
                                  author=excluded.author,
                                  series=excluded.series,
                                  series_index=excluded.series_index,
                                  title=excluded.title,
                                  tags=excluded.tags,
                                  author_norm=excluded.author_norm,
                                  series_norm=excluded.series_norm,
                                  title_norm=excluded.title_norm,
                                  work_key=excluded.work_key,
                                  last_seen_scan_id=excluded.last_seen_scan_id
                                """,
                                (
                                    root_id, folder_path, p, name, ext, sig.size, sig.mtime_ns, ctime_ns,
                                    is_arch, inner_guess,
                                    parsed.author, parsed.series, parsed.series_index, parsed.title, " | ".join(parsed.tags),
                                    parsed.author_norm, parsed.series_norm, parsed.title_norm, parsed.work_key,
                                    scan_id
                                )
                            )
                            existing_by_path[p] = (sig.size, sig.mtime_ns)

                            series_index_norm = self._series_index_norm(parsed.series_index)

                            self.db.execute(
                                """
                                INSERT INTO works(
                                  work_key, author_norm, series_norm, series_index_norm, title_norm,
                                  display_author, display_series, display_series_index, display_title
                                ) VALUES(?,?,?,?,?,?,?,?,?)
                                ON CONFLICT(work_key) DO UPDATE SET
                                  author_norm=excluded.author_norm,
                                  series_norm=excluded.series_norm,
                                  series_index_norm=excluded.series_index_norm,
                                  title_norm=excluded.title_norm
                                """,
                                (
                                    parsed.work_key, parsed.author_norm, parsed.series_norm, series_index_norm, parsed.title_norm,
                                    parsed.author, parsed.series or "", str(parsed.series_index or ""), parsed.title
                                )
                            )

                            ext_for_count = (inner_guess if (is_arch and inner_guess) else ext) or ""
                            if ext_for_count:
                                self.db.execute(
                                    """
                                    INSERT INTO filetypes(ext,count_total,count_archives_guess,last_seen_scan_id)
                                    VALUES(?,?,?,?)
                                    ON CONFLICT(ext) DO UPDATE SET
                                      count_total=count_total+1,
                                      count_archives_guess=count_archives_guess+excluded.count_archives_guess,
                                      last_seen_scan_id=excluded.last_seen_scan_id
                                    """,
                                    (ext_for_count, 1, 1 if is_arch else 0, scan_id)
                                )

                            changed_since_commit += 1

                            if changed_since_commit >= self.cfg.commit_every:
                                self.db.commit()
                                self.db.begin()
                                changed_since_commit = 0

                            if time.time() - last_checkpoint_time >= self.cfg.checkpoint_every_s:
                                self._save_checkpoint(scan_id, stack, current_dir, last_path, stats)
                                self.stats.emit(stats.copy())
                                last_checkpoint_time = time.time()

                except Exception:
                    continue

                if dir_interrupted and current_dir:
                    # Preserve exact resumability: re-queue the in-progress folder so a
                    # stop/reload resumes this directory rather than skipping remaining files.
                    if current_dir not in stack:
                        stack.append(current_dir)
                    last_path = current_dir

            self.db.commit()

        except Exception:
            self.db.rollback()
            raise

        if self._stop:
            self._set_scan_status(scan_id, "aborted")
            self.progress.emit(f"Scan paused/stopped; checkpoint saved at folder: {current_dir}")
            self._save_checkpoint(scan_id, stack, current_dir, last_path, stats)
            return False, "Scan aborted (progress saved for resume)."
        else:
            self._set_scan_status(scan_id, "completed")
            self._save_checkpoint(scan_id, [], "", "", stats)
            self.db.set_state("scan_completed", "1")
            return True, "Scan completed."
