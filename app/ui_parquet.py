from __future__ import annotations

import os
import re
import sys
import time
from datetime import datetime
from pathlib import Path

from PySide6.QtCore import QObject, QProcess, QThread, QTimer, Signal, Qt
from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QPushButton, QLabel,
    QLineEdit, QFileDialog, QMessageBox, QGroupBox, QFormLayout,
    QTextEdit, QProgressBar, QFrame,
)


# ---------------------------------------------------------------------------
# Parquet file discovery
# ---------------------------------------------------------------------------

# Final output patterns: fiction_eng_authors_YYYY-MM-DD.parquet etc.
_FINAL_PATTERNS = [
    re.compile(r"^fiction_eng_authors_(\d{4}-\d{2}-\d{2})\.parquet$"),
    re.compile(r"^fiction_eng_author_aliases_(\d{4}-\d{2}-\d{2})\.parquet$"),
    re.compile(r"^author_redirects_resolved_(\d{4}-\d{2}-\d{2})\.parquet$"),
]

# Intermediate files (no date in name)
_INTERMEDIATE_FILES = [
    "author_redirects_resolved.parquet",
    "fiction_work_keys.parquet",
    "fiction_eng_work_keys.parquet",
    "canon_author_keys.parquet",
]

# Phase completion map: which intermediate/final file marks each phase done
_PHASE_FILES = {
    1: "_intermediate/author_redirects_resolved.parquet",
    2: "_intermediate/fiction_work_keys.parquet",
    3: "_intermediate/fiction_eng_work_keys.parquet",
    4: "_intermediate/canon_author_keys.parquet",
    # Phase 5 produces the final outputs — check any one
}

_PHASE_NAMES = {
    1: "Author redirects",
    2: "Fiction work-key classification",
    3: "English-edition fiction works",
    4: "Author key extraction",
    5: "Author detail export",
}


def _scan_parquet_files(out_dir: str) -> list[dict]:
    """Return a list of dicts describing parquet files found (or expected) in out_dir."""
    results: list[dict] = []
    out = Path(out_dir)
    inter = out / "_intermediate"

    # Final outputs
    for pat in _FINAL_PATTERNS:
        found = False
        if out.is_dir():
            for f in out.iterdir():
                m = pat.match(f.name)
                if m:
                    st = f.stat()
                    results.append({
                        "name": f.name,
                        "path": str(f),
                        "exists": True,
                        "size": st.st_size,
                        "modified": datetime.fromtimestamp(st.st_mtime).strftime("%Y-%m-%d %H:%M"),
                        "date": m.group(1),
                        "category": "final",
                    })
                    found = True
                    break
        if not found:
            # Show expected name placeholder
            label = pat.pattern.replace(r"(\d{4}-\d{2}-\d{2})", "YYYY-MM-DD").lstrip("^").rstrip("$")
            results.append({
                "name": label,
                "path": "",
                "exists": False,
                "size": 0,
                "modified": "",
                "date": "",
                "category": "final",
            })

    # Intermediate files
    for fname in _INTERMEDIATE_FILES:
        fp = inter / fname
        if fp.exists():
            st = fp.stat()
            results.append({
                "name": f"_intermediate/{fname}",
                "path": str(fp),
                "exists": True,
                "size": st.st_size,
                "modified": datetime.fromtimestamp(st.st_mtime).strftime("%Y-%m-%d %H:%M"),
                "date": "",
                "category": "intermediate",
            })
        else:
            results.append({
                "name": f"_intermediate/{fname}",
                "path": "",
                "exists": False,
                "size": 0,
                "modified": "",
                "date": "",
                "category": "intermediate",
            })

    return results


def _format_size(n: int) -> str:
    if n < 1024:
        return f"{n} B"
    if n < 1024 * 1024:
        return f"{n / 1024:.1f} KB"
    if n < 1024 * 1024 * 1024:
        return f"{n / (1024 * 1024):.1f} MB"
    return f"{n / (1024 * 1024 * 1024):.2f} GB"


def _detect_completed_phase(out_dir: str, dump_date: str) -> int:
    """Return the highest completed phase (0 if none)."""
    out = Path(out_dir)
    # Check phases 1-4 via intermediate files
    for phase in (4, 3, 2, 1):
        rel = _PHASE_FILES[phase]
        if (out / rel).exists():
            # Check if phase 5 final outputs exist too
            if phase == 4:
                final = out / f"fiction_eng_authors_{dump_date}.parquet"
                if final.exists():
                    return 5
            return phase
    return 0


# ---------------------------------------------------------------------------
# Parquet → SQLite worker  (runs on QThread, uses duckdb in-process)
# ---------------------------------------------------------------------------

class _ParquetToSqliteWorker(QObject):
    """Loads parquet author/alias/redirect data into authors.sqlite via duckdb."""
    progress = Signal(str, float)   # message, pct
    finished = Signal(bool, str)    # ok, message

    def __init__(self, author_db, authors_parquet: str, aliases_parquet: str,
                 redirects_parquet: str):
        super().__init__()
        self.author_db = author_db
        self.authors_parquet = authors_parquet
        self.aliases_parquet = aliases_parquet
        self.redirects_parquet = redirects_parquet
        self._stop = False

    def request_stop(self):
        self._stop = True

    def run(self):
        try:
            import duckdb
        except ImportError:
            self.finished.emit(False, "duckdb is not installed. Run: pip install duckdb")
            return

        db = self.author_db
        total_authors = 0
        total_aliases = 0
        total_redirects = 0

        try:
            # --- Authors ---
            if self.authors_parquet and os.path.isfile(self.authors_parquet):
                self.progress.emit("Reading authors parquet...", 0.0)
                con = duckdb.connect(":memory:")
                try:
                    rows = con.execute(
                        f"SELECT canonical_author_key, name, personal_name "
                        f"FROM read_parquet('{_qp(self.authors_parquet)}')"
                    ).fetchall()
                finally:
                    con.close()

                total = len(rows)
                self.progress.emit(f"Loading {total} authors into SQLite...", 5.0)
                db.begin()
                for i, (ol_key, name, personal_name) in enumerate(rows):
                    if self._stop:
                        db.commit()
                        self.finished.emit(False, f"Cancelled after {i} authors.")
                        return
                    name = str(name or "").strip()
                    if not name:
                        continue
                    from .util import normalize_text
                    norm = normalize_text(name)
                    if not norm:
                        continue
                    db.execute(
                        "INSERT INTO known_authors(normalized_name,canonical_name,preferred_name,frequency,created_at,updated_at)"
                        " VALUES(?,?,?,1,strftime('%s','now'),strftime('%s','now'))"
                        " ON CONFLICT(normalized_name) DO UPDATE SET"
                        " canonical_name=excluded.canonical_name,"
                        " preferred_name=excluded.preferred_name,"
                        " updated_at=excluded.updated_at",
                        (norm, name, name),
                    )
                    db.execute(
                        "INSERT INTO author_dump_records(ol_key,last_modified,author_norm,canonical_name,dump_date,updated_at)"
                        " VALUES(?,?,?,?,?,strftime('%s','now'))"
                        " ON CONFLICT(ol_key) DO UPDATE SET"
                        " last_modified=excluded.last_modified,"
                        " author_norm=excluded.author_norm,"
                        " canonical_name=excluded.canonical_name,"
                        " dump_date=excluded.dump_date,"
                        " updated_at=excluded.updated_at",
                        (str(ol_key or ""), "", norm, name, "parquet"),
                    )
                    total_authors += 1
                    if (i + 1) % 5000 == 0:
                        db.commit()
                        db.begin()
                        pct = 5.0 + 30.0 * (i + 1) / max(1, total)
                        self.progress.emit(f"Authors: {i + 1}/{total}", pct)
                db.commit()
                self.progress.emit(f"Authors done: {total_authors} loaded", 35.0)

            # --- Aliases ---
            if self.aliases_parquet and os.path.isfile(self.aliases_parquet):
                self.progress.emit("Reading aliases parquet...", 36.0)
                con = duckdb.connect(":memory:")
                try:
                    rows = con.execute(
                        f"SELECT canonical_author_key, author_display, alias "
                        f"FROM read_parquet('{_qp(self.aliases_parquet)}')"
                    ).fetchall()
                finally:
                    con.close()

                total = len(rows)
                self.progress.emit(f"Loading {total} aliases into SQLite...", 38.0)
                db.begin()
                for i, (ol_key, author_display, alias) in enumerate(rows):
                    if self._stop:
                        db.commit()
                        self.finished.emit(False, f"Cancelled after {i} aliases.")
                        return
                    alias_text = str(alias or "").strip()
                    if not alias_text:
                        continue
                    from .util import normalize_text
                    alias_norm = normalize_text(alias_text)
                    if not alias_norm:
                        continue
                    # Find the author_norm for this canonical_author_key
                    author_name = str(author_display or "").strip()
                    author_norm = normalize_text(author_name) if author_name else ""
                    if not author_norm:
                        continue
                    db.execute(
                        "INSERT INTO author_aliases(alias_norm,author_norm,author_display,confidence,source,updated_at)"
                        " VALUES(?,?,?,1.0,'parquet',strftime('%s','now'))"
                        " ON CONFLICT(alias_norm) DO UPDATE SET"
                        " author_norm=excluded.author_norm,"
                        " author_display=excluded.author_display,"
                        " confidence=excluded.confidence,"
                        " source='parquet',"
                        " updated_at=excluded.updated_at",
                        (alias_norm, author_norm, alias_text),
                    )
                    total_aliases += 1
                    if (i + 1) % 5000 == 0:
                        db.commit()
                        db.begin()
                        pct = 38.0 + 30.0 * (i + 1) / max(1, total)
                        self.progress.emit(f"Aliases: {i + 1}/{total}", pct)
                db.commit()
                self.progress.emit(f"Aliases done: {total_aliases} loaded", 68.0)

            # --- Redirects ---
            if self.redirects_parquet and os.path.isfile(self.redirects_parquet):
                self.progress.emit("Reading redirects parquet...", 69.0)
                con = duckdb.connect(":memory:")
                try:
                    rows = con.execute(
                        f"SELECT from_key, to_key, to_key_resolved, revision, last_modified "
                        f"FROM read_parquet('{_qp(self.redirects_parquet)}')"
                    ).fetchall()
                finally:
                    con.close()

                total = len(rows)
                self.progress.emit(f"Loading {total} redirects into SQLite...", 71.0)
                db.begin()
                for i, (from_key, to_key, to_key_resolved, revision, last_modified) in enumerate(rows):
                    if self._stop:
                        db.commit()
                        self.finished.emit(False, f"Cancelled after {i} redirects.")
                        return
                    db.execute(
                        "INSERT INTO ol_author_redirects(from_key,to_key,to_key_resolved,dump_date,last_modified,revision,updated_at)"
                        " VALUES(?,?,?,'parquet',?,?,strftime('%s','now'))"
                        " ON CONFLICT(from_key) DO UPDATE SET"
                        " to_key=excluded.to_key,"
                        " to_key_resolved=excluded.to_key_resolved,"
                        " dump_date=excluded.dump_date,"
                        " last_modified=excluded.last_modified,"
                        " revision=excluded.revision,"
                        " updated_at=excluded.updated_at",
                        (str(from_key or ""), str(to_key or ""),
                         str(to_key_resolved or ""),
                         str(last_modified or ""),
                         int(revision or 0)),
                    )
                    total_redirects += 1
                    if (i + 1) % 5000 == 0:
                        db.commit()
                        db.begin()
                        pct = 71.0 + 28.0 * (i + 1) / max(1, total)
                        self.progress.emit(f"Redirects: {i + 1}/{total}", pct)
                db.commit()
                self.progress.emit(f"Redirects done: {total_redirects} loaded", 99.0)

            self.finished.emit(
                True,
                f"Loaded {total_authors} authors, {total_aliases} aliases, "
                f"{total_redirects} redirects into authors.sqlite.",
            )
        except Exception as e:
            try:
                db.rollback()
            except Exception:
                pass
            self.finished.emit(False, f"Failed: {e!r}")


def _qp(p: str) -> str:
    """Convert path to forward-slash string for DuckDB SQL."""
    return str(p).replace("\\", "/")


# ---------------------------------------------------------------------------
# ParquetTab  (always-available tab)
# ---------------------------------------------------------------------------

class ParquetTab(QWidget):
    def __init__(self, get_author_db=None, on_activity_progress=None):
        super().__init__()
        self.get_author_db = get_author_db
        self.on_activity_progress = on_activity_progress
        self._process: QProcess | None = None
        self._poll_timer: QTimer | None = None
        self._load_thread: QThread | None = None
        self._load_worker: _ParquetToSqliteWorker | None = None

        lay = QVBoxLayout(self)

        # --- Parquet file status ---
        status_group = QGroupBox("Parquet File Status")
        status_lay = QVBoxLayout(status_group)
        self.status_label = QLabel("Configure output directory and click Refresh.")
        self.status_label.setWordWrap(True)
        status_lay.addWidget(self.status_label)
        lay.addWidget(status_group)

        # --- Parameters ---
        params_group = QGroupBox("Conversion Parameters")
        form = QFormLayout(params_group)

        self.dumps_dir_edit = QLineEdit("K:\\")
        dumps_row = QHBoxLayout()
        dumps_row.addWidget(self.dumps_dir_edit)
        btn_dumps_browse = QPushButton("Browse...")
        btn_dumps_browse.clicked.connect(lambda: self._browse_dir(self.dumps_dir_edit, "Dumps directory"))
        dumps_row.addWidget(btn_dumps_browse)
        form.addRow("Dumps dir:", dumps_row)

        self.out_dir_edit = QLineEdit(r"W:\Dany\BookDeduper")
        out_row = QHBoxLayout()
        out_row.addWidget(self.out_dir_edit)
        btn_out_browse = QPushButton("Browse...")
        btn_out_browse.clicked.connect(lambda: self._browse_dir(self.out_dir_edit, "Output directory"))
        out_row.addWidget(btn_out_browse)
        form.addRow("Output dir:", out_row)

        self.temp_dir_edit = QLineEdit(r"W:\Dany\BookDeduper\duckdb_tmp")
        temp_row = QHBoxLayout()
        temp_row.addWidget(self.temp_dir_edit)
        btn_temp_browse = QPushButton("Browse...")
        btn_temp_browse.clicked.connect(lambda: self._browse_dir(self.temp_dir_edit, "Temp directory"))
        temp_row.addWidget(btn_temp_browse)
        form.addRow("Temp dir:", temp_row)

        self.dump_date_edit = QLineEdit("2026-01-31")
        form.addRow("Dump date:", self.dump_date_edit)

        self.memory_edit = QLineEdit("40GB")
        form.addRow("Memory limit:", self.memory_edit)

        self.threads_edit = QLineEdit("2")
        form.addRow("Threads:", self.threads_edit)

        self.max_line_edit = QLineEdit("50000000")
        form.addRow("Max line size:", self.max_line_edit)

        lay.addWidget(params_group)

        # --- Control buttons ---
        ctrl_row = QHBoxLayout()
        self.btn_start = QPushButton("Start Conversion")
        self.btn_start.clicked.connect(self.start_conversion)
        ctrl_row.addWidget(self.btn_start)

        self.btn_stop = QPushButton("Stop")
        self.btn_stop.setEnabled(False)
        self.btn_stop.clicked.connect(self.stop_conversion)
        ctrl_row.addWidget(self.btn_stop)

        btn_refresh = QPushButton("Refresh Status")
        btn_refresh.clicked.connect(self.refresh)
        ctrl_row.addWidget(btn_refresh)
        lay.addLayout(ctrl_row)

        # --- Progress ---
        self.phase_label = QLabel("")
        lay.addWidget(self.phase_label)

        self.progress_bar = QProgressBar()
        self.progress_bar.setRange(0, 100)
        self.progress_bar.setValue(0)
        lay.addWidget(self.progress_bar)

        # --- Log output ---
        self.log_output = QTextEdit()
        self.log_output.setReadOnly(True)
        self.log_output.setMaximumHeight(150)
        self.log_output.setStyleSheet("QTextEdit { font-family: Consolas, monospace; font-size: 9pt; }")
        lay.addWidget(self.log_output)

        # --- Separator ---
        sep = QFrame()
        sep.setFrameShape(QFrame.HLine)
        sep.setFrameShadow(QFrame.Sunken)
        lay.addWidget(sep)

        # --- Parquet → SQLite section ---
        self.sqlite_group = QGroupBox("Load Parquet into authors.sqlite")
        sqlite_lay = QVBoxLayout(self.sqlite_group)
        self.sqlite_status = QLabel("No author DB connected.")
        sqlite_lay.addWidget(self.sqlite_status)

        sqlite_btns = QHBoxLayout()
        self.btn_load_all = QPushButton("Load All (Authors + Aliases + Redirects)")
        self.btn_load_all.clicked.connect(self.load_all_to_sqlite)
        self.btn_load_all.setEnabled(False)
        sqlite_btns.addWidget(self.btn_load_all)
        sqlite_lay.addLayout(sqlite_btns)

        self.sqlite_progress = QProgressBar()
        self.sqlite_progress.setRange(0, 100)
        self.sqlite_progress.setValue(0)
        sqlite_lay.addWidget(self.sqlite_progress)

        lay.addWidget(self.sqlite_group)

        lay.addStretch(1)

    def _browse_dir(self, edit: QLineEdit, title: str):
        d = QFileDialog.getExistingDirectory(self, title, edit.text().strip())
        if d:
            edit.setText(d)

    def refresh(self):
        """Refresh parquet file status display and SQLite section visibility."""
        out_dir = self.out_dir_edit.text().strip()
        if not out_dir:
            self.status_label.setText("Set an output directory first.")
            return

        files = _scan_parquet_files(out_dir)
        lines: list[str] = []
        dates_found: set[str] = set()
        for f in files:
            if f["exists"]:
                mark = "[ok]"
                detail = f"  {_format_size(f['size'])}  {f['modified']}"
                if f["date"]:
                    dates_found.add(f["date"])
            else:
                mark = "[--]"
                detail = ""
            lines.append(f"  {mark}  {f['name']}{detail}")

        header = "Parquet files in: " + out_dir
        if dates_found:
            header += f"\n  Dump date(s): {', '.join(sorted(dates_found))}"
        self.status_label.setText(header + "\n" + "\n".join(lines))

        # Update SQLite section
        author_db = self.get_author_db() if self.get_author_db else None
        if author_db:
            self.sqlite_status.setText(f"Author DB: {os.path.basename(author_db.db_path)}")
            # Enable load button only if at least authors parquet exists
            has_authors = any(f["exists"] and f["category"] == "final" and "authors_" in f["name"]
                             and "aliases" not in f["name"]
                             for f in files)
            self.btn_load_all.setEnabled(has_authors and self._load_thread is None)
        else:
            self.sqlite_status.setText("No author DB connected. Open one on the Project tab.")
            self.btn_load_all.setEnabled(False)

    # ------------------------------------------------------------------
    # DuckDB export subprocess
    # ------------------------------------------------------------------

    def start_conversion(self):
        if self._process is not None:
            QMessageBox.information(self, "Conversion", "A conversion is already running.")
            return

        # Locate the script
        script = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                              "ol_duckdb_export_fiction_eng.py")
        if not os.path.isfile(script):
            QMessageBox.warning(self, "Conversion",
                                f"Cannot find export script:\n{script}")
            return

        args = [
            script,
            "--dumps-dir", self.dumps_dir_edit.text().strip(),
            "--out-dir", self.out_dir_edit.text().strip(),
            "--temp-dir", self.temp_dir_edit.text().strip(),
            "--dump-date", self.dump_date_edit.text().strip(),
            "--memory-limit", self.memory_edit.text().strip(),
            "--threads", self.threads_edit.text().strip(),
            "--max-line-size", self.max_line_edit.text().strip(),
        ]

        self.log_output.clear()
        self.progress_bar.setValue(0)
        self.phase_label.setText("Starting conversion...")

        proc = QProcess(self)
        proc.setProcessChannelMode(QProcess.MergedChannels)
        proc.readyReadStandardOutput.connect(self._on_process_output)
        proc.finished.connect(self._on_process_finished)
        proc.start(sys.executable, args)

        if not proc.waitForStarted(5000):
            QMessageBox.warning(self, "Conversion", "Failed to start subprocess.")
            return

        self._process = proc
        self.btn_start.setEnabled(False)
        self.btn_stop.setEnabled(True)

        # Poll for phase completion every 2 seconds
        self._poll_timer = QTimer(self)
        self._poll_timer.timeout.connect(self._poll_progress)
        self._poll_timer.start(2000)

        if self.on_activity_progress:
            self.on_activity_progress("Converting dumps...", 0.0)

    def stop_conversion(self):
        if self._process is not None:
            self._process.kill()
            self.phase_label.setText("Stopping...")

    def _on_process_output(self):
        if self._process is None:
            return
        data = self._process.readAllStandardOutput()
        text = bytes(data).decode("utf-8", errors="replace")
        self.log_output.append(text.rstrip("\n"))
        # Auto-scroll to bottom
        sb = self.log_output.verticalScrollBar()
        sb.setValue(sb.maximum())

    def _poll_progress(self):
        out_dir = self.out_dir_edit.text().strip()
        dump_date = self.dump_date_edit.text().strip()
        if not out_dir:
            return
        phase = _detect_completed_phase(out_dir, dump_date)
        pct = phase * 20
        if phase > 0:
            name = _PHASE_NAMES.get(phase, "")
            if phase == 5:
                self.phase_label.setText(f"Phase 5/5 complete: {name}")
            else:
                next_name = _PHASE_NAMES.get(phase + 1, "")
                self.phase_label.setText(f"Phase {phase}/5 done ({name}). Running phase {phase + 1}: {next_name}...")
        self.progress_bar.setValue(pct)
        if self.on_activity_progress:
            self.on_activity_progress(f"Converting phase {min(phase + 1, 5)}/5", float(pct))

    def _on_process_finished(self, exit_code: int, _exit_status):
        if self._poll_timer:
            self._poll_timer.stop()
            self._poll_timer = None
        self._process = None
        self.btn_start.setEnabled(True)
        self.btn_stop.setEnabled(False)

        if exit_code == 0:
            self.phase_label.setText("Conversion completed successfully.")
            self.progress_bar.setValue(100)
        else:
            self.phase_label.setText(f"Conversion exited with code {exit_code}.")

        if self.on_activity_progress:
            self.on_activity_progress("Idle", -1.0)

        self.refresh()

    # ------------------------------------------------------------------
    # Parquet → SQLite loading
    # ------------------------------------------------------------------

    def _find_final_parquet(self, prefix: str) -> str | None:
        """Find the first matching final parquet file for a given prefix."""
        out_dir = Path(self.out_dir_edit.text().strip())
        if not out_dir.is_dir():
            return None
        for f in out_dir.iterdir():
            if f.name.startswith(prefix) and f.suffix == ".parquet":
                return str(f)
        return None

    def load_all_to_sqlite(self):
        author_db = self.get_author_db() if self.get_author_db else None
        if not author_db:
            QMessageBox.warning(self, "Load Parquet", "No author DB connected.")
            return
        if self._load_thread is not None:
            QMessageBox.information(self, "Load Parquet", "A load is already in progress.")
            return

        authors_pq = self._find_final_parquet("fiction_eng_authors_")
        aliases_pq = self._find_final_parquet("fiction_eng_author_aliases_")
        redirects_pq = self._find_final_parquet("author_redirects_resolved_")

        if not authors_pq:
            QMessageBox.warning(self, "Load Parquet",
                                "No fiction_eng_authors_*.parquet found in output dir.")
            return

        worker = _ParquetToSqliteWorker(author_db, authors_pq, aliases_pq, redirects_pq)
        thread = QThread()
        self._load_thread = thread
        self._load_worker = worker

        worker.progress.connect(self._on_load_progress)
        worker.finished.connect(self._on_load_finished)
        worker.moveToThread(thread)
        thread.started.connect(worker.run)

        self.btn_load_all.setEnabled(False)
        self.sqlite_progress.setValue(0)
        thread.start()

        if self.on_activity_progress:
            self.on_activity_progress("Loading parquet to SQLite...", 0.0)

    def _on_load_progress(self, msg: str, pct: float):
        self.sqlite_status.setText(msg)
        self.sqlite_progress.setValue(int(pct))
        if self.on_activity_progress:
            self.on_activity_progress(msg, pct)

    def _on_load_finished(self, ok: bool, msg: str):
        if self._load_thread:
            self._load_thread.quit()
            self._load_thread.wait(5000)
        self._load_thread = None
        self._load_worker = None

        self.sqlite_progress.setValue(100 if ok else 0)
        self.sqlite_status.setText(msg)

        if self.on_activity_progress:
            self.on_activity_progress("Idle", -1.0)

        if ok:
            QMessageBox.information(self, "Load Parquet", msg)
        else:
            QMessageBox.warning(self, "Load Parquet", msg)

        self.refresh()
