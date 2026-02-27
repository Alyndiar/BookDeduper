from __future__ import annotations
import os
from PySide6.QtCore import QThread
from PySide6.QtWidgets import QWidget, QVBoxLayout, QHBoxLayout, QPushButton, QLabel, QTextEdit, QMessageBox, QFileDialog
from .scanner import ScanWorker, ScanConfig, ReparseWorker
from .sevenzip import detect_7z

class ScanTab(QWidget):
    def _scan_tuning(self, profile: str) -> dict:
        p = (profile or "balanced").lower()
        if p == "safe":
            return {"commit_every": 2500, "checkpoint_every_s": 1.0}
        if p == "extreme":
            return {"commit_every": 20000, "checkpoint_every_s": 4.0}
        if p == "extreme+":
            return {"commit_every": 200000, "checkpoint_every_s": 12.0}
        return {"commit_every": 8000, "checkpoint_every_s": 2.0}

    def __init__(self, get_db, get_author_db, on_scan_completed, on_activity_progress=None):
        super().__init__()
        self.get_db = get_db
        self.get_author_db = get_author_db
        self.on_scan_completed = on_scan_completed
        self.on_activity_progress = on_activity_progress

        lay = QVBoxLayout(self)

        self.status = QLabel("Scan status: idle")
        lay.addWidget(self.status)

        self.log = QTextEdit()
        self.log.setReadOnly(True)
        lay.addWidget(self.log, 1)

        row = QHBoxLayout()
        self.btn_start = QPushButton("Start / Resume Scan")
        self.btn_start.clicked.connect(self.start_scan)
        row.addWidget(self.btn_start)

        self.btn_pause = QPushButton("Pause")
        self.btn_pause.clicked.connect(self.pause)
        self.btn_pause.setEnabled(False)
        row.addWidget(self.btn_pause)

        self.btn_resume = QPushButton("Resume")
        self.btn_resume.clicked.connect(self.resume)
        self.btn_resume.setEnabled(False)
        row.addWidget(self.btn_resume)

        self.btn_stop = QPushButton("Stop")
        self.btn_stop.clicked.connect(self.stop)
        self.btn_stop.setEnabled(False)
        row.addWidget(self.btn_stop)

        row.addSpacing(20)

        self.btn_reparse = QPushButton("Reparse Filenames")
        self.btn_reparse.setToolTip(
            "Re-derive author/series/title/work_key from stored filenames "
            "using current parser logic and author database. No filesystem I/O."
        )
        self.btn_reparse.clicked.connect(self.start_reparse)
        row.addWidget(self.btn_reparse)

        self.btn_export_unknown = QPushButton("Export Unknown Authors")
        self.btn_export_unknown.setToolTip(
            "Export a list of all file paths whose author could not be determined."
        )
        self.btn_export_unknown.clicked.connect(self.export_unknown_authors)
        row.addWidget(self.btn_export_unknown)

        lay.addLayout(row)


        self.thread: QThread | None = None
        self.worker: ScanWorker | None = None
        self._reparse_thread: QThread | None = None
        self._reparse_worker: ReparseWorker | None = None


    def refresh(self):
        db = self.get_db()
        if not db:
            self.status.setText("Scan status: (no project)")
            return
        last = db.get_state("last_action", "")
        scan_run = db.get_state("scan_run_id", "")
        self.status.setText(f"Scan status: ready (last_action={last}, scan_run_id={scan_run})")

    def append(self, s: str):
        self.log.append(s)

    def start_scan(self):
        db = self.get_db()
        if not db:
            QMessageBox.information(self, "Scan", "Open a project first.")
            return

        if self.thread:
            QMessageBox.information(self, "Scan", "Scan already running.")
            return

        if db.get_state("backup_before_scan", "0") == "1":
            try:
                path = db.create_timestamped_backup(label="scan")
                self.append(f"Backup created: {path}")
            except Exception as e:
                QMessageBox.warning(self, "Scan", f"Backup failed, continuing anyway:\n{e!r}")

        folder_skip = (db.get_state("folder_skip_enabled", "0") == "1")
        sevenzip_path = detect_7z(db.get_state("7z_path", None))
        if sevenzip_path:
            db.set_state("7z_path", sevenzip_path)

        tuning = self._scan_tuning(db.get_state("memory_profile", "balanced"))
        cfg = ScanConfig(folder_skip_enabled=folder_skip, sevenzip_path=sevenzip_path, commit_every=tuning["commit_every"], checkpoint_every_s=tuning["checkpoint_every_s"])

        self.thread = QThread()
        self.worker = ScanWorker(db, cfg, author_db=self.get_author_db())
        self.worker.moveToThread(self.thread)

        self.thread.started.connect(self.worker.run)
        self.worker.progress.connect(self.on_progress)
        self.worker.stats.connect(self.on_stats)
        self.worker.finished.connect(self.on_finished)

        self.btn_start.setEnabled(False)
        self.btn_pause.setEnabled(True)
        self.btn_stop.setEnabled(True)
        self.btn_resume.setEnabled(False)

        self.append("=== Scan started/resumed ===")
        if self.on_activity_progress:
            self.on_activity_progress("Scanning", 0.0)
        self.thread.start()

    def pause(self):
        if self.worker:
            self.worker.request_pause()
            self.btn_pause.setEnabled(False)
            self.btn_resume.setEnabled(True)
            self.append("=== Pause requested ===")

    def resume(self):
        if self.worker:
            self.worker.request_resume()
            self.btn_pause.setEnabled(True)
            self.btn_resume.setEnabled(False)
            self.append("=== Resume requested ===")

    def stop(self):
        if self.worker:
            self.worker.request_stop()
            self.append("=== Stop requested ===")

    def on_progress(self, msg: str):
        self.status.setText(f"Scan status: {msg}")
        self.append(msg)
        if self.on_activity_progress:
            self.on_activity_progress("Scanning", 0.0)

    def on_stats(self, st: dict):
        self.status.setText(
            f"Scan: dirs={st.get('dirs')} files={st.get('files')} changed={st.get('files_changed')} "
            f"archives_listed={st.get('archives_listed')} skipped_files={st.get('skipped_unchanged')} "
            f"skipped_folders={st.get('skipped_folders')}"
        )

    def on_finished(self, ok: bool, msg: str):
        self.append(msg)
        self.status.setText(f"Scan status: {msg}")

        if self.thread:
            self.thread.quit()
            self.thread.wait(2000)
        self.thread = None
        self.worker = None

        self.btn_start.setEnabled(True)
        self.btn_pause.setEnabled(False)
        self.btn_resume.setEnabled(False)
        self.btn_stop.setEnabled(False)

        if self.on_activity_progress:
            self.on_activity_progress("Idle", -1.0)
        self.on_scan_completed(ok)

    # ------------------------------------------------------------------
    #  Reparse Filenames
    # ------------------------------------------------------------------

    def start_reparse(self):
        db = self.get_db()
        if not db:
            QMessageBox.information(self, "Reparse", "Open a project first.")
            return

        if self.thread or self._reparse_thread:
            QMessageBox.information(self, "Reparse", "A scan or reparse is already running.")
            return

        reply = QMessageBox.question(
            self,
            "Reparse Filenames",
            "This will re-derive author, series, title, and work_key for every file "
            "using the current parser logic and author database.\n\n"
            "Existing analysis results (duplicates, deletion queue) will be cleared "
            "so you can re-analyse with the updated data.\n\n"
            "Continue?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.No,
        )
        if reply != QMessageBox.StandardButton.Yes:
            return

        self._reparse_thread = QThread()
        self._reparse_worker = ReparseWorker(db, author_db=self.get_author_db())
        self._reparse_worker.moveToThread(self._reparse_thread)

        self._reparse_thread.started.connect(self._reparse_worker.run)
        self._reparse_worker.progress.connect(self.on_progress)
        self._reparse_worker.stats.connect(self.on_stats)
        self._reparse_worker.finished.connect(self._on_reparse_finished)

        self.btn_start.setEnabled(False)
        self.btn_reparse.setEnabled(False)
        self.btn_stop.setEnabled(True)

        # Allow the Stop button to abort the reparse too
        self.btn_stop.clicked.disconnect()
        self.btn_stop.clicked.connect(self._stop_reparse)

        self.append("=== Reparse started ===")
        if self.on_activity_progress:
            self.on_activity_progress("Reparsing", 0.0)
        self._reparse_thread.start()

    def _stop_reparse(self):
        if self._reparse_worker:
            self._reparse_worker.request_stop()
            self.append("=== Reparse stop requested ===")

    def _on_reparse_finished(self, ok: bool, msg: str):
        self.append(msg)
        self.status.setText(f"Reparse: {msg}")

        if self._reparse_thread:
            self._reparse_thread.quit()
            self._reparse_thread.wait(2000)
        self._reparse_thread = None
        self._reparse_worker = None

        # Restore Stop button to its normal scan handler
        self.btn_stop.clicked.disconnect()
        self.btn_stop.clicked.connect(self.stop)

        self.btn_start.setEnabled(True)
        self.btn_reparse.setEnabled(True)
        self.btn_pause.setEnabled(False)
        self.btn_resume.setEnabled(False)
        self.btn_stop.setEnabled(False)

        if self.on_activity_progress:
            self.on_activity_progress("Idle", -1.0)
        self.on_scan_completed(ok)

    # ------------------------------------------------------------------
    #  Export Unknown Authors
    # ------------------------------------------------------------------

    def export_unknown_authors(self):
        db = self.get_db()
        if not db:
            QMessageBox.information(self, "Export", "Open a project first.")
            return

        rows = db.query_all(
            "SELECT path FROM files WHERE author_norm = 'unknown' ORDER BY path"
        )

        if not rows:
            QMessageBox.information(self, "Export", "No files with unknown author found.")
            return

        default_name = os.path.join(
            os.path.dirname(db.db_path),
            "unknown_authors.txt",
        )
        dest, _ = QFileDialog.getSaveFileName(
            self, "Save Unknown Authors Report", default_name,
            "Text files (*.txt);;All files (*)",
        )
        if not dest:
            return

        with open(dest, "w", encoding="utf-8") as f:
            for r in rows:
                f.write(str(r["path"] or "") + "\n")

        self.append(f"Exported {len(rows)} unknown-author files to {dest}")
        QMessageBox.information(
            self, "Export",
            f"Exported {len(rows)} files with unknown author to:\n{dest}",
        )
