from __future__ import annotations
from PySide6.QtCore import QThread
from PySide6.QtWidgets import QWidget, QVBoxLayout, QHBoxLayout, QPushButton, QLabel, QTextEdit, QMessageBox
from .scanner import ScanWorker, ScanConfig
from .sevenzip import detect_7z

class ScanTab(QWidget):
    def __init__(self, get_db, on_scan_completed):
        super().__init__()
        self.get_db = get_db
        self.on_scan_completed = on_scan_completed

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

        lay.addLayout(row)

        self.thread: QThread | None = None
        self.worker: ScanWorker | None = None

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

        folder_skip = (db.get_state("folder_skip_enabled", "0") == "1")
        sevenzip_path = detect_7z(db.get_state("7z_path", None))
        if sevenzip_path:
            db.set_state("7z_path", sevenzip_path)

        cfg = ScanConfig(folder_skip_enabled=folder_skip, sevenzip_path=sevenzip_path)

        self.thread = QThread()
        self.worker = ScanWorker(db, cfg)
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

        self.on_scan_completed(ok)
