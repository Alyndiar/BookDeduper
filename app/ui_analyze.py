from __future__ import annotations
from PySide6.QtCore import QThread
from PySide6.QtWidgets import QWidget, QVBoxLayout, QHBoxLayout, QPushButton, QLabel, QTextEdit, QMessageBox
from .analyzer import AnalyzeWorker

class AnalyzeTab(QWidget):
    def __init__(self, get_db, on_analyze_completed):
        super().__init__()
        self.get_db = get_db
        self.on_analyze_completed = on_analyze_completed

        lay = QVBoxLayout(self)
        self.status = QLabel("Analyze status: idle")
        lay.addWidget(self.status)

        self.log = QTextEdit()
        self.log.setReadOnly(True)
        lay.addWidget(self.log, 1)

        row = QHBoxLayout()
        self.btn_start = QPushButton("Start / Resume Analyze")
        self.btn_start.clicked.connect(self.start)
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
        self.worker: AnalyzeWorker | None = None

    def refresh(self):
        db = self.get_db()
        if not db:
            self.status.setText("Analyze status: (no project)")
            return
        scan_done = (db.get_state("scan_completed", "0") == "1")
        self.btn_start.setEnabled(scan_done and self.thread is None)
        last = db.get_state("analyze_last_work_key", "")
        self.status.setText(f"Analyze status: ready (resume_key={last[:60]})")

    def append(self, s: str):
        self.log.append(s)

    def start(self):
        db = self.get_db()
        if not db:
            QMessageBox.information(self, "Analyze", "Open a project first.")
            return
        if db.get_state("scan_completed", "0") != "1":
            QMessageBox.warning(self, "Analyze", "Scan must complete first.")
            return
        if self.thread:
            QMessageBox.information(self, "Analyze", "Analyze already running.")
            return

        self.thread = QThread()
        self.worker = AnalyzeWorker(db)
        self.worker.moveToThread(self.thread)

        self.thread.started.connect(self.worker.run)
        self.worker.progress.connect(self.on_progress)
        self.worker.stats.connect(self.on_stats)
        self.worker.finished.connect(self.on_finished)

        self.btn_start.setEnabled(False)
        self.btn_pause.setEnabled(True)
        self.btn_stop.setEnabled(True)
        self.btn_resume.setEnabled(False)

        self.append("=== Analyze started/resumed ===")
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
        self.status.setText(f"Analyze: {msg}")
        self.append(msg)

    def on_stats(self, st: dict):
        self.status.setText(
            f"Analyze: works={st.get('works')} works_with_dupes={st.get('works_with_dupes')} queued={st.get('queued')}"
        )

    def on_finished(self, ok: bool, msg: str):
        self.append(msg)
        self.status.setText(f"Analyze status: {msg}")

        if self.thread:
            self.thread.quit()
            self.thread.wait(2000)
        self.thread = None
        self.worker = None

        self.btn_start.setEnabled(True)
        self.btn_pause.setEnabled(False)
        self.btn_resume.setEnabled(False)
        self.btn_stop.setEnabled(False)

        self.on_analyze_completed(ok)
