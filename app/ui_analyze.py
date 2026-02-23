from __future__ import annotations
from PySide6.QtCore import QThread
from PySide6.QtWidgets import QWidget, QVBoxLayout, QHBoxLayout, QPushButton, QLabel, QTextEdit, QMessageBox, QProgressBar
from .analyzer import AnalyzeWorker

class AnalyzeTab(QWidget):
    def _analyze_commit_every(self, profile: str) -> int:
        p = (profile or "balanced").lower()
        if p == "safe":
            return 1500
        if p == "extreme":
            return 12000
        if p == "extreme+":
            return 20000
        return 5000

    def __init__(self, get_db, on_analyze_completed):
        super().__init__()
        self.get_db = get_db
        self.on_analyze_completed = on_analyze_completed
        self.current_phase = "duplicates"

        lay = QVBoxLayout(self)
        self.status = QLabel("Analyze status: idle")
        lay.addWidget(self.status)

        self.progress_bar = QProgressBar()
        self.progress_bar.setRange(0, 10000)
        self.progress_bar.setValue(0)
        self.progress_bar.setFormat("0.00%")
        lay.addWidget(self.progress_bar)

        self.log = QTextEdit()
        self.log.setReadOnly(True)
        lay.addWidget(self.log, 1)

        row = QHBoxLayout()
        self.btn_authors = QPushButton("Analyze Authors")
        self.btn_authors.clicked.connect(lambda: self.start("authors"))
        row.addWidget(self.btn_authors)

        self.btn_duplicates = QPushButton("Analyze Duplicates")
        self.btn_duplicates.clicked.connect(lambda: self.start("duplicates"))
        row.addWidget(self.btn_duplicates)

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
        self.btn_authors.setEnabled(scan_done and self.thread is None)
        self.btn_duplicates.setEnabled(scan_done and self.thread is None)
        last = db.get_state("analyze_last_work_key", "")
        ad = db.get_state("analyze_authors_completed", "0")
        dd = db.get_state("analyze_duplicates_completed", db.get_state("analyze_completed", "0"))
        self.status.setText(f"Analyze: authors_done={ad} duplicates_done={dd} resume_key={last[:40]}")

    def append(self, s: str):
        self.log.append(s)

    def start(self, phase: str):
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

        self.current_phase = phase
        self.thread = QThread()
        commit_every = self._analyze_commit_every(db.get_state("memory_profile", "balanced"))
        self.worker = AnalyzeWorker(db, phase=phase, commit_every=commit_every)
        self.worker.moveToThread(self.thread)

        self.thread.started.connect(self.worker.run)
        self.worker.progress.connect(self.on_progress)
        self.worker.progress_percent.connect(self.on_progress_percent)
        self.worker.stats.connect(self.on_stats)
        self.worker.finished.connect(self.on_finished)

        self.btn_authors.setEnabled(False)
        self.btn_duplicates.setEnabled(False)
        self.btn_pause.setEnabled(True)
        self.btn_stop.setEnabled(True)
        self.btn_resume.setEnabled(False)

        self.progress_bar.setValue(0)
        self.progress_bar.setFormat("0.00%")
        self.append(f"=== {phase} started ===")
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

    def on_progress_percent(self, pct: float, stage: str):
        if stage != "authors_suggestions":
            return
        pct = max(0.0, min(100.0, float(pct)))
        self.progress_bar.setValue(int(round(pct * 100)))
        self.progress_bar.setFormat(f"{pct:.2f}%")

    def on_stats(self, st: dict):
        self.status.setText(
            f"Analyze: works={st.get('works')} works_with_dupes={st.get('works_with_dupes')} queued={st.get('queued')}"
        )

    def on_finished(self, ok: bool, msg: str):
        self.append(msg)
        self.status.setText(f"Analyze status: {msg}")
        if ok and self.current_phase == "authors":
            self.progress_bar.setValue(10000)
            self.progress_bar.setFormat("100.00%")

        if self.thread:
            self.thread.quit()
            self.thread.wait(2000)
        self.thread = None
        self.worker = None

        self.btn_authors.setEnabled(True)
        self.btn_duplicates.setEnabled(True)
        self.btn_pause.setEnabled(False)
        self.btn_resume.setEnabled(False)
        self.btn_stop.setEnabled(False)

        self.on_analyze_completed(ok)
