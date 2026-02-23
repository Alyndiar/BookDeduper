from __future__ import annotations

from PySide6.QtCore import Signal
from PySide6.QtGui import QColor, QCloseEvent, QPalette
from PySide6.QtWidgets import QMainWindow, QWidget, QVBoxLayout, QTabWidget, QLabel, QStatusBar

from .db import DB
from .ui_project import ProjectTab
from .ui_roots import RootsTab
from .ui_scan import ScanTab
from .ui_analyze import AnalyzeTab
from .ui_review import ReviewTab
from .ui_authors import AuthorsTab


class MainWindow(QMainWindow):
    io_signal = Signal(str, bool)

    def __init__(self):
        super().__init__()
        self.setWindowTitle("BookDeduper (Project DB)")

        self.db: DB | None = None
        self._io_reads = 0
        self._io_writes = 0

        self.tabs = QTabWidget()
        self.tabs.setTabsClosable(False)
        self.tabs.currentChanged.connect(lambda _: self.refresh_all_statuses())

        self.project_tab = ProjectTab(on_project_opened=self.on_project_opened)
        self.roots_tab = RootsTab(get_db=lambda: self.db)
        self.scan_tab = ScanTab(get_db=lambda: self.db, on_scan_completed=self.on_scan_completed)
        self.analyze_tab = AnalyzeTab(get_db=lambda: self.db, on_analyze_completed=self.on_analyze_completed)
        self.review_tab = ReviewTab(get_db=lambda: self.db)
        self.authors_tab = AuthorsTab(get_db=lambda: self.db)

        self.tabs.addTab(self.project_tab, "1) Project")
        self.tabs.addTab(self.roots_tab, "2) Roots")
        self.tabs.addTab(self.scan_tab, "3) Scan")
        self.tabs.addTab(self.analyze_tab, "4) Analyze")
        self.tabs.addTab(self.review_tab, "5) Review/Delete")
        self.tabs.addTab(self.authors_tab, "6) Authors DB")

        for i in range(1, 6):
            self.tabs.setTabEnabled(i, False)

        w = QWidget()
        lay = QVBoxLayout(w)
        lay.addWidget(self.tabs)
        self.setCentralWidget(w)

        self.status = QStatusBar()
        self.setStatusBar(self.status)
        self.status_label = QLabel("Ready")
        self.status.addWidget(self.status_label, 1)
        self.read_box = self._make_io_box("R")
        self.write_box = self._make_io_box("W")
        self.status.addPermanentWidget(self.read_box)
        self.status.addPermanentWidget(self.write_box)
        self._set_io_box(self.read_box, "black")
        self._set_io_box(self.write_box, "black")

        self.io_signal.connect(self._on_io_signal)

        self.resize(1200, 800)

    def _make_io_box(self, label: str) -> QLabel:
        box = QLabel(label)
        box.setFixedWidth(24)
        box.setAutoFillBackground(True)
        box.setStyleSheet("QLabel { color: white; font-weight: bold; border: 1px solid #555; padding: 2px; }")
        return box

    def _set_io_box(self, box: QLabel, color: str):
        pal = box.palette()
        pal.setColor(QPalette.Window, QColor(color))
        box.setPalette(pal)

    def _io_callback(self, operation: str, active: bool):
        self.io_signal.emit(operation, active)

    def _on_io_signal(self, operation: str, active: bool):
        if operation == "read":
            self._io_reads = max(0, self._io_reads + (1 if active else -1))
            self._set_io_box(self.read_box, "green" if self._io_reads > 0 else "black")
        else:
            self._io_writes = max(0, self._io_writes + (1 if active else -1))
            self._set_io_box(self.write_box, "red" if self._io_writes > 0 else "black")
        if self._io_writes > 0:
            self.status_label.setText("Saving to disk…")
        elif self._io_reads > 0:
            self.status_label.setText("Reading from disk…")
        else:
            self.status_label.setText("Ready")

    def _safe_i(self, row, key: str) -> int:
        if not row:
            return 0
        try:
            return int(row[key] or 0)
        except Exception:
            return 0

    def _collect_stats(self) -> dict:
        if not self.db:
            return {}
        files = self._safe_i(self.db.query_one("SELECT COUNT(*) AS c FROM files"), "c")
        folders = self._safe_i(self.db.query_one("SELECT COUNT(*) AS c FROM folders"), "c")
        authors = self._safe_i(self.db.query_one("SELECT COUNT(*) AS c FROM known_authors"), "c")
        dup_found = self.db.query_one(
            "SELECT COUNT(*) AS files, COUNT(DISTINCT work_key) AS groups FROM deletion_queue"
        )
        dup_todo = self.db.query_one(
            "SELECT COUNT(*) AS files, COUNT(DISTINCT work_key) AS groups FROM deletion_queue WHERE checked=1"
        )
        roots = self._safe_i(self.db.query_one("SELECT COUNT(*) AS c FROM roots"), "c")
        roots_on = self._safe_i(self.db.query_one("SELECT COUNT(*) AS c FROM roots WHERE enabled=1"), "c")
        invalid = self._safe_i(self.db.query_one("SELECT COUNT(*) AS c FROM invalid_authors"), "c")

        return {
            "files": files,
            "folders": folders,
            "authors": authors,
            "dup_found_files": self._safe_i(dup_found, "files"),
            "dup_found_groups": self._safe_i(dup_found, "groups"),
            "dup_todo_files": self._safe_i(dup_todo, "files"),
            "dup_todo_groups": self._safe_i(dup_todo, "groups"),
            "roots": roots,
            "roots_enabled": roots_on,
            "invalid_authors": invalid,
        }

    def refresh_all_statuses(self):
        st = self._collect_stats()
        if not st:
            return
        self.project_tab.set_project_status(st)
        self.roots_tab.set_status(st)
        self.scan_tab.set_status(st)
        self.analyze_tab.set_status(st)
        self.review_tab.set_status(st)
        self.authors_tab.set_status(st)

    def on_project_opened(self, db: DB):
        if self.db:
            try:
                self.db.remove_io_callback(self._io_callback)
                self.db.close()
            except Exception:
                pass
        self.db = db
        self.db.add_io_callback(self._io_callback)

        self.tabs.setTabEnabled(1, True)
        self.roots_tab.refresh()

        scan_done = (self.db.get_state("scan_completed", "0") == "1")
        analyze_done = (self.db.get_state("analyze_duplicates_completed", self.db.get_state("analyze_completed", "0")) == "1")
        authors_done = (self.db.get_state("analyze_authors_completed", "0") == "1")

        self.tabs.setTabEnabled(2, True)
        self.tabs.setTabEnabled(3, scan_done)
        self.tabs.setTabEnabled(4, analyze_done)
        has_author_db = bool(self.db.query_one("SELECT 1 FROM known_authors LIMIT 1") or self.db.query_one("SELECT 1 FROM invalid_authors LIMIT 1"))
        self.tabs.setTabEnabled(5, (authors_done or analyze_done) and has_author_db)

        self.scan_tab.refresh()
        self.analyze_tab.refresh()
        self.review_tab.refresh()
        self.authors_tab.refresh()
        self.refresh_all_statuses()

        self.tabs.setCurrentIndex(1)

    def on_scan_completed(self, ok: bool):
        if not self.db:
            return
        if ok:
            self.tabs.setTabEnabled(3, True)
            self.analyze_tab.refresh()
            self.tabs.setCurrentIndex(3)
        else:
            self.tabs.setTabEnabled(3, False)
        self.refresh_all_statuses()

    def on_analyze_completed(self, ok: bool):
        if not self.db:
            return
        if ok:
            self.tabs.setTabEnabled(4, True)
            self.review_tab.refresh()
            self.authors_tab.refresh()
            has_author_db = bool(self.db.query_one("SELECT 1 FROM known_authors LIMIT 1") or self.db.query_one("SELECT 1 FROM invalid_authors LIMIT 1"))
            self.tabs.setTabEnabled(5, has_author_db)
            self.tabs.setCurrentIndex(4)
        else:
            self.tabs.setTabEnabled(4, False)
            self.tabs.setTabEnabled(5, False)
        self.refresh_all_statuses()

    def closeEvent(self, event: QCloseEvent):
        # Ensure processing is safely stopped so resumable checkpoints are persisted.
        try:
            if self.scan_tab.thread and self.scan_tab.worker:
                self.status_label.setText("Stopping scan and saving checkpoint…")
                self.scan_tab.worker.request_stop()
                self.scan_tab.thread.quit()
                self.scan_tab.thread.wait(30000)
            if self.analyze_tab.thread and self.analyze_tab.worker:
                self.status_label.setText("Stopping analyze and saving progress…")
                self.analyze_tab.worker.request_stop()
                self.analyze_tab.thread.quit()
                self.analyze_tab.thread.wait(30000)
            if self.db:
                self.status_label.setText("Final save to disk…")
                self.db.commit()
        except Exception:
            pass
        finally:
            if self.db:
                try:
                    self.db.close()
                except Exception:
                    pass
            super().closeEvent(event)
