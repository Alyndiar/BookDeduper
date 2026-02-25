from __future__ import annotations

from PySide6.QtCore import Signal
from PySide6.QtGui import QCloseEvent, QFontDatabase, QFontMetrics
from PySide6.QtWidgets import QMainWindow, QWidget, QVBoxLayout, QTabWidget, QLabel, QStatusBar, QProgressBar

from .db import DB
from .author_db import AuthorDB
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
        self.author_db: AuthorDB | None = None
        self._io_reads = 0
        self._io_writes = 0
        self._last_stats = {
            "folders": 0, "files": 0, "authors": 0,
            "dup_found_groups": 0, "dup_found_files": 0,
            "dup_todo_groups": 0, "dup_todo_files": 0,
        }
        self._refreshing_status = False

        self.tabs = QTabWidget()
        self.tabs.setTabsClosable(False)
        self.project_tab = ProjectTab(on_project_opened=self.on_project_opened, on_activity_progress=self.on_activity_progress, on_author_db_opened=self.on_author_db_opened_standalone)
        self.roots_tab = RootsTab(get_db=lambda: self.db)
        self.scan_tab = ScanTab(get_db=lambda: self.db, get_author_db=lambda: self.author_db, on_scan_completed=self.on_scan_completed, on_activity_progress=self.on_activity_progress)
        self.analyze_tab = AnalyzeTab(get_db=lambda: self.db, get_author_db=lambda: self.author_db, on_analyze_completed=self.on_analyze_completed, on_activity_progress=self.on_activity_progress)
        self.review_tab = ReviewTab(get_db=lambda: self.db, get_author_db=lambda: self.author_db, on_activity_progress=self.on_activity_progress)
        self.authors_tab = AuthorsTab(get_db=lambda: self.db, get_author_db=lambda: self.author_db, on_activity_progress=self.on_activity_progress)

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
        self._build_global_status_bar()

        self.io_signal.connect(self._on_io_signal)

        self.resize(max(1200, self._status_required_width + 80), 800)

    def _build_global_status_bar(self):
        mono = QFontDatabase.systemFont(QFontDatabase.FixedFont)
        mono.setPointSize(max(8, mono.pointSize() - 1))
        fm = QFontMetrics(mono)

        self.status_label = self._make_field_label(mono, " Analyze Duplicates ")
        self.dirs_label = self._make_field_label(mono, " Dirs 999999 ")
        self.files_label = self._make_field_label(mono, " Files 9999999 ")
        self.authors_label = self._make_field_label(mono, " Authors 9999999 ")
        self.groups_files_label = self._make_field_label(mono, " Dupes 9999999/9999999 ")
        self.left_label = self._make_field_label(mono, " Left 9999999/9999999 ")

        self._set_field_texts("Idle", 0, 0, 0, 0, 0, 0, 0)

        self._status_required_width = (
            self.status_label.width()
            + self.dirs_label.width()
            + self.files_label.width()
            + self.authors_label.width()
            + self.groups_files_label.width()
            + self.left_label.width()
            + 60
        )

        self.status.addWidget(self.status_label)
        self.status.addWidget(self.dirs_label)
        self.status.addWidget(self.files_label)
        self.status.addWidget(self.authors_label)
        self.status.addWidget(self.groups_files_label)
        self.status.addWidget(self.left_label, 1)

        self.activity_progress = QProgressBar()
        self.activity_progress.setFixedWidth(180)
        self.activity_progress.setRange(0, 100)
        self.activity_progress.setValue(0)
        self.activity_progress.setFormat("")
        self.status.addPermanentWidget(self.activity_progress)

        self.read_box = self._make_io_box("R", mono)
        self.write_box = self._make_io_box("W", mono)
        self.status.addPermanentWidget(self.read_box)
        self.status.addPermanentWidget(self.write_box)
        self._set_io_box(self.read_box, "black")
        self._set_io_box(self.write_box, "black")

    def _make_field_label(self, font, template: str) -> QLabel:
        fm = QFontMetrics(font)
        label = QLabel("")
        label.setFont(font)
        label.setFixedWidth(fm.horizontalAdvance(template) + 10)
        return label

    def _make_io_box(self, label: str, font) -> QLabel:
        box = QLabel(label)
        box.setFont(font)
        box.setFixedWidth(24)
        box.setAutoFillBackground(True)
        box.setStyleSheet("QLabel { color: white; font-weight: bold; border: 1px solid #555; padding: 2px; text-align: center; }")
        return box

    def _set_io_box(self, box: QLabel, color: str):
        box.setStyleSheet(
            f"QLabel {{ background-color: {color}; color: white; font-weight: bold; border: 1px solid #555; padding: 2px; text-align: center; }}"
        )

    def _io_callback(self, operation: str, active: bool):
        self.io_signal.emit(operation, active)

    def _current_mode(self) -> str:
        if self._io_reads > 0 or self._io_writes > 0:
            return "Disk IO"
        if self.scan_tab.thread is not None:
            return "Scanning"
        if self.analyze_tab.thread is not None:
            if self.analyze_tab.current_phase == "authors":
                return "Analyze Authors"
            if self.analyze_tab.current_phase == "authors_seed":
                return "Pre-seed Authors"
            return "Analyze Duplicates"
        idx = self.tabs.currentIndex()
        if idx == 4:
            return "Review"
        return "Idle"

    def _on_tab_changed(self, _idx: int):
        if not hasattr(self, "status_label"):
            return
        self.refresh_all_statuses()

    def _on_io_signal(self, operation: str, active: bool):
        if operation == "read":
            self._io_reads = max(0, self._io_reads + (1 if active else -1))
            self._set_io_box(self.read_box, "green" if self._io_reads > 0 else "black")
        else:
            self._io_writes = max(0, self._io_writes + (1 if active else -1))
            self._set_io_box(self.write_box, "red" if self._io_writes > 0 else "black")

        if (self._io_reads + self._io_writes) > 0 and self.activity_progress.value() == 0:
            self.activity_progress.setRange(0, 0)
        elif (self._io_reads + self._io_writes) == 0 and self.activity_progress.maximum() == 0:
            self.activity_progress.setRange(0, 100)
            self.activity_progress.setValue(0)
            self.activity_progress.setFormat("")

        self._set_field_texts(
            self._current_mode(),
            self._last_stats.get("folders", 0),
            self._last_stats.get("files", 0),
            self._last_stats.get("authors", 0),
            self._last_stats.get("dup_found_groups", 0),
            self._last_stats.get("dup_found_files", 0),
            self._last_stats.get("dup_todo_groups", 0),
            self._last_stats.get("dup_todo_files", 0),
        )

    def _safe_i(self, row, key: str) -> int:
        if not row:
            return 0
        try:
            return int(row[key] or 0)
        except Exception:
            return 0

    def _collect_stats(self) -> dict:
        if not self.db and not self.author_db:
            return {}
        files = 0
        folders = 0
        dup_found = None
        dup_todo = None
        if self.db:
            files = self._safe_i(self.db.query_one("SELECT COUNT(*) AS c FROM files"), "c")
            folders = self._safe_i(self.db.query_one("SELECT COUNT(*) AS c FROM folders"), "c")
            dup_found = self.db.query_one(
                "SELECT COUNT(*) AS files, COUNT(DISTINCT work_key) AS groups FROM deletion_queue"
            )
            dup_todo = self.db.query_one(
                "SELECT COUNT(*) AS files, COUNT(DISTINCT work_key) AS groups FROM deletion_queue WHERE checked=1"
            )
        authors = 0
        if self.author_db:
            authors = self._safe_i(self.author_db.query_one("SELECT COUNT(*) AS c FROM known_authors"), "c")
        return {
            "files": files,
            "folders": folders,
            "authors": authors,
            "dup_found_files": self._safe_i(dup_found, "files"),
            "dup_found_groups": self._safe_i(dup_found, "groups"),
            "dup_todo_files": self._safe_i(dup_todo, "files"),
            "dup_todo_groups": self._safe_i(dup_todo, "groups"),
        }

    def _set_field_texts(self, mode: str, dirs: int, files: int, authors: int, found_groups: int, found_files: int, left_groups: int, left_files: int):
        self.status_label.setText(f" {mode} ")
        self.dirs_label.setText(f" Dirs {dirs} ")
        self.files_label.setText(f" Files {files} ")
        self.authors_label.setText(f" Authors {authors} ")
        self.groups_files_label.setText(f" Dupes {found_groups}/{found_files} ")
        self.left_label.setText(f" Left {left_groups}/{left_files} ")

    def refresh_all_statuses(self):
        if self._refreshing_status:
            return
        self._refreshing_status = True
        try:
            st = self._collect_stats()
            if not st:
                self._last_stats = {
                    "folders": 0, "files": 0, "authors": 0,
                    "dup_found_groups": 0, "dup_found_files": 0,
                    "dup_todo_groups": 0, "dup_todo_files": 0,
                }
                self._set_field_texts(self._current_mode(), 0, 0, 0, 0, 0, 0, 0)
                return
            self._last_stats = st
            self._set_field_texts(
                self._current_mode(),
                st.get("folders", 0),
                st.get("files", 0),
                st.get("authors", 0),
                st.get("dup_found_groups", 0),
                st.get("dup_found_files", 0),
                st.get("dup_todo_groups", 0),
                st.get("dup_todo_groups", 0),
            )
        finally:
            self._refreshing_status = False

    def on_activity_progress(self, text: str, pct: float = -1.0):
        if pct < 0:
            self.activity_progress.setRange(0, 100)
            self.activity_progress.setValue(0)
            self.activity_progress.setFormat("")
            return
        if pct > 100:
            pct = 100
        self.status_label.setText(f" {text} ")
        self.activity_progress.setRange(0, 100)
        self.activity_progress.setValue(int(pct))
        self.activity_progress.setFormat(f"{pct:.0f}%")

    def on_project_opened(self, db: DB, author_db: AuthorDB | None):
        if self.db:
            try:
                self.db.remove_io_callback(self._io_callback)
                self.db.close()
            except Exception:
                pass
        if self.author_db:
            try:
                self.author_db.remove_io_callback(self._io_callback)
                self.author_db.close()
            except Exception:
                pass

        self.db = db
        self.author_db = author_db
        self.db.add_io_callback(self._io_callback)
        if self.author_db:
            self.author_db.add_io_callback(self._io_callback)

        self.tabs.setTabEnabled(1, True)
        self.roots_tab.refresh()

        scan_done = (self.db.get_state("scan_completed", "0") == "1")
        analyze_done = (self.db.get_state("analyze_duplicates_completed", self.db.get_state("analyze_completed", "0")) == "1")
        authors_done = (self.db.get_state("analyze_authors_completed", "0") == "1")

        self.tabs.setTabEnabled(2, True)
        self.tabs.setTabEnabled(3, scan_done)
        self.tabs.setTabEnabled(4, analyze_done)
        has_author_db = self.author_db is not None
        self.tabs.setTabEnabled(5, has_author_db or authors_done or analyze_done)

        self.scan_tab.refresh()
        self.analyze_tab.refresh()
        self.review_tab.refresh()
        self.authors_tab.refresh()
        self.refresh_all_statuses()

        self.tabs.setCurrentIndex(1)

    def on_author_db_opened_standalone(self, author_db: AuthorDB):
        """Called when the user opens just an author DB without a project DB."""
        if self.author_db and self.author_db is not author_db:
            try:
                self.author_db.remove_io_callback(self._io_callback)
                self.author_db.close()
            except Exception:
                pass
        self.author_db = author_db
        self.author_db.add_io_callback(self._io_callback)
        self.tabs.setTabEnabled(5, True)
        self.authors_tab.refresh()
        self.tabs.setCurrentIndex(5)
        self.refresh_all_statuses()

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
            self.tabs.setTabEnabled(5, True)
            self.tabs.setCurrentIndex(4)
        else:
            self.tabs.setTabEnabled(4, False)
        self.refresh_all_statuses()

    def closeEvent(self, event: QCloseEvent):
        try:
            if self.scan_tab.thread and self.scan_tab.worker:
                self.scan_tab.worker.request_stop()
                self.scan_tab.thread.quit()
                self.scan_tab.thread.wait(30000)
            if self.analyze_tab.thread and self.analyze_tab.worker:
                self.analyze_tab.worker.request_stop()
                self.analyze_tab.thread.quit()
                self.analyze_tab.thread.wait(30000)
            if self.db:
                self.db.commit()
        except Exception:
            pass
        finally:
            if self.db:
                try:
                    self.db.close()
                except Exception:
                    pass
            if self.author_db:
                try:
                    self.author_db.close()
                except Exception:
                    pass
            super().closeEvent(event)
