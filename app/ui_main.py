from __future__ import annotations
from PySide6.QtWidgets import QMainWindow, QWidget, QVBoxLayout, QTabWidget
from .db import DB
from .ui_project import ProjectTab
from .ui_roots import RootsTab
from .ui_scan import ScanTab
from .ui_analyze import AnalyzeTab
from .ui_review import ReviewTab
from .ui_authors import AuthorsTab

class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("BookDeduper (Project DB)")

        self.db: DB | None = None

        self.tabs = QTabWidget()
        self.tabs.setTabsClosable(False)

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

        self.resize(1200, 800)

    def on_project_opened(self, db: DB):
        if self.db:
            try:
                self.db.close()
            except Exception:
                pass
        self.db = db

        self.tabs.setTabEnabled(1, True)
        self.roots_tab.refresh()

        scan_done = (self.db.get_state("scan_completed", "0") == "1")
        analyze_done = (self.db.get_state("analyze_completed", "0") == "1")

        self.tabs.setTabEnabled(2, True)
        self.tabs.setTabEnabled(3, scan_done)
        self.tabs.setTabEnabled(4, analyze_done)
        has_author_db = bool(self.db.query_one("SELECT 1 FROM known_authors LIMIT 1") or self.db.query_one("SELECT 1 FROM invalid_authors LIMIT 1"))
        self.tabs.setTabEnabled(5, analyze_done and has_author_db)

        self.scan_tab.refresh()
        self.analyze_tab.refresh()
        self.review_tab.refresh()
        self.authors_tab.refresh()

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
