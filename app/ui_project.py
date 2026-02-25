from __future__ import annotations
import os
import json
from PySide6.QtCore import QObject, QThread, Signal
from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QPushButton, QLabel,
    QFileDialog, QLineEdit, QMessageBox, QCheckBox, QComboBox
)
from .db import DB, MEMORY_PROFILES
from .author_db import AuthorDB
from .sevenzip import detect_7z


class _OpenProjectWorker(QObject):
    progress = Signal(int, str)
    finished = Signal(object, object, object, object)  # db, author_db, detected_7z, error

    def __init__(self, path: str, author_db_path: str):
        super().__init__()
        self.path = path
        self.author_db_path = author_db_path

    def run(self):
        try:
            self.progress.emit(5, "Preparing project path")
            parent = os.path.dirname(self.path)
            if parent:
                os.makedirs(parent, exist_ok=True)

            self.progress.emit(30, "Opening project database")
            db = DB(self.path)

            self.progress.emit(55, "Loading project settings")
            profile = db.memory_profile()
            db.apply_memory_profile(profile, db.memory_profile_config(profile))

            author_db = None
            if self.author_db_path:
                self.progress.emit(70, "Opening author database")
                try:
                    adb_parent = os.path.dirname(self.author_db_path)
                    if adb_parent:
                        os.makedirs(adb_parent, exist_ok=True)
                    author_db = AuthorDB(self.author_db_path)
                except Exception as e:
                    # Non-fatal: project works without author DB
                    author_db = None

            self.progress.emit(85, "Detecting 7z")
            existing = db.get_state("7z_path", None)
            p = detect_7z(existing)
            self.progress.emit(100, "Project open complete")
            self.finished.emit(db, author_db, p, None)
        except Exception as e:
            self.finished.emit(None, None, None, e)


class ProjectTab(QWidget):
    def __init__(self, on_project_opened, on_activity_progress=None, on_author_db_opened=None):
        super().__init__()
        self.on_project_opened = on_project_opened
        self.on_activity_progress = on_activity_progress
        self.on_author_db_opened = on_author_db_opened

        lay = QVBoxLayout(self)
        self.info = QLabel("Open or create a project (books.sqlite) and link an author DB (authors.sqlite).")
        lay.addWidget(self.info)

        # Project DB row
        lay.addWidget(QLabel("Project DB (books):"))
        prow = QHBoxLayout()
        self.path_edit = QLineEdit()
        self.path_edit.setPlaceholderText(r"C:\path\to\myproject.sqlite")
        prow.addWidget(self.path_edit)
        self.btn_browse = QPushButton("Browse…")
        self.btn_browse.clicked.connect(self.browse_open)
        prow.addWidget(self.btn_browse)
        self.btn_open = QPushButton("Open")
        self.btn_open.clicked.connect(self.open_project)
        prow.addWidget(self.btn_open)
        self.btn_new = QPushButton("New…")
        self.btn_new.clicked.connect(self.new_project)
        prow.addWidget(self.btn_new)
        lay.addLayout(prow)

        # Author DB row
        lay.addWidget(QLabel("Author DB (shared, optional):"))
        arow = QHBoxLayout()
        self.author_path_edit = QLineEdit()
        self.author_path_edit.setPlaceholderText(r"C:\path\to\authors.sqlite  (leave blank to skip)")
        arow.addWidget(self.author_path_edit)
        self.btn_author_browse = QPushButton("Browse…")
        self.btn_author_browse.clicked.connect(self.browse_author)
        arow.addWidget(self.btn_author_browse)
        self.btn_author_new = QPushButton("New…")
        self.btn_author_new.clicked.connect(self.new_author_db)
        arow.addWidget(self.btn_author_new)
        self.btn_author_standalone = QPushButton("Open Standalone")
        self.btn_author_standalone.setToolTip(
            "Open just the Author DB without a project DB — enables the Authors tab immediately")
        self.btn_author_standalone.clicked.connect(self.open_author_only)
        arow.addWidget(self.btn_author_standalone)
        lay.addLayout(arow)

        lay.addWidget(QLabel("Settings stored in project DB state:"))

        self.chk_folder_skip = QCheckBox("Enable optional folder skipping (heuristic)")
        self.chk_folder_skip.stateChanged.connect(self.save_settings_if_open)
        lay.addWidget(self.chk_folder_skip)

        self.chk_backup_before_analyze = QCheckBox("Create DB backup before each Analyze phase")
        self.chk_backup_before_analyze.setChecked(True)
        self.chk_backup_before_analyze.stateChanged.connect(self.save_settings_if_open)
        lay.addWidget(self.chk_backup_before_analyze)

        self.chk_backup_before_scan = QCheckBox("Create DB backup before Scan phase")
        self.chk_backup_before_scan.stateChanged.connect(self.save_settings_if_open)
        lay.addWidget(self.chk_backup_before_scan)

        mrow = QHBoxLayout()
        mrow.addWidget(QLabel("Memory profile:"))
        self.cmb_memory_profile = QComboBox()
        self.cmb_memory_profile.addItems(["safe", "balanced", "extreme", "extreme+", "custom"])
        self.cmb_memory_profile.currentTextChanged.connect(self.on_memory_profile_changed)
        mrow.addWidget(self.cmb_memory_profile)
        lay.addLayout(mrow)

        crow = QHBoxLayout()
        self.custom_sync = QComboBox()
        self.custom_sync.addItems(["OFF", "NORMAL", "FULL", "EXTRA"])
        self.custom_sync.currentTextChanged.connect(self.save_settings_if_open)
        crow.addWidget(QLabel("custom synchronous"))
        crow.addWidget(self.custom_sync)

        self.custom_cache = QLineEdit()
        self.custom_cache.setPlaceholderText("cache_size (e.g. -524288)")
        self.custom_cache.editingFinished.connect(self.save_settings_if_open)
        crow.addWidget(self.custom_cache)

        self.custom_mmap = QLineEdit()
        self.custom_mmap.setPlaceholderText("mmap_size")
        self.custom_mmap.editingFinished.connect(self.save_settings_if_open)
        crow.addWidget(self.custom_mmap)

        self.custom_wal = QLineEdit()
        self.custom_wal.setPlaceholderText("wal_autocheckpoint")
        self.custom_wal.editingFinished.connect(self.save_settings_if_open)
        crow.addWidget(self.custom_wal)
        lay.addLayout(crow)

        prow2 = QHBoxLayout()
        self.btn_import_profile = QPushButton("Import preset → custom")
        self.btn_import_profile.clicked.connect(self.import_preset_to_custom)
        prow2.addWidget(self.btn_import_profile)

        self.btn_save_profile = QPushButton("Save custom profile…")
        self.btn_save_profile.clicked.connect(self.save_custom_profile_to_disk)
        prow2.addWidget(self.btn_save_profile)

        self.btn_load_profile = QPushButton("Load custom profile…")
        self.btn_load_profile.clicked.connect(self.load_custom_profile_from_disk)
        prow2.addWidget(self.btn_load_profile)
        lay.addLayout(prow2)

        zrow = QHBoxLayout()
        self.zlabel = QLabel("7z.exe: (auto-detect after open)")
        zrow.addWidget(self.zlabel)
        btn_find7z = QPushButton("Re-detect 7z")
        btn_find7z.clicked.connect(self.redetect_7z)
        zrow.addWidget(btn_find7z)
        lay.addLayout(zrow)

        lay.addStretch(1)

        self._db: DB | None = None
        self._open_thread: QThread | None = None
        self._open_worker: _OpenProjectWorker | None = None

    def _emit_activity(self, text: str, pct: float = -1.0):
        if self.on_activity_progress:
            self.on_activity_progress(text, pct)

    def _set_open_controls(self, enabled: bool):
        self.btn_open.setEnabled(enabled)
        self.btn_new.setEnabled(enabled)
        self.btn_browse.setEnabled(enabled)
        self.btn_author_browse.setEnabled(enabled)
        self.btn_author_new.setEnabled(enabled)
        self.btn_author_standalone.setEnabled(enabled)

    def _read_custom_cfg_from_ui(self) -> dict:
        def _ival(edit: QLineEdit, dflt: int) -> int:
            try:
                return int((edit.text() or "").strip())
            except Exception:
                return dflt
        return {
            "synchronous": self.custom_sync.currentText().strip().upper() or "NORMAL",
            "cache_size": _ival(self.custom_cache, MEMORY_PROFILES["balanced"]["cache_size"]),
            "mmap_size": _ival(self.custom_mmap, MEMORY_PROFILES["balanced"]["mmap_size"]),
            "wal_autocheckpoint": _ival(self.custom_wal, MEMORY_PROFILES["balanced"]["wal_autocheckpoint"]),
        }

    def _set_custom_cfg_to_ui(self, cfg: dict):
        self.custom_sync.setCurrentText(str(cfg.get("synchronous", "NORMAL")).upper())
        self.custom_cache.setText(str(int(cfg.get("cache_size", MEMORY_PROFILES["balanced"]["cache_size"]))))
        self.custom_mmap.setText(str(int(cfg.get("mmap_size", MEMORY_PROFILES["balanced"]["mmap_size"]))))
        self.custom_wal.setText(str(int(cfg.get("wal_autocheckpoint", MEMORY_PROFILES["balanced"]["wal_autocheckpoint"]))))

    def _refresh_custom_controls_enabled(self):
        on = (self.cmb_memory_profile.currentText().strip().lower() == "custom")
        for w in (self.custom_sync, self.custom_cache, self.custom_mmap, self.custom_wal,
                  self.btn_import_profile, self.btn_save_profile, self.btn_load_profile):
            w.setEnabled(on)

    def browse_open(self):
        fn, _ = QFileDialog.getOpenFileName(self, "Open project DB", "", "SQLite DB (*.sqlite *.db);;All files (*.*)")
        if fn:
            self.path_edit.setText(fn)
            # Auto-suggest authors.sqlite alongside the project
            if not self.author_path_edit.text().strip():
                suggested = os.path.join(os.path.dirname(fn), "authors.sqlite")
                self.author_path_edit.setText(suggested)

    def browse_author(self):
        fn, _ = QFileDialog.getOpenFileName(self, "Open author DB", "", "SQLite DB (*.sqlite *.db);;All files (*.*)")
        if fn:
            self.author_path_edit.setText(fn)

    def new_project(self):
        fn, _ = QFileDialog.getSaveFileName(self, "Create project DB", "", "SQLite DB (*.sqlite)")
        if not fn:
            return
        if not fn.lower().endswith(".sqlite"):
            fn += ".sqlite"
        self.path_edit.setText(fn)
        if not self.author_path_edit.text().strip():
            suggested = os.path.join(os.path.dirname(fn), "authors.sqlite")
            self.author_path_edit.setText(suggested)
        self.open_project(create=True)

    def new_author_db(self):
        fn, _ = QFileDialog.getSaveFileName(self, "Create author DB", "", "SQLite DB (*.sqlite)")
        if not fn:
            return
        if not fn.lower().endswith(".sqlite"):
            fn += ".sqlite"
        self.author_path_edit.setText(fn)

    def open_author_only(self):
        """Open just the author DB (no project DB required) and notify MainWindow."""
        path = self.author_path_edit.text().strip()
        if not path:
            fn, _ = QFileDialog.getOpenFileName(
                self, "Open author DB", "", "SQLite DB (*.sqlite *.db);;All files (*.*)")
            if not fn:
                return
            self.author_path_edit.setText(fn)
            path = fn
        try:
            parent = os.path.dirname(path)
            if parent:
                os.makedirs(parent, exist_ok=True)
            author_db = AuthorDB(path)
        except Exception as e:
            QMessageBox.critical(self, "Author DB", f"Failed to open:\n{e!r}")
            return
        self.info.setText(f"Author DB opened standalone: {os.path.basename(path)}")
        if self.on_author_db_opened:
            self.on_author_db_opened(author_db)

    def open_project(self, create: bool = False):
        _ = create
        path = self.path_edit.text().strip()
        if not path:
            QMessageBox.warning(self, "Project", "Choose a project DB file.")
            return
        if self._open_thread:
            QMessageBox.information(self, "Project", "Project open already in progress.")
            return

        author_db_path = self.author_path_edit.text().strip()

        self._set_open_controls(False)
        self.info.setText("Opening project…")
        self._emit_activity("Opening DB", 0.0)

        self._open_thread = QThread()
        self._open_worker = _OpenProjectWorker(path, author_db_path)
        self._open_worker.moveToThread(self._open_thread)
        self._open_thread.started.connect(self._open_worker.run)
        self._open_worker.progress.connect(self._on_open_progress)
        self._open_worker.finished.connect(self._on_open_finished)
        self._open_thread.start()

    def _on_open_progress(self, pct: int, msg: str):
        self.info.setText(msg)
        self._emit_activity(msg, float(pct))

    def _on_open_finished(self, db_obj, author_db_obj, detected_7z, err):
        if self._open_thread:
            self._open_thread.quit()
            self._open_thread.wait(3000)
        self._open_thread = None
        self._open_worker = None
        self._set_open_controls(True)

        if err is not None or db_obj is None:
            self._emit_activity("Idle", -1.0)
            QMessageBox.critical(self, "Project", f"Failed to open DB:\n{err!r}")
            self.info.setText("Open or create a project.")
            return

        db = db_obj
        self._db = db

        # Persist the author DB path in the project state so it reloads next time
        if author_db_obj is not None:
            db.set_state("author_db_path", author_db_obj.db_path)
        elif self.author_path_edit.text().strip():
            db.set_state("author_db_path", self.author_path_edit.text().strip())

        self.chk_folder_skip.setChecked(db.get_state("folder_skip_enabled", "0") == "1")

        profile = db.memory_profile()
        cfg = db.memory_profile_config(profile)
        self._set_custom_cfg_to_ui(cfg)
        self.cmb_memory_profile.setCurrentText(profile)
        db.apply_memory_profile(profile, cfg if profile == "custom" else None)

        self.chk_backup_before_analyze.setChecked(db.get_state("backup_before_analyze", "1") == "1")
        self.chk_backup_before_scan.setChecked(db.get_state("backup_before_scan", "0") == "1")

        if detected_7z:
            db.set_state("7z_path", detected_7z)
            self.zlabel.setText(f"7z.exe: {detected_7z}")
        else:
            self.zlabel.setText("7z.exe: NOT FOUND (install 7-Zip or put 7z.exe in PATH)")

        self._refresh_custom_controls_enabled()
        self.on_project_opened(db, author_db_obj)
        status = "Project opened."
        if author_db_obj is None and self.author_path_edit.text().strip():
            status += " (Author DB could not be opened — check path.)"
        elif author_db_obj is None:
            status += " (No author DB configured.)"
        self.info.setText(status)
        self._emit_activity("Idle", -1.0)

    def on_memory_profile_changed(self, _text: str):
        self._refresh_custom_controls_enabled()
        self.save_settings_if_open()

    def import_preset_to_custom(self):
        p = self.cmb_memory_profile.currentText().strip().lower()
        if p == "custom":
            p = "balanced"
        cfg = MEMORY_PROFILES.get(p, MEMORY_PROFILES["balanced"])
        self._set_custom_cfg_to_ui(cfg)
        self.cmb_memory_profile.setCurrentText("custom")
        self.save_settings_if_open()

    def save_custom_profile_to_disk(self):
        fn, _ = QFileDialog.getSaveFileName(self, "Save custom memory profile", "", "JSON (*.json)")
        if not fn:
            return
        if not fn.lower().endswith('.json'):
            fn += '.json'
        cfg = self._read_custom_cfg_from_ui()
        with open(fn, 'w', encoding='utf-8') as f:
            json.dump(cfg, f, ensure_ascii=False, indent=2)

    def load_custom_profile_from_disk(self):
        fn, _ = QFileDialog.getOpenFileName(self, "Load custom memory profile", "", "JSON (*.json)")
        if not fn:
            return
        try:
            with open(fn, 'r', encoding='utf-8') as f:
                cfg = json.load(f)
            if not isinstance(cfg, dict):
                raise ValueError("Profile JSON must be an object")
        except Exception as e:
            QMessageBox.warning(self, "Memory profile", f"Failed to load profile: {e!r}")
            return
        self._set_custom_cfg_to_ui(cfg)
        self.cmb_memory_profile.setCurrentText("custom")
        self.save_settings_if_open()

    def save_settings_if_open(self):
        if not self._db:
            return
        self._db.set_state("folder_skip_enabled", "1" if self.chk_folder_skip.isChecked() else "0")
        self._db.set_state("backup_before_analyze", "1" if self.chk_backup_before_analyze.isChecked() else "0")
        self._db.set_state("backup_before_scan", "1" if self.chk_backup_before_scan.isChecked() else "0")
        profile = self.cmb_memory_profile.currentText().strip().lower()
        if profile == "custom":
            self._db.apply_memory_profile("custom", self._read_custom_cfg_from_ui())
        else:
            self._db.apply_memory_profile(profile)

    def redetect_7z(self):
        if not self._db:
            QMessageBox.information(self, "7z", "Open a project first.")
            return
        existing = self._db.get_state("7z_path", None)
        p = detect_7z(existing)
        if p:
            self._db.set_state("7z_path", p)
            self.zlabel.setText(f"7z.exe: {p}")
        else:
            self.zlabel.setText("7z.exe: NOT FOUND (install 7-Zip or put 7z.exe in PATH)")
