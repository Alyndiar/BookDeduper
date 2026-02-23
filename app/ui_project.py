from __future__ import annotations
import os
from PySide6.QtCore import QObject, QThread, Signal
from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QPushButton, QLabel,
    QFileDialog, QLineEdit, QMessageBox, QCheckBox, QComboBox
)
from .db import DB
from .sevenzip import detect_7z


class _OpenProjectWorker(QObject):
    progress = Signal(int, str)
    finished = Signal(object, object, object)  # db, detected_7z, error

    def __init__(self, path: str):
        super().__init__()
        self.path = path

    def run(self):
        try:
            self.progress.emit(5, "Preparing project path")
            parent = os.path.dirname(self.path)
            if parent:
                os.makedirs(parent, exist_ok=True)

            self.progress.emit(35, "Opening database")
            db = DB(self.path)

            self.progress.emit(70, "Loading project settings")
            profile = db.memory_profile()
            db.apply_memory_profile(profile)

            self.progress.emit(85, "Detecting 7z")
            existing = db.get_state("7z_path", None)
            p = detect_7z(existing)
            self.progress.emit(100, "Project open complete")
            self.finished.emit(db, p, None)
        except Exception as e:
            self.finished.emit(None, None, e)


class ProjectTab(QWidget):
    def __init__(self, on_project_opened, on_activity_progress=None):
        super().__init__()
        self.on_project_opened = on_project_opened
        self.on_activity_progress = on_activity_progress

        lay = QVBoxLayout(self)

        self.info = QLabel("Open or create a project (single SQLite DB file).")
        lay.addWidget(self.info)

        row = QHBoxLayout()
        self.path_edit = QLineEdit()
        self.path_edit.setPlaceholderText(r"C:\path\to\project.sqlite")
        row.addWidget(self.path_edit)

        self.btn_browse = QPushButton("Browse…")
        self.btn_browse.clicked.connect(self.browse_open)
        row.addWidget(self.btn_browse)

        self.btn_open = QPushButton("Open")
        self.btn_open.clicked.connect(self.open_project)
        row.addWidget(self.btn_open)

        self.btn_new = QPushButton("New…")
        self.btn_new.clicked.connect(self.new_project)
        row.addWidget(self.btn_new)

        lay.addLayout(row)

        lay.addWidget(QLabel("Settings stored in DB state:"))

        srow = QHBoxLayout()
        self.chk_folder_skip = QCheckBox("Enable optional folder skipping (heuristic)")
        self.chk_folder_skip.setChecked(False)
        self.chk_folder_skip.stateChanged.connect(self.save_settings_if_open)
        srow.addWidget(self.chk_folder_skip)
        lay.addLayout(srow)

        brow = QHBoxLayout()
        self.chk_backup_before_analyze = QCheckBox("Create DB backup before each Analyze phase")
        self.chk_backup_before_analyze.setChecked(True)
        self.chk_backup_before_analyze.stateChanged.connect(self.save_settings_if_open)
        brow.addWidget(self.chk_backup_before_analyze)
        lay.addLayout(brow)

        sbrow = QHBoxLayout()
        self.chk_backup_before_scan = QCheckBox("Create DB backup before Scan phase")
        self.chk_backup_before_scan.setChecked(False)
        self.chk_backup_before_scan.stateChanged.connect(self.save_settings_if_open)
        sbrow.addWidget(self.chk_backup_before_scan)
        lay.addLayout(sbrow)

        mrow = QHBoxLayout()
        mrow.addWidget(QLabel("Memory profile:"))
        self.cmb_memory_profile = QComboBox()
        self.cmb_memory_profile.addItems(["safe", "balanced", "extreme", "extreme+"])
        self.cmb_memory_profile.currentTextChanged.connect(self.save_settings_if_open)
        mrow.addWidget(self.cmb_memory_profile)
        lay.addLayout(mrow)

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

    def browse_open(self):
        fn, _ = QFileDialog.getOpenFileName(self, "Open project DB", "", "SQLite DB (*.sqlite *.db);;All files (*.*)")
        if fn:
            self.path_edit.setText(fn)

    def new_project(self):
        fn, _ = QFileDialog.getSaveFileName(self, "Create project DB", "", "SQLite DB (*.sqlite)")
        if not fn:
            return
        if not fn.lower().endswith(".sqlite"):
            fn += ".sqlite"
        self.path_edit.setText(fn)
        self.open_project(create=True)

    def open_project(self, create: bool = False):
        _ = create
        path = self.path_edit.text().strip()
        if not path:
            QMessageBox.warning(self, "Project", "Choose a project DB file.")
            return
        if self._open_thread:
            QMessageBox.information(self, "Project", "Project open already in progress.")
            return

        self._set_open_controls(False)
        self.info.setText("Opening project…")
        self._emit_activity("Opening DB", 0.0)

        self._open_thread = QThread()
        self._open_worker = _OpenProjectWorker(path)
        self._open_worker.moveToThread(self._open_thread)
        self._open_thread.started.connect(self._open_worker.run)
        self._open_worker.progress.connect(self._on_open_progress)
        self._open_worker.finished.connect(self._on_open_finished)
        self._open_thread.start()

    def _on_open_progress(self, pct: int, msg: str):
        self.info.setText(msg)
        self._emit_activity(msg, float(pct))

    def _on_open_finished(self, db_obj, detected_7z, err):
        if self._open_thread:
            self._open_thread.quit()
            self._open_thread.wait(3000)
        self._open_thread = None
        self._open_worker = None
        self._set_open_controls(True)

        if err is not None or db_obj is None:
            self._emit_activity("Idle", -1.0)
            QMessageBox.critical(self, "Project", f"Failed to open DB:\n{err!r}")
            self.info.setText("Open or create a project (single SQLite DB file).")
            return

        db = db_obj
        self._db = db

        folder_skip = (db.get_state("folder_skip_enabled", "0") == "1")
        self.chk_folder_skip.setChecked(folder_skip)

        profile = db.memory_profile()
        self.cmb_memory_profile.setCurrentText(profile)
        db.apply_memory_profile(profile)

        backup_before = (db.get_state("backup_before_analyze", "1") == "1")
        self.chk_backup_before_analyze.setChecked(backup_before)
        backup_before_scan = (db.get_state("backup_before_scan", "0") == "1")
        self.chk_backup_before_scan.setChecked(backup_before_scan)

        if detected_7z:
            db.set_state("7z_path", detected_7z)
            self.zlabel.setText(f"7z.exe: {detected_7z}")
        else:
            self.zlabel.setText("7z.exe: NOT FOUND (install 7-Zip or put 7z.exe in PATH)")

        self.on_project_opened(db)
        self.info.setText("Project opened.")
        self._emit_activity("Idle", -1.0)

    def save_settings_if_open(self):
        if not self._db:
            return
        self._db.set_state("folder_skip_enabled", "1" if self.chk_folder_skip.isChecked() else "0")
        self._db.set_state("backup_before_analyze", "1" if self.chk_backup_before_analyze.isChecked() else "0")
        self._db.set_state("backup_before_scan", "1" if self.chk_backup_before_scan.isChecked() else "0")
        self._db.apply_memory_profile(self.cmb_memory_profile.currentText())

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
