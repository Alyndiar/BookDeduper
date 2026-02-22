from __future__ import annotations
import os
from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QPushButton, QLabel,
    QFileDialog, QLineEdit, QMessageBox, QCheckBox
)
from .db import DB
from .sevenzip import detect_7z

class ProjectTab(QWidget):
    def __init__(self, on_project_opened):
        super().__init__()
        self.on_project_opened = on_project_opened

        lay = QVBoxLayout(self)

        self.info = QLabel("Open or create a project (single SQLite DB file).")
        lay.addWidget(self.info)

        row = QHBoxLayout()
        self.path_edit = QLineEdit()
        self.path_edit.setPlaceholderText(r"C:\path\to\project.sqlite")
        row.addWidget(self.path_edit)

        btn_browse = QPushButton("Browse…")
        btn_browse.clicked.connect(self.browse_open)
        row.addWidget(btn_browse)

        btn_open = QPushButton("Open")
        btn_open.clicked.connect(self.open_project)
        row.addWidget(btn_open)

        btn_new = QPushButton("New…")
        btn_new.clicked.connect(self.new_project)
        row.addWidget(btn_new)

        lay.addLayout(row)

        lay.addWidget(QLabel("Settings stored in DB state:"))

        srow = QHBoxLayout()
        self.chk_folder_skip = QCheckBox("Enable optional folder skipping (heuristic)")
        self.chk_folder_skip.setChecked(False)
        self.chk_folder_skip.stateChanged.connect(self.save_settings_if_open)
        srow.addWidget(self.chk_folder_skip)
        lay.addLayout(srow)

        zrow = QHBoxLayout()
        self.zlabel = QLabel("7z.exe: (auto-detect after open)")
        zrow.addWidget(self.zlabel)
        btn_find7z = QPushButton("Re-detect 7z")
        btn_find7z.clicked.connect(self.redetect_7z)
        zrow.addWidget(btn_find7z)
        lay.addLayout(zrow)

        lay.addStretch(1)

        self._db: DB | None = None

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
        path = self.path_edit.text().strip()
        if not path:
            QMessageBox.warning(self, "Project", "Choose a project DB file.")
            return

        parent = os.path.dirname(path)
        if parent:
            os.makedirs(parent, exist_ok=True)
        try:
            db = DB(path)
        except Exception as e:
            QMessageBox.critical(self, "Project", f"Failed to open DB:\n{e!r}")
            return

        self._db = db

        folder_skip = (db.get_state("folder_skip_enabled", "0") == "1")
        self.chk_folder_skip.setChecked(folder_skip)

        existing = db.get_state("7z_path", None)
        p = detect_7z(existing)
        if p:
            db.set_state("7z_path", p)
            self.zlabel.setText(f"7z.exe: {p}")
        else:
            self.zlabel.setText("7z.exe: NOT FOUND (install 7-Zip or put 7z.exe in PATH)")

        self.on_project_opened(db)

    def save_settings_if_open(self):
        if not self._db:
            return
        self._db.set_state("folder_skip_enabled", "1" if self.chk_folder_skip.isChecked() else "0")

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
