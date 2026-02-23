from __future__ import annotations
from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QPushButton, QListWidget, QListWidgetItem,
    QFileDialog, QMessageBox, QLabel
)


class RootsTab(QWidget):
    def __init__(self, get_db):
        super().__init__()
        self.get_db = get_db

        lay = QVBoxLayout(self)
        lay.addWidget(QLabel("Roots (base folders) in this project:"))

        self.list = QListWidget()
        lay.addWidget(self.list, 1)

        row = QHBoxLayout()
        btn_add = QPushButton("Add folder…")
        btn_add.clicked.connect(self.add_root)
        row.addWidget(btn_add)

        btn_remove = QPushButton("Remove selected")
        btn_remove.clicked.connect(self.remove_selected)
        row.addWidget(btn_remove)

        btn_toggle = QPushButton("Enable/Disable selected")
        btn_toggle.clicked.connect(self.toggle_selected)
        row.addWidget(btn_toggle)

        btn_refresh = QPushButton("Refresh")
        btn_refresh.clicked.connect(self.refresh)
        row.addWidget(btn_refresh)

        row.addStretch(1)
        lay.addLayout(row)


    def refresh(self):
        db = self.get_db()
        if not db:
            return
        self.list.clear()
        rows = db.query_all("SELECT id,path,enabled FROM roots ORDER BY id")
        for r in rows:
            item = QListWidgetItem(f"[{'ON' if r['enabled'] else 'OFF'}] {r['path']}")
            item.setData(256, int(r["id"]))
            self.list.addItem(item)

    def add_root(self):
        db = self.get_db()
        if not db:
            QMessageBox.information(self, "Roots", "Open a project first.")
            return
        folder = QFileDialog.getExistingDirectory(self, "Add root folder")
        if not folder:
            return
        import time
        try:
            db.execute("INSERT OR IGNORE INTO roots(path,enabled,added_at) VALUES(?,?,?)", (folder, 1, int(time.time())))
        except Exception as e:
            QMessageBox.critical(self, "Roots", f"Failed to add:\n{e!r}")
        self.refresh()

    def remove_selected(self):
        db = self.get_db()
        if not db:
            return
        item = self.list.currentItem()
        if not item:
            return
        rid = int(item.data(256))
        db.execute("DELETE FROM roots WHERE id=?", (rid,))
        self.refresh()

    def toggle_selected(self):
        db = self.get_db()
        if not db:
            return
        item = self.list.currentItem()
        if not item:
            return
        rid = int(item.data(256))
        row = db.query_one("SELECT enabled FROM roots WHERE id=?", (rid,))
        if not row:
            return
        enabled = 0 if int(row["enabled"]) == 1 else 1
        db.execute("UPDATE roots SET enabled=? WHERE id=?", (enabled, rid))
        self.refresh()
