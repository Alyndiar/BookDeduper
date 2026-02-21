from __future__ import annotations
from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QPushButton, QLabel, QListWidget, QListWidgetItem,
    QTableWidget, QTableWidgetItem, QMessageBox
)
from PySide6.QtCore import Qt
from .deleter import delete_checked

class ReviewTab(QWidget):
    def __init__(self, get_db):
        super().__init__()
        self.get_db = get_db
        self.current_work_key: str | None = None

        lay = QVBoxLayout(self)

        self.top = QLabel("Review & Delete (from deletion_queue).")
        lay.addWidget(self.top)

        row = QHBoxLayout()

        self.work_list = QListWidget()
        self.work_list.currentItemChanged.connect(self.on_work_selected)
        row.addWidget(self.work_list, 1)

        right = QVBoxLayout()
        self.table = QTableWidget(0, 8)
        self.table.setHorizontalHeaderLabels(["Delete?", "Reason", "EffectiveFmt", "Tags", "Size", "MTime(ns)", "Name", "Path"])
        self.table.horizontalHeader().setStretchLastSection(True)
        right.addWidget(self.table, 1)

        btnrow = QHBoxLayout()
        self.btn_refresh = QPushButton("Refresh")
        self.btn_refresh.clicked.connect(self.refresh)
        btnrow.addWidget(self.btn_refresh)

        self.btn_toggle_all = QPushButton("Toggle all in work")
        self.btn_toggle_all.clicked.connect(self.toggle_all)
        btnrow.addWidget(self.btn_toggle_all)

        self.btn_save_checks = QPushButton("Save checkmarks")
        self.btn_save_checks.clicked.connect(self.save_checks)
        btnrow.addWidget(self.btn_save_checks)

        self.btn_delete = QPushButton("Delete checked (Recycle Bin)")
        self.btn_delete.clicked.connect(self.delete_checked_files)
        btnrow.addWidget(self.btn_delete)

        right.addLayout(btnrow)
        row.addLayout(right, 3)

        lay.addLayout(row, 1)

    def refresh(self):
        db = self.get_db()
        if not db:
            return
        if db.get_state("analyze_completed", "0") != "1":
            self.top.setText("Review & Delete: Analyze must complete first.")
            return

        self.work_list.clear()
        rows = db.query_all(
            """
            SELECT work_key, COUNT(*) AS cnt, SUM(checked) AS delcnt
            FROM deletion_queue
            GROUP BY work_key
            HAVING cnt > 1
            ORDER BY delcnt DESC, cnt DESC
            LIMIT 5000
            """
        )
        for r in rows:
            wk = r["work_key"]
            cnt = int(r["cnt"])
            delcnt = int(r["delcnt"] or 0)
            item = QListWidgetItem(f"[{delcnt} delete] ({cnt} files) {wk[:80]}")
            item.setData(256, wk)
            self.work_list.addItem(item)

        self.top.setText(f"Review & Delete: loaded {len(rows)} works (showing up to 5000). Select a work.")

    def on_work_selected(self, cur: QListWidgetItem, prev: QListWidgetItem):
        if not cur:
            return
        self.current_work_key = cur.data(256)
        self.load_work(self.current_work_key)

    def load_work(self, work_key: str):
        db = self.get_db()
        if not db:
            return

        rows = db.query_all(
            """
            SELECT dq.id AS dqid, dq.checked, dq.reason,
                   f.id AS file_id, f.path, f.name, f.ext, f.is_archive, f.inner_ext_guess,
                   f.size, f.mtime_ns, f.tags
            FROM deletion_queue dq
            JOIN files f ON f.id = dq.file_id
            WHERE dq.work_key=?
            ORDER BY dq.checked DESC, f.mtime_ns DESC, f.size DESC
            """,
            (work_key,)
        )
        self.table.setRowCount(0)

        for r in rows:
            rowi = self.table.rowCount()
            self.table.insertRow(rowi)

            chk = QTableWidgetItem("")
            chk.setFlags(Qt.ItemIsEnabled | Qt.ItemIsUserCheckable | Qt.ItemIsSelectable)
            chk.setCheckState(Qt.Checked if int(r["checked"]) == 1 else Qt.Unchecked)
            chk.setData(256, int(r["dqid"]))
            self.table.setItem(rowi, 0, chk)

            self.table.setItem(rowi, 1, QTableWidgetItem(str(r["reason"] or "")))
            eff = (r["inner_ext_guess"] if int(r["is_archive"] or 0) == 1 and r["inner_ext_guess"] else r["ext"]) or ""
            self.table.setItem(rowi, 2, QTableWidgetItem(eff))
            self.table.setItem(rowi, 3, QTableWidgetItem(str(r["tags"] or "")))
            self.table.setItem(rowi, 4, QTableWidgetItem(str(r["size"])))
            self.table.setItem(rowi, 5, QTableWidgetItem(str(r["mtime_ns"])))
            self.table.setItem(rowi, 6, QTableWidgetItem(str(r["name"])))
            self.table.setItem(rowi, 7, QTableWidgetItem(str(r["path"])))

        self.top.setText(f"Work: {work_key}  ({len(rows)} files)")

    def toggle_all(self):
        if self.table.rowCount() == 0:
            return
        any_unchecked = any(self.table.item(i, 0).checkState() == Qt.Unchecked for i in range(self.table.rowCount()))
        new_state = Qt.Checked if any_unchecked else Qt.Unchecked
        for i in range(self.table.rowCount()):
            self.table.item(i, 0).setCheckState(new_state)

    def save_checks(self):
        db = self.get_db()
        if not db:
            return
        db.begin()
        try:
            for i in range(self.table.rowCount()):
                item = self.table.item(i, 0)
                dqid = int(item.data(256))
                checked = 1 if item.checkState() == Qt.Checked else 0
                db.execute("UPDATE deletion_queue SET checked=? WHERE id=?", (checked, dqid))
            db.commit()
        except Exception as e:
            db.rollback()
            QMessageBox.critical(self, "Save", f"Failed:\n{e!r}")
            return
        QMessageBox.information(self, "Save", "Checkmarks saved.")

    def delete_checked_files(self):
        db = self.get_db()
        if not db:
            return
        self.save_checks()
        if QMessageBox.question(self, "Delete", "Send all checked files to Recycle Bin?") != QMessageBox.Yes:
            return
        try:
            deleted, failed = delete_checked(db)
        except Exception as e:
            QMessageBox.critical(self, "Delete", f"Delete failed:\n{e!r}")
            return

        QMessageBox.information(self, "Delete", f"Deleted: {deleted}\nFailed: {failed}")
        self.refresh()
        if self.current_work_key:
            self.load_work(self.current_work_key)
