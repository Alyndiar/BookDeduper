from __future__ import annotations
from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QPushButton, QLabel, QListWidget, QListWidgetItem,
    QTableWidget, QTableWidgetItem, QMessageBox, QCheckBox, QDialog, QDialogButtonBox, QTextEdit
)
from PySide6.QtCore import Qt
from .deleter import delete_checked

class ReviewTab(QWidget):
    def __init__(self, get_db, get_author_db):
        super().__init__()
        self.get_db = get_db
        self.get_author_db = get_author_db
        self.current_work_key: str | None = None

        lay = QVBoxLayout(self)

        self.top = QLabel("Review & Delete (from deletion_queue).")
        lay.addWidget(self.top)

        self.chk_rename_keep = QCheckBox("Rename kept files to canonical author before delete")
        self.chk_rename_keep.setChecked(False)
        lay.addWidget(self.chk_rename_keep)

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

        self.btn_skip_work = QPushButton("Skip work")
        self.btn_skip_work.clicked.connect(self.skip_current_work)
        btnrow.addWidget(self.btn_skip_work)

        self.btn_invalid_author = QPushButton("Invalid Author Detected")
        self.btn_invalid_author.clicked.connect(self.mark_invalid_author_for_work)
        btnrow.addWidget(self.btn_invalid_author)

        self.btn_preview_rename = QPushButton("Preview canonical renames")
        self.btn_preview_rename.clicked.connect(self.preview_and_apply_canonical_renames)
        btnrow.addWidget(self.btn_preview_rename)

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
            HAVING cnt > 1 OR SUM(CASE WHEN reason LIKE 'RENAME REVIEW%' THEN 1 ELSE 0 END) > 0
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


    def skip_current_work(self):
        db = self.get_db()
        if not db or not self.current_work_key:
            return
        db.execute(
            "UPDATE deletion_queue SET checked=0, reason='SKIP (manual)' WHERE work_key=?",
            (self.current_work_key,),
        )
        self.load_work(self.current_work_key)

    def mark_invalid_author_for_work(self):
        db = self.get_db()
        author_db = self.get_author_db()
        if not db or not self.current_work_key:
            return
        if not author_db:
            QMessageBox.warning(self, "Invalid Author", "No author database connected. Connect an author DB to mark invalid authors.")
            return
        rows = db.query_all(
            """
            SELECT DISTINCT f.author_norm, f.author
            FROM deletion_queue dq
            JOIN files f ON f.id = dq.file_id
            WHERE dq.work_key=?
            """,
            (self.current_work_key,),
        )
        if not rows:
            return
        if QMessageBox.question(self, "Invalid Author", "Mark author(s) from this work as invalid and skip deletion for this work?") != QMessageBox.Yes:
            return

        author_db.begin()
        try:
            for r in rows:
                author_norm = str(r["author_norm"] or "").strip()
                author = str(r["author"] or "").strip()
                if not author_norm or author_norm == "unknown":
                    continue
                author_db.execute(
                    """
                    INSERT INTO invalid_authors(normalized_name,canonical_name,reason,updated_at)
                    VALUES(?,?,?,strftime('%s','now'))
                    ON CONFLICT(normalized_name) DO UPDATE SET
                      canonical_name=excluded.canonical_name,
                      reason='manual-review',
                      updated_at=excluded.updated_at
                    """,
                    (author_norm, author or author_norm, "manual-review"),
                )
            author_db.commit()
        except Exception as e:
            author_db.rollback()
            QMessageBox.critical(self, "Invalid Author", f"Failed:\n{e!r}")
            return

        db.execute(
            "UPDATE deletion_queue SET checked=0, reason='SKIP (invalid author)' WHERE work_key=?",
            (self.current_work_key,),
        )
        QMessageBox.information(self, "Invalid Author", "Author(s) marked invalid. Run Analyze again to rebuild author DB.")
        self.load_work(self.current_work_key)

    def _build_canonical_rename_plan(self):
        db = self.get_db()
        author_db = self.get_author_db()
        if not db:
            return []
        rows = db.query_all(
            """
            SELECT f.id, f.path, f.name, f.author_norm
            FROM deletion_queue dq
            JOIN files f ON f.id = dq.file_id
            WHERE dq.checked=0
            """
        )
        canon_map: dict = {}
        if author_db:
            for ka in author_db.query_all("SELECT normalized_name, canonical_name FROM known_authors"):
                canon_map[str(ka["normalized_name"])] = str(ka["canonical_name"] or "")
        plan = []
        for r in rows:
            src = str(r["path"] or "")
            name = str(r["name"] or "")
            canonical = canon_map.get(str(r["author_norm"] or ""), "").strip()
            if not src or not canonical or " - " not in name:
                continue
            prefix, rest = name.split(" - ", 1)
            new_name = f"{canonical} - {rest}".strip()
            if new_name == name:
                continue
            dst = src.rsplit("/", 1)[0] + "/" + new_name if "/" in src else new_name
            if "\\" in src and "\\" in src:
                import os
                dst = os.path.join(os.path.dirname(src), new_name)
            plan.append({"file_id": int(r["id"]), "src": src, "dst": dst, "new_name": new_name})
        return plan

    def preview_and_apply_canonical_renames(self):
        db = self.get_db()
        if not db:
            return
        plan = self._build_canonical_rename_plan()
        if not plan:
            QMessageBox.information(self, "Canonical Rename", "No keep-file rename candidates found.")
            return

        lines = [f"{p['src']} -> {p['dst']}" for p in plan[:50]]
        text = "\n".join(lines)
        dlg = QDialog(self)
        dlg.setWindowTitle("Canonical Rename Preview")
        v = QVBoxLayout(dlg)
        v.addWidget(QLabel(f"{len(plan)} rename(s) planned. Showing up to 50:"))
        te = QTextEdit()
        te.setReadOnly(True)
        te.setPlainText(text)
        v.addWidget(te)
        btns = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        btns.accepted.connect(dlg.accept)
        btns.rejected.connect(dlg.reject)
        v.addWidget(btns)
        if dlg.exec() != QDialog.Accepted:
            return

        import os
        rollback = []
        db.begin()
        try:
            for p in plan:
                if not os.path.exists(p["src"]):
                    continue
                if os.path.exists(p["dst"]):
                    continue
                os.rename(p["src"], p["dst"])
                rollback.append((p["dst"], p["src"]))
                folder = os.path.dirname(p["dst"])
                db.execute("UPDATE files SET path=?, name=?, folder_path=? WHERE id=?", (p["dst"], p["new_name"], folder, p["file_id"]))
                db.execute("UPDATE folders SET force_rescan=1 WHERE path=?", (folder,))
            db.commit()
        except Exception as e:
            db.rollback()
            for dst, src in reversed(rollback):
                try:
                    if os.path.exists(dst) and not os.path.exists(src):
                        os.rename(dst, src)
                except Exception:
                    pass
            QMessageBox.critical(self, "Canonical Rename", f"Failed:\n{e!r}")
            return
        QMessageBox.information(self, "Canonical Rename", f"Applied {len(rollback)} rename(s).")
        self.refresh()
        if self.current_work_key:
            self.load_work(self.current_work_key)

    def delete_checked_files(self):
        db = self.get_db()
        if not db:
            return
        self.save_checks()
        if self.chk_rename_keep.isChecked():
            self.preview_and_apply_canonical_renames()
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
