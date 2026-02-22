from __future__ import annotations
from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QPushButton, QLabel, QListWidget, QListWidgetItem, QMessageBox
)

class AuthorsTab(QWidget):
    def __init__(self, get_db):
        super().__init__()
        self.get_db = get_db

        lay = QVBoxLayout(self)
        self.top = QLabel("Author DB")
        lay.addWidget(self.top)

        row = QHBoxLayout()
        self.valid_list = QListWidget()
        self.invalid_list = QListWidget()
        row.addWidget(self.valid_list, 1)
        row.addWidget(self.invalid_list, 1)
        lay.addLayout(row, 1)

        btns = QHBoxLayout()
        b_refresh = QPushButton("Refresh")
        b_refresh.clicked.connect(self.refresh)
        btns.addWidget(b_refresh)

        b_invalidate = QPushButton("Mark invalid →")
        b_invalidate.clicked.connect(self.mark_invalid)
        btns.addWidget(b_invalidate)

        b_restore = QPushButton("← Restore valid")
        b_restore.clicked.connect(self.restore_valid)
        btns.addWidget(b_restore)

        b_delete = QPushButton("Delete invalid")
        b_delete.clicked.connect(self.delete_invalid)
        btns.addWidget(b_delete)

        lay.addLayout(btns)

    def _selected_norm(self, list_widget: QListWidget):
        item = list_widget.currentItem()
        if not item:
            return None
        return str(item.data(256) or "")

    def refresh(self):
        db = self.get_db()
        if not db:
            return
        self.valid_list.clear()
        self.invalid_list.clear()
        valid = db.query_all("SELECT normalized_name, canonical_name, frequency FROM known_authors ORDER BY frequency DESC, canonical_name")
        invalid = db.query_all("SELECT normalized_name, canonical_name FROM invalid_authors ORDER BY canonical_name")
        for r in valid:
            it = QListWidgetItem(f"{r['canonical_name']} ({int(r['frequency'] or 0)})")
            it.setData(256, r["normalized_name"])
            self.valid_list.addItem(it)
        for r in invalid:
            it = QListWidgetItem(str(r["canonical_name"]))
            it.setData(256, r["normalized_name"])
            self.invalid_list.addItem(it)
        self.top.setText(f"Author DB: valid={len(valid)} invalid={len(invalid)}")

    def mark_invalid(self):
        db = self.get_db()
        if not db:
            return
        norm = self._selected_norm(self.valid_list)
        if not norm:
            return
        row = db.query_one("SELECT canonical_name FROM known_authors WHERE normalized_name=?", (norm,))
        if not row:
            return
        db.begin()
        try:
            db.execute(
                "INSERT INTO invalid_authors(normalized_name,canonical_name,reason,updated_at) VALUES(?,?,?,strftime('%s','now')) ON CONFLICT(normalized_name) DO UPDATE SET canonical_name=excluded.canonical_name, updated_at=excluded.updated_at",
                (norm, row["canonical_name"], "manual"),
            )
            db.execute("DELETE FROM known_authors WHERE normalized_name=?", (norm,))
            db.commit()
        except Exception as e:
            db.rollback()
            QMessageBox.critical(self, "Authors", f"Failed: {e!r}")
            return
        self.refresh()

    def restore_valid(self):
        db = self.get_db()
        if not db:
            return
        norm = self._selected_norm(self.invalid_list)
        if not norm:
            return
        db.execute("DELETE FROM invalid_authors WHERE normalized_name=?", (norm,))
        self.refresh()

    def delete_invalid(self):
        db = self.get_db()
        if not db:
            return
        norm = self._selected_norm(self.invalid_list)
        if not norm:
            return
        db.execute("DELETE FROM invalid_authors WHERE normalized_name=?", (norm,))
        self.refresh()
