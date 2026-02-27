from __future__ import annotations

import os
import logging
from typing import Dict, List, Optional, Tuple

from PySide6.QtCore import Qt
from PySide6.QtGui import QDesktopServices
from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QFormLayout,
    QPushButton, QLabel, QLineEdit, QTextEdit, QMessageBox,
)

from .db import DB
from .author_db import AuthorDB
from .parser import parse_filename, make_work_key, detect_quality_tags
from .util import normalize_text

logger = logging.getLogger(__name__)


def _load_known_authors(author_db: Optional[AuthorDB]) -> Dict[str, Dict]:
    """Load known_authors from authors.db for parse_filename() scoring boosts."""
    out: Dict[str, Dict] = {}
    if not author_db:
        return out
    try:
        rows = author_db.query_all(
            "SELECT normalized_name, canonical_name, frequency FROM known_authors"
        )
        for r in rows:
            norm = str(r["normalized_name"] or "").strip()
            if norm:
                out[norm] = {
                    "canonical_name": str(r["canonical_name"] or ""),
                    "frequency": int(r["frequency"] or 0),
                }
    except Exception:
        pass
    return out


def _load_author_aliases(db: DB, author_db: Optional[AuthorDB]) -> Dict[str, Tuple[str, str]]:
    """Load alias map (alias_norm -> (author_norm, author_display))."""
    out: Dict[str, Tuple[str, str]] = {}
    # OL-sourced aliases from authors.db (lower priority)
    if author_db:
        try:
            rows = author_db.query_all(
                "SELECT alias_norm, author_norm, author_display FROM author_aliases"
            )
            for r in rows:
                a = str(r["alias_norm"] or "").strip()
                n = str(r["author_norm"] or "").strip()
                d = str(r["author_display"] or "").strip()
                if a and n and d:
                    out[a] = (n, d)
        except Exception:
            pass
    # Project-derived aliases (higher priority, overwrites)
    try:
        rows = db.query_all(
            "SELECT alias_norm, author_norm, author_display FROM author_aliases"
        )
        for r in rows:
            a = str(r["alias_norm"] or "").strip()
            n = str(r["author_norm"] or "").strip()
            d = str(r["author_display"] or "").strip()
            if a and n and d:
                out[a] = (n, d)
    except Exception:
        pass
    return out


def _try_correct_author(parsed, filename: str,
                        alias_map: Dict[str, Tuple[str, str]]) -> bool:
    """Canonicalize author via alias map (standalone version)."""
    if not parsed.author_norm or parsed.author_norm == "unknown":
        # Try candidate aliases from filename segments
        stem = os.path.basename(filename)
        if "." in stem:
            stem = stem.rsplit(".", 1)[0]
        candidates: List[str] = []
        for sep in (" - ", " — ", " – ", "_"):
            if sep in stem:
                first = stem.split(sep, 1)[0].strip()
                if first:
                    candidates.append(first)
        if not candidates and stem:
            candidates.append(stem)
        for c in candidates:
            norm = normalize_text(c)
            if not norm:
                continue
            hit = alias_map.get(norm)
            if hit:
                parsed.author = hit[1]
                parsed.author_norm = hit[0]
                si_norm = ""
                if parsed.series_index is not None:
                    si_norm = f"{parsed.series_index:05.1f}".lstrip("0")
                    if si_norm.startswith("."):
                        si_norm = "0" + si_norm
                parsed.work_key = make_work_key(
                    parsed.author_norm, parsed.series_norm,
                    parsed.title_norm, si_norm,
                )
                return True
        return False

    # Author was found but may be a variant — canonicalize
    hit = alias_map.get(parsed.author_norm)
    if hit and hit[0] != parsed.author_norm:
        parsed.author = hit[1]
        parsed.author_norm = hit[0]
        si_norm = ""
        if parsed.series_index is not None:
            si_norm = f"{parsed.series_index:05.1f}".lstrip("0")
            if si_norm.startswith("."):
                si_norm = "0" + si_norm
        parsed.work_key = make_work_key(
            parsed.author_norm, parsed.series_norm,
            parsed.title_norm, si_norm,
        )
        return True
    return False


def _series_index_norm(series_index) -> str:
    if series_index is None:
        return ""
    s = f"{series_index:05.1f}".lstrip("0")
    if s.startswith("."):
        s = "0" + s
    return s


class FixAuthorsTab(QWidget):
    def __init__(self, get_db, get_author_db, on_activity_progress=None):
        super().__init__()
        self.get_db = get_db
        self.get_author_db = get_author_db
        self.on_activity_progress = on_activity_progress

        self._rows: List[dict] = []
        self._index: int = 0
        self._undo_stack: List[dict] = []
        self._known_authors: Dict[str, Dict] = {}
        self._alias_map: Dict[str, Tuple[str, str]] = {}

        lay = QVBoxLayout(self)

        # Counter
        self.counter_label = QLabel("No files loaded")
        self.counter_label.setStyleSheet("font-weight: bold; font-size: 13px;")
        lay.addWidget(self.counter_label)

        # Original path (read-only)
        form = QFormLayout()
        self.original_edit = QLineEdit()
        self.original_edit.setReadOnly(True)
        self.original_edit.setStyleSheet("background-color: #3a3a3a; color: #cccccc;")
        form.addRow("Original:", self.original_edit)

        # Editable filename
        self.name_edit = QLineEdit()
        self.name_edit.returnPressed.connect(self._on_rename)
        form.addRow("New name:", self.name_edit)
        lay.addLayout(form)

        # Buttons
        row = QHBoxLayout()
        self.btn_prev = QPushButton("Previous")
        self.btn_prev.clicked.connect(self._go_prev)
        row.addWidget(self.btn_prev)

        self.btn_next = QPushButton("Next")
        self.btn_next.clicked.connect(self._go_next)
        row.addWidget(self.btn_next)

        row.addSpacing(20)

        self.btn_open = QPushButton("Open File")
        self.btn_open.clicked.connect(self._open_file)
        row.addWidget(self.btn_open)

        self.btn_rename = QPushButton("Rename File")
        self.btn_rename.clicked.connect(self._on_rename)
        row.addWidget(self.btn_rename)

        self.btn_delete = QPushButton("Delete File")
        self.btn_delete.clicked.connect(self._delete_file)
        row.addWidget(self.btn_delete)

        row.addSpacing(20)

        self.btn_undo = QPushButton("Undo Last")
        self.btn_undo.clicked.connect(self._undo_last)
        self.btn_undo.setEnabled(False)
        row.addWidget(self.btn_undo)

        lay.addLayout(row)

        # Log area
        self.log = QTextEdit()
        self.log.setReadOnly(True)
        lay.addWidget(self.log, 1)

    # ------------------------------------------------------------------
    #  Data loading
    # ------------------------------------------------------------------

    def refresh(self):
        db = self.get_db()
        if not db:
            self._rows = []
            self._index = 0
            self.counter_label.setText("No project open")
            self.original_edit.clear()
            self.name_edit.clear()
            self._update_buttons()
            return

        self._rows = [
            dict(r) for r in db.query_all(
                "SELECT id, path, name, ext FROM files "
                "WHERE author_norm = 'unknown' ORDER BY path"
            )
        ]
        self._index = 0

        # Load helpers for reparse
        author_db = self.get_author_db()
        self._known_authors = _load_known_authors(author_db)
        self._alias_map = _load_author_aliases(db, author_db)

        self.log.append(f"Loaded {len(self._rows)} files with unknown author.")
        self._show_current()

    # ------------------------------------------------------------------
    #  Navigation
    # ------------------------------------------------------------------

    def _show_current(self):
        if not self._rows:
            self.counter_label.setText("No files with unknown author")
            self.original_edit.clear()
            self.name_edit.clear()
            self._update_buttons()
            return

        self._index = max(0, min(self._index, len(self._rows) - 1))
        r = self._rows[self._index]
        self.counter_label.setText(
            f"File {self._index + 1} of {len(self._rows)}"
        )
        self.original_edit.setText(str(r["path"] or ""))
        self.name_edit.setText(str(r["name"] or ""))
        self.name_edit.setFocus()
        self.name_edit.selectAll()
        self._update_buttons()

    def _update_buttons(self):
        has_rows = bool(self._rows)
        self.btn_prev.setEnabled(has_rows and self._index > 0)
        self.btn_next.setEnabled(has_rows and self._index < len(self._rows) - 1)
        self.btn_open.setEnabled(has_rows)
        self.btn_rename.setEnabled(has_rows)
        self.btn_delete.setEnabled(has_rows)
        self.btn_undo.setEnabled(bool(self._undo_stack))

    def _go_prev(self):
        if self._index > 0:
            self._index -= 1
            self._show_current()

    def _go_next(self):
        if self._index < len(self._rows) - 1:
            self._index += 1
            self._show_current()

    # ------------------------------------------------------------------
    #  Open file
    # ------------------------------------------------------------------

    def _open_file(self):
        if not self._rows:
            return
        r = self._rows[self._index]
        path = str(r["path"] or "")
        if not path or not os.path.exists(path):
            QMessageBox.warning(self, "Open File", f"File not found:\n{path}")
            return
        from PySide6.QtCore import QUrl
        QDesktopServices.openUrl(QUrl.fromLocalFile(path))

    # ------------------------------------------------------------------
    #  Delete file
    # ------------------------------------------------------------------

    def _delete_file(self):
        db = self.get_db()
        if not db or not self._rows:
            return

        r = self._rows[self._index]
        file_id = int(r["id"])
        path = str(r["path"] or "")

        reply = QMessageBox.question(
            self,
            "Delete File",
            f"Send this file to the Recycle Bin?\n\n{path}",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.No,
        )
        if reply != QMessageBox.StandardButton.Yes:
            return

        if not os.path.exists(path):
            QMessageBox.warning(self, "Delete", f"File not found:\n{path}")
            # Still clean up the DB record
        else:
            try:
                from send2trash import send2trash
                send2trash(path)
            except Exception as e:
                QMessageBox.critical(self, "Delete", f"Failed to delete:\n{e!r}")
                return

        # Remove from DB
        old_wk = ""
        wk_row = db.query_one("SELECT work_key FROM files WHERE id=?", (file_id,))
        if wk_row:
            old_wk = str(wk_row["work_key"] or "")

        db.begin()
        try:
            db.execute("DELETE FROM files WHERE id=?", (file_id,))
            db.execute("DELETE FROM deletion_queue WHERE file_id=?", (file_id,))
            # Clean up orphaned work_key
            if old_wk:
                orphan = db.query_one(
                    "SELECT COUNT(*) AS c FROM files WHERE work_key=?", (old_wk,)
                )
                if orphan and int(orphan["c"]) == 0:
                    db.execute("DELETE FROM works WHERE work_key=?", (old_wk,))
            db.commit()
        except Exception as e:
            db.rollback()
            QMessageBox.critical(self, "Delete", f"DB cleanup failed:\n{e!r}")
            return

        self.log.append(f"Deleted: {path}")

        # Remove from list and show next
        self._rows.pop(self._index)
        if self._index >= len(self._rows):
            self._index = max(0, len(self._rows) - 1)
        self._show_current()

    # ------------------------------------------------------------------
    #  Rename + reparse
    # ------------------------------------------------------------------

    def _on_rename(self):
        db = self.get_db()
        if not db or not self._rows:
            return

        r = self._rows[self._index]
        file_id = int(r["id"])
        old_path = str(r["path"] or "")
        old_name = str(r["name"] or "")
        new_name = self.name_edit.text().strip()

        # Same name → just advance
        if new_name == old_name:
            self._advance_after_rename(removed=False)
            return

        if not new_name:
            QMessageBox.warning(self, "Rename", "Filename cannot be empty.")
            return

        folder = os.path.dirname(old_path)
        new_path = os.path.join(folder, new_name)

        if os.path.exists(new_path):
            QMessageBox.warning(
                self, "Rename",
                f"Destination already exists:\n{new_path}",
            )
            return

        if not os.path.exists(old_path):
            QMessageBox.warning(
                self, "Rename",
                f"Source file not found:\n{old_path}",
            )
            return

        # Filesystem rename
        try:
            os.rename(old_path, new_path)
        except Exception as e:
            QMessageBox.critical(self, "Rename", f"Rename failed:\n{e!r}")
            return

        # Reparse new filename
        ext = new_name.rsplit(".", 1)[1] if "." in new_name else ""
        parsed = parse_filename(new_name, known_authors=self._known_authors)
        _try_correct_author(parsed, new_name, self._alias_map)
        si_norm = _series_index_norm(parsed.series_index)
        tags_str = " | ".join(parsed.tags)

        old_work_key = db.query_one(
            "SELECT work_key FROM files WHERE id=?", (file_id,)
        )
        old_wk = str(old_work_key["work_key"]) if old_work_key else ""

        # DB update
        db.begin()
        try:
            db.execute(
                """UPDATE files SET
                     path=?, name=?, ext=?, folder_path=?,
                     author=?, series=?, series_index=?, title=?, tags=?,
                     author_norm=?, series_norm=?, title_norm=?, work_key=?
                   WHERE id=?""",
                (
                    new_path, new_name, ext, folder,
                    parsed.author, parsed.series, parsed.series_index,
                    parsed.title, tags_str,
                    parsed.author_norm, parsed.series_norm, parsed.title_norm,
                    parsed.work_key, file_id,
                ),
            )

            # Upsert works for new work_key
            db.execute(
                """INSERT INTO works(
                     work_key, author_norm, series_norm, series_index_norm, title_norm,
                     display_author, display_series, display_series_index, display_title
                   ) VALUES(?,?,?,?,?,?,?,?,?)
                   ON CONFLICT(work_key) DO UPDATE SET
                     author_norm=excluded.author_norm,
                     series_norm=excluded.series_norm,
                     series_index_norm=excluded.series_index_norm,
                     title_norm=excluded.title_norm""",
                (
                    parsed.work_key, parsed.author_norm, parsed.series_norm,
                    si_norm, parsed.title_norm,
                    parsed.author, parsed.series or "",
                    str(parsed.series_index or ""), parsed.title,
                ),
            )

            # Clean up orphaned old work_key
            if old_wk and old_wk != parsed.work_key:
                orphan = db.query_one(
                    "SELECT COUNT(*) AS c FROM files WHERE work_key=?",
                    (old_wk,),
                )
                if orphan and int(orphan["c"]) == 0:
                    db.execute("DELETE FROM works WHERE work_key=?", (old_wk,))

            # Mark folder for rescan
            db.execute(
                "UPDATE folders SET force_rescan=1 WHERE path=?", (folder,)
            )

            db.commit()
        except Exception as e:
            db.rollback()
            # Rollback filesystem rename
            try:
                if os.path.exists(new_path) and not os.path.exists(old_path):
                    os.rename(new_path, old_path)
            except Exception:
                pass
            QMessageBox.critical(self, "Rename", f"DB update failed:\n{e!r}")
            return

        # Push undo entry
        self._undo_stack.append({
            "file_id": file_id,
            "old_path": old_path,
            "new_path": new_path,
            "old_name": old_name,
            "new_name": new_name,
        })

        author_resolved = parsed.author_norm != "unknown"
        self.log.append(
            f"Renamed: {old_name} → {new_name}"
            + (f"  [author: {parsed.author}]" if author_resolved else "  [still unknown]")
        )

        # Update the row in our list
        r["path"] = new_path
        r["name"] = new_name

        if author_resolved:
            # Remove from list — author is no longer unknown
            self._rows.pop(self._index)
            # Keep index in bounds (naturally shows next file)
            if self._index >= len(self._rows):
                self._index = max(0, len(self._rows) - 1)
            self._show_current()
        else:
            self._advance_after_rename(removed=False)

    def _advance_after_rename(self, removed: bool):
        """Move to next file after a rename or skip."""
        if not removed and self._index < len(self._rows) - 1:
            self._index += 1
        elif not removed and self._index == len(self._rows) - 1:
            # Already at the end — stay
            pass
        self._show_current()

    # ------------------------------------------------------------------
    #  Undo
    # ------------------------------------------------------------------

    def _undo_last(self):
        db = self.get_db()
        if not db or not self._undo_stack:
            return

        entry = self._undo_stack.pop()
        file_id = entry["file_id"]
        old_path = entry["old_path"]
        new_path = entry["new_path"]
        old_name = entry["old_name"]

        # Reverse filesystem rename
        if not os.path.exists(new_path):
            QMessageBox.warning(
                self, "Undo",
                f"Current file not found (was it moved?):\n{new_path}",
            )
            self._update_buttons()
            return

        if os.path.exists(old_path):
            QMessageBox.warning(
                self, "Undo",
                f"Original path already exists:\n{old_path}",
            )
            self._update_buttons()
            return

        try:
            os.rename(new_path, old_path)
        except Exception as e:
            QMessageBox.critical(self, "Undo", f"Filesystem undo failed:\n{e!r}")
            self._update_buttons()
            return

        # Reparse with original name
        folder = os.path.dirname(old_path)
        ext = old_name.rsplit(".", 1)[1] if "." in old_name else ""
        parsed = parse_filename(old_name, known_authors=self._known_authors)
        _try_correct_author(parsed, old_name, self._alias_map)
        si_norm = _series_index_norm(parsed.series_index)
        tags_str = " | ".join(parsed.tags)

        current_wk = db.query_one(
            "SELECT work_key FROM files WHERE id=?", (file_id,)
        )
        current_wk_str = str(current_wk["work_key"]) if current_wk else ""

        db.begin()
        try:
            db.execute(
                """UPDATE files SET
                     path=?, name=?, ext=?, folder_path=?,
                     author=?, series=?, series_index=?, title=?, tags=?,
                     author_norm=?, series_norm=?, title_norm=?, work_key=?
                   WHERE id=?""",
                (
                    old_path, old_name, ext, folder,
                    parsed.author, parsed.series, parsed.series_index,
                    parsed.title, tags_str,
                    parsed.author_norm, parsed.series_norm, parsed.title_norm,
                    parsed.work_key, file_id,
                ),
            )

            db.execute(
                """INSERT INTO works(
                     work_key, author_norm, series_norm, series_index_norm, title_norm,
                     display_author, display_series, display_series_index, display_title
                   ) VALUES(?,?,?,?,?,?,?,?,?)
                   ON CONFLICT(work_key) DO UPDATE SET
                     author_norm=excluded.author_norm,
                     series_norm=excluded.series_norm,
                     series_index_norm=excluded.series_index_norm,
                     title_norm=excluded.title_norm""",
                (
                    parsed.work_key, parsed.author_norm, parsed.series_norm,
                    si_norm, parsed.title_norm,
                    parsed.author, parsed.series or "",
                    str(parsed.series_index or ""), parsed.title,
                ),
            )

            # Clean up orphaned work_key from the rename we're undoing
            if current_wk_str and current_wk_str != parsed.work_key:
                orphan = db.query_one(
                    "SELECT COUNT(*) AS c FROM files WHERE work_key=?",
                    (current_wk_str,),
                )
                if orphan and int(orphan["c"]) == 0:
                    db.execute("DELETE FROM works WHERE work_key=?", (current_wk_str,))

            db.execute(
                "UPDATE folders SET force_rescan=1 WHERE path=?", (folder,)
            )

            db.commit()
        except Exception as e:
            db.rollback()
            try:
                if os.path.exists(old_path) and not os.path.exists(new_path):
                    os.rename(old_path, new_path)
            except Exception:
                pass
            QMessageBox.critical(self, "Undo", f"DB undo failed:\n{e!r}")
            self._update_buttons()
            return

        self.log.append(f"Undo: {entry['new_name']} → {old_name}")

        # If author is unknown again, re-insert into our list
        if parsed.author_norm == "unknown":
            restored = {"id": file_id, "path": old_path, "name": old_name, "ext": ext}
            # Insert at a position that keeps sort-by-path order
            insert_idx = 0
            for i, row in enumerate(self._rows):
                if str(row["path"] or "") > old_path:
                    insert_idx = i
                    break
                insert_idx = i + 1
            self._rows.insert(insert_idx, restored)
            self._index = insert_idx
        else:
            # File was already removed from list; it stays resolved
            pass

        self._show_current()
