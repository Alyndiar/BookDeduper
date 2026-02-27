from __future__ import annotations
import time

from PySide6.QtCore import QObject, QThread, Signal
from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QPushButton, QLabel, QListWidget, QListWidgetItem,
    QMessageBox, QAbstractItemView, QDialog, QFormLayout, QLineEdit, QTextEdit, QDialogButtonBox,
    QInputDialog,
)

from .util import normalize_text


# ---------------------------------------------------------------------------
# Background worker for loading author lists
# ---------------------------------------------------------------------------

class _AuthorRefreshWorker(QObject):
    """Loads the three author lists from DB in a background thread."""
    finished = Signal(object)   # tuple (approved, tentative, invalid) of list[dict], or None on error

    def __init__(self, db, author_db):
        super().__init__()
        self.db = db
        self.author_db = author_db

    def run(self):
        try:
            approved = []
            invalid = []
            tentative = []
            if self.author_db:
                approved = [dict(r) for r in self.author_db.query_all(
                    "SELECT normalized_name, COALESCE(preferred_name, canonical_name) AS shown,"
                    " canonical_name, frequency FROM known_authors ORDER BY frequency DESC, canonical_name"
                )]
                invalid = [dict(r) for r in self.author_db.query_all(
                    "SELECT normalized_name, canonical_name FROM invalid_authors ORDER BY canonical_name"
                )]
            if self.db:
                tentative = [dict(r) for r in self.db.query_all(
                    "SELECT normalized_name, COALESCE(preferred_name, canonical_name) AS shown,"
                    " canonical_name, frequency, confidence FROM tentative_authors"
                    " ORDER BY frequency DESC, canonical_name"
                )]
            self.finished.emit((approved, tentative, invalid))
        except Exception:
            self.finished.emit(None)


class _AuthorRefreshHandler(QObject):
    """Holds a strong reference to the _AuthorRefreshWorker.finished slot."""

    def __init__(self, tab, thread: QThread):
        super().__init__(tab)
        self._tab = tab
        self._thread = thread

    def on_finished(self, result):
        if self._thread:
            self._thread.quit()
            self._thread.wait(5000)
        self._tab._refresh_thread = None
        self._tab._refresh_worker = None
        self._tab._refresh_handler = None
        if result is not None:
            approved, tentative, invalid = result
            self._tab._populate_authors(approved, tentative, invalid)
        if self._tab.on_activity_progress:
            self._tab.on_activity_progress("Idle", -1.0)


class AuthorEditDialog(QDialog):
    def __init__(self, parent, title: str, canonical: str, preferred: str, confidence: float, aliases: list[tuple[str, float]]):
        super().__init__(parent)
        self.setWindowTitle(title)
        lay = QVBoxLayout(self)
        form = QFormLayout()

        self.canonical_edit = QLineEdit(canonical)
        self.preferred_edit = QLineEdit(preferred or canonical)
        self.confidence_edit = QLineEdit(f"{confidence:.2f}")
        self.aliases_edit = QTextEdit()
        self.aliases_edit.setPlaceholderText("One alias per line. Optional confidence: alias|0.90")
        self.aliases_edit.setPlainText("\n".join([f"{a}|{c:.2f}" for a, c in aliases]))

        form.addRow("Canonical", self.canonical_edit)
        form.addRow("Preferred", self.preferred_edit)
        form.addRow("Confidence", self.confidence_edit)
        form.addRow("Aliases", self.aliases_edit)
        lay.addLayout(form)

        bb = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        bb.accepted.connect(self.accept)
        bb.rejected.connect(self.reject)
        lay.addWidget(bb)

    def values(self):
        canonical = self.canonical_edit.text().strip()
        preferred = self.preferred_edit.text().strip() or canonical
        try:
            conf = float(self.confidence_edit.text().strip() or "0")
        except Exception:
            conf = 0.0
        conf = max(0.0, min(conf, 1.0))

        aliases: list[tuple[str, float]] = []
        for line in self.aliases_edit.toPlainText().splitlines():
            line = line.strip()
            if not line:
                continue
            if "|" in line:
                a, c = line.split("|", 1)
                a = a.strip()
                try:
                    cv = float(c.strip())
                except Exception:
                    cv = conf
            else:
                a = line
                cv = conf
            if a:
                aliases.append((a, max(0.0, min(cv, 1.0))))
        return canonical, preferred, conf, aliases


class AuthorsTab(QWidget):
    def __init__(self, get_db, get_author_db, on_activity_progress=None):
        super().__init__()
        self.get_db = get_db          # books.db — holds tentative_authors, author_variants, derived aliases
        self.get_author_db = get_author_db  # authors.db — holds known_authors, invalid_authors, OL aliases
        self.on_activity_progress = on_activity_progress
        self._refresh_thread: QThread | None = None
        self._refresh_worker = None
        self._refresh_handler = None

        lay = QVBoxLayout(self)
        self.top = QLabel("Author DB")
        lay.addWidget(self.top)

        row = QHBoxLayout()
        self.approved_list = QListWidget()
        self.tentative_list = QListWidget()
        self.invalid_list = QListWidget()
        for lw in (self.approved_list, self.tentative_list, self.invalid_list):
            lw.setSelectionMode(QAbstractItemView.ExtendedSelection)
        self.approved_list.itemDoubleClicked.connect(lambda _i: self.edit_selected_author(self.approved_list, "approved"))
        self.tentative_list.itemDoubleClicked.connect(lambda _i: self.edit_selected_author(self.tentative_list, "tentative"))

        left = QVBoxLayout()
        left.addWidget(QLabel("Approved (authors.db)"))
        left.addWidget(self.approved_list, 1)
        mid = QVBoxLayout()
        mid.addWidget(QLabel("Tentative (this project)"))
        mid.addWidget(self.tentative_list, 1)
        right = QVBoxLayout()
        right.addWidget(QLabel("Invalid (authors.db)"))
        right.addWidget(self.invalid_list, 1)
        row.addLayout(left, 1)
        row.addLayout(mid, 1)
        row.addLayout(right, 1)
        lay.addLayout(row, 1)

        btns = QHBoxLayout()
        b_refresh = QPushButton("Refresh")
        b_refresh.clicked.connect(self.refresh)
        btns.addWidget(b_refresh)

        b_to_approved = QPushButton("Move selected → Approved")
        b_to_approved.clicked.connect(lambda: self.move_selected("approved"))
        btns.addWidget(b_to_approved)

        b_to_tentative = QPushButton("Move selected → Tentative")
        b_to_tentative.clicked.connect(lambda: self.move_selected("tentative"))
        btns.addWidget(b_to_tentative)

        b_to_invalid = QPushButton("Move selected → Invalid")
        b_to_invalid.clicked.connect(lambda: self.move_selected("invalid"))
        btns.addWidget(b_to_invalid)

        lay.addLayout(btns)

        btns2 = QHBoxLayout()
        b_add = QPushButton("Add author")
        b_add.clicked.connect(self.add_author)
        btns2.addWidget(b_add)

        b_edit = QPushButton("Edit selected")
        b_edit.clicked.connect(self.edit_current)
        btns2.addWidget(b_edit)

        b_clear_approved = QPushButton("Clear Approved")
        b_clear_approved.clicked.connect(lambda: self.clear_list("approved"))
        btns2.addWidget(b_clear_approved)

        b_clear_tent = QPushButton("Clear Tentative")
        b_clear_tent.clicked.connect(lambda: self.clear_list("tentative"))
        btns2.addWidget(b_clear_tent)

        b_clear_invalid = QPushButton("Clear Invalid")
        b_clear_invalid.clicked.connect(lambda: self.clear_list("invalid"))
        btns2.addWidget(b_clear_invalid)

        b_rean = QPushButton("Mark authors for reanalyze")
        b_rean.clicked.connect(self.mark_reanalyze)
        btns2.addWidget(b_rean)

        lay.addLayout(btns2)

    def _selected_items(self):
        if self.approved_list.selectedItems():
            return "approved", self.approved_list.selectedItems()
        if self.tentative_list.selectedItems():
            return "tentative", self.tentative_list.selectedItems()
        if self.invalid_list.selectedItems():
            return "invalid", self.invalid_list.selectedItems()
        return None, []

    def refresh(self):
        """Start a background refresh of all three author lists."""
        # If a refresh is already in-flight, skip it — the existing one will finish shortly.
        if self._refresh_thread and self._refresh_thread.isRunning():
            return

        if self.on_activity_progress:
            self.on_activity_progress("Loading authors…", -1.0)

        thread = QThread()
        worker = _AuthorRefreshWorker(self.get_db(), self.get_author_db())
        handler = _AuthorRefreshHandler(self, thread)

        self._refresh_thread = thread
        self._refresh_worker = worker
        self._refresh_handler = handler

        worker.finished.connect(handler.on_finished)
        worker.moveToThread(thread)
        thread.started.connect(worker.run)
        thread.start()

    def _populate_authors(self, approved: list, tentative: list, invalid: list):
        """Populate the three list widgets from the already-loaded row dicts (main thread)."""
        self.approved_list.clear()
        self.tentative_list.clear()
        self.invalid_list.clear()

        for r in approved:
            it = QListWidgetItem(f"{r['shown']} ({int(r['frequency'] or 0)})")
            it.setData(256, (r["normalized_name"], r["canonical_name"], r["shown"], float(1.0)))
            self.approved_list.addItem(it)
        for r in tentative:
            it = QListWidgetItem(f"{r['shown']} ({int(r['frequency'] or 0)}) [{float(r['confidence'] or 0.0):.2f}]")
            it.setData(256, (r["normalized_name"], r["canonical_name"], r["shown"], float(r["confidence"] or 0.0)))
            self.tentative_list.addItem(it)
        for r in invalid:
            it = QListWidgetItem(str(r["canonical_name"]))
            it.setData(256, (r["normalized_name"], r["canonical_name"], r["canonical_name"], 0.0))
            self.invalid_list.addItem(it)

        no_author_db = " (no authors.db)" if not self.get_author_db() else ""
        self.top.setText(f"Author DB{no_author_db}: approved={len(approved)} tentative={len(tentative)} invalid={len(invalid)}")

    def _aliases_for(self, norm: str) -> list[tuple[str, float]]:
        """Return all aliases for this author norm, combining authors.db and books.db."""
        result = {}
        author_db = self.get_author_db()
        if author_db:
            for r in author_db.query_all("SELECT author_display, confidence FROM author_aliases WHERE author_norm=? ORDER BY author_display", (norm,)):
                result[str(r["author_display"])] = float(r["confidence"] or 0.0)
        db = self.get_db()
        if db:
            for r in db.query_all("SELECT author_display, confidence FROM author_aliases WHERE author_norm=? ORDER BY author_display", (norm,)):
                result[str(r["author_display"])] = float(r["confidence"] or 0.0)
        return sorted(result.items())

    def edit_selected_author(self, list_widget: QListWidget, source: str):
        author_db = self.get_author_db()
        db = self.get_db()
        item = list_widget.currentItem()
        if not item:
            return
        norm, canonical, preferred, conf = item.data(256)
        dlg = AuthorEditDialog(self, "Edit author", canonical, preferred, float(conf), self._aliases_for(norm))
        if dlg.exec() != QDialog.Accepted:
            return
        canonical2, preferred2, conf2, aliases = dlg.values()
        if not canonical2:
            return
        norm2 = normalize_text(preferred2 or canonical2)

        if source == "approved":
            if not author_db:
                QMessageBox.warning(self, "Authors", "No author DB connected.")
                return
            author_db.begin()
            try:
                author_db.execute("DELETE FROM known_authors WHERE normalized_name=?", (norm,))
                author_db.execute(
                    "INSERT INTO known_authors(normalized_name,canonical_name,preferred_name,frequency,created_at,updated_at) VALUES(?,?,?,?,strftime('%s','now'),strftime('%s','now')) ON CONFLICT(normalized_name) DO UPDATE SET canonical_name=excluded.canonical_name, preferred_name=excluded.preferred_name, updated_at=excluded.updated_at",
                    (norm2, canonical2, preferred2, 1),
                )
                author_db.execute("DELETE FROM author_aliases WHERE author_norm=?", (norm,))
                for alias, c in aliases:
                    alias_norm = normalize_text(alias)
                    if not alias_norm:
                        continue
                    author_db.execute(
                        "INSERT INTO author_aliases(alias_norm,author_norm,author_display,confidence,source,updated_at) VALUES(?,?,?,?,?,?) ON CONFLICT(alias_norm) DO UPDATE SET author_norm=excluded.author_norm, author_display=excluded.author_display, confidence=excluded.confidence, source='manual', updated_at=excluded.updated_at",
                        (alias_norm, norm2, alias, float(c), "manual", int(time.time())),
                    )
                author_db.commit()
            except Exception as e:
                author_db.rollback()
                QMessageBox.critical(self, "Authors", f"Failed: {e!r}")
                return
        elif source == "tentative":
            if not db:
                QMessageBox.warning(self, "Authors", "No project DB connected.")
                return
            db.begin()
            try:
                db.execute("DELETE FROM tentative_authors WHERE normalized_name=?", (norm,))
                db.execute(
                    "INSERT INTO tentative_authors(normalized_name,canonical_name,preferred_name,frequency,confidence,created_at,updated_at) VALUES(?,?,?,?,?,strftime('%s','now'),strftime('%s','now')) ON CONFLICT(normalized_name) DO UPDATE SET canonical_name=excluded.canonical_name, preferred_name=excluded.preferred_name, confidence=excluded.confidence, updated_at=excluded.updated_at",
                    (norm2, canonical2, preferred2, 1, conf2),
                )
                db.commit()
            except Exception as e:
                db.rollback()
                QMessageBox.critical(self, "Authors", f"Failed: {e!r}")
                return
        self.refresh()

    def edit_current(self):
        if self.approved_list.currentItem() is not None:
            self.edit_selected_author(self.approved_list, "approved")
            return
        if self.tentative_list.currentItem() is not None:
            self.edit_selected_author(self.tentative_list, "tentative")

    def add_author(self):
        author_db = self.get_author_db()
        db = self.get_db()
        name, ok = QInputDialog.getText(self, "Add author", "Preferred/Canonical author name:")
        if not ok or not name.strip():
            return
        target, ok2 = QInputDialog.getItem(self, "Target list", "Add to:", ["approved", "tentative", "invalid"], 0, False)
        if not ok2:
            return
        name = name.strip()
        norm = normalize_text(name)
        if not norm:
            return

        if target in ("approved", "invalid"):
            if not author_db:
                QMessageBox.warning(self, "Authors", "No author DB connected. Cannot add approved/invalid authors.")
                return
            author_db.begin()
            try:
                if target == "approved":
                    author_db.execute(
                        "INSERT INTO known_authors(normalized_name,canonical_name,preferred_name,frequency,created_at,updated_at) VALUES(?,?,?,?,strftime('%s','now'),strftime('%s','now')) ON CONFLICT(normalized_name) DO UPDATE SET canonical_name=excluded.canonical_name, preferred_name=excluded.preferred_name, updated_at=excluded.updated_at",
                        (norm, name, name, 1),
                    )
                else:
                    author_db.execute(
                        "INSERT INTO invalid_authors(normalized_name,canonical_name,reason,updated_at) VALUES(?,?,?,strftime('%s','now')) ON CONFLICT(normalized_name) DO UPDATE SET canonical_name=excluded.canonical_name, updated_at=excluded.updated_at",
                        (norm, name, "manual"),
                    )
                author_db.commit()
            except Exception as e:
                author_db.rollback()
                QMessageBox.critical(self, "Authors", f"Failed: {e!r}")
                return
        else:  # tentative
            if not db:
                QMessageBox.warning(self, "Authors", "No project DB connected.")
                return
            db.begin()
            try:
                db.execute(
                    "INSERT INTO tentative_authors(normalized_name,canonical_name,preferred_name,frequency,confidence,created_at,updated_at) VALUES(?,?,?,?,?,strftime('%s','now'),strftime('%s','now')) ON CONFLICT(normalized_name) DO UPDATE SET canonical_name=excluded.canonical_name, preferred_name=excluded.preferred_name, updated_at=excluded.updated_at",
                    (norm, name, name, 1, 1.0),
                )
                db.commit()
            except Exception as e:
                db.rollback()
                QMessageBox.critical(self, "Authors", f"Failed: {e!r}")
                return
        self.refresh()

    def move_selected(self, target: str):
        author_db = self.get_author_db()
        db = self.get_db()
        source, items = self._selected_items()
        if not items or not source:
            return

        if target in ("approved", "invalid") and not author_db:
            QMessageBox.warning(self, "Authors", "No author DB connected. Cannot move to approved/invalid.")
            return
        if target == "tentative" and not db:
            QMessageBox.warning(self, "Authors", "No project DB connected.")
            return

        # Collect data before modifying
        rows_data = [it.data(256) for it in items]

        try:
            # Remove from source
            if source == "approved" and author_db:
                author_db.begin()
                for norm, canonical, preferred, conf in rows_data:
                    author_db.execute("DELETE FROM known_authors WHERE normalized_name=?", (str(norm),))
                author_db.commit()
            elif source == "tentative" and db:
                db.begin()
                for norm, canonical, preferred, conf in rows_data:
                    db.execute("DELETE FROM tentative_authors WHERE normalized_name=?", (str(norm),))
                db.commit()
            elif source == "invalid" and author_db:
                author_db.begin()
                for norm, canonical, preferred, conf in rows_data:
                    author_db.execute("DELETE FROM invalid_authors WHERE normalized_name=?", (str(norm),))
                author_db.commit()

            # Insert into target
            if target == "approved":
                author_db.begin()
                for norm, canonical, preferred, conf in rows_data:
                    author_db.execute(
                        "INSERT INTO known_authors(normalized_name,canonical_name,preferred_name,frequency,created_at,updated_at) VALUES(?,?,?,?,strftime('%s','now'),strftime('%s','now'))",
                        (str(norm), str(canonical), str(preferred) or str(canonical), 1),
                    )
                author_db.commit()
            elif target == "tentative":
                db.begin()
                for norm, canonical, preferred, conf in rows_data:
                    db.execute(
                        "INSERT INTO tentative_authors(normalized_name,canonical_name,preferred_name,frequency,confidence,created_at,updated_at) VALUES(?,?,?,?,?,strftime('%s','now'),strftime('%s','now'))",
                        (str(norm), str(canonical), str(preferred) or str(canonical), 1, float(conf or 0.0)),
                    )
                db.commit()
            else:  # invalid
                author_db.begin()
                for norm, canonical, preferred, conf in rows_data:
                    author_db.execute(
                        "INSERT INTO invalid_authors(normalized_name,canonical_name,reason,updated_at) VALUES(?,?,?,strftime('%s','now'))",
                        (str(norm), str(canonical), f"moved-from-{source}"),
                    )
                author_db.commit()
        except Exception as e:
            QMessageBox.critical(self, "Authors", f"Failed: {e!r}")
            return
        self.refresh()

    def clear_list(self, which: str):
        author_db = self.get_author_db()
        db = self.get_db()
        if QMessageBox.question(self, "Clear authors", f"Clear {which} authors?") != QMessageBox.Yes:
            return
        if self.on_activity_progress:
            self.on_activity_progress(f"Clearing {which} authors…", -1.0)
        if which == "approved":
            if not author_db:
                QMessageBox.warning(self, "Authors", "No author DB connected.")
                if self.on_activity_progress:
                    self.on_activity_progress("Idle", -1.0)
                return
            author_db.execute("DELETE FROM known_authors")
        elif which == "invalid":
            if not author_db:
                QMessageBox.warning(self, "Authors", "No author DB connected.")
                if self.on_activity_progress:
                    self.on_activity_progress("Idle", -1.0)
                return
            author_db.execute("DELETE FROM invalid_authors")
        elif which == "tentative":
            if not db:
                if self.on_activity_progress:
                    self.on_activity_progress("Idle", -1.0)
                return
            db.execute("DELETE FROM tentative_authors")
        # refresh() will set "Idle" via _AuthorRefreshHandler when the worker finishes.
        self.refresh()

    def mark_reanalyze(self):
        db = self.get_db()
        if not db:
            return
        db.set_state("author_db_dirty", "1")
        db.set_state("analyze_authors_completed", "0")
        QMessageBox.information(self, "Authors", "Author DB marked dirty. Run Analyze Authors to rebuild.")

