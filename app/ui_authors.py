from __future__ import annotations
import os
import time

from PySide6.QtCore import QObject, QThread, Signal, Qt
from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QPushButton, QLabel, QListWidget, QListWidgetItem,
    QMessageBox, QAbstractItemView, QDialog, QFormLayout, QLineEdit, QTextEdit, QDialogButtonBox,
    QInputDialog, QProgressDialog
)

from .util import normalize_text, discover_latest_author_dump_file, parse_author_dump_line
from .ol_redirects import import_latest_redirect_dump, purge_redirect_table


DUMP_PREFIX = "ol_dump_authors_"


# ---------------------------------------------------------------------------
# Background workers for long-running import operations
# ---------------------------------------------------------------------------

class _ImportDumpWorker(QObject):
    """Imports an OpenLibrary author dump file in a background thread."""
    progress = Signal(str, float)   # message, pct  (-1.0 = indeterminate)
    finished = Signal(bool, str)    # ok, message

    def __init__(self, author_db, dump_path: str, dump_date: str,
                 start_line: int, restart: bool):
        super().__init__()
        self.author_db = author_db
        self.dump_path = dump_path
        self.dump_date = dump_date
        self.start_line = start_line
        self.restart = restart
        self._stop = False

    def request_stop(self):
        self._stop = True

    def run(self):
        author_db = self.author_db
        dump_path = self.dump_path
        dump_date = self.dump_date
        imported = 0
        skipped = 0
        line_no = 0
        try:
            file_size = max(1, os.path.getsize(dump_path))
            author_db.begin()
            author_db.set_state("authors_dump_completed", "0")
            author_db.set_state("authors_dump_last_file", dump_path)
            author_db.set_state("authors_dump_last_date", dump_date)
            if self.restart:
                author_db.set_state("authors_dump_last_line", "0")

            with open(dump_path, "r", encoding="utf-8", errors="replace") as fh:
                for line in fh:
                    if self._stop:
                        author_db.set_state("authors_dump_last_line", str(line_no))
                        author_db.commit()
                        self.finished.emit(False, f"Cancelled at line {line_no}.")
                        return
                    line_no += 1
                    if line_no <= self.start_line:
                        continue
                    rec = parse_author_dump_line(line)
                    if not rec:
                        skipped += 1
                        continue
                    norm = str(rec["canonical_norm"] or "")
                    if not norm:
                        skipped += 1
                        continue
                    if int(rec.get("token_count") or 0) > 10:
                        skipped += 1
                        continue
                    ol_key = str(rec["ol_key"])
                    last_modified = str(rec.get("last_modified") or "")
                    prev = author_db.query_one(
                        "SELECT last_modified FROM author_dump_records WHERE ol_key=?", (ol_key,))
                    if prev and str(prev["last_modified"] or "") == last_modified:
                        skipped += 1
                        continue
                    name = str(rec["canonical_name"])
                    author_db.execute(
                        "INSERT INTO known_authors(normalized_name,canonical_name,preferred_name,frequency,created_at,updated_at)"
                        " VALUES(?,?,?,?,strftime('%s','now'),strftime('%s','now'))"
                        " ON CONFLICT(normalized_name) DO UPDATE SET canonical_name=excluded.canonical_name,"
                        " preferred_name=excluded.preferred_name, updated_at=excluded.updated_at",
                        (norm, name, name, 1),
                    )
                    author_db.execute(
                        "INSERT INTO author_dump_imported(normalized_name,dump_date,updated_at)"
                        " VALUES(?,?,strftime('%s','now'))"
                        " ON CONFLICT(normalized_name) DO UPDATE SET dump_date=excluded.dump_date, updated_at=excluded.updated_at",
                        (norm, dump_date),
                    )
                    author_db.execute(
                        "INSERT INTO author_dump_records(ol_key,last_modified,author_norm,canonical_name,dump_date,updated_at)"
                        " VALUES(?,?,?,?,?,strftime('%s','now')) ON CONFLICT(ol_key) DO UPDATE SET"
                        " last_modified=excluded.last_modified, author_norm=excluded.author_norm,"
                        " canonical_name=excluded.canonical_name, dump_date=excluded.dump_date, updated_at=excluded.updated_at",
                        (ol_key, last_modified, norm, name, dump_date),
                    )
                    for alias_norm in list(rec.get("aliases_norm") or []):
                        author_db.execute(
                            "INSERT INTO author_aliases(alias_norm,author_norm,author_display,confidence,source,updated_at)"
                            " VALUES(?,?,?,?,?,strftime('%s','now')) ON CONFLICT(alias_norm) DO UPDATE SET"
                            " author_norm=excluded.author_norm, author_display=excluded.author_display,"
                            " confidence=excluded.confidence, source='dump', updated_at=excluded.updated_at",
                            (alias_norm, norm, alias_norm, 1.0, "dump"),
                        )
                    imported += 1
                    if line_no % 2000 == 0:
                        pct = min(99.0, 100.0 * fh.tell() / file_size)
                        self.progress.emit(
                            f"Line {line_no}: imported {imported}, skipped {skipped}", pct)
                        author_db.set_state("authors_dump_last_line", str(line_no))
                        author_db.commit()
                        author_db.begin()

            author_db.set_state("authors_dump_last_line", str(line_no))
            author_db.set_state("authors_dump_completed", "1")
            author_db.commit()
        except Exception as e:
            try:
                author_db.rollback()
            except Exception:
                pass
            self.finished.emit(False, f"Failed at line {line_no}: {e!r}")
            return
        self.finished.emit(
            True,
            f"Processed {os.path.basename(dump_path)}.\nImported/updated {imported}; skipped {skipped}.",
        )


class _ImportRedirectWorker(QObject):
    """Imports an OpenLibrary redirect dump file in a background thread."""
    progress = Signal(str, float)
    finished = Signal(bool, str)

    def __init__(self, author_db, folder: str):
        super().__init__()
        self.author_db = author_db
        self.folder = folder

    def run(self):
        self.progress.emit("Importing redirect dump…", -1.0)
        try:
            res = import_latest_redirect_dump(self.author_db, self.folder)
            if res.get("ok"):
                self.finished.emit(True, str(res.get("message") or "Done."))
            else:
                self.finished.emit(False, str(res.get("message") or "Failed."))
        except Exception as e:
            self.finished.emit(False, repr(e))


class _ImportProgressHandler(QObject):
    """Holds strong references to slots for import worker progress/finished signals."""

    def __init__(self, tab, thread: QThread, dlg: QProgressDialog, title: str):
        super().__init__(tab)
        self._tab = tab
        self._thread = thread
        self._dlg = dlg
        self._title = title

    def on_progress(self, msg: str, pct: float):
        if pct < 0:
            self._dlg.setRange(0, 0)
        else:
            if self._dlg.maximum() == 0:
                self._dlg.setRange(0, 100)
            self._dlg.setValue(int(pct))
        self._dlg.setLabelText(msg)
        if self._tab.on_activity_progress:
            self._tab.on_activity_progress(msg, max(0.0, pct))

    def on_finished(self, ok: bool, msg: str):
        if self._thread:
            self._thread.quit()
            self._thread.wait(5000)
        self._dlg.close()
        if self._tab.on_activity_progress:
            self._tab.on_activity_progress("Idle", -1.0)
        self._tab._import_thread = None
        self._tab._import_worker = None
        self._tab._import_handler = None
        if ok:
            QMessageBox.information(self._tab, self._title, msg)
        else:
            QMessageBox.warning(self._tab, self._title, msg)
        self._tab.refresh()

    def on_canceled(self):
        worker = self._tab._import_worker
        if worker and hasattr(worker, "request_stop"):
            worker.request_stop()


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
        self._import_thread: QThread | None = None
        self._import_worker = None
        self._import_handler = None

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

        b_import = QPushButton("Import author dump")
        b_import.clicked.connect(self.import_author_dump)
        btns2.addWidget(b_import)

        b_redirects = QPushButton("Import redirect dump")
        b_redirects.clicked.connect(self.import_redirect_dump)
        btns2.addWidget(b_redirects)

        b_purge = QPushButton("Purge redirect table")
        b_purge.clicked.connect(self.purge_redirects)
        btns2.addWidget(b_purge)

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

    def _start_import_worker(self, worker, title: str):
        """Start a background import worker with a modal progress dialog."""
        if self._import_thread and self._import_thread.isRunning():
            QMessageBox.information(self, title, "An import is already in progress.")
            return

        thread = QThread()
        self._import_thread = thread
        self._import_worker = worker

        dlg = QProgressDialog(title, "Cancel", 0, 100, self)
        dlg.setWindowModality(Qt.WindowModal)
        dlg.setMinimumDuration(0)
        dlg.setRange(0, 0)   # indeterminate until first progress signal arrives
        dlg.setAutoClose(False)
        dlg.setAutoReset(False)

        handler = _ImportProgressHandler(self, thread, dlg, title)
        self._import_handler = handler

        worker.progress.connect(handler.on_progress)
        worker.finished.connect(handler.on_finished)
        dlg.canceled.connect(handler.on_canceled)

        worker.moveToThread(thread)
        thread.started.connect(worker.run)
        thread.start()
        dlg.show()

    def _selected_items(self):
        if self.approved_list.selectedItems():
            return "approved", self.approved_list.selectedItems()
        if self.tentative_list.selectedItems():
            return "tentative", self.tentative_list.selectedItems()
        if self.invalid_list.selectedItems():
            return "invalid", self.invalid_list.selectedItems()
        return None, []

    def refresh(self):
        db = self.get_db()
        author_db = self.get_author_db()
        self.approved_list.clear()
        self.tentative_list.clear()
        self.invalid_list.clear()

        approved = []
        invalid = []
        if author_db:
            approved = author_db.query_all("SELECT normalized_name, COALESCE(preferred_name, canonical_name) AS shown, canonical_name, frequency FROM known_authors ORDER BY frequency DESC, canonical_name")
            invalid = author_db.query_all("SELECT normalized_name, canonical_name FROM invalid_authors ORDER BY canonical_name")

        tentative = []
        if db:
            tentative = db.query_all("SELECT normalized_name, COALESCE(preferred_name, canonical_name) AS shown, canonical_name, frequency, confidence FROM tentative_authors ORDER BY frequency DESC, canonical_name")

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

        no_author_db = " (no authors.db)" if not author_db else ""
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
        if which == "approved":
            if not author_db:
                QMessageBox.warning(self, "Authors", "No author DB connected.")
                return
            author_db.execute("DELETE FROM known_authors")
        elif which == "invalid":
            if not author_db:
                QMessageBox.warning(self, "Authors", "No author DB connected.")
                return
            author_db.execute("DELETE FROM invalid_authors")
        elif which == "tentative":
            if not db:
                return
            db.execute("DELETE FROM tentative_authors")
        self.refresh()

    def mark_reanalyze(self):
        db = self.get_db()
        if not db:
            return
        db.set_state("author_db_dirty", "1")
        db.set_state("analyze_authors_completed", "0")
        QMessageBox.information(self, "Authors", "Author DB marked dirty. Run Analyze Authors to rebuild.")

    def import_author_dump(self):
        author_db = self.get_author_db()
        if not author_db:
            QMessageBox.warning(self, "Author dump", "No author DB connected. Open an author DB first.")
            return

        folder = os.path.dirname(author_db.db_path) or os.getcwd()
        dump_path, dump_date = discover_latest_author_dump_file(folder)
        if not dump_path or not dump_date:
            QMessageBox.warning(self, "Author dump", f"No {DUMP_PREFIX}YYYY-MM-DD.txt found in\n{folder}")
            return

        prev_date = author_db.get_state("authors_dump_last_date", "") or ""
        prev_file = author_db.get_state("authors_dump_last_file", "") or ""
        prev_done = author_db.get_state("authors_dump_completed", "0") == "1"

        restart = False
        start_line = 0
        if prev_done and prev_date and dump_date > prev_date:
            restart = True
            start_line = 0
        elif prev_file and os.path.normcase(prev_file) == os.path.normcase(dump_path):
            start_line = max(0, int(author_db.get_state("authors_dump_last_line", "0") or "0"))

        worker = _ImportDumpWorker(author_db, dump_path, dump_date, start_line, restart)
        self._start_import_worker(worker, "Author dump import")

    def import_redirect_dump(self):
        author_db = self.get_author_db()
        if not author_db:
            QMessageBox.warning(self, "Redirect dump", "No author DB connected.")
            return
        folder = os.path.dirname(author_db.db_path) or os.getcwd()
        worker = _ImportRedirectWorker(author_db, folder)
        self._start_import_worker(worker, "Redirect dump import")

    def purge_redirects(self):
        author_db = self.get_author_db()
        if not author_db:
            QMessageBox.warning(self, "Purge redirects", "No author DB connected.")
            return
        if QMessageBox.question(
            self, "Purge redirect table",
            "Delete all rows from ol_author_redirects?\n\nDo this only after redirect migration is complete.\nAliases are preserved in author_aliases."
        ) != QMessageBox.Yes:
            return
        author_db.execute("DELETE FROM ol_author_redirects")
        QMessageBox.information(self, "Purge redirects", "ol_author_redirects cleared.")
        self.refresh()
