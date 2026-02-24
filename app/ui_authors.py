from __future__ import annotations
import os
import time

from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QPushButton, QLabel, QListWidget, QListWidgetItem,
    QMessageBox, QAbstractItemView, QDialog, QFormLayout, QLineEdit, QTextEdit, QDialogButtonBox
)

from .util import normalize_text, discover_latest_author_dump_file, parse_author_dump_line
from .ol_redirects import import_latest_redirect_dump


DUMP_PREFIX = "ol_dump_authors_"

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
    def __init__(self, get_db):
        super().__init__()
        self.get_db = get_db

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
        left.addWidget(QLabel("Approved"))
        left.addWidget(self.approved_list, 1)
        mid = QVBoxLayout()
        mid.addWidget(QLabel("Tentative"))
        mid.addWidget(self.tentative_list, 1)
        right = QVBoxLayout()
        right.addWidget(QLabel("Invalid"))
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
        db = self.get_db()
        if not db:
            return
        self.approved_list.clear()
        self.tentative_list.clear()
        self.invalid_list.clear()

        approved = db.query_all("SELECT normalized_name, COALESCE(preferred_name, canonical_name) AS shown, canonical_name, frequency FROM known_authors ORDER BY frequency DESC, canonical_name")
        tentative = db.query_all("SELECT normalized_name, COALESCE(preferred_name, canonical_name) AS shown, canonical_name, frequency, confidence FROM tentative_authors ORDER BY frequency DESC, canonical_name")
        invalid = db.query_all("SELECT normalized_name, canonical_name FROM invalid_authors ORDER BY canonical_name")

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

        self.top.setText(f"Author DB: approved={len(approved)} tentative={len(tentative)} invalid={len(invalid)}")

    def _aliases_for(self, norm: str):
        db = self.get_db()
        if not db:
            return []
        rows = db.query_all("SELECT author_display, confidence FROM author_aliases WHERE author_norm=? ORDER BY author_display", (norm,))
        return [(str(r["author_display"]), float(r["confidence"] or 0.0)) for r in rows]

    def edit_selected_author(self, list_widget: QListWidget, source: str):
        db = self.get_db()
        if not db:
            return
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
        db.begin()
        try:
            if source == "approved":
                db.execute("DELETE FROM known_authors WHERE normalized_name=?", (norm,))
                db.execute(
                    "INSERT INTO known_authors(normalized_name,canonical_name,preferred_name,frequency,created_at,updated_at) VALUES(?,?,?,?,strftime('%s','now'),strftime('%s','now')) ON CONFLICT(normalized_name) DO UPDATE SET canonical_name=excluded.canonical_name, preferred_name=excluded.preferred_name, updated_at=excluded.updated_at",
                    (norm2, canonical2, preferred2, 1),
                )
            elif source == "tentative":
                db.execute("DELETE FROM tentative_authors WHERE normalized_name=?", (norm,))
                db.execute(
                    "INSERT INTO tentative_authors(normalized_name,canonical_name,preferred_name,frequency,confidence,created_at,updated_at) VALUES(?,?,?,?,?,strftime('%s','now'),strftime('%s','now')) ON CONFLICT(normalized_name) DO UPDATE SET canonical_name=excluded.canonical_name, preferred_name=excluded.preferred_name, confidence=excluded.confidence, updated_at=excluded.updated_at",
                    (norm2, canonical2, preferred2, 1, conf2),
                )

            db.execute("DELETE FROM author_aliases WHERE author_norm=?", (norm,))
            for alias, c in aliases:
                alias_norm = normalize_text(alias)
                if not alias_norm:
                    continue
                db.execute(
                    "INSERT INTO author_aliases(alias_norm,author_norm,author_display,confidence,source,updated_at) VALUES(?,?,?,?,?,?) ON CONFLICT(alias_norm) DO UPDATE SET author_norm=excluded.author_norm, author_display=excluded.author_display, confidence=excluded.confidence, source='manual', updated_at=excluded.updated_at",
                    (alias_norm, norm2, alias, float(c), "manual", int(time.time())),
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
        db = self.get_db()
        if not db:
            return
        name, ok = QInputDialog.getText(self, "Add author", "Preferred/Cannonical author name:")
        if not ok or not name.strip():
            return
        target, ok2 = QInputDialog.getItem(self, "Target list", "Add to:", ["approved", "tentative", "invalid"], 0, False)
        if not ok2:
            return
        name = name.strip()
        norm = normalize_text(name)
        if not norm:
            return
        db.begin()
        try:
            if target == "approved":
                db.execute(
                    "INSERT INTO known_authors(normalized_name,canonical_name,preferred_name,frequency,created_at,updated_at) VALUES(?,?,?,?,strftime('%s','now'),strftime('%s','now')) ON CONFLICT(normalized_name) DO UPDATE SET canonical_name=excluded.canonical_name, preferred_name=excluded.preferred_name, updated_at=excluded.updated_at",
                    (norm, name, name, 1),
                )
            elif target == "tentative":
                db.execute(
                    "INSERT INTO tentative_authors(normalized_name,canonical_name,preferred_name,frequency,confidence,created_at,updated_at) VALUES(?,?,?,?,?,strftime('%s','now'),strftime('%s','now')) ON CONFLICT(normalized_name) DO UPDATE SET canonical_name=excluded.canonical_name, preferred_name=excluded.preferred_name, updated_at=excluded.updated_at",
                    (norm, name, name, 1, 1.0),
                )
            else:
                db.execute(
                    "INSERT INTO invalid_authors(normalized_name,canonical_name,reason,updated_at) VALUES(?,?,?,strftime('%s','now')) ON CONFLICT(normalized_name) DO UPDATE SET canonical_name=excluded.canonical_name, updated_at=excluded.updated_at",
                    (norm, name, "manual"),
                )
            db.commit()
        except Exception as e:
            db.rollback()
            QMessageBox.critical(self, "Authors", f"Failed: {e!r}")
            return
        self.refresh()

    def move_selected(self, target: str):
        db = self.get_db()
        if not db:
            return
        source, items = self._selected_items()
        if not items or not source:
            return

        db.begin()
        try:
            for it in items:
                norm, canonical, preferred, conf = it.data(256)
                norm = str(norm)
                canonical = str(canonical)
                preferred = str(preferred)
                conf = float(conf or 0.0)

                db.execute("DELETE FROM known_authors WHERE normalized_name=?", (norm,))
                db.execute("DELETE FROM tentative_authors WHERE normalized_name=?", (norm,))
                db.execute("DELETE FROM invalid_authors WHERE normalized_name=?", (norm,))

                if target == "approved":
                    db.execute(
                        "INSERT INTO known_authors(normalized_name,canonical_name,preferred_name,frequency,created_at,updated_at) VALUES(?,?,?,?,strftime('%s','now'),strftime('%s','now'))",
                        (norm, canonical, preferred or canonical, 1),
                    )
                elif target == "tentative":
                    db.execute(
                        "INSERT INTO tentative_authors(normalized_name,canonical_name,preferred_name,frequency,confidence,created_at,updated_at) VALUES(?,?,?,?,?,strftime('%s','now'),strftime('%s','now'))",
                        (norm, canonical, preferred or canonical, 1, conf),
                    )
                else:
                    db.execute(
                        "INSERT INTO invalid_authors(normalized_name,canonical_name,reason,updated_at) VALUES(?,?,?,strftime('%s','now'))",
                        (norm, canonical, f"moved-from-{source}"),
                    )
            db.commit()
        except Exception as e:
            db.rollback()
            QMessageBox.critical(self, "Authors", f"Failed: {e!r}")
            return
        self.refresh()

    def clear_list(self, which: str):
        db = self.get_db()
        if not db:
            return
        if QMessageBox.question(self, "Clear authors", f"Clear {which} authors?") != QMessageBox.Yes:
            return
        table = {"approved": "known_authors", "tentative": "tentative_authors", "invalid": "invalid_authors"}[which]
        db.execute(f"DELETE FROM {table}")
        self.refresh()

    def mark_reanalyze(self):
        db = self.get_db()
        if not db:
            return
        db.set_state("author_db_dirty", "1")
        db.set_state("analyze_authors_completed", "0")
        QMessageBox.information(self, "Authors", "Author DB marked dirty. Run Analyze Authors to rebuild.")

    def import_author_dump(self):
        db = self.get_db()
        if not db:
            return

        folder = os.path.dirname(db.db_path) or os.getcwd()
        dump_path, dump_date = discover_latest_author_dump_file(folder)
        if not dump_path or not dump_date:
            QMessageBox.warning(self, "Author dump", f"No {DUMP_PREFIX}YYYY-MM-DD.txt found in\n{folder}")
            return

        prev_date = db.get_state("authors_dump_last_date", "") or ""
        prev_file = db.get_state("authors_dump_last_file", "") or ""
        prev_done = db.get_state("authors_dump_completed", "0") == "1"
        prev_line = int(db.get_state("authors_dump_last_line", "0") or "0")

        restart = False
        start_line = 0
        if prev_done and prev_date and dump_date > prev_date:
            restart = True
            start_line = 0
        elif prev_file and os.path.normcase(prev_file) == os.path.normcase(dump_path):
            start_line = max(0, prev_line)
        else:
            start_line = 0

        imported = 0
        skipped = 0

        try:
            db.begin()
            db.set_state("authors_dump_completed", "0")
            db.set_state("authors_dump_last_file", dump_path)
            db.set_state("authors_dump_last_date", dump_date)

            if restart:
                db.set_state("authors_dump_last_line", "0")

            line_no = 0
            with open(dump_path, "r", encoding="utf-8", errors="replace") as fh:
                for line in fh:
                    line_no += 1
                    if line_no <= start_line:
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
                    prev = db.query_one("SELECT last_modified FROM author_dump_records WHERE ol_key=?", (ol_key,))
                    if prev and str(prev["last_modified"] or "") == last_modified:
                        skipped += 1
                        continue

                    name = str(rec["canonical_name"])
                    db.execute(
                        "INSERT INTO known_authors(normalized_name,canonical_name,preferred_name,frequency,created_at,updated_at) VALUES(?,?,?,?,strftime('%s','now'),strftime('%s','now')) ON CONFLICT(normalized_name) DO UPDATE SET canonical_name=excluded.canonical_name, preferred_name=excluded.preferred_name, updated_at=excluded.updated_at",
                        (norm, name, name, 1),
                    )
                    db.execute(
                        "INSERT INTO author_dump_imported(normalized_name,dump_date,updated_at) VALUES(?,?,strftime('%s','now')) ON CONFLICT(normalized_name) DO UPDATE SET dump_date=excluded.dump_date, updated_at=excluded.updated_at",
                        (norm, dump_date),
                    )
                    db.execute(
                        "INSERT INTO author_dump_records(ol_key,last_modified,author_norm,canonical_name,dump_date,updated_at) VALUES(?,?,?,?,?,strftime('%s','now')) ON CONFLICT(ol_key) DO UPDATE SET last_modified=excluded.last_modified, author_norm=excluded.author_norm, canonical_name=excluded.canonical_name, dump_date=excluded.dump_date, updated_at=excluded.updated_at",
                        (ol_key, last_modified, norm, name, dump_date),
                    )

                    for alias_norm in list(rec.get("aliases_norm") or []):
                        db.execute(
                            "INSERT INTO author_aliases(alias_norm,author_norm,author_display,confidence,source,updated_at) VALUES(?,?,?,?,?,strftime('%s','now')) ON CONFLICT(alias_norm) DO UPDATE SET author_norm=excluded.author_norm, author_display=excluded.author_display, confidence=excluded.confidence, source='dump', updated_at=excluded.updated_at",
                            (alias_norm, norm, alias_norm, 1.0, "dump"),
                        )

                    imported += 1

                    if line_no % 2000 == 0:
                        db.set_state("authors_dump_last_line", str(line_no))
                        db.commit()
                        db.begin()

            db.set_state("authors_dump_last_line", str(line_no))
            db.set_state("authors_dump_completed", "1")
            db.commit()
        except Exception as e:
            db.rollback()
            QMessageBox.warning(self, "Author dump", f"Failed at line {line_no}: {e!r}")
            return

        QMessageBox.information(
            self,
            "Author dump",
            f"Processed dump {os.path.basename(dump_path)}. Imported/updated {imported} author(s); skipped {skipped}."
        )
        self.refresh()


    def import_redirect_dump(self):
        db = self.get_db()
        if not db:
            return
        folder = os.path.dirname(db.db_path) or os.getcwd()
        res = import_latest_redirect_dump(db, folder)
        if not res.get("ok"):
            QMessageBox.warning(self, "Redirect dump", str(res.get("message") or "Failed."))
            return
        QMessageBox.information(self, "Redirect dump", str(res.get("message") or "Done."))
        self.refresh()
