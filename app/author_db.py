from __future__ import annotations
import sqlite3
import os
from typing import Callable, Iterable, Optional

AUTHOR_DB_SCHEMA_VERSION = 1

AUTHOR_DB_SCHEMA = r"""
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
PRAGMA temp_store=MEMORY;

CREATE TABLE IF NOT EXISTS state(
  key TEXT PRIMARY KEY,
  value TEXT
);

CREATE TABLE IF NOT EXISTS known_authors(
  normalized_name TEXT PRIMARY KEY,
  canonical_name TEXT NOT NULL,
  preferred_name TEXT,
  frequency INTEGER NOT NULL DEFAULT 0,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS invalid_authors(
  normalized_name TEXT PRIMARY KEY,
  canonical_name TEXT NOT NULL,
  reason TEXT,
  updated_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS author_aliases(
  alias_norm TEXT PRIMARY KEY,
  author_norm TEXT NOT NULL,
  author_display TEXT NOT NULL,
  confidence REAL NOT NULL DEFAULT 1.0,
  source TEXT NOT NULL DEFAULT 'manual',
  source_key TEXT,
  source_project TEXT,
  updated_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS author_dump_records(
  ol_key TEXT PRIMARY KEY,
  last_modified TEXT NOT NULL,
  author_norm TEXT NOT NULL,
  canonical_name TEXT NOT NULL,
  dump_date TEXT NOT NULL,
  updated_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS author_dump_imported(
  normalized_name TEXT PRIMARY KEY,
  dump_date TEXT NOT NULL,
  updated_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS ol_import_runs(
  id INTEGER PRIMARY KEY,
  import_type TEXT NOT NULL,
  dump_date TEXT NOT NULL,
  dump_filename TEXT NOT NULL,
  status TEXT NOT NULL,
  started_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  completed_at INTEGER,
  progress_line INTEGER NOT NULL DEFAULT 0,
  rows_processed INTEGER NOT NULL DEFAULT 0,
  redirects_stored INTEGER NOT NULL DEFAULT 0,
  aliases_added INTEGER NOT NULL DEFAULT 0,
  errors_count INTEGER NOT NULL DEFAULT 0,
  last_error TEXT
);

CREATE TABLE IF NOT EXISTS ol_author_redirects(
  from_key TEXT PRIMARY KEY,
  to_key TEXT NOT NULL,
  to_key_resolved TEXT,
  dump_date TEXT NOT NULL,
  last_modified TEXT,
  revision INTEGER,
  updated_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_known_authors_frequency ON known_authors(frequency);
CREATE INDEX IF NOT EXISTS idx_invalid_authors_name ON invalid_authors(canonical_name);
CREATE INDEX IF NOT EXISTS idx_author_aliases_author_norm ON author_aliases(author_norm);
CREATE INDEX IF NOT EXISTS idx_author_dump_records_author_norm ON author_dump_records(author_norm);
CREATE INDEX IF NOT EXISTS idx_author_dump_records_dump_date ON author_dump_records(dump_date);
CREATE INDEX IF NOT EXISTS idx_author_dump_imported_date ON author_dump_imported(dump_date);
CREATE INDEX IF NOT EXISTS idx_ol_import_runs_type_date ON ol_import_runs(import_type,dump_date);
CREATE INDEX IF NOT EXISTS idx_ol_author_redirects_dump_date ON ol_author_redirects(dump_date);
"""


class AuthorDB:
    """Shared author database (authors.db), independent of any specific project.

    Holds curated author data: approved known authors, invalid authors, OL-sourced
    aliases, and OpenLibrary dump ingestion tables. Written to only during OL dump
    ingestion and manual author review actions; read by scanner and analyzer.
    """

    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path, isolation_level=None, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self._io_callbacks: list[Callable[[str, bool], None]] = []
        self._init_schema()

    def add_io_callback(self, callback: Callable[[str, bool], None]):
        if callback not in self._io_callbacks:
            self._io_callbacks.append(callback)

    def remove_io_callback(self, callback: Callable[[str, bool], None]):
        self._io_callbacks = [cb for cb in self._io_callbacks if cb is not callback]

    def _emit_io(self, operation: str, active: bool):
        for cb in list(self._io_callbacks):
            try:
                cb(operation, active)
            except Exception:
                continue

    def _operation_kind(self, sql: str) -> str:
        head = (sql or "").strip().split(None, 1)
        token = head[0].upper() if head else ""
        if token in ("SELECT", "PRAGMA"):
            return "read"
        return "write"

    def _init_schema(self):
        cur = self.conn.cursor()
        for stmt in AUTHOR_DB_SCHEMA.split(";"):
            s = stmt.strip()
            if s:
                cur.execute(s + ";")
        raw = self.get_state("schema_version", "0")
        try:
            version = int(raw or "0")
        except Exception:
            version = 0
        if version < AUTHOR_DB_SCHEMA_VERSION:
            self.set_state("schema_version", str(AUTHOR_DB_SCHEMA_VERSION))

    def close(self):
        self.conn.close()

    def execute(self, sql: str, params: tuple = ()):
        op = self._operation_kind(sql)
        self._emit_io(op, True)
        try:
            return self.conn.execute(sql, params)
        finally:
            self._emit_io(op, False)

    def executemany(self, sql: str, seq: Iterable[tuple]):
        op = self._operation_kind(sql)
        self._emit_io(op, True)
        try:
            return self.conn.executemany(sql, seq)
        finally:
            self._emit_io(op, False)

    def query_one(self, sql: str, params: tuple = ()):
        self._emit_io("read", True)
        try:
            cur = self.conn.execute(sql, params)
            return cur.fetchone()
        finally:
            self._emit_io("read", False)

    def query_all(self, sql: str, params: tuple = ()):
        self._emit_io("read", True)
        try:
            cur = self.conn.execute(sql, params)
            return cur.fetchall()
        finally:
            self._emit_io("read", False)

    def set_state(self, key: str, value: str):
        self.conn.execute(
            "INSERT INTO state(key,value) VALUES(?,?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, value),
        )

    def get_state(self, key: str, default: Optional[str] = None) -> Optional[str]:
        row = self.query_one("SELECT value FROM state WHERE key=?", (key,))
        return row["value"] if row else default

    def begin(self):
        self._emit_io("write", True)
        try:
            self.conn.execute("BEGIN;")
        finally:
            self._emit_io("write", False)

    def commit(self):
        self._emit_io("write", True)
        try:
            self.conn.execute("COMMIT;")
        finally:
            self._emit_io("write", False)

    def rollback(self):
        self._emit_io("write", True)
        try:
            self.conn.execute("ROLLBACK;")
        finally:
            self._emit_io("write", False)

    def backup_to(self, backup_path: str):
        parent = os.path.dirname(backup_path)
        if parent:
            os.makedirs(parent, exist_ok=True)
        self._emit_io("read", True)
        self._emit_io("write", True)
        dst = sqlite3.connect(backup_path)
        try:
            self.conn.backup(dst)
        finally:
            dst.close()
            self._emit_io("read", False)
            self._emit_io("write", False)
