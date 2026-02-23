from __future__ import annotations
import sqlite3
import os
import time
from typing import Callable, Iterable, Optional

SCHEMA_VERSION = 4

MEMORY_PROFILES = {
    "safe": {
        "cache_size": -131072,      # 128 MiB
        "mmap_size": 268435456,     # 256 MiB
        "wal_autocheckpoint": 2000,
        "synchronous": "NORMAL",
    },
    "balanced": {
        "cache_size": -524288,      # 512 MiB
        "mmap_size": 1073741824,    # 1 GiB
        "wal_autocheckpoint": 8000,
        "synchronous": "NORMAL",
    },
    "extreme": {
        "cache_size": -2097152,     # 2 GiB
        "mmap_size": 4294967296,    # 4 GiB
        "wal_autocheckpoint": 20000,
        "synchronous": "NORMAL",
    },
    "extreme+": {
        "cache_size": -8388608,      # 8 GiB
        "mmap_size": 17179869184,    # 16 GiB
        "wal_autocheckpoint": 120000,
        "synchronous": "NORMAL",
    },
}

SCHEMA = r"""
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
PRAGMA temp_store=MEMORY;

CREATE TABLE IF NOT EXISTS roots(
  id INTEGER PRIMARY KEY,
  path TEXT UNIQUE NOT NULL,
  enabled INTEGER NOT NULL DEFAULT 1,
  label TEXT,
  added_at INTEGER NOT NULL,
  last_scan_run_id INTEGER
);

CREATE TABLE IF NOT EXISTS folders(
  id INTEGER PRIMARY KEY,
  root_id INTEGER NOT NULL,
  path TEXT UNIQUE NOT NULL,
  fingerprint_dir_mtime INTEGER,
  fingerprint_entry_count INTEGER,
  fingerprint_sum_child_sizes INTEGER,
  fingerprint_max_child_mtime INTEGER,
  last_seen_scan_id INTEGER,
  force_rescan INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS files(
  id INTEGER PRIMARY KEY,
  root_id INTEGER NOT NULL,
  folder_path TEXT NOT NULL,
  path TEXT UNIQUE NOT NULL,
  name TEXT NOT NULL,
  ext TEXT NOT NULL,
  size INTEGER NOT NULL,
  mtime_ns INTEGER NOT NULL,
  ctime_ns INTEGER NOT NULL,
  is_archive INTEGER NOT NULL DEFAULT 0,
  inner_ext_guess TEXT,

  author TEXT,
  series TEXT,
  series_index REAL,
  title TEXT,
  tags TEXT,

  author_norm TEXT,
  series_norm TEXT,
  title_norm TEXT,
  work_key TEXT,

  last_seen_scan_id INTEGER
);

CREATE TABLE IF NOT EXISTS works(
  work_key TEXT PRIMARY KEY,
  author_norm TEXT,
  series_norm TEXT,
  series_index_norm TEXT,
  title_norm TEXT,
  display_author TEXT,
  display_series TEXT,
  display_series_index TEXT,
  display_title TEXT
);

CREATE TABLE IF NOT EXISTS filetypes(
  ext TEXT PRIMARY KEY,
  count_total INTEGER NOT NULL DEFAULT 0,
  count_archives_guess INTEGER NOT NULL DEFAULT 0,
  last_seen_scan_id INTEGER
);

CREATE TABLE IF NOT EXISTS scan_runs(
  id INTEGER PRIMARY KEY,
  started_at INTEGER NOT NULL,
  finished_at INTEGER,
  status TEXT NOT NULL,
  stats_json TEXT
);

CREATE TABLE IF NOT EXISTS state(
  key TEXT PRIMARY KEY,
  value TEXT
);

CREATE TABLE IF NOT EXISTS deletion_queue(
  id INTEGER PRIMARY KEY,
  work_key TEXT NOT NULL,
  file_id INTEGER NOT NULL,
  checked INTEGER NOT NULL DEFAULT 1,
  reason TEXT,
  created_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_files_work_key ON files(work_key);
CREATE INDEX IF NOT EXISTS idx_files_ext ON files(ext);
CREATE INDEX IF NOT EXISTS idx_files_folder_path ON files(folder_path);
CREATE INDEX IF NOT EXISTS idx_files_root_mtime ON files(root_id, mtime_ns);
CREATE INDEX IF NOT EXISTS idx_folders_root ON folders(root_id);
"""

class DB:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path, isolation_level=None, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self._io_callbacks: list[Callable[[str, bool], None]] = []
        self._init_schema()
        self.apply_memory_profile(self.get_state("memory_profile", "balanced") or "balanced")

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
        for stmt in SCHEMA.split(";"):
            s = stmt.strip()
            if s:
                cur.execute(s + ";")
        self._migrate_schema()

    def _migrate_schema(self):
        raw = self.get_state("schema_version", "1")
        try:
            version = int(raw or "1")
        except Exception:
            version = 1

        if version < 2:
            self.execute(
                """
                CREATE TABLE IF NOT EXISTS author_aliases(
                  alias_norm TEXT PRIMARY KEY,
                  author_norm TEXT NOT NULL,
                  author_display TEXT NOT NULL,
                  confidence REAL NOT NULL DEFAULT 1.0,
                  source TEXT NOT NULL DEFAULT 'derived',
                  updated_at INTEGER NOT NULL
                )
                """
            )
            self.execute("CREATE INDEX IF NOT EXISTS idx_author_aliases_author_norm ON author_aliases(author_norm)")
            version = 2

        if version < 3:
            self.execute(
                """
                CREATE TABLE IF NOT EXISTS known_authors(
                  normalized_name TEXT PRIMARY KEY,
                  canonical_name TEXT NOT NULL,
                  frequency INTEGER NOT NULL DEFAULT 0,
                  created_at INTEGER NOT NULL,
                  updated_at INTEGER NOT NULL
                )
                """
            )
            self.execute(
                """
                CREATE TABLE IF NOT EXISTS author_variants(
                  normalized_name TEXT NOT NULL,
                  variant_text TEXT NOT NULL,
                  frequency INTEGER NOT NULL DEFAULT 0,
                  updated_at INTEGER NOT NULL,
                  PRIMARY KEY(normalized_name, variant_text)
                )
                """
            )
            self.execute("CREATE INDEX IF NOT EXISTS idx_known_authors_frequency ON known_authors(frequency)")
            self.execute("CREATE INDEX IF NOT EXISTS idx_author_variants_norm ON author_variants(normalized_name)")
            version = 3

        if version < 4:
            self.execute(
                """
                CREATE TABLE IF NOT EXISTS invalid_authors(
                  normalized_name TEXT PRIMARY KEY,
                  canonical_name TEXT NOT NULL,
                  reason TEXT,
                  updated_at INTEGER NOT NULL
                )
                """
            )
            self.execute("CREATE INDEX IF NOT EXISTS idx_invalid_authors_name ON invalid_authors(canonical_name)")
            version = 4

        self.set_state("schema_version", str(max(version, SCHEMA_VERSION)))


    def apply_memory_profile(self, profile: str):
        profile = (profile or "balanced").strip().lower()
        if profile not in MEMORY_PROFILES:
            profile = "balanced"
        cfg = MEMORY_PROFILES[profile]
        self.execute(f"PRAGMA synchronous={cfg['synchronous']};")
        self.execute("PRAGMA temp_store=MEMORY;")
        self.execute("PRAGMA cache_spill=OFF;")
        # SQLite PRAGMA statements do not support parameter placeholders here.
        # Use validated integer values from MEMORY_PROFILES to avoid syntax errors.
        self.execute(f"PRAGMA cache_size={int(cfg['cache_size'])};")
        self.execute(f"PRAGMA mmap_size={int(cfg['mmap_size'])};")
        self.execute(f"PRAGMA wal_autocheckpoint={int(cfg['wal_autocheckpoint'])};")
        self.set_state("memory_profile", profile)

    def memory_profile(self) -> str:
        p = (self.get_state("memory_profile", "balanced") or "balanced").strip().lower()
        return p if p in MEMORY_PROFILES else "balanced"


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

    def create_timestamped_backup(self, label: str = "manual") -> str:
        ts = int(time.time())
        backup_path = f"{self.db_path}.{label}.{ts}.bak"
        self.backup_to(backup_path)
        self.set_state("last_backup_path", backup_path)
        self.set_state("last_backup_ts", str(ts))
        return backup_path

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
