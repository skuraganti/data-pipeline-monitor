"""SQLite persistence and forward migrations for Relay."""

from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

SCHEMA_VERSION = 2
SCHEMA = """
CREATE TABLE IF NOT EXISTS schema_version(
  version INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS settings(
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS sources(
  id INTEGER PRIMARY KEY,
  name TEXT NOT NULL,
  sha256 TEXT NOT NULL,
  bytes INTEGER NOT NULL,
  rows INTEGER NOT NULL,
  url TEXT,
  retrieved_at TEXT,
  registered_at REAL NOT NULL,
  UNIQUE(name,
  sha256)
);
CREATE TABLE IF NOT EXISTS runs(
  id INTEGER PRIMARY KEY,
  state TEXT NOT NULL,
  trigger TEXT NOT NULL,
  simulated INTEGER NOT NULL,
  fault_mode TEXT,
  fault_fired INTEGER NOT NULL DEFAULT 0,
  source_sha TEXT NOT NULL,
  lookup_sha TEXT NOT NULL,
  partition TEXT NOT NULL,
  identity TEXT NOT NULL,
  created_at REAL NOT NULL,
  started_at REAL,
  finished_at REAL,
  attempts INTEGER NOT NULL DEFAULT 0,
  max_attempts INTEGER NOT NULL DEFAULT 3,
  cursor INTEGER NOT NULL DEFAULT 0,
  total_rows INTEGER NOT NULL,
  accepted INTEGER NOT NULL DEFAULT 0,
  rejected INTEGER NOT NULL DEFAULT 0,
  warning_rows INTEGER NOT NULL DEFAULT 0,
  next_retry REAL,
  error TEXT,
  published INTEGER NOT NULL DEFAULT 0,
  retry_cycle INTEGER NOT NULL DEFAULT 0
);
CREATE UNIQUE INDEX IF NOT EXISTS runs_identity_normal ON runs(identity) WHERE simulated=0;
CREATE TABLE IF NOT EXISTS attempts(
  run_id INTEGER NOT NULL,
  cycle INTEGER NOT NULL,
  number INTEGER NOT NULL,
  started_at REAL NOT NULL,
  finished_at REAL,
  state TEXT NOT NULL,
  error TEXT,
  resume_cursor INTEGER NOT NULL,
  PRIMARY KEY(run_id,
  cycle,
  number)
);
CREATE TABLE IF NOT EXISTS events(
  id INTEGER PRIMARY KEY,
  run_id INTEGER,
  at REAL NOT NULL,
  kind TEXT NOT NULL,
  message TEXT NOT NULL,
  cursor INTEGER
);
CREATE TABLE IF NOT EXISTS quality(
  run_id INTEGER NOT NULL,
  code TEXT NOT NULL,
  severity TEXT NOT NULL,
  count INTEGER NOT NULL,
  PRIMARY KEY(run_id,
  code)
);
CREATE TABLE IF NOT EXISTS examples(
  id INTEGER PRIMARY KEY,
  run_id INTEGER NOT NULL,
  row_index INTEGER NOT NULL,
  codes TEXT NOT NULL,
  severity TEXT NOT NULL,
  observed TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS quarantine(
  run_id INTEGER NOT NULL,
  row_index INTEGER NOT NULL,
  codes TEXT NOT NULL,
  observed TEXT NOT NULL,
  PRIMARY KEY(run_id,
  row_index)
);
CREATE TABLE IF NOT EXISTS staging_daily(
  run_id INTEGER NOT NULL,
  day TEXT NOT NULL,
  borough TEXT NOT NULL,
  trips INTEGER NOT NULL,
  net_cents INTEGER NOT NULL,
  warning_rows INTEGER NOT NULL,
  PRIMARY KEY(run_id,
  day,
  borough)
);
CREATE TABLE IF NOT EXISTS publications(
  singleton INTEGER PRIMARY KEY CHECK(singleton=1),
  run_id INTEGER NOT NULL,
  published_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS alerts(
  id INTEGER PRIMARY KEY,
  run_id INTEGER NOT NULL,
  message TEXT NOT NULL,
  created_at REAL NOT NULL,
  acknowledged INTEGER NOT NULL DEFAULT 0,
  simulated INTEGER NOT NULL
);
"""


class Database:
    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.initialize()

    def connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=10)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA journal_mode=WAL")
        return connection

    @contextmanager
    def transaction(self, *, immediate: bool = False) -> Iterator[sqlite3.Connection]:
        connection = self.connect()
        try:
            connection.execute("BEGIN IMMEDIATE" if immediate else "BEGIN")
            yield connection
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()

    def initialize(self) -> None:
        connection = self.connect()
        try:
            existing_version = connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='schema_version'"
            ).fetchone()
            is_new = existing_version is None
            connection.executescript(SCHEMA)
            version = connection.execute("SELECT version FROM schema_version").fetchone()
            if version is None:
                connection.execute("INSERT INTO schema_version VALUES (?)", (SCHEMA_VERSION,))
            elif version["version"] not in {1, SCHEMA_VERSION}:
                raise RuntimeError(f"unsupported database schema version {version['version']}")
            columns = {row["name"] for row in connection.execute("PRAGMA table_info(runs)")}
            if "fault_fired" not in columns:
                connection.execute(
                    "ALTER TABLE runs ADD COLUMN fault_fired INTEGER NOT NULL DEFAULT 0"
                )
            if version is not None and version["version"] == 1:
                connection.execute("UPDATE schema_version SET version=?", (SCHEMA_VERSION,))
                connection.execute(
                    "INSERT OR IGNORE INTO quarantine(run_id,row_index,codes,observed) "
                    "SELECT run_id,row_index,codes,observed FROM examples WHERE severity='error'"
                )
            defaults = {
                "schedule_enabled": "1",
                "scan_interval": "300",
                "last_scan": "",
                "next_scan": "",
                "unchanged_scans": "0",
                "last_scan_error": "",
                "schedule_initialized": "0" if is_new else "1",
            }
            connection.executemany(
                "INSERT OR IGNORE INTO settings(key,value) VALUES (?,?)", defaults.items()
            )
            connection.commit()
        finally:
            connection.close()

    def rows(self, sql: str, parameters: tuple = ()) -> list[dict]:
        connection = self.connect()
        try:
            return [dict(row) for row in connection.execute(sql, parameters)]
        finally:
            connection.close()

    def row(self, sql: str, parameters: tuple = ()) -> dict | None:
        rows = self.rows(sql, parameters)
        return rows[0] if rows else None

    def value(self, key: str) -> str:
        row = self.row("SELECT value FROM settings WHERE key=?", (key,))
        if row is None:
            raise KeyError(key)
        return row["value"]

    def set_values(self, values: dict[str, str]) -> None:
        with self.transaction(immediate=True) as connection:
            connection.executemany(
                "INSERT INTO settings(key,value) VALUES (?,?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                values.items(),
            )
