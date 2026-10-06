from __future__ import annotations

import json
import os
import sqlite3
import threading
from contextlib import AbstractContextManager
from datetime import datetime
from pathlib import Path
from typing import Any

from .core import SafetyError


class InstanceLock(AbstractContextManager):
    def __init__(self, path: Path):
        self.path = path
        self.handle = None

    def __enter__(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.handle = self.path.open("a+b")
        self.handle.seek(0, os.SEEK_END)
        if self.handle.tell() == 0:
            self.handle.write(b"0")
            self.handle.flush()
        self.handle.seek(0)
        try:
            if os.name == "nt":
                import msvcrt
                msvcrt.locking(self.handle.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(self.handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            self.handle.close()
            raise SafetyError(f"Another process holds {self.path.name}.") from exc
        return self

    def __exit__(self, *args):
        if self.handle is not None:
            self.handle.close()


class Store(AbstractContextManager):
    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.path = path
        self.lock = threading.RLock()
        self.db = sqlite3.connect(path, timeout=10, check_same_thread=False)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS state (
                key TEXT PRIMARY KEY, value TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS audit (
                id INTEGER PRIMARY KEY, at TEXT NOT NULL,
                kind TEXT NOT NULL, payload TEXT NOT NULL, event_key TEXT UNIQUE
            );
            CREATE TABLE IF NOT EXISTS news_seen (
                digest TEXT PRIMARY KEY, seen_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS ai_spend (
                day TEXT PRIMARY KEY, calls INTEGER NOT NULL,
                tokens INTEGER NOT NULL, micros INTEGER NOT NULL,
                last_at TEXT NOT NULL
            );
        """)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.db.close()

    def get(self, key: str) -> Any:
        with self.lock:
            row = self.db.execute("SELECT value FROM state WHERE key=?", (key,)).fetchone()
        return None if row is None else json.loads(row[0])

    def put(self, key: str, value: Any) -> None:
        payload = json.dumps(value, sort_keys=True, allow_nan=False)
        with self.lock, self.db:
            self.db.execute(
                "INSERT INTO state VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value "
                "WHERE state.value != excluded.value", (key, payload)
            )

    def audit(self, at: datetime, kind: str, **payload: Any) -> None:
        with self.lock, self.db:
            self.db.execute(
                "INSERT INTO audit(at,kind,payload) VALUES (?,?,?)",
                (at.isoformat(), kind, json.dumps(payload, sort_keys=True, allow_nan=False)),
            )

    def events(self, kind: str | None = None) -> list[dict[str, Any]]:
        with self.lock:
            rows = self.db.execute(
                "SELECT at,kind,payload FROM audit WHERE (? IS NULL OR kind=?) ORDER BY id",
                (kind, kind),
            ).fetchall()
        return [{"at": at, "kind": event, **json.loads(payload)} for at, event, payload in rows]

    def audit_once(self, key: str, at: datetime, kind: str, **payload: Any) -> None:
        with self.lock, self.db:
            self.db.execute(
                "INSERT OR IGNORE INTO audit(at,kind,payload,event_key) VALUES (?,?,?,?)",
                (at.isoformat(), kind, json.dumps(payload, sort_keys=True, allow_nan=False), key),
            )

    def seen_news(self, digest: str) -> bool:
        with self.lock:
            return self.db.execute(
                "SELECT 1 FROM news_seen WHERE digest=?", (digest,)
            ).fetchone() is not None

    def claim_news(self, digest: str, at: datetime) -> bool:
        with self.lock, self.db:
            cursor = self.db.execute(
                "INSERT OR IGNORE INTO news_seen VALUES (?,?)", (digest, at.isoformat())
            )
        return cursor.rowcount == 1

    def reserve_ai(
        self, at: datetime, tokens: int, micros: int, max_calls: int,
        max_tokens: int, max_micros: int, cooldown: int,
    ) -> bool:
        day = at.date().isoformat()
        with self.lock, self.db:
            self.db.execute("BEGIN IMMEDIATE")
            row = self.db.execute(
                "SELECT calls,tokens,micros,last_at FROM ai_spend WHERE day=?", (day,)
            ).fetchone()
            calls, old_tokens, old_micros = (row[:3] if row else (0, 0, 0))
            if row and (at - datetime.fromisoformat(row[3])).total_seconds() < cooldown:
                return False
            if (calls + 1 > max_calls or old_tokens + tokens > max_tokens
                    or old_micros + micros > max_micros):
                return False
            self.db.execute(
                "INSERT OR REPLACE INTO ai_spend VALUES (?,?,?,?,?)",
                (day, calls + 1, old_tokens + tokens, old_micros + micros, at.isoformat()),
            )
        return True
