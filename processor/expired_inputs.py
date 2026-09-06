"""Remember already-audited immutable raw pointers that no longer exist."""

from __future__ import annotations

import sqlite3
import threading
from pathlib import Path


class ExpiredInputIndex:
    def __init__(self, path: str) -> None:
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._db = sqlite3.connect(path, check_same_thread=False)
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.execute("CREATE TABLE IF NOT EXISTS expired (uri TEXT PRIMARY KEY)")
        self._db.commit()

    def contains(self, uri: str) -> bool:
        with self._lock:
            return (
                self._db.execute("SELECT 1 FROM expired WHERE uri=?", (uri,)).fetchone() is not None
            )

    def record(self, uri: str) -> None:
        # Caller first commits the processing-failure audit. A replay then needs
        # neither another failed S3 GET nor another failure object or counter.
        with self._lock, self._db:
            self._db.execute("INSERT OR IGNORE INTO expired VALUES (?)", (uri,))

    def close(self) -> None:
        self._db.close()
