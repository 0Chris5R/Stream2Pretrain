"""Durable deterministic curation cache for at-least-once Kafka replay."""

from __future__ import annotations

import sqlite3
from pathlib import Path


class DecisionCache:
    """Store scored decision bytes by input-and-recipe fingerprint.

    The cache is written before Bytewax hands the decision to its Kafka sink.
    If the process dies between those operations, replay returns the identical
    decision without rerunning classifiers or mutating the near-duplicate
    index a second time.
    """

    def __init__(self, path: str | Path) -> None:
        target = Path(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(target, timeout=30.0)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS decisions (
              cache_key TEXT PRIMARY KEY,
              payload BLOB NOT NULL,
              trainable INTEGER NOT NULL CHECK (trainable IN (0, 1))
            )
            """
        )
        self._conn.commit()

    def get(self, cache_key: str) -> tuple[bytes, bool] | None:
        row = self._conn.execute(
            "SELECT payload, trainable FROM decisions WHERE cache_key = ?",
            (cache_key,),
        ).fetchone()
        if row is None:
            return None
        return bytes(row[0]), bool(row[1])

    def put(self, cache_key: str, payload: bytes, *, trainable: bool) -> None:
        self._conn.execute(
            "INSERT OR IGNORE INTO decisions(cache_key, payload, trainable) VALUES (?, ?, ?)",
            (cache_key, payload, int(trainable)),
        )
        self._conn.commit()

    def close(self) -> None:
        self._conn.close()
