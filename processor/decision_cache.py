"""Durable deterministic curation cache for at-least-once Kafka replay."""

from __future__ import annotations

import sqlite3
from collections.abc import Callable
from contextlib import suppress
from pathlib import Path
from typing import Any


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


class PostgresDecisionCache:
    """Shared decision cache for horizontally scaled curator processes."""

    def __init__(
        self,
        database_url: str,
        *,
        connect: Callable[..., Any] | None = None,
        connection_errors: tuple[type[BaseException], ...] | None = None,
    ) -> None:
        if connect is None:
            import psycopg  # type: ignore[import-not-found]

            connect = psycopg.connect
            connection_errors = (
                psycopg.OperationalError,
                psycopg.InterfaceError,
            )
        elif connection_errors is None:
            connection_errors = (ConnectionError,)
        self._database_url = database_url
        self._connect = connect
        self._connection_errors = connection_errors
        self._conn: Any | None = None
        self._closed = False
        self._execute(
            """
            CREATE TABLE IF NOT EXISTS curator_decisions (
              cache_key TEXT PRIMARY KEY,
              payload BYTEA NOT NULL,
              trainable BOOLEAN NOT NULL,
              created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

    def get(self, cache_key: str) -> tuple[bytes, bool] | None:
        row = self._execute(
            "SELECT payload, trainable FROM curator_decisions WHERE cache_key = %s",
            (cache_key,),
        ).fetchone()
        if row is None:
            return None
        return bytes(row[0]), bool(row[1])

    def put(self, cache_key: str, payload: bytes, *, trainable: bool) -> None:
        self._execute(
            """
            INSERT INTO curator_decisions(cache_key, payload, trainable)
            VALUES (%s, %s, %s)
            ON CONFLICT (cache_key) DO NOTHING
            """,
            (cache_key, payload, trainable),
        )

    def _execute(self, sql: str, parameters: tuple[Any, ...] = ()) -> Any:
        """Retry one idempotent cache statement on a fresh primary connection."""
        for attempt in range(2):
            connection = self._ensure_connection()
            try:
                return connection.execute(sql, parameters)
            except self._connection_errors:
                self._discard_connection()
                if attempt == 1:
                    raise
        raise AssertionError("unreachable")

    def _ensure_connection(self) -> Any:
        connection = self._conn
        if connection is not None and not bool(getattr(connection, "closed", False)):
            return connection
        self._conn = None
        if self._closed:
            raise RuntimeError("PostgreSQL decision cache is closed")
        self._conn = self._connect(self._database_url, autocommit=True)
        return self._conn

    def _discard_connection(self) -> None:
        connection = self._conn
        self._conn = None
        if connection is not None:
            with suppress(Exception):
                connection.close()

    def close(self) -> None:
        self._closed = True
        self._discard_connection()


def build_decision_cache(path: str | Path, database_url: str | None) -> Any:
    """Select shared production state or the local development backend."""
    if database_url:
        return PostgresDecisionCache(database_url)
    return DecisionCache(path)
