"""Small DB-API compatibility layer for local SQLite and shared PostgreSQL."""

from __future__ import annotations

import os
import re
import sqlite3
from collections.abc import Callable, Iterable
from contextlib import suppress
from pathlib import Path
from typing import Any

FOUNDRY_SCHEMA_LOCK_NAME = "stream2pretrain-foundry-schema"


def connect_database(
    target: str,
    *,
    read_only: bool = False,
) -> sqlite3.Connection | PostgresConnection:
    """Open a local SQLite path or a PostgreSQL coordination URL."""
    if target.startswith(("postgresql://", "postgres://")):
        return PostgresConnection(target, read_only=read_only)
    database = Path(target)
    if read_only:
        connection = sqlite3.connect(
            f"{database.resolve().as_uri()}?mode=ro",
            uri=True,
            check_same_thread=False,
            isolation_level=None,
        )
    else:
        database.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(target, check_same_thread=False, isolation_level=None)
    connection.row_factory = sqlite3.Row
    return connection


def database_dialect(connection: object) -> str:
    return "postgresql" if isinstance(connection, PostgresConnection) else "sqlite"


def coordination_database_target(state_dir: str, sqlite_name: str) -> str:
    """Select the shared coordination database or the local SQLite fallback."""
    database_url = os.environ.get("S2P_COORDINATION_DATABASE_URL", "").strip()
    return database_url or str(Path(state_dir) / sqlite_name)


class PostgresConnection:
    """Expose the subset of sqlite3.Connection used by the Foundry stores."""

    def __init__(self, dsn: str, *, read_only: bool = False) -> None:
        try:
            import psycopg  # type: ignore[import-not-found]
            from psycopg.rows import dict_row  # type: ignore[import-not-found]
        except ImportError as exc:
            raise RuntimeError(
                "PostgreSQL coordination requires the psycopg binary dependency"
            ) from exc
        self._psycopg = psycopg
        self._dict_row = dict_row
        self._dsn = dsn
        self._read_only = read_only
        self._connection: Any | None = None
        self._in_transaction = False
        self._closed = False
        self._connect()

    def _connect(self) -> None:
        connection = self._psycopg.connect(
            self._dsn,
            autocommit=True,
            row_factory=self._dict_row,
        )
        try:
            if self._read_only:
                connection.execute("SET default_transaction_read_only = on")
        except Exception:
            with suppress(Exception):
                connection.close()
            raise
        self._connection = connection

    def _discard_connection(self) -> None:
        connection = self._connection
        self._connection = None
        if connection is None:
            return
        with suppress(Exception):
            connection.close()

    def _ensure_connection(self) -> Any:
        connection = self._connection
        if connection is not None and not bool(getattr(connection, "closed", False)):
            return connection
        self._connection = None
        if self._closed:
            raise self._psycopg.InterfaceError("PostgreSQL connection is closed")
        if self._in_transaction:
            raise self._psycopg.InterfaceError(
                "PostgreSQL connection was lost during an explicit transaction"
            )
        self._connect()
        assert self._connection is not None
        return self._connection

    def execute(self, sql: str, parameters: Iterable[Any] = ()) -> Any:
        statement = _postgres_sql(sql)
        try:
            cursor = self._ensure_connection().cursor()
            cursor.execute(statement, tuple(parameters))
        except (self._psycopg.OperationalError, self._psycopg.InterfaceError):
            self._discard_connection()
            raise
        if statement == "BEGIN":
            self._in_transaction = True
        return cursor

    def executemany(self, sql: str, parameters: Iterable[Iterable[Any]]) -> Any:
        try:
            cursor = self._ensure_connection().cursor()
            cursor.executemany(_postgres_sql(sql), [tuple(values) for values in parameters])
            return cursor
        except (self._psycopg.OperationalError, self._psycopg.InterfaceError):
            self._discard_connection()
            raise

    def executescript(self, script: str) -> None:
        for statement in script.split(";"):
            normalized = statement.strip()
            if normalized and not normalized.upper().startswith("PRAGMA "):
                self.execute(normalized)

    def commit(self) -> None:
        connection = self._connection
        if connection is None or bool(getattr(connection, "closed", False)):
            self._in_transaction = False
            self._discard_connection()
            raise self._psycopg.InterfaceError(
                "PostgreSQL connection was lost before commit completed"
            )
        try:
            connection.commit()
        except (self._psycopg.OperationalError, self._psycopg.InterfaceError):
            self._discard_connection()
            raise
        finally:
            self._in_transaction = False

    def rollback(self) -> None:
        connection = self._connection
        try:
            if connection is None or bool(getattr(connection, "closed", False)):
                self._discard_connection()
                return
            connection.rollback()
        except (self._psycopg.OperationalError, self._psycopg.InterfaceError):
            self._discard_connection()
        finally:
            self._in_transaction = False

    def close(self) -> None:
        self._closed = True
        self._in_transaction = False
        self._discard_connection()


def initialize_database_schema(
    connection: sqlite3.Connection | PostgresConnection,
    *,
    dialect: str,
    lock_name: str,
    script: str,
    migrations: Iterable[Callable[[], None]] = (),
) -> None:
    """Create and migrate a schema under one PostgreSQL transaction lock."""

    def apply_schema() -> None:
        connection.executescript(script)
        for migration in migrations:
            migration()

    if dialect != "postgresql":
        apply_schema()
        return

    connection.execute("BEGIN")
    try:
        connection.execute(
            "SELECT pg_advisory_xact_lock(hashtext(?))",
            (lock_name,),
        )
        apply_schema()
        connection.commit()
    except Exception:
        connection.rollback()
        raise


def _postgres_sql(sql: str) -> str:
    statement = sql.strip()
    if statement == "BEGIN IMMEDIATE":
        return "BEGIN"
    statement = statement.replace("?", "%s")
    statement = re.sub(r"\bBLOB\b", "BYTEA", statement, flags=re.IGNORECASE)
    # SQLite REAL values are IEEE-754 doubles. PostgreSQL REAL is only
    # single precision, so preserve ranking values and migration fingerprints.
    statement = re.sub(
        r"\bREAL\b",
        "DOUBLE PRECISION",
        statement,
        flags=re.IGNORECASE,
    )
    statement = re.sub(r"\bMAX\(0,\s*", "GREATEST(0, ", statement, flags=re.IGNORECASE)
    statement = re.sub(r"\bMAX\(value,\s*", "GREATEST(value, ", statement, flags=re.IGNORECASE)
    statement = statement.replace("X''", "''::bytea")
    replacements = {
        "json_extract(event_json, '$.attempt')": (
            "(convert_from(event_json, 'UTF8')::jsonb ->> 'attempt')"
        ),
        "json_extract(started.event_json, '$.attempt')": (
            "(convert_from(started.event_json, 'UTF8')::jsonb ->> 'attempt')"
        ),
        "json_extract(terminal.event_json, '$.attempt')": (
            "(convert_from(terminal.event_json, 'UTF8')::jsonb ->> 'attempt')"
        ),
        "json_extract(event_json, '$.metadata.role')": (
            "(convert_from(event_json, 'UTF8')::jsonb #>> '{metadata,role}')"
        ),
        "json_extract(started.event_json, '$.metadata.role')": (
            "(convert_from(started.event_json, 'UTF8')::jsonb #>> '{metadata,role}')"
        ),
        "json_extract(terminal.event_json, '$.metadata.role')": (
            "(convert_from(terminal.event_json, 'UTF8')::jsonb #>> '{metadata,role}')"
        ),
        "json_extract(trace_json, '$.input_tokens')": (
            "(convert_from(trace_json, 'UTF8')::jsonb ->> 'input_tokens')"
        ),
        "json_extract(trace_json, '$.output_tokens')": (
            "(convert_from(trace_json, 'UTF8')::jsonb ->> 'output_tokens')"
        ),
    }
    for sqlite_expression, postgres_expression in replacements.items():
        statement = statement.replace(sqlite_expression, postgres_expression)
    return statement


__all__ = [
    "FOUNDRY_SCHEMA_LOCK_NAME",
    "PostgresConnection",
    "connect_database",
    "coordination_database_target",
    "database_dialect",
    "initialize_database_schema",
]
