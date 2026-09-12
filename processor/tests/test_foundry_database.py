"""Focused tests for Foundry database compatibility and schema locking."""

from __future__ import annotations

import sqlite3
import sys
from collections.abc import Callable, Iterable
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest

import processor.foundry.quota as quota_module
import processor.foundry.store as store_module
from processor.foundry.database import (
    FOUNDRY_SCHEMA_LOCK_NAME,
    PostgresConnection,
    _postgres_sql,
    connect_database,
    initialize_database_schema,
)


class _FakeConnection:
    def __init__(self) -> None:
        self.events: list[tuple[Any, ...]] = []

    def execute(self, sql: str, parameters: Iterable[Any] = ()) -> object:
        self.events.append(("execute", sql, tuple(parameters)))
        return object()

    def executescript(self, script: str) -> None:
        self.events.append(("executescript", script))

    def commit(self) -> None:
        self.events.append(("commit",))

    def rollback(self) -> None:
        self.events.append(("rollback",))


class _FakeOperationalError(Exception):
    pass


class _FakeInterfaceError(Exception):
    pass


class _FakePostgresCursor:
    def __init__(self, connection: _FakePostgresConnection) -> None:
        self.connection = connection

    def execute(self, sql: str, parameters: tuple[Any, ...]) -> None:
        self.connection.executions.append((sql, parameters))
        if self.connection.fail_next_execute:
            self.connection.fail_next_execute = False
            raise _FakeOperationalError("connection lost")

    def executemany(self, sql: str, parameters: list[tuple[Any, ...]]) -> None:
        self.execute(sql, tuple(parameters))

    def fetchone(self) -> dict[str, int]:
        return {"result": 1}


class _FakePostgresConnection:
    def __init__(self) -> None:
        self.closed = False
        self.fail_next_execute = False
        self.fail_commit = False
        self.executions: list[tuple[str, tuple[Any, ...]]] = []
        self.session_sql: list[str] = []
        self.commits = 0
        self.rollbacks = 0

    def cursor(self) -> _FakePostgresCursor:
        return _FakePostgresCursor(self)

    def execute(self, sql: str) -> None:
        self.session_sql.append(sql)

    def commit(self) -> None:
        self.commits += 1
        if self.fail_commit:
            raise _FakeOperationalError("commit result unknown")

    def rollback(self) -> None:
        self.rollbacks += 1

    def close(self) -> None:
        self.closed = True


def _postgres_connection(
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[PostgresConnection, list[_FakePostgresConnection]]:
    connections: list[_FakePostgresConnection] = []
    psycopg = ModuleType("psycopg")
    psycopg.OperationalError = _FakeOperationalError  # type: ignore[attr-defined]
    psycopg.InterfaceError = _FakeInterfaceError  # type: ignore[attr-defined]

    def connect(*_args: Any, **_kwargs: Any) -> _FakePostgresConnection:
        connection = _FakePostgresConnection()
        connections.append(connection)
        return connection

    psycopg.connect = connect  # type: ignore[attr-defined]
    rows = ModuleType("psycopg.rows")
    rows.dict_row = object()  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "psycopg", psycopg)
    monkeypatch.setitem(sys.modules, "psycopg.rows", rows)
    return PostgresConnection("postgresql://coordination/foundry"), connections


def test_postgres_schema_initialization_locks_and_commits() -> None:
    connection = _FakeConnection()

    def migration() -> None:
        connection.events.append(("migration",))

    initialize_database_schema(
        connection,  # type: ignore[arg-type]
        dialect="postgresql",
        lock_name=FOUNDRY_SCHEMA_LOCK_NAME,
        script="CREATE TABLE example (id TEXT)",
        migrations=(migration,),
    )

    assert connection.events == [
        ("execute", "BEGIN", ()),
        (
            "execute",
            "SELECT pg_advisory_xact_lock(hashtext(?))",
            (FOUNDRY_SCHEMA_LOCK_NAME,),
        ),
        ("executescript", "CREATE TABLE example (id TEXT)"),
        ("migration",),
        ("commit",),
    ]


def test_postgres_schema_initialization_rolls_back_on_failure() -> None:
    connection = _FakeConnection()

    def fail() -> None:
        connection.events.append(("migration",))
        raise RuntimeError("migration failed")

    with pytest.raises(RuntimeError, match="migration failed"):
        initialize_database_schema(
            connection,  # type: ignore[arg-type]
            dialect="postgresql",
            lock_name=FOUNDRY_SCHEMA_LOCK_NAME,
            script="CREATE TABLE example (id TEXT)",
            migrations=(fail,),
        )

    assert connection.events[-1] == ("rollback",)
    assert ("commit",) not in connection.events


def test_sqlite_schema_initialization_keeps_executescript_semantics() -> None:
    connection = _FakeConnection()

    def migration() -> None:
        connection.events.append(("migration",))

    initialize_database_schema(
        connection,  # type: ignore[arg-type]
        dialect="sqlite",
        lock_name=FOUNDRY_SCHEMA_LOCK_NAME,
        script="CREATE TABLE example (id TEXT)",
        migrations=(migration,),
    )

    assert connection.events == [
        ("executescript", "CREATE TABLE example (id TEXT)"),
        ("migration",),
    ]


def test_sqlite_read_only_connection_rejects_writes(tmp_path: Path) -> None:
    database = tmp_path / "control.sqlite3"
    writable = sqlite3.connect(database)
    writable.execute("CREATE TABLE example (id TEXT)")
    writable.commit()
    writable.close()

    connection = connect_database(str(database), read_only=True)
    assert connection.execute("SELECT COUNT(*) FROM example").fetchone()[0] == 0
    with pytest.raises(sqlite3.OperationalError, match="readonly"):
        connection.execute("INSERT INTO example(id) VALUES ('blocked')")
    connection.close()


def test_postgres_schema_uses_double_precision_for_sqlite_real_columns() -> None:
    assert _postgres_sql("CREATE TABLE scores (value REAL)") == (
        "CREATE TABLE scores (value DOUBLE PRECISION)"
    )


def test_postgres_reconnects_before_a_fresh_operation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection, raw_connections = _postgres_connection(monkeypatch)
    raw_connections[0].closed = True

    connection.execute("SELECT 1")

    assert len(raw_connections) == 2
    assert raw_connections[1].executions == [("SELECT 1", ())]


def test_postgres_does_not_replay_a_failed_operation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection, raw_connections = _postgres_connection(monkeypatch)
    raw_connections[0].fail_next_execute = True

    with pytest.raises(_FakeOperationalError, match="connection lost"):
        connection.execute("INSERT INTO example(id) VALUES (?)", ("one",))

    assert len(raw_connections) == 1
    connection.execute("SELECT 1")
    assert len(raw_connections) == 2


def test_postgres_never_reconnects_mid_transaction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection, raw_connections = _postgres_connection(monkeypatch)
    connection.execute("BEGIN")
    raw_connections[0].closed = True

    with pytest.raises(_FakeInterfaceError, match="explicit transaction"):
        connection.execute("SELECT 1")

    assert len(raw_connections) == 1
    connection.rollback()
    connection.execute("SELECT 1")
    assert len(raw_connections) == 2


def test_postgres_does_not_retry_an_uncertain_commit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection, raw_connections = _postgres_connection(monkeypatch)
    connection.execute("BEGIN")
    raw_connections[0].fail_commit = True

    with pytest.raises(_FakeOperationalError, match="commit result unknown"):
        connection.commit()

    assert len(raw_connections) == 1
    assert raw_connections[0].commits == 1
    connection.execute("SELECT 1")
    assert len(raw_connections) == 2


@pytest.mark.parametrize(
    ("module", "factory"),
    (
        (store_module, lambda: store_module.FoundryStore("ignored")),
        (quota_module, lambda: quota_module.QuotaLedger("ignored", {})),
    ),
)
def test_foundry_stores_use_shared_schema_lock(
    monkeypatch: pytest.MonkeyPatch,
    module: Any,
    factory: Callable[[], object],
) -> None:
    connection = object()
    calls: list[dict[str, Any]] = []

    monkeypatch.setattr(module, "connect_database", lambda _path: connection)
    monkeypatch.setattr(module, "database_dialect", lambda _connection: "postgresql")
    monkeypatch.setattr(
        module,
        "initialize_database_schema",
        lambda _connection, **kwargs: calls.append(kwargs),
    )

    factory()

    assert len(calls) == 1
    assert calls[0]["dialect"] == "postgresql"
    assert calls[0]["lock_name"] == FOUNDRY_SCHEMA_LOCK_NAME
