from __future__ import annotations

from typing import Any

from processor.decision_cache import DecisionCache, PostgresDecisionCache


def test_sqlite_decision_cache_replays_exact_payload(tmp_path) -> None:
    cache = DecisionCache(tmp_path / "decisions.sqlite3")
    cache.put("recipe", b"decision", trainable=True)

    assert cache.get("recipe") == (b"decision", True)


def test_postgres_decision_cache_is_shared_between_replicas() -> None:
    rows: dict[str, tuple[bytes, bool]] = {}

    class Result:
        def __init__(self, row: tuple[bytes, bool] | None = None) -> None:
            self._row = row

        def fetchone(self) -> tuple[bytes, bool] | None:
            return self._row

    class Connection:
        def execute(self, sql: str, params: tuple[Any, ...] = ()) -> Result:
            if "SELECT payload" in sql:
                return Result(rows.get(str(params[0])))
            if "INSERT INTO curator_decisions" in sql:
                rows.setdefault(str(params[0]), (bytes(params[1]), bool(params[2])))
            return Result()

        def close(self) -> None:
            return None

    def connect(_url: str, *, autocommit: bool) -> Connection:
        assert autocommit is True
        return Connection()

    first = PostgresDecisionCache("postgresql://coordination", connect=connect)
    second = PostgresDecisionCache("postgresql://coordination", connect=connect)
    first.put("recipe", b"first", trainable=True)
    second.put("recipe", b"second", trainable=False)

    assert first.get("recipe") == (b"first", True)
    assert second.get("recipe") == (b"first", True)


def test_postgres_decision_cache_reconnects_after_primary_failover() -> None:
    rows = {"recipe": (b"decision", True)}
    connections: list[Connection] = []

    class FailoverError(Exception):
        pass

    class Result:
        def __init__(self, row: tuple[bytes, bool] | None = None) -> None:
            self._row = row

        def fetchone(self) -> tuple[bytes, bool] | None:
            return self._row

    class Connection:
        def __init__(self, *, fail_reads: bool) -> None:
            self.fail_reads = fail_reads
            self.closed = False

        def execute(self, sql: str, params: tuple[Any, ...] = ()) -> Result:
            if "SELECT payload" in sql and self.fail_reads:
                self.fail_reads = False
                raise FailoverError("primary changed")
            if "SELECT payload" in sql:
                return Result(rows.get(str(params[0])))
            return Result()

        def close(self) -> None:
            self.closed = True

    def connect(_url: str, *, autocommit: bool) -> Connection:
        assert autocommit is True
        connection = Connection(fail_reads=not connections)
        connections.append(connection)
        return connection

    cache = PostgresDecisionCache(
        "postgresql://coordination",
        connect=connect,
        connection_errors=(FailoverError,),
    )

    assert cache.get("recipe") == (b"decision", True)
    assert len(connections) == 2
    assert connections[0].closed
