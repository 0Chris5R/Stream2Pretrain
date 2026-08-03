from __future__ import annotations

from collections.abc import Sequence
from typing import Any

import pytest

from processor.duckdb_api import DuckDBQueryService, _create_empty_gold_relation


class _FakeConnection:
    def __init__(self) -> None:
        self.description: Sequence[tuple[Any, ...]] | None = None
        self.rows: list[tuple[Any, ...]] = []
        self.calls: list[tuple[str, Sequence[Any] | None]] = []

    def execute(self, sql: str, parameters: Sequence[Any] | None = None) -> _FakeConnection:
        self.calls.append((sql, parameters))
        if "GROUP BY source_feed" in sql:
            self.description = [("source_feed",), ("tokens",), ("documents",)]
            self.rows = [("arxiv", 10, 2)]
        elif "quality_score" in sql:
            self.description = [("score",), ("count",)]
            self.rows = [(3.5, 7)]
        else:
            self.description = [("answer",)]
            self.rows = [(42,)]
        return self

    def fetchall(self) -> list[tuple[Any, ...]]:
        return self.rows


def test_as_of_uses_half_open_validity_predicate() -> None:
    conn = _FakeConnection()
    service = DuckDBQueryService(conn)

    rows = service.as_of("2026-06-17T10:00:00Z")

    assert rows == [{"source_feed": "arxiv", "tokens": 10, "documents": 2}]
    sql, params = conn.calls[-1]
    assert "valid_from <= CAST(? AS TIMESTAMP)" in sql
    assert "valid_to IS NULL OR valid_to > CAST(? AS TIMESTAMP)" in sql
    assert params == ["2026-06-17T10:00:00Z", "2026-06-17T10:00:00Z"]


def test_quality_histogram_shape() -> None:
    service = DuckDBQueryService(_FakeConnection())

    assert service.quality_histogram() == {"buckets": [{"score": 3.5, "count": 7}]}


def test_safe_query_rejects_writes_and_multiple_statements() -> None:
    service = DuckDBQueryService(_FakeConnection())

    with pytest.raises(ValueError, match="only SELECT"):
        service.safe_query("DELETE FROM gold", [])
    with pytest.raises(ValueError, match="multiple"):
        service.safe_query("SELECT 1; SELECT 2", [])


def test_safe_query_returns_rows_and_duration() -> None:
    service = DuckDBQueryService(_FakeConnection())

    payload = service.safe_query("SELECT 42 AS answer", [])

    assert payload["rows"] == [{"answer": 42}]
    assert payload["durationMs"] >= 0


def test_gold_relation_is_validated() -> None:
    with pytest.raises(ValueError, match="gold_relation"):
        DuckDBQueryService(_FakeConnection(), gold_relation="gold; DROP TABLE gold")


def test_empty_gold_relation_is_gold_shaped() -> None:
    conn = _FakeConnection()

    _create_empty_gold_relation(conn, "gold")

    sql, _params = conn.calls[-1]
    assert "CREATE OR REPLACE VIEW gold" in sql
    assert "source_feed" in sql
    assert "valid_from" in sql
    assert "quality_score" in sql
    assert "WHERE FALSE" in sql
