"""Tests for backend-neutral Foundry status scripts."""

from __future__ import annotations

import json
from collections.abc import Iterator
from typing import Any

import pytest

import scripts.check_pipeline_live as pipeline_status
import scripts.foundry_diagnostics as diagnostics


class _Cursor:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self._rows = rows

    def __iter__(self) -> Iterator[dict[str, Any]]:
        return iter(self._rows)

    def fetchall(self) -> list[dict[str, Any]]:
        return self._rows


class _Connection:
    def __init__(self) -> None:
        self.queries: list[str] = []
        self.closed = False

    def execute(self, sql: str) -> _Cursor:
        self.queries.append(sql)
        if "COUNT(scientific_payload) AS retained_evidence" in sql:
            return _Cursor([{"state": "queued", "count": 2, "retained_evidence": 1}])
        if "SELECT event_json FROM events" in sql:
            return _Cursor([{"event_json": b'{"state":"RECEIVED"}'}])
        if "LENGTH(partial_text)" in sql:
            return _Cursor(
                [
                    {
                        "job_id": "job-1",
                        "call_key": "solver",
                        "attempt": 1,
                        "characters": 20,
                        "updated_at": "2026-09-12T00:00:00+00:00",
                    }
                ]
            )
        if "COUNT(scientific_payload) AS cached_evidence" in sql:
            return _Cursor(
                [
                    {
                        "state": "queued",
                        "candidates": 2,
                        "cached_evidence": 1,
                        "oldest": "2026-09-12T00:00:00+00:00",
                        "newest": "2026-09-12T00:00:00+00:00",
                    }
                ]
            )
        if "FROM candidate_queue GROUP BY state" in sql:
            return _Cursor([{"state": "queued", "count": 2}])
        return _Cursor([])

    def close(self) -> None:
        self.closed = True


@pytest.mark.parametrize("module", (pipeline_status, diagnostics))
def test_foundry_status_scripts_use_shared_postgres_read_only_connection(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    module: Any,
) -> None:
    connection = _Connection()
    calls: list[tuple[str, bool]] = []

    def connect(target: str, *, read_only: bool = False) -> _Connection:
        calls.append((target, read_only))
        return connection

    monkeypatch.setenv(
        "S2P_COORDINATION_DATABASE_URL",
        "postgresql://coordination/foundry",
    )
    monkeypatch.setattr(module, "connect_database", connect)
    monkeypatch.setattr(module, "database_dialect", lambda _connection: "postgresql")

    if module is pipeline_status:
        payload = pipeline_status._foundry_status()
    else:
        diagnostics.main()
        payload = json.loads(capsys.readouterr().out)

    assert calls == [("postgresql://coordination/foundry", True)]
    assert payload["backend"] == "postgresql"
    assert payload["database_bytes"] is None
    assert connection.closed
    assert all("SUM(scientific_payload IS NOT NULL)" not in query for query in connection.queries)
    assert any("COUNT(scientific_payload)" in query for query in connection.queries)
