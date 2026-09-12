"""Contract tests for the legacy Foundry control-state migration."""

from __future__ import annotations

import sqlite3
from pathlib import Path

from processor.foundry.quota import QuotaLedger
from processor.foundry.store import FoundryStore
from scripts.migrate_foundry_sqlite_to_postgres import (
    TABLES,
    _copy_table,
    _fingerprints,
    _target_is_pristine,
)


def _connection(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(path, isolation_level=None)
    connection.row_factory = sqlite3.Row
    return connection


def test_migration_table_contract_matches_current_sqlite_schemas(tmp_path: Path) -> None:
    control_path = tmp_path / "control.sqlite3"
    quota_path = tmp_path / "quota.sqlite3"
    FoundryStore(str(control_path)).close()
    QuotaLedger(str(quota_path), {}).close()
    connections = {
        "control": _connection(control_path),
        "quota": _connection(quota_path),
    }
    try:
        for spec in TABLES:
            columns = tuple(
                str(row["name"])
                for row in connections[spec.source]
                .execute(f"PRAGMA table_info({spec.name})")
                .fetchall()
            )
            assert columns == spec.columns
    finally:
        for connection in connections.values():
            connection.close()


def test_migration_copy_preserves_row_fingerprints(tmp_path: Path) -> None:
    control_path = tmp_path / "control.sqlite3"
    quota_path = tmp_path / "quota.sqlite3"
    target_path = tmp_path / "target.sqlite3"
    FoundryStore(str(control_path)).close()
    QuotaLedger(str(quota_path), {}).close()
    FoundryStore(str(target_path)).close()
    QuotaLedger(str(target_path), {}).close()
    sources = {
        "control": _connection(control_path),
        "quota": _connection(quota_path),
    }
    target = _connection(target_path)
    try:
        sources["control"].execute(
            "INSERT INTO candidate_control(key,value) VALUES ('generation','v1')"
        )
        sources["control"].execute(
            """
            INSERT INTO candidate_queue(
              doc_id,payload,state,reasoning_score,quality_score,ranking_score,
              domain_key,valid_from,enqueued_at,updated_at
            ) VALUES ('doc-1',X'0102','queued',0.125,4.0,0.5,'physics','2026-09-12',
                      '2026-09-12T00:00:00+00:00','2026-09-12T00:00:00+00:00')
            """
        )
        sources["quota"].execute(
            """
            INSERT INTO quota_windows(provider,window_kind,window_start,requests_used)
            VALUES ('hetzner','minute','2026-09-12T00:00:00+00:00',1)
            """
        )
        source_fingerprints = _fingerprints(sources)
        assert _target_is_pristine(target)

        target.execute("DELETE FROM control_sequences")
        for spec in TABLES:
            _copy_table(sources[spec.source], target, spec)

        assert _fingerprints({"control": target, "quota": target}) == source_fingerprints
        assert not _target_is_pristine(target)
    finally:
        target.close()
        for connection in sources.values():
            connection.close()
