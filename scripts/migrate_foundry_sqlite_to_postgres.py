"""Migrate legacy Foundry SQLite control state into shared PostgreSQL."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import sqlite3
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from processor.foundry.database import (
    FOUNDRY_SCHEMA_LOCK_NAME,
    connect_database,
)
from processor.foundry.quota import QuotaLedger
from processor.foundry.store import FoundryStore


@dataclass(frozen=True, slots=True)
class TableSpec:
    name: str
    columns: tuple[str, ...]
    primary_key: tuple[str, ...]
    source: str


TABLES = (
    TableSpec(
        "jobs",
        (
            "job_id",
            "idempotency_key",
            "paper_id",
            "paper_hash",
            "doc_id",
            "state",
            "reason",
            "received_at",
            "updated_at",
            "bundle_json",
            "graph_json",
            "lakehouse_published_at",
        ),
        ("job_id",),
        "control",
    ),
    TableSpec(
        "events",
        (
            "event_id",
            "job_id",
            "sequence",
            "idempotency_key",
            "state",
            "occurred_at",
            "event_json",
        ),
        ("event_id",),
        "control",
    ),
    TableSpec(
        "provider_traces",
        (
            "trace_id",
            "job_id",
            "provider",
            "role",
            "returned_model",
            "completed_at",
            "trace_json",
        ),
        ("trace_id",),
        "control",
    ),
    TableSpec(
        "artifacts",
        (
            "artifact_id",
            "job_id",
            "paper_id",
            "task_id",
            "family",
            "kind",
            "status",
            "created_at",
            "artifact_json",
        ),
        ("artifact_id",),
        "control",
    ),
    TableSpec(
        "artifact_audits",
        (
            "audit_id",
            "artifact_id",
            "job_id",
            "decision",
            "reviewer",
            "created_at",
            "audit_json",
        ),
        ("audit_id",),
        "control",
    ),
    TableSpec(
        "model_snapshots",
        ("provider", "response_hash", "discovered_at", "drifted", "snapshot_json"),
        ("provider", "response_hash"),
        "control",
    ),
    TableSpec(
        "provider_results",
        (
            "job_id",
            "call_key",
            "prompt_version",
            "request_hash",
            "response_json",
            "trace_json",
            "created_at",
        ),
        ("job_id", "call_key", "prompt_version", "request_hash"),
        "control",
    ),
    TableSpec(
        "stream_checkpoints",
        (
            "job_id",
            "call_key",
            "attempt",
            "partial_hash",
            "partial_text",
            "updated_at",
        ),
        ("job_id", "call_key", "attempt"),
        "control",
    ),
    TableSpec(
        "candidate_queue",
        (
            "doc_id",
            "payload",
            "state",
            "reasoning_score",
            "quality_score",
            "ranking_score",
            "domain_key",
            "valid_from",
            "enqueue_ordinal",
            "enqueued_at",
            "updated_at",
            "scientific_payload",
            "attempt_count",
            "next_attempt_at",
            "last_error",
            "claim_owner",
            "claim_token",
            "claimed_at",
            "lease_expires_at",
        ),
        ("doc_id",),
        "control",
    ),
    TableSpec(
        "daily_runs",
        (
            "run_date",
            "state",
            "cutoff_at",
            "cutoff_ordinal",
            "started_at",
            "completed_at",
            "candidate_count",
            "processed_count",
            "stop_reason",
        ),
        ("run_date",),
        "control",
    ),
    TableSpec(
        "daily_run_candidates",
        ("run_date", "rank", "doc_id"),
        ("run_date", "doc_id"),
        "control",
    ),
    TableSpec(
        "manual_runs",
        (
            "run_id",
            "state",
            "cutoff_at",
            "cutoff_ordinal",
            "requested_at",
            "started_at",
            "completed_at",
            "candidate_count",
            "max_candidates",
            "processed_count",
            "stop_reason",
        ),
        ("run_id",),
        "control",
    ),
    TableSpec(
        "pool_assignments",
        ("allocation_key", "pool", "ordinal", "dataset_split", "assigned_at"),
        ("allocation_key",),
        "control",
    ),
    TableSpec(
        "control_sequences",
        ("name", "value"),
        ("name",),
        "control",
    ),
    TableSpec(
        "candidate_control",
        ("key", "value"),
        ("key",),
        "control",
    ),
    TableSpec(
        "candidate_admissions",
        ("identity", "doc_id", "outcome", "observed_at"),
        ("identity",),
        "control",
    ),
    TableSpec(
        "quota_windows",
        (
            "provider",
            "window_kind",
            "window_start",
            "requests_used",
            "input_used",
            "output_used",
            "requests_reserved",
            "input_reserved",
            "output_reserved",
        ),
        ("provider", "window_kind", "window_start"),
        "quota",
    ),
    TableSpec(
        "quota_reservations",
        (
            "reservation_id",
            "provider",
            "requests",
            "input_tokens",
            "output_tokens",
            "minute_start",
            "day_start",
            "state",
            "created_at",
            "reconciled_at",
            "lease_expires_at",
        ),
        ("reservation_id",),
        "quota",
    ),
)


def _select_sql(spec: TableSpec) -> str:
    columns = ",".join(spec.columns)
    order = ",".join(spec.primary_key)
    return f"SELECT {columns} FROM {spec.name} ORDER BY {order}"


def _update_digest(digest: Any, value: Any) -> None:
    if value is None:
        tag = b"null"
        payload = b""
    elif isinstance(value, (bytes, bytearray, memoryview)):
        tag = b"bytes"
        payload = bytes(value)
    elif isinstance(value, bool):
        tag = b"bool"
        payload = b"true" if value else b"false"
    elif isinstance(value, int):
        tag = b"int"
        payload = str(value).encode("ascii")
    elif isinstance(value, float):
        tag = b"float"
        payload = value.hex().encode("ascii")
    elif isinstance(value, str):
        tag = b"text"
        payload = value.encode("utf-8")
    else:
        raise TypeError(f"unsupported database value type: {type(value).__name__}")
    digest.update(len(tag).to_bytes(1, "big"))
    digest.update(tag)
    digest.update(len(payload).to_bytes(8, "big"))
    digest.update(payload)


def _fingerprint_table(connection: Any, spec: TableSpec) -> dict[str, int | str]:
    digest = hashlib.sha256()
    count = 0
    for row in connection.execute(_select_sql(spec)):
        digest.update(b"row")
        for column in spec.columns:
            _update_digest(digest, row[column])
        count += 1
    return {"rows": count, "sha256": digest.hexdigest()}


def _fingerprints(connections: dict[str, Any]) -> dict[str, dict[str, int | str]]:
    return {spec.name: _fingerprint_table(connections[spec.source], spec) for spec in TABLES}


def _target_is_pristine(connection: Any) -> bool:
    for spec in TABLES:
        row = connection.execute(f"SELECT COUNT(*) AS count FROM {spec.name}").fetchone()
        count = int(row["count"])
        if count == 0:
            continue
        if spec.name == "control_sequences" and count == 1:
            sequence = connection.execute(_select_sql(spec)).fetchone()
            if sequence["name"] == "candidate_enqueue" and int(sequence["value"]) == 0:
                continue
        return False
    return True


def _copy_table(source: Any, target: Any, spec: TableSpec) -> None:
    columns = ",".join(spec.columns)
    placeholders = ",".join("?" for _ in spec.columns)
    insert = f"INSERT INTO {spec.name}({columns}) VALUES ({placeholders})"
    for row in source.execute(_select_sql(spec)):
        target.execute(insert, tuple(row[column] for column in spec.columns))


def _snapshot_sqlite(source: Path, destination: Path) -> None:
    if not source.is_file():
        raise FileNotFoundError(source)
    source_connection = sqlite3.connect(
        f"{source.resolve().as_uri()}?mode=ro",
        uri=True,
    )
    destination_connection = sqlite3.connect(destination)
    try:
        source_connection.backup(destination_connection)
    finally:
        destination_connection.close()
        source_connection.close()


def _sha256_file(path: Path) -> str:
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def migrate(state_dir: Path, snapshot_dir: Path, database_url: str) -> dict[str, Any]:
    if not database_url.startswith(("postgresql://", "postgres://")):
        raise ValueError("S2P_COORDINATION_DATABASE_URL must be a PostgreSQL URL")
    snapshot_dir.mkdir(parents=True, exist_ok=False)
    control_backup = snapshot_dir / "control.sqlite3"
    quota_backup = snapshot_dir / "quota.sqlite3"
    _snapshot_sqlite(state_dir / "control.sqlite3", control_backup)
    _snapshot_sqlite(state_dir / "quota.sqlite3", quota_backup)
    source_files = {
        "control.sqlite3": _sha256_file(control_backup),
        "quota.sqlite3": _sha256_file(quota_backup),
    }

    with tempfile.TemporaryDirectory(prefix="s2p-foundry-migration-") as temp_dir:
        normalized_control = Path(temp_dir) / "control.sqlite3"
        normalized_quota = Path(temp_dir) / "quota.sqlite3"
        shutil.copy2(control_backup, normalized_control)
        shutil.copy2(quota_backup, normalized_quota)
        FoundryStore(str(normalized_control)).close()
        QuotaLedger(str(normalized_quota), {}).close()
        FoundryStore(database_url).close()
        QuotaLedger(database_url, {}).close()

        sources = {
            "control": connect_database(str(normalized_control), read_only=True),
            "quota": connect_database(str(normalized_quota), read_only=True),
        }
        target = connect_database(database_url)
        try:
            source_fingerprints = _fingerprints(sources)
            target.execute("BEGIN")
            try:
                target.execute(
                    "SELECT pg_advisory_xact_lock(hashtext(?))",
                    (FOUNDRY_SCHEMA_LOCK_NAME,),
                )
                target.execute(
                    "LOCK TABLE "
                    + ",".join(spec.name for spec in TABLES)
                    + " IN ACCESS EXCLUSIVE MODE NOWAIT"
                )
                target_fingerprints = _fingerprints({"control": target, "quota": target})
                if target_fingerprints == source_fingerprints:
                    mode = "verified-existing"
                else:
                    if not _target_is_pristine(target):
                        raise RuntimeError(
                            "PostgreSQL Foundry tables are not empty and do not match the source"
                        )
                    target.execute("DELETE FROM control_sequences")
                    for spec in TABLES:
                        _copy_table(sources[spec.source], target, spec)
                    target_fingerprints = _fingerprints({"control": target, "quota": target})
                    if target_fingerprints != source_fingerprints:
                        raise RuntimeError("post-import Foundry table verification failed")
                    mode = "migrated"
                target.commit()
            except Exception:
                target.rollback()
                raise
        finally:
            target.close()
            for connection in sources.values():
                connection.close()

    return {
        "format": "stream2pretrain-foundry-sqlite-migration-v1",
        "mode": mode,
        "source_claim": "state-stream2pretrain-foundry-0",
        "source_files": source_files,
        "tables": source_fingerprints,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-dir", type=Path, required=True)
    parser.add_argument("--snapshot-dir", type=Path, required=True)
    args = parser.parse_args()
    database_url = os.environ.get("S2P_COORDINATION_DATABASE_URL", "").strip()
    manifest = migrate(args.state_dir, args.snapshot_dir, database_url)
    manifest_bytes = (json.dumps(manifest, indent=2, sort_keys=True) + "\n").encode()
    manifest_path = args.snapshot_dir / "migration-manifest.json"
    manifest_path.write_bytes(manifest_bytes)
    result = {
        "manifest_path": str(manifest_path),
        "manifest_sha256": hashlib.sha256(manifest_bytes).hexdigest(),
        "mode": manifest["mode"],
    }
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
