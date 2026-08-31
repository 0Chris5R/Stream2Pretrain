"""Durable idempotent control-plane store for foundry jobs and provenance."""

from __future__ import annotations

import json
import sqlite3
import threading
import uuid
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any, cast

from processor.foundry.util import canonical_json, sha256, stable_id
from schemas.foundry import (
    ArtifactAuditRecord,
    DatasetSplit,
    FoundryArtifactRecord,
    FoundryEvent,
    PosttrainPool,
    ProviderModelSnapshot,
    ProviderTrace,
)

_TERMINAL = {"ACCEPTED_SFT", "ACCEPTED_RL", "REJECTED", "DEPRECATED"}
_ACTIVITY_WINDOWS: dict[str, tuple[timedelta, int, int]] = {
    "5m": (timedelta(minutes=5), 10, 30),
    "1h": (timedelta(hours=1), 60, 60),
    "24h": (timedelta(hours=24), 1_800, 48),
}
_CALL_ACTIVITY_STATES = {
    "CALL_STARTED": "started",
    "CALL_SUCCEEDED": "succeeded",
    "CALL_FAILED": "failed",
    "CALL_RATE_LIMITED": "rate_limited",
}
_PIPELINE_ACTIVITY_STATES = {
    "RECEIVED": "received",
    "GRAPH_COMPILED": "graph_compiled",
    "GRAPH_CRITIQUED": "graph_critiqued",
    "TASKS_PROPOSED": "tasks_proposed",
    "SOLUTIONS_GENERATED": "solutions_generated",
    "VERIFIERS_COMPILED": "verifiers_compiled",
    "ADVERSARIAL_VALIDATED": "adversarial_validated",
}


class FoundryStore:
    def __init__(self, path: str, *, recover_processing: bool = False) -> None:
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.path = path
        self._conn = sqlite3.connect(path, check_same_thread=False, isolation_level=None)
        self._conn.row_factory = sqlite3.Row
        self._lock = threading.RLock()
        self._conn.executescript(
            """
            PRAGMA journal_mode=WAL;
            PRAGMA synchronous=FULL;
            PRAGMA foreign_keys=ON;
            CREATE TABLE IF NOT EXISTS jobs (
              job_id TEXT PRIMARY KEY,
              idempotency_key TEXT UNIQUE NOT NULL,
              paper_id TEXT NOT NULL,
              paper_hash TEXT NOT NULL,
              doc_id TEXT NOT NULL,
              state TEXT NOT NULL,
              reason TEXT,
              received_at TEXT NOT NULL,
              updated_at TEXT NOT NULL,
              bundle_json BLOB,
              graph_json BLOB
            );
            CREATE TABLE IF NOT EXISTS events (
              event_id TEXT PRIMARY KEY,
              job_id TEXT NOT NULL REFERENCES jobs(job_id),
              sequence INTEGER NOT NULL,
              idempotency_key TEXT UNIQUE NOT NULL,
              state TEXT NOT NULL,
              occurred_at TEXT NOT NULL,
              event_json BLOB NOT NULL,
              UNIQUE(job_id, sequence)
            );
            CREATE TABLE IF NOT EXISTS provider_traces (
              trace_id TEXT PRIMARY KEY,
              job_id TEXT NOT NULL REFERENCES jobs(job_id),
              provider TEXT NOT NULL,
              role TEXT NOT NULL,
              returned_model TEXT NOT NULL,
              completed_at TEXT NOT NULL,
              trace_json BLOB NOT NULL
            );
            CREATE TABLE IF NOT EXISTS artifacts (
              artifact_id TEXT PRIMARY KEY,
              job_id TEXT NOT NULL REFERENCES jobs(job_id),
              paper_id TEXT NOT NULL,
              task_id TEXT NOT NULL,
              family TEXT NOT NULL,
              kind TEXT NOT NULL,
              status TEXT NOT NULL,
              created_at TEXT NOT NULL,
              artifact_json BLOB NOT NULL
            );
            CREATE TABLE IF NOT EXISTS artifact_audits (
              audit_id TEXT PRIMARY KEY,
              artifact_id TEXT NOT NULL REFERENCES artifacts(artifact_id),
              job_id TEXT NOT NULL REFERENCES jobs(job_id),
              decision TEXT NOT NULL CHECK(decision IN ('approved', 'rejected')),
              reviewer TEXT NOT NULL,
              created_at TEXT NOT NULL,
              audit_json BLOB NOT NULL
            );
            CREATE TABLE IF NOT EXISTS model_snapshots (
              provider TEXT NOT NULL,
              response_hash TEXT NOT NULL,
              discovered_at TEXT NOT NULL,
              drifted INTEGER NOT NULL,
              snapshot_json BLOB NOT NULL,
              PRIMARY KEY(provider, response_hash)
            );
            CREATE TABLE IF NOT EXISTS provider_results (
              job_id TEXT NOT NULL REFERENCES jobs(job_id),
              call_key TEXT NOT NULL,
              prompt_version TEXT NOT NULL,
              request_hash TEXT NOT NULL,
              response_json BLOB NOT NULL,
              trace_json BLOB NOT NULL,
              created_at TEXT NOT NULL,
              PRIMARY KEY(job_id, call_key, prompt_version, request_hash)
            );
            CREATE TABLE IF NOT EXISTS stream_checkpoints (
              job_id TEXT NOT NULL REFERENCES jobs(job_id),
              call_key TEXT NOT NULL,
              attempt INTEGER NOT NULL,
              partial_hash TEXT NOT NULL,
              partial_text BLOB NOT NULL,
              updated_at TEXT NOT NULL,
              PRIMARY KEY(job_id, call_key, attempt)
            );
            CREATE TABLE IF NOT EXISTS candidate_queue (
              doc_id TEXT PRIMARY KEY,
              payload BLOB NOT NULL,
              state TEXT NOT NULL,
              reasoning_score REAL NOT NULL DEFAULT 0,
              quality_score REAL NOT NULL DEFAULT 0,
              ranking_score REAL NOT NULL DEFAULT 0,
              domain_key TEXT NOT NULL DEFAULT 'general_scientific',
              valid_from TEXT NOT NULL DEFAULT '',
              enqueue_ordinal INTEGER NOT NULL DEFAULT 0,
              enqueued_at TEXT NOT NULL,
              updated_at TEXT NOT NULL,
              scientific_payload BLOB
            );
            CREATE TABLE IF NOT EXISTS daily_runs (
              run_date TEXT PRIMARY KEY,
              state TEXT NOT NULL,
              cutoff_at TEXT NOT NULL,
              cutoff_ordinal INTEGER NOT NULL DEFAULT 0,
              started_at TEXT NOT NULL,
              completed_at TEXT,
              candidate_count INTEGER NOT NULL,
              processed_count INTEGER NOT NULL DEFAULT 0,
              stop_reason TEXT
            );
            CREATE TABLE IF NOT EXISTS daily_run_candidates (
              run_date TEXT NOT NULL REFERENCES daily_runs(run_date),
              rank INTEGER NOT NULL,
              doc_id TEXT NOT NULL,
              PRIMARY KEY(run_date,doc_id),
              UNIQUE(run_date,rank)
            );
            CREATE TABLE IF NOT EXISTS manual_runs (
              run_id TEXT PRIMARY KEY,
              state TEXT NOT NULL,
              cutoff_at TEXT NOT NULL,
              cutoff_ordinal INTEGER NOT NULL DEFAULT 0,
              requested_at TEXT NOT NULL,
              started_at TEXT,
              completed_at TEXT,
              candidate_count INTEGER NOT NULL,
              max_candidates INTEGER,
              processed_count INTEGER NOT NULL DEFAULT 0,
              stop_reason TEXT
            );
            CREATE TABLE IF NOT EXISTS pool_assignments (
              allocation_key TEXT PRIMARY KEY,
              pool TEXT NOT NULL,
              ordinal INTEGER NOT NULL,
              dataset_split TEXT NOT NULL,
              assigned_at TEXT NOT NULL,
              UNIQUE(pool, ordinal)
            );
            CREATE TABLE IF NOT EXISTS control_sequences (
              name TEXT PRIMARY KEY,
              value INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS jobs_state_idx ON jobs(state, updated_at DESC);
            CREATE INDEX IF NOT EXISTS artifacts_created_idx ON artifacts(created_at DESC);
            CREATE INDEX IF NOT EXISTS traces_provider_idx ON provider_traces(provider, completed_at DESC);
            """
        )
        self._ensure_candidate_queue_columns()
        self._ensure_daily_run_columns()
        self._ensure_manual_run_columns()
        self._ensure_pool_assignment_columns()
        self._initialize_candidate_sequence()
        if recover_processing:
            self._conn.execute("UPDATE candidate_queue SET state='queued' WHERE state='processing'")

    def _ensure_candidate_queue_columns(self) -> None:
        existing = {
            str(row["name"])
            for row in self._conn.execute("PRAGMA table_info(candidate_queue)").fetchall()
        }
        additions = {
            "reasoning_score": "REAL NOT NULL DEFAULT 0",
            "quality_score": "REAL NOT NULL DEFAULT 0",
            "ranking_score": "REAL NOT NULL DEFAULT 0",
            "domain_key": "TEXT NOT NULL DEFAULT 'general_scientific'",
            "valid_from": "TEXT NOT NULL DEFAULT ''",
            "enqueue_ordinal": "INTEGER NOT NULL DEFAULT 0",
            "scientific_payload": "BLOB",
        }
        for name, declaration in additions.items():
            if name not in existing:
                self._conn.execute(f"ALTER TABLE candidate_queue ADD COLUMN {name} {declaration}")
        self._conn.execute(
            "CREATE INDEX IF NOT EXISTS candidate_queue_snapshot_idx "
            "ON candidate_queue(state,enqueue_ordinal)"
        )

    def _ensure_daily_run_columns(self) -> None:
        existing = {
            str(row["name"])
            for row in self._conn.execute("PRAGMA table_info(daily_runs)").fetchall()
        }
        if "cutoff_ordinal" not in existing:
            self._conn.execute(
                "ALTER TABLE daily_runs ADD COLUMN cutoff_ordinal INTEGER NOT NULL DEFAULT 0"
            )

    def _initialize_candidate_sequence(self) -> None:
        """Migrate existing candidates and initialize a transaction-safe sequence."""
        self._conn.execute(
            "UPDATE candidate_queue SET enqueue_ordinal=rowid WHERE enqueue_ordinal=0"
        )
        row = self._conn.execute(
            "SELECT COALESCE(MAX(enqueue_ordinal), 0) AS value FROM candidate_queue"
        ).fetchone()
        highest = int(row["value"])
        self._conn.execute(
            "INSERT OR IGNORE INTO control_sequences(name,value) VALUES ('candidate_enqueue', ?)",
            (highest,),
        )
        self._conn.execute(
            "UPDATE control_sequences SET value=MAX(value, ?) WHERE name='candidate_enqueue'",
            (highest,),
        )

    def _ensure_pool_assignment_columns(self) -> None:
        existing = {
            str(row["name"])
            for row in self._conn.execute("PRAGMA table_info(pool_assignments)").fetchall()
        }
        if "task_id" in existing and "allocation_key" not in existing:
            self._conn.execute(
                "ALTER TABLE pool_assignments RENAME COLUMN task_id TO allocation_key"
            )

    def _ensure_manual_run_columns(self) -> None:
        existing = {
            str(row["name"])
            for row in self._conn.execute("PRAGMA table_info(manual_runs)").fetchall()
        }
        if "max_candidates" not in existing:
            self._conn.execute("ALTER TABLE manual_runs ADD COLUMN max_candidates INTEGER")
        if "cutoff_ordinal" not in existing:
            self._conn.execute(
                "ALTER TABLE manual_runs ADD COLUMN cutoff_ordinal INTEGER NOT NULL DEFAULT 0"
            )

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def start_job(
        self,
        *,
        paper_id: str,
        paper_hash: str,
        doc_id: str,
        policy_version: str,
    ) -> tuple[str, bool]:
        key = sha256(
            {
                "paper_id": paper_id,
                "paper_hash": paper_hash,
                "doc_id": doc_id,
                "policy_version": policy_version,
            }
        )
        now = datetime.now(UTC).isoformat()
        job_id = stable_id("foundry-job", key)
        with self._lock:
            self._conn.execute(
                """
                INSERT OR IGNORE INTO jobs(
                  job_id,idempotency_key,paper_id,paper_hash,doc_id,state,received_at,updated_at
                ) VALUES (?, ?, ?, ?, ?, 'RECEIVED', ?, ?)
                """,
                (job_id, key, paper_id, paper_hash, doc_id, now, now),
            )
            row = self._conn.execute(
                "SELECT job_id,state FROM jobs WHERE idempotency_key=?", (key,)
            ).fetchone()
            assert row is not None
            created = self.event_count(job_id) == 0 or str(row["state"]) not in _TERMINAL
            return str(row["job_id"]), created

    def cached_provider_result(
        self,
        *,
        job_id: str,
        call_key: str,
        prompt_version: str,
        request_hash: str,
    ) -> tuple[dict[str, Any] | list[Any], ProviderTrace] | None:
        row = self._conn.execute(
            """
            SELECT response_json,trace_json FROM provider_results
            WHERE job_id=? AND call_key=? AND prompt_version=? AND request_hash=?
            """,
            (job_id, call_key, prompt_version, request_hash),
        ).fetchone()
        if row is None:
            return None
        response = json.loads(row["response_json"])
        return response, ProviderTrace.model_validate_json(row["trace_json"])

    def record_provider_result(
        self,
        *,
        job_id: str,
        call_key: str,
        prompt_version: str,
        request_hash: str,
        response: dict[str, Any] | list[Any],
        trace: ProviderTrace,
    ) -> None:
        with self._lock:
            self._conn.execute(
                """
                INSERT OR IGNORE INTO provider_results
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    job_id,
                    call_key,
                    prompt_version,
                    request_hash,
                    canonical_json(response),
                    canonical_json(trace),
                    datetime.now(UTC).isoformat(),
                ),
            )

    def save_stream_checkpoint(
        self,
        *,
        job_id: str,
        call_key: str,
        attempt: int,
        partial_text: str,
        partial_hash: str,
    ) -> None:
        with self._lock:
            self._conn.execute(
                """
                INSERT OR REPLACE INTO stream_checkpoints
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    job_id,
                    call_key,
                    attempt,
                    partial_hash,
                    partial_text.encode("utf-8"),
                    datetime.now(UTC).isoformat(),
                ),
            )

    def append_event(
        self,
        *,
        job_id: str,
        paper_id: str,
        state: str,
        reason: str | None = None,
        provider_trace_id: str | None = None,
        artifact_hash: str | None = None,
        metadata: dict[str, Any] | None = None,
        attempt: int = 1,
        idempotency_suffix: str = "",
        update_job_state: bool = True,
    ) -> FoundryEvent:
        with self._lock:
            row = self._conn.execute(
                "SELECT COALESCE(MAX(sequence), -1) AS seq FROM events WHERE job_id=?", (job_id,)
            ).fetchone()
            sequence = int(row["seq"]) + 1
            idempotency_key = sha256(
                {
                    "job_id": job_id,
                    "state": state,
                    "attempt": attempt,
                    "suffix": idempotency_suffix,
                }
            )
            existing = self._conn.execute(
                "SELECT event_json FROM events WHERE idempotency_key=?", (idempotency_key,)
            ).fetchone()
            if existing is not None:
                event = FoundryEvent.model_validate_json(existing["event_json"])
                if update_job_state:
                    self._conn.execute(
                        "UPDATE jobs SET state=?, reason=?, updated_at=? WHERE job_id=?",
                        (
                            event.state,
                            event.reason,
                            datetime.now(UTC).isoformat(),
                            job_id,
                        ),
                    )
                return event
            event = FoundryEvent(
                event_id=f"event:{uuid.uuid4()}",
                job_id=job_id,
                paper_id=paper_id,
                sequence=sequence,
                state=state,  # type: ignore[arg-type]
                occurred_at=datetime.now(UTC),
                attempt=attempt,
                idempotency_key=idempotency_key,
                provider_trace_id=provider_trace_id,
                artifact_hash=artifact_hash,
                reason=reason,
                metadata=metadata or {},
            )
            payload = canonical_json(event)
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                self._conn.execute(
                    "INSERT INTO events VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (
                        event.event_id,
                        job_id,
                        sequence,
                        idempotency_key,
                        state,
                        event.occurred_at.isoformat(),
                        payload,
                    ),
                )
                if update_job_state:
                    self._conn.execute(
                        "UPDATE jobs SET state=?, reason=?, updated_at=? WHERE job_id=?",
                        (state, reason, event.occurred_at.isoformat(), job_id),
                    )
                self._conn.commit()
            except Exception:
                self._conn.rollback()
                raise
            return event

    def next_provider_call_attempt(self, job_id: str) -> int:
        """Return a monotonically increasing provider-call event attempt."""
        row = self._conn.execute(
            """
            SELECT COALESCE(MAX(CAST(json_extract(event_json, '$.attempt') AS INTEGER)), 0) AS n
            FROM events
            WHERE job_id=? AND state='CALL_STARTED'
            """,
            (job_id,),
        ).fetchone()
        return int(row["n"]) + 1

    def save_bundle(self, job_id: str, payload: bytes) -> None:
        with self._lock:
            self._conn.execute("UPDATE jobs SET bundle_json=? WHERE job_id=?", (payload, job_id))

    def load_bundle(self, job_id: str) -> bytes | None:
        row = self._conn.execute(
            "SELECT bundle_json FROM jobs WHERE job_id=?", (job_id,)
        ).fetchone()
        if row is None or row["bundle_json"] is None:
            return None
        return bytes(row["bundle_json"])

    def save_graph(self, job_id: str, payload: bytes) -> None:
        with self._lock:
            self._conn.execute("UPDATE jobs SET graph_json=? WHERE job_id=?", (payload, job_id))

    def load_graph(self, job_id: str) -> bytes | None:
        row = self._conn.execute("SELECT graph_json FROM jobs WHERE job_id=?", (job_id,)).fetchone()
        if row is None or row["graph_json"] is None:
            return None
        return bytes(row["graph_json"])

    def record_trace(self, job_id: str, trace: ProviderTrace) -> None:
        with self._lock:
            self._conn.execute(
                """
                INSERT OR IGNORE INTO provider_traces VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    trace.trace_id,
                    job_id,
                    trace.provider,
                    trace.role,
                    trace.returned_model,
                    trace.completed_at.isoformat(),
                    canonical_json(trace),
                ),
            )

    def record_model_snapshot(self, snapshot: ProviderModelSnapshot) -> ProviderModelSnapshot:
        with self._lock:
            previous = self._conn.execute(
                "SELECT response_hash FROM model_snapshots WHERE provider=? ORDER BY discovered_at DESC LIMIT 1",
                (snapshot.provider,),
            ).fetchone()
            drifted = previous is not None and previous["response_hash"] != snapshot.response_hash
            value = snapshot.model_copy(
                update={
                    "drifted": drifted,
                    "previous_response_hash": previous["response_hash"] if previous else None,
                }
            )
            self._conn.execute(
                """
                INSERT INTO model_snapshots VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(provider,response_hash) DO UPDATE SET
                  discovered_at=excluded.discovered_at,
                  drifted=excluded.drifted,
                  snapshot_json=excluded.snapshot_json
                """,
                (
                    value.provider,
                    value.response_hash,
                    value.discovered_at.isoformat(),
                    int(value.drifted),
                    canonical_json(value),
                ),
            )
            return value

    def record_artifact(self, artifact: FoundryArtifactRecord) -> None:
        with self._lock:
            self._conn.execute(
                """
                INSERT OR REPLACE INTO artifacts VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    artifact.artifact_id,
                    artifact.job_id,
                    artifact.paper_id,
                    artifact.task_id,
                    artifact.family,
                    artifact.kind,
                    artifact.status,
                    artifact.created_at.isoformat(),
                    canonical_json(artifact),
                ),
            )

    def audit_artifact(
        self,
        *,
        artifact_id: str,
        decision: str,
        reviewer: str,
        reason: str | None = None,
    ) -> ArtifactAuditRecord:
        clean_reviewer = reviewer.strip()
        clean_reason = reason.strip() if reason and reason.strip() else None
        if decision not in {"approved", "rejected"}:
            raise ValueError("decision must be approved or rejected")
        if not clean_reviewer:
            raise ValueError("reviewer is required")
        with self._lock:
            row = self._conn.execute(
                "SELECT job_id FROM artifacts WHERE artifact_id=?", (artifact_id,)
            ).fetchone()
            if row is None:
                raise KeyError(artifact_id)
            audit = ArtifactAuditRecord(
                audit_id=f"audit:{uuid.uuid4()}",
                artifact_id=artifact_id,
                job_id=str(row["job_id"]),
                decision=decision,
                reviewer=clean_reviewer,
                reason=clean_reason,
                created_at=datetime.now(UTC),
            )
            self._conn.execute(
                "INSERT INTO artifact_audits VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    audit.audit_id,
                    audit.artifact_id,
                    audit.job_id,
                    audit.decision,
                    audit.reviewer,
                    audit.created_at.isoformat(),
                    canonical_json(audit),
                ),
            )
        return audit

    def artifact_audits(self, *, artifact_id: str | None = None) -> list[dict[str, Any]]:
        if artifact_id:
            rows = self._conn.execute(
                """
                SELECT audit_json FROM artifact_audits
                WHERE artifact_id=?
                ORDER BY created_at DESC,rowid DESC
                """,
                (artifact_id,),
            ).fetchall()
        else:
            rows = self._conn.execute(
                "SELECT audit_json FROM artifact_audits ORDER BY created_at DESC,rowid DESC"
            ).fetchall()
        return [
            ArtifactAuditRecord.model_validate_json(row["audit_json"]).model_dump(mode="json")
            for row in rows
        ]

    def enqueue_candidate(
        self,
        *,
        doc_id: str,
        payload: bytes,
        reasoning_score: float,
        quality_score: float,
        valid_from: datetime,
        scientific_payload: bytes | None = None,
        ranking_score: float | None = None,
        domain_key: str = "general_scientific",
    ) -> None:
        now = datetime.now(UTC).isoformat()
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                self._conn.execute(
                    "UPDATE control_sequences SET value=value+1 WHERE name='candidate_enqueue'"
                )
                sequence = int(
                    self._conn.execute(
                        "SELECT value FROM control_sequences WHERE name='candidate_enqueue'"
                    ).fetchone()["value"]
                )
                self._conn.execute(
                    """
                    INSERT INTO candidate_queue
                    (doc_id,payload,state,reasoning_score,quality_score,
                     ranking_score,domain_key,valid_from,enqueue_ordinal,enqueued_at,updated_at,
                     scientific_payload)
                    VALUES (?, ?, 'queued', ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(doc_id) DO UPDATE SET
                      payload=excluded.payload,
                      reasoning_score=excluded.reasoning_score,
                      quality_score=excluded.quality_score,
                      ranking_score=excluded.ranking_score,
                      domain_key=excluded.domain_key,
                      valid_from=excluded.valid_from,
                      enqueue_ordinal=excluded.enqueue_ordinal,
                      enqueued_at=excluded.enqueued_at,
                      updated_at=excluded.updated_at,
                      scientific_payload=excluded.scientific_payload
                    WHERE candidate_queue.state='queued'
                      AND (
                        candidate_queue.payload<>excluded.payload
                        OR COALESCE(candidate_queue.scientific_payload, X'')<>
                           COALESCE(excluded.scientific_payload, X'')
                      )
                    """,
                    (
                        doc_id,
                        payload,
                        reasoning_score,
                        quality_score,
                        ranking_score
                        if ranking_score is not None
                        else (reasoning_score + quality_score / 5.0) / 2.0,
                        domain_key,
                        valid_from.isoformat(),
                        sequence,
                        now,
                        now,
                        scientific_payload,
                    ),
                )
                self._conn.commit()
            except Exception:
                self._conn.rollback()
                raise

    def claim_candidate(
        self,
        *,
        cutoff_at: datetime,
        cutoff_ordinal: int | None = None,
        daily_run_date: date | None = None,
    ) -> tuple[str, bytes] | None:
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                boundary = "enqueue_ordinal<=?" if cutoff_ordinal is not None else "enqueued_at<=?"
                boundary_value: int | str = (
                    cutoff_ordinal if cutoff_ordinal is not None else cutoff_at.isoformat()
                )
                if daily_run_date is not None:
                    row = self._conn.execute(
                        f"""
                        SELECT candidate_queue.doc_id,candidate_queue.payload
                        FROM daily_run_candidates
                        JOIN candidate_queue USING(doc_id)
                        WHERE daily_run_candidates.run_date=?
                          AND candidate_queue.state='queued' AND {boundary}
                        ORDER BY daily_run_candidates.rank ASC
                        LIMIT 1
                        """,
                        (daily_run_date.isoformat(), boundary_value),
                    ).fetchone()
                else:
                    row = self._conn.execute(
                        f"""
                        SELECT doc_id,payload FROM candidate_queue
                        WHERE state='queued' AND {boundary}
                        ORDER BY ranking_score DESC, reasoning_score DESC,
                                 quality_score DESC,valid_from DESC,doc_id ASC
                        LIMIT 1
                        """,
                        (boundary_value,),
                    ).fetchone()
                if row is None:
                    self._conn.rollback()
                    return None
                self._conn.execute(
                    "UPDATE candidate_queue SET state='processing',updated_at=? WHERE doc_id=?",
                    (datetime.now(UTC).isoformat(), row["doc_id"]),
                )
                self._conn.commit()
                return str(row["doc_id"]), bytes(row["payload"])
            except Exception:
                self._conn.rollback()
                raise

    def candidate_scientific_payload(
        self,
        doc_id: str,
        *,
        expected_gold_payload: bytes | None = None,
    ) -> bytes | None:
        row = self._conn.execute(
            "SELECT payload,scientific_payload FROM candidate_queue WHERE doc_id=?",
            (doc_id,),
        ).fetchone()
        if row is None or row["scientific_payload"] is None:
            return None
        if expected_gold_payload is not None and bytes(row["payload"]) != expected_gold_payload:
            return None
        return bytes(row["scientific_payload"])

    def cache_candidate_scientific_payload(self, doc_id: str, payload: bytes) -> None:
        """Persist the validated source projection before provider work begins."""
        with self._lock:
            cursor = self._conn.execute(
                """
                UPDATE candidate_queue SET scientific_payload=?,updated_at=?
                WHERE doc_id=? AND state='processing'
                """,
                (payload, datetime.now(UTC).isoformat(), doc_id),
            )
            if cursor.rowcount != 1:
                raise KeyError(f"candidate is not processing: {doc_id}")

    def interrupted_provider_calls(self) -> list[dict[str, Any]]:
        """Return calls left without a terminal event by the prior worker."""
        rows = self._conn.execute(
            """
            SELECT events.event_json FROM events
            JOIN jobs ON jobs.job_id=events.job_id
            WHERE jobs.state NOT IN ('ACCEPTED_SFT','ACCEPTED_RL','REJECTED','DEPRECATED')
              AND events.state IN (
              'CALL_PLANNED','CALL_STARTED','CALL_SUCCEEDED','CALL_FAILED','CALL_RATE_LIMITED'
            )
            ORDER BY events.job_id,events.sequence
            """
        ).fetchall()
        planned: dict[tuple[str, int, str], FoundryEvent] = {}
        started: set[tuple[str, int, str]] = set()
        terminal: set[tuple[str, int, str]] = set()
        for row in rows:
            event = FoundryEvent.model_validate_json(row["event_json"])
            role = str(event.metadata.get("role", "unknown"))
            key = (event.job_id, event.attempt, role)
            if event.state == "CALL_PLANNED":
                planned[key] = event
            elif event.state == "CALL_STARTED":
                started.add(key)
            else:
                terminal.add(key)
        result: list[dict[str, Any]] = []
        for key, event in planned.items():
            if key in terminal:
                continue
            result.append(
                {
                    "job_id": event.job_id,
                    "paper_id": event.paper_id,
                    "attempt": event.attempt,
                    "role": key[2],
                    "provider": str(event.metadata.get("provider", "unknown")),
                    "was_started": key in started,
                }
            )
        return result

    def start_daily_run(
        self,
        day: date,
        *,
        boundary_at: datetime | None = None,
        candidate_limit: int | None = None,
    ) -> dict[str, Any]:
        """Freeze the ranked cohort from the 24 hours ending at ``boundary_at``."""
        if candidate_limit is not None and candidate_limit < 1:
            raise ValueError("daily candidate limit must be positive")
        now = datetime.now(UTC)
        cutoff_at = boundary_at or now
        if cutoff_at.tzinfo is None:
            raise ValueError("daily cohort boundary must be timezone-aware")
        cutoff_at = cutoff_at.astimezone(UTC)
        window_start = cutoff_at - timedelta(hours=24)
        day_text = day.isoformat()
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                self._conn.execute(
                    """
                    UPDATE daily_runs
                    SET state='superseded',completed_at=?,
                        stop_reason='superseded by next UTC run'
                    WHERE state='running' AND run_date<?
                    """,
                    (now.isoformat(), day_text),
                )
                existing = self._conn.execute(
                    "SELECT * FROM daily_runs WHERE run_date=?", (day_text,)
                ).fetchone()
                if existing is not None and str(existing["cutoff_at"]) == cutoff_at.isoformat():
                    self._conn.commit()
                    return dict(existing)
                # A changed configured boundary on the same UTC date replaces
                # the old snapshot once. This is needed when moving the
                # production schedule without waiting an extra day. The run's
                # stable date key is retained, while its candidate membership
                # is rebuilt against the new immutable cutoff.
                if existing is not None:
                    self._conn.execute(
                        "DELETE FROM daily_run_candidates WHERE run_date=?", (day_text,)
                    )
                    self._conn.execute("DELETE FROM daily_runs WHERE run_date=?", (day_text,))
                cutoff_ordinal = int(
                    self._conn.execute(
                        "SELECT value FROM control_sequences WHERE name='candidate_enqueue'"
                    ).fetchone()["value"]
                )
                # The daily cohort is intentionally fresh-only. Unprocessed
                # older rows are removed instead of accumulating a permanent
                # backlog that can starve new research.
                self._conn.execute(
                    "DELETE FROM candidate_queue WHERE state='queued' AND enqueued_at<=?",
                    (window_start.isoformat(),),
                )
                ranked_rows = self._conn.execute(
                    """
                    SELECT doc_id,ranking_score,reasoning_score,quality_score,
                           valid_from,domain_key
                    FROM candidate_queue
                    WHERE state='queued' AND enqueue_ordinal<=?
                      AND enqueued_at>? AND enqueued_at<=?
                    ORDER BY ranking_score DESC,reasoning_score DESC,quality_score DESC,
                             valid_from DESC,doc_id ASC
                    LIMIT ?
                    """,
                    (
                        cutoff_ordinal,
                        window_start.isoformat(),
                        cutoff_at.isoformat(),
                        candidate_limit if candidate_limit is not None else -1,
                    ),
                ).fetchall()
                selected_ids = [str(row["doc_id"]) for row in ranked_rows]
                candidate_count = len(selected_ids)
                state = "running" if candidate_count else "completed"
                completed_at = None if candidate_count else now.isoformat()
                stop_reason = None if candidate_count else "ranked 24-hour cohort is empty"
                self._conn.execute(
                    """
                    INSERT OR IGNORE INTO daily_runs(
                      run_date,state,cutoff_at,cutoff_ordinal,started_at,completed_at,
                      candidate_count,stop_reason
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        day_text,
                        state,
                        cutoff_at.isoformat(),
                        cutoff_ordinal,
                        now.isoformat(),
                        completed_at,
                        candidate_count,
                        stop_reason,
                    ),
                )
                self._conn.executemany(
                    """
                    INSERT OR IGNORE INTO daily_run_candidates(run_date,rank,doc_id)
                    VALUES (?, ?, ?)
                    """,
                    [(day_text, rank, doc_id) for rank, doc_id in enumerate(selected_ids, start=1)],
                )
                # The boundary is also the queue reset: candidates that were
                # eligible for this snapshot but ranked below the configured
                # cohort do not accumulate into an all-time backlog. Arrivals
                # after the frozen ordinal remain queued for tomorrow.
                self._conn.execute(
                    """
                    DELETE FROM candidate_queue
                    WHERE state='queued' AND enqueue_ordinal<=?
                      AND enqueued_at<=?
                      AND doc_id NOT IN (
                        SELECT doc_id FROM daily_run_candidates WHERE run_date=?
                      )
                    """,
                    (cutoff_ordinal, cutoff_at.isoformat(), day_text),
                )
                row = self._conn.execute(
                    "SELECT * FROM daily_runs WHERE run_date=?", (day_text,)
                ).fetchone()
                self._conn.commit()
            except Exception:
                self._conn.rollback()
                raise
        assert row is not None
        return dict(row)

    def expire_active_manual_runs(self, *, reason: str) -> int:
        """Stop diagnostic snapshots when a scheduled daily boundary takes ownership."""
        with self._lock:
            cursor = self._conn.execute(
                """
                UPDATE manual_runs
                SET state='failed',completed_at=?,stop_reason=?
                WHERE state IN ('pending','running')
                """,
                (datetime.now(UTC).isoformat(), reason),
            )
        return int(cursor.rowcount)

    def daily_run(self, day: date) -> dict[str, Any] | None:
        row = self._conn.execute(
            "SELECT * FROM daily_runs WHERE run_date=?", (day.isoformat(),)
        ).fetchone()
        return dict(row) if row is not None else None

    def finish_daily_run(self, day: date, *, state: str, reason: str | None = None) -> None:
        if state not in {"completed", "quota_exhausted"}:
            raise ValueError(f"invalid daily run terminal state {state}")
        with self._lock:
            self._conn.execute(
                """
                UPDATE daily_runs SET state=?,completed_at=?,stop_reason=?
                WHERE run_date=?
                """,
                (state, datetime.now(UTC).isoformat(), reason, day.isoformat()),
            )

    def record_daily_processed(self, day: date) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE daily_runs SET processed_count=processed_count+1 WHERE run_date=?",
                (day.isoformat(),),
            )

    def daily_runs(self, *, limit: int = 14) -> list[dict[str, Any]]:
        rows = self._conn.execute(
            "SELECT * FROM daily_runs ORDER BY run_date DESC LIMIT ?",
            (max(1, min(limit, 90)),),
        ).fetchall()
        return [dict(row) for row in rows]

    def request_manual_run(
        self, *, max_candidates: int | None = None
    ) -> tuple[dict[str, Any], bool]:
        """Queue one ranked snapshot of the fresh 24-hour candidate cohort.

        Manual operation follows the same freshness contract as the scheduled
        run: old queued work is discarded and arrivals after the snapshot
        boundary are deferred to the next cohort.
        """
        if max_candidates is not None and max_candidates < 1:
            raise ValueError("max_candidates must be positive")
        now = datetime.now(UTC)
        window_start = now - timedelta(hours=24)
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                active = self._conn.execute(
                    """
                    SELECT * FROM manual_runs
                    WHERE state IN ('pending', 'running')
                    ORDER BY requested_at ASC LIMIT 1
                    """
                ).fetchone()
                if active is not None:
                    self._conn.commit()
                    return dict(active), False
                self._conn.execute(
                    "DELETE FROM candidate_queue WHERE state='queued' AND enqueued_at<=?",
                    (window_start.isoformat(),),
                )
                queued_count = int(
                    self._conn.execute(
                        """
                        SELECT COUNT(*) AS n FROM candidate_queue
                        WHERE state='queued' AND enqueued_at>? AND enqueued_at<=?
                        """,
                        (window_start.isoformat(), now.isoformat()),
                    ).fetchone()["n"]
                )
                cutoff_ordinal = int(
                    self._conn.execute(
                        "SELECT value FROM control_sequences WHERE name='candidate_enqueue'"
                    ).fetchone()["value"]
                )
                candidate_count = (
                    min(queued_count, max_candidates)
                    if max_candidates is not None
                    else queued_count
                )
                run_id = stable_id("manual-foundry-run", f"{now.isoformat()}:{uuid.uuid4()}")
                state = "pending" if candidate_count else "completed"
                completed_at = None if candidate_count else now.isoformat()
                reason = None if candidate_count else "ranked snapshot is empty"
                self._conn.execute(
                    """
                    INSERT INTO manual_runs(
                      run_id,state,cutoff_at,cutoff_ordinal,requested_at,completed_at,
                      candidate_count,max_candidates,stop_reason
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        run_id,
                        state,
                        now.isoformat(),
                        cutoff_ordinal,
                        now.isoformat(),
                        completed_at,
                        candidate_count,
                        max_candidates,
                        reason,
                    ),
                )
                row = self._conn.execute(
                    "SELECT * FROM manual_runs WHERE run_id=?", (run_id,)
                ).fetchone()
                self._conn.commit()
            except Exception:
                self._conn.rollback()
                raise
        assert row is not None
        return dict(row), True

    def claim_manual_run(self) -> dict[str, Any] | None:
        """Claim the oldest pending run or resume the active run after restart."""
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                row = self._conn.execute(
                    """
                    SELECT * FROM manual_runs
                    WHERE state IN ('running', 'pending')
                    ORDER BY CASE state WHEN 'running' THEN 0 ELSE 1 END,
                             requested_at ASC LIMIT 1
                    """
                ).fetchone()
                if row is None:
                    self._conn.rollback()
                    return None
                if row["state"] == "pending":
                    self._conn.execute(
                        "UPDATE manual_runs SET state='running',started_at=? WHERE run_id=?",
                        (datetime.now(UTC).isoformat(), row["run_id"]),
                    )
                result = self._conn.execute(
                    "SELECT * FROM manual_runs WHERE run_id=?", (row["run_id"],)
                ).fetchone()
                self._conn.commit()
            except Exception:
                self._conn.rollback()
                raise
        return dict(result) if result is not None else None

    def record_manual_processed(self, run_id: str) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE manual_runs SET processed_count=processed_count+1 WHERE run_id=?",
                (run_id,),
            )

    def finish_manual_run(self, run_id: str, *, state: str, reason: str | None = None) -> None:
        if state not in {"completed", "quota_exhausted", "failed"}:
            raise ValueError(f"invalid manual run terminal state {state}")
        with self._lock:
            self._conn.execute(
                """
                UPDATE manual_runs SET state=?,completed_at=?,stop_reason=?
                WHERE run_id=?
                """,
                (state, datetime.now(UTC).isoformat(), reason, run_id),
            )

    def manual_runs(self, *, limit: int = 20) -> list[dict[str, Any]]:
        rows = self._conn.execute(
            "SELECT * FROM manual_runs ORDER BY requested_at DESC LIMIT ?",
            (max(1, min(limit, 100)),),
        ).fetchall()
        return [dict(row) for row in rows]

    def assign_pool_split(
        self,
        *,
        allocation_key: str,
        pool: PosttrainPool,
    ) -> tuple[DatasetSplit, int]:
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                existing = self._conn.execute(
                    "SELECT dataset_split,ordinal,pool FROM pool_assignments WHERE allocation_key=?",
                    (allocation_key,),
                ).fetchone()
                if existing is not None:
                    if existing["pool"] != pool:
                        raise ValueError("allocation pool changed across a retry")
                    self._conn.commit()
                    return cast(DatasetSplit, str(existing["dataset_split"])), int(
                        existing["ordinal"]
                    )
                row = self._conn.execute(
                    "SELECT COALESCE(MAX(ordinal), 0) AS n FROM pool_assignments WHERE pool=?",
                    (pool,),
                ).fetchone()
                ordinal = int(row["n"]) + 1
                dataset_split: DatasetSplit = "benchmark" if ordinal % 5 == 0 else "train"
                self._conn.execute(
                    "INSERT INTO pool_assignments VALUES (?, ?, ?, ?, ?)",
                    (
                        allocation_key,
                        pool,
                        ordinal,
                        dataset_split,
                        datetime.now(UTC).isoformat(),
                    ),
                )
                self._conn.commit()
                return dataset_split, ordinal
            except Exception:
                self._conn.rollback()
                raise

    def finish_candidate(self, doc_id: str) -> None:
        with self._lock:
            self._conn.execute("DELETE FROM candidate_queue WHERE doc_id=?", (doc_id,))

    def remove_queued_candidate(self, doc_id: str) -> None:
        with self._lock:
            self._conn.execute(
                "DELETE FROM candidate_queue WHERE doc_id=? AND state='queued'",
                (doc_id,),
            )

    def release_candidate(self, doc_id: str) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE candidate_queue SET state='queued',updated_at=? WHERE doc_id=?",
                (datetime.now(UTC).isoformat(), doc_id),
            )

    def queued_candidates(self) -> int:
        row = self._conn.execute(
            "SELECT COUNT(*) AS n FROM candidate_queue WHERE state='queued'"
        ).fetchone()
        return int(row["n"])

    def event_count(self, job_id: str) -> int:
        row = self._conn.execute(
            "SELECT COUNT(*) AS n FROM events WHERE job_id=?", (job_id,)
        ).fetchone()
        return int(row["n"])

    def job(self, job_id: str) -> dict[str, Any] | None:
        row = self._conn.execute("SELECT * FROM jobs WHERE job_id=?", (job_id,)).fetchone()
        if row is None:
            return None
        item = dict(row)
        item.pop("bundle_json", None)
        item.pop("graph_json", None)
        item["events"] = self.events(job_id)
        item["artifacts"] = self.artifacts(job_id=job_id, limit=200)
        item["provider_traces"] = self.traces(job_id=job_id)
        return item

    def jobs(self, *, limit: int = 100, state: str | None = None) -> list[dict[str, Any]]:
        params: list[Any] = []
        where = ""
        if state:
            where = "WHERE state=?"
            params.append(state)
        params.append(max(1, min(limit, 500)))
        rows = self._conn.execute(
            f"SELECT job_id,paper_id,doc_id,state,reason,received_at,updated_at FROM jobs {where} ORDER BY updated_at DESC LIMIT ?",
            params,
        ).fetchall()
        return [dict(row) for row in rows]

    def events(self, job_id: str) -> list[dict[str, Any]]:
        return [value.model_dump(mode="json") for value in self.event_records(job_id)]

    def event_records(self, job_id: str) -> list[FoundryEvent]:
        rows = self._conn.execute(
            "SELECT event_json FROM events WHERE job_id=? ORDER BY sequence", (job_id,)
        ).fetchall()
        return [FoundryEvent.model_validate_json(row["event_json"]) for row in rows]

    def artifact_records(self, job_id: str) -> list[FoundryArtifactRecord]:
        rows = self._conn.execute(
            "SELECT artifact_json FROM artifacts WHERE job_id=? ORDER BY created_at", (job_id,)
        ).fetchall()
        return [FoundryArtifactRecord.model_validate_json(row["artifact_json"]) for row in rows]

    def artifact(self, artifact_id: str) -> dict[str, Any] | None:
        row = self._conn.execute(
            "SELECT artifact_json FROM artifacts WHERE artifact_id=?", (artifact_id,)
        ).fetchone()
        if row is None:
            return None
        value = FoundryArtifactRecord.model_validate_json(row["artifact_json"]).model_dump(
            mode="json"
        )
        audits = self.artifact_audits(artifact_id=artifact_id)
        value["human_audit"] = audits[0] if audits else None
        value["human_audit_history"] = audits
        return value

    def artifacts(
        self,
        *,
        limit: int = 100,
        job_id: str | None = None,
    ) -> list[dict[str, Any]]:
        if job_id:
            rows = self._conn.execute(
                "SELECT artifact_json FROM artifacts WHERE job_id=? ORDER BY created_at DESC LIMIT ?",
                (job_id, max(1, min(limit, 500))),
            ).fetchall()
        else:
            rows = self._conn.execute(
                "SELECT artifact_json FROM artifacts ORDER BY created_at DESC LIMIT ?",
                (max(1, min(limit, 500)),),
            ).fetchall()
        values: list[dict[str, Any]] = []
        for row in rows:
            value = FoundryArtifactRecord.model_validate_json(row["artifact_json"]).model_dump(
                mode="json"
            )
            audits = self.artifact_audits(artifact_id=str(value["artifact_id"]))
            value["human_audit"] = audits[0] if audits else None
            value["human_audit_history"] = audits
            values.append(value)
        return values

    def traces(self, *, job_id: str | None = None) -> list[dict[str, Any]]:
        if job_id:
            rows = self._conn.execute(
                "SELECT trace_json FROM provider_traces WHERE job_id=? ORDER BY completed_at",
                (job_id,),
            ).fetchall()
        else:
            rows = self._conn.execute(
                "SELECT trace_json FROM provider_traces ORDER BY completed_at DESC LIMIT 500"
            ).fetchall()
        return [
            ProviderTrace.model_validate_json(row["trace_json"]).model_dump(mode="json")
            for row in rows
        ]

    def replay_fixture(self, *, job_id: str) -> dict[str, dict[str, list[Any]]]:
        """Return recorded structured outputs in their original per-role order."""
        rows = self._conn.execute(
            """
            SELECT response_json,trace_json,created_at,rowid FROM provider_results
            WHERE job_id=? ORDER BY created_at,rowid
            """,
            (job_id,),
        ).fetchall()
        fixture: dict[str, dict[str, list[Any]]] = {}
        for row in rows:
            trace = ProviderTrace.model_validate_json(row["trace_json"])
            fixture.setdefault(trace.provider, {}).setdefault(trace.role, []).append(
                json.loads(row["response_json"])
            )
        return fixture

    def provider_results(self, *, job_id: str) -> list[dict[str, Any]]:
        """Return durable structured generation results for artifact inspection."""
        rows = self._conn.execute(
            """
            SELECT call_key,prompt_version,response_json,trace_json,created_at,rowid
            FROM provider_results
            WHERE job_id=? ORDER BY created_at,rowid
            """,
            (job_id,),
        ).fetchall()
        return [
            {
                "call_key": str(row["call_key"]),
                "prompt_version": str(row["prompt_version"]),
                "response": json.loads(row["response_json"]),
                "trace": ProviderTrace.model_validate_json(row["trace_json"]).model_dump(
                    mode="json"
                ),
                "created_at": str(row["created_at"]),
            }
            for row in rows
        ]

    def dashboard(self) -> dict[str, Any]:
        counts = {
            row["state"]: int(row["n"])
            for row in self._conn.execute(
                "SELECT state, COUNT(*) AS n FROM jobs GROUP BY state"
            ).fetchall()
        }
        artifact_counts = {
            f"{row['kind']}:{row['status']}": int(row["n"])
            for row in self._conn.execute(
                "SELECT kind,status,COUNT(*) AS n FROM artifacts GROUP BY kind,status"
            ).fetchall()
        }
        family_counts = {
            row["family"]: int(row["n"])
            for row in self._conn.execute(
                "SELECT family,COUNT(*) AS n FROM artifacts WHERE status='accepted' GROUP BY family"
            ).fetchall()
        }
        split_counts = {
            f"{row['pool']}:{row['dataset_split']}": int(row["n"])
            for row in self._conn.execute(
                """
                SELECT pool,dataset_split,COUNT(*) AS n
                FROM pool_assignments GROUP BY pool,dataset_split
                """
            ).fetchall()
        }
        provider_counts = {
            row["provider"]: {
                "calls": int(row["calls"]),
                "input_tokens": int(row["input_tokens"] or 0),
                "output_tokens": int(row["output_tokens"] or 0),
            }
            for row in self._conn.execute(
                """
                SELECT provider,COUNT(*) AS calls,
                       SUM(CAST(json_extract(trace_json, '$.input_tokens') AS INTEGER)) AS input_tokens,
                       SUM(CAST(json_extract(trace_json, '$.output_tokens') AS INTEGER)) AS output_tokens
                FROM provider_traces GROUP BY provider
                """
            ).fetchall()
        }
        stage_counts = {
            row["state"]: int(row["n"])
            for row in self._conn.execute(
                "SELECT state,COUNT(*) AS n FROM events GROUP BY state"
            ).fetchall()
        }
        provider_statuses: dict[str, dict[str, Any]] = {}
        for row in self._conn.execute(
            """
            SELECT state,occurred_at,event_json FROM events
            WHERE state IN ('CALL_SUCCEEDED','CALL_FAILED','CALL_RATE_LIMITED')
            ORDER BY occurred_at DESC
            """
        ).fetchall():
            event = FoundryEvent.model_validate_json(row["event_json"])
            provider = event.metadata.get("provider")
            if not isinstance(provider, str) or provider in provider_statuses:
                continue
            provider_statuses[provider] = {
                "state": event.state,
                "reason": event.reason,
                "occurred_at": event.occurred_at.isoformat(),
            }
        audit_counts = {
            row["decision"]: int(row["n"])
            for row in self._conn.execute(
                """
                SELECT decision,COUNT(*) AS n
                FROM artifact_audits AS audit
                WHERE audit.audit_id=(
                  SELECT latest.audit_id
                  FROM artifact_audits AS latest
                  WHERE latest.artifact_id=audit.artifact_id
                  ORDER BY latest.created_at DESC,latest.rowid DESC
                  LIMIT 1
                )
                GROUP BY decision
                """
            ).fetchall()
        }
        return {
            "jobs": counts,
            "artifacts": artifact_counts,
            "families": family_counts,
            "splits": split_counts,
            "providers": provider_counts,
            "provider_statuses": provider_statuses,
            "stages": stage_counts,
            "recent_jobs": self.jobs(limit=12),
            "human_audits": audit_counts,
            "queued_candidates": self.queued_candidates(),
            "daily_runs": self.daily_runs(),
            "manual_runs": self.manual_runs(),
        }

    def activity(self, window: str, *, now: datetime | None = None) -> dict[str, Any]:
        """Return exact event and completed-token activity plus active stream checkpoints."""
        try:
            duration, bucket_seconds, point_count = _ACTIVITY_WINDOWS[window]
        except KeyError as exc:
            raise ValueError("window must be one of 5m, 1h, or 24h") from exc
        end = now or datetime.now(UTC)
        end = end.replace(tzinfo=UTC) if end.tzinfo is None else end.astimezone(UTC)
        start = end - duration
        start_epoch = start.timestamp()

        def empty_point(index: int) -> dict[str, Any]:
            ts = datetime.fromtimestamp(start_epoch + index * bucket_seconds, UTC)
            return {
                "ts": ts.isoformat(),
                "calls": {key: 0 for key in _CALL_ACTIVITY_STATES.values()},
                "tokens": {"input": 0, "output": 0},
                "stages": {key: 0 for key in _PIPELINE_ACTIVITY_STATES.values()},
            }

        points = [empty_point(index) for index in range(point_count)]
        totals = {
            "calls": {key: 0 for key in _CALL_ACTIVITY_STATES.values()},
            "tokens": {"input": 0, "output": 0},
            "stages": {key: 0 for key in _PIPELINE_ACTIVITY_STATES.values()},
        }

        def bucket_index(occurred_at: datetime) -> int | None:
            index = int((occurred_at.timestamp() - start_epoch) // bucket_seconds)
            if index == point_count and occurred_at <= end:
                return point_count - 1
            if index < 0 or index >= point_count:
                return None
            return index

        event_rows = self._conn.execute(
            "SELECT event_json FROM events WHERE occurred_at>=? AND occurred_at<=?",
            (start.isoformat(), end.isoformat()),
        ).fetchall()
        for row in event_rows:
            event = FoundryEvent.model_validate_json(row["event_json"])
            index = bucket_index(event.occurred_at)
            if index is None:
                continue
            call_key = _CALL_ACTIVITY_STATES.get(event.state)
            if call_key is not None:
                points[index]["calls"][call_key] += 1
                totals["calls"][call_key] += 1
            stage_key = _PIPELINE_ACTIVITY_STATES.get(event.state)
            if stage_key is not None:
                points[index]["stages"][stage_key] += 1
                totals["stages"][stage_key] += 1

        trace_rows = self._conn.execute(
            "SELECT trace_json FROM provider_traces WHERE completed_at>=? AND completed_at<=?",
            (start.isoformat(), end.isoformat()),
        ).fetchall()
        for row in trace_rows:
            trace = ProviderTrace.model_validate_json(row["trace_json"])
            index = bucket_index(trace.completed_at)
            if index is None:
                continue
            points[index]["tokens"]["input"] += trace.input_tokens
            points[index]["tokens"]["output"] += trace.output_tokens
            totals["tokens"]["input"] += trace.input_tokens
            totals["tokens"]["output"] += trace.output_tokens

        active_calls: list[dict[str, Any]] = []
        active_rows = self._conn.execute(
            """
            SELECT event_json FROM events AS started
            WHERE started.state='CALL_STARTED'
              AND NOT EXISTS (
                SELECT 1 FROM events AS superseding
                WHERE superseding.job_id=started.job_id
                  AND superseding.sequence>started.sequence
                  AND superseding.state='CALL_STARTED'
              )
              AND NOT EXISTS (
                SELECT 1 FROM events AS terminal
                WHERE terminal.job_id=started.job_id
                  AND terminal.sequence>started.sequence
                  AND terminal.state IN ('CALL_SUCCEEDED','CALL_FAILED','CALL_RATE_LIMITED')
                  AND CAST(json_extract(terminal.event_json, '$.attempt') AS INTEGER)=
                      CAST(json_extract(started.event_json, '$.attempt') AS INTEGER)
                  AND json_extract(terminal.event_json, '$.metadata.role')=
                      json_extract(started.event_json, '$.metadata.role')
              )
            ORDER BY started.occurred_at DESC
            """
        ).fetchall()
        for row in active_rows:
            event = FoundryEvent.model_validate_json(row["event_json"])
            role = str(event.metadata.get("role", "unknown"))
            provider = str(event.metadata.get("provider", "unknown"))
            checkpoint = self._conn.execute(
                """
                SELECT call_key,LENGTH(partial_text) AS partial_characters,updated_at
                FROM stream_checkpoints WHERE job_id=? AND attempt=?
                ORDER BY updated_at DESC LIMIT 1
                """,
                (event.job_id, event.attempt),
            ).fetchone()
            planned = self._conn.execute(
                """
                SELECT event_json FROM events
                WHERE job_id=? AND state='CALL_PLANNED'
                  AND CAST(json_extract(event_json, '$.attempt') AS INTEGER)=?
                  AND json_extract(event_json, '$.metadata.role')=?
                ORDER BY sequence DESC LIMIT 1
                """,
                (event.job_id, event.attempt, role),
            ).fetchone()
            planned_event = (
                FoundryEvent.model_validate_json(planned["event_json"])
                if planned is not None
                else None
            )
            job = self._conn.execute(
                "SELECT paper_id FROM jobs WHERE job_id=?", (event.job_id,)
            ).fetchone()
            active_calls.append(
                {
                    "job_id": event.job_id,
                    "paper_id": str(job["paper_id"]) if job is not None else event.paper_id,
                    "call_key": str(checkpoint["call_key"]) if checkpoint is not None else role,
                    "role": role,
                    "provider": provider,
                    "attempt": event.attempt,
                    "started_at": event.occurred_at.isoformat(),
                    "checkpoint_at": (
                        str(checkpoint["updated_at"]) if checkpoint is not None else None
                    ),
                    "partial_characters": (
                        int(checkpoint["partial_characters"]) if checkpoint is not None else 0
                    ),
                    "estimated_input_tokens": (
                        int(planned_event.metadata.get("estimated_input_tokens", 0))
                        if planned_event is not None
                        else 0
                    ),
                    "max_output_tokens": (
                        int(planned_event.metadata.get("max_output_tokens", 0))
                        if planned_event is not None
                        else 0
                    ),
                }
            )

        return {
            "window": window,
            "start": start.isoformat(),
            "end": end.isoformat(),
            "bucket_seconds": bucket_seconds,
            "totals": totals,
            "points": points,
            "active_calls": active_calls,
        }

    def model_snapshots(self) -> list[dict[str, Any]]:
        rows = self._conn.execute(
            """
            SELECT snapshot_json FROM model_snapshots
            WHERE (provider, discovered_at) IN (
              SELECT provider, MAX(discovered_at) FROM model_snapshots GROUP BY provider
            )
            ORDER BY provider
            """
        ).fetchall()
        return [
            ProviderModelSnapshot.model_validate_json(row["snapshot_json"]).model_dump(mode="json")
            for row in rows
        ]


__all__ = ["FoundryStore"]
