"""Incremental Kafka-backed serving index for the monitoring UI.

The Iceberg tables remain authoritative. This read model exists so normal UI
requests never scan their complete snapshot history. It consumes the durable
decision and licence topics, upserts current rows into a retained local DuckDB
file, and acknowledges each Kafka message only after the local transaction.
"""

from __future__ import annotations

import logging
import os
import threading
import time
import uuid
from pathlib import Path
from typing import Any

from schemas.gold import GoldRecord
from schemas.license_admission import LicenseAdmissionDecision

_LOG = logging.getLogger("s2p.serving-index")
_DECISION_TABLE = "_serving_decision_records"
_ADMISSION_TABLE = "_serving_license_admission_records"


def _decision_values(record: GoldRecord) -> dict[str, Any]:
    import orjson

    row = record.model_dump(mode="python")
    row.pop("row_id", None)
    scores = row.pop("segment_scores", [])
    row["segment_scores_json"] = orjson.dumps(
        [
            score.model_dump(mode="json") if hasattr(score, "model_dump") else score
            for score in scores
        ]
    ).decode("utf-8")
    return row


def _admission_values(record: LicenseAdmissionDecision) -> dict[str, Any]:
    return record.model_dump(mode="json")


class ServingIndex:
    """Persistent current-state projection of the two monitoring topics."""

    def __init__(
        self,
        *,
        database_path: str,
        brokers: str,
        decisions_topic: str,
        admissions_topic: str,
    ) -> None:
        self.database_path = database_path
        self.brokers = brokers
        self.decisions_topic = decisions_topic
        self.admissions_topic = admissions_topic
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._running = threading.Event()
        self._initialize()

    @classmethod
    def from_env(cls) -> ServingIndex:
        return cls(
            database_path=os.environ.get(
                "S2P_SERVING_INDEX_DATABASE", "/var/lib/s2p-serving/serving.duckdb"
            ),
            brokers=os.environ.get("REDPANDA_BROKERS", "redpanda:9092"),
            decisions_topic=os.environ.get("S2P_DECISIONS_TOPIC", "curation.decisions"),
            admissions_topic=os.environ.get("S2P_LICENSE_ADMISSIONS_TOPIC", "license.admissions"),
        )

    @property
    def running(self) -> bool:
        return self._running.is_set() and self._thread is not None and self._thread.is_alive()

    def start(self) -> None:
        if self._thread is not None:
            return
        self._thread = threading.Thread(
            target=self._consume,
            name="serving-index-consumer",
            daemon=True,
        )
        self._thread.start()

    def close(self) -> None:
        self._stop.set()
        thread = self._thread
        if thread is not None:
            thread.join(timeout=10)

    def query_service(self) -> Any:
        """Return the existing typed query service over local current views."""
        import duckdb  # type: ignore[import-untyped]

        from processor.duckdb_api import (
            DuckDBQueryService,
            ScientificArtifactStore,
            _configure_runtime_limits,
        )

        connection: Any = duckdb.connect(self.database_path, read_only=False)
        _configure_runtime_limits(connection)
        return DuckDBQueryService(
            connection,
            gold_relation="serving_gold",
            decisions_relation="serving_decisions",
            license_admissions_relation="serving_license_admissions",
            refresh_iceberg=False,
            artifact_store=ScientificArtifactStore.from_env(),
        )

    def counts(self) -> dict[str, int]:
        import duckdb  # type: ignore[import-untyped]

        connection: Any = duckdb.connect(self.database_path, read_only=False)
        try:
            decision_row = connection.execute(f"SELECT COUNT(*) FROM {_DECISION_TABLE}").fetchone()
            admission_row = connection.execute(
                f"SELECT COUNT(*) FROM {_ADMISSION_TABLE}"
            ).fetchone()
            assert decision_row is not None and admission_row is not None
            decisions = int(decision_row[0])
            admissions = int(admission_row[0])
            return {"decisions": decisions, "license_admissions": admissions}
        finally:
            connection.close()

    def apply_decision(self, connection: Any, record: GoldRecord) -> None:
        row = _decision_values(record)
        columns = self._columns(connection, _DECISION_TABLE)
        missing = sorted(set(columns) - set(row))
        if missing:
            raise ValueError(f"decision is missing serving columns: {missing}")
        values = [row[column] for column in columns]
        connection.execute("BEGIN TRANSACTION")
        try:
            existing = connection.execute(
                f"SELECT trace_id FROM {_DECISION_TABLE} WHERE doc_id = ? "
                "AND scoring_version = ? AND classifier_revision = ? AND policy_revision = ?",
                [
                    record.doc_id,
                    record.scoring_version,
                    record.classifier_revision,
                    record.policy_revision,
                ],
            ).fetchone()
            if existing is not None and str(existing[0]) <= record.trace_id:
                connection.execute("COMMIT")
                return
            connection.execute(
                f"DELETE FROM {_DECISION_TABLE} WHERE doc_id = ? AND scoring_version = ? "
                "AND classifier_revision = ? AND policy_revision = ?",
                [
                    record.doc_id,
                    record.scoring_version,
                    record.classifier_revision,
                    record.policy_revision,
                ],
            )
            placeholders = ", ".join("?" for _ in columns)
            names = ", ".join(columns)
            connection.execute(
                f"INSERT INTO {_DECISION_TABLE} ({names}) VALUES ({placeholders})", values
            )
            connection.execute("COMMIT")
        except Exception:
            connection.execute("ROLLBACK")
            raise

    def apply_admission(self, connection: Any, record: LicenseAdmissionDecision) -> None:
        row = _admission_values(record)
        columns = self._columns(connection, _ADMISSION_TABLE)
        missing = sorted(set(columns) - set(row))
        if missing:
            raise ValueError(f"admission is missing serving columns: {missing}")
        values = [row[column] for column in columns]
        connection.execute("BEGIN TRANSACTION")
        try:
            connection.execute(
                f"DELETE FROM {_ADMISSION_TABLE} WHERE decision_id = ?", [record.decision_id]
            )
            placeholders = ", ".join("?" for _ in columns)
            names = ", ".join(columns)
            connection.execute(
                f"INSERT INTO {_ADMISSION_TABLE} ({names}) VALUES ({placeholders})", values
            )
            connection.execute("COMMIT")
        except Exception:
            connection.execute("ROLLBACK")
            raise

    def _initialize(self) -> None:
        import duckdb  # type: ignore[import-untyped]

        from processor.duckdb_api import (
            _configure_runtime_limits,
            _create_empty_gold_relation,
            _create_empty_license_relation,
        )

        path = Path(self.database_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        connection: Any = duckdb.connect(self.database_path, read_only=False)
        try:
            _configure_runtime_limits(connection)
            _create_empty_gold_relation(connection, "_serving_gold_shape")
            _create_empty_license_relation(connection, "_serving_admission_shape")
            connection.execute(
                f"CREATE TABLE IF NOT EXISTS {_DECISION_TABLE} AS SELECT * FROM _serving_gold_shape"
            )
            connection.execute(
                f"CREATE TABLE IF NOT EXISTS {_ADMISSION_TABLE} AS "
                "SELECT * FROM _serving_admission_shape"
            )
            connection.execute(
                f"CREATE UNIQUE INDEX IF NOT EXISTS serving_decision_key ON {_DECISION_TABLE} "
                "(doc_id, scoring_version, classifier_revision, policy_revision)"
            )
            connection.execute(
                f"CREATE UNIQUE INDEX IF NOT EXISTS serving_admission_key ON {_ADMISSION_TABLE} "
                "(decision_id)"
            )
            connection.execute(
                "CREATE TABLE IF NOT EXISTS _serving_metadata (key VARCHAR PRIMARY KEY, value VARCHAR)"
            )
            existing = connection.execute(
                "SELECT value FROM _serving_metadata WHERE key = 'instance_id'"
            ).fetchone()
            if existing is None:
                connection.execute(
                    "INSERT INTO _serving_metadata VALUES ('instance_id', ?)", [uuid.uuid4().hex]
                )
            trainable = (
                "risk_tier = 1 AND route IN ('pretrain', 'broad_pretraining', "
                "'posttrain_candidate', 'reasoning_candidate') "
                "AND ARRAY_LENGTH(reject_reasons) = 0 AND ARRAY_LENGTH(pii_flags) = 0"
            )
            connection.execute(
                f"CREATE OR REPLACE VIEW serving_decisions AS SELECT * FROM {_DECISION_TABLE}"
            )
            connection.execute(
                f"CREATE OR REPLACE VIEW serving_gold AS SELECT * FROM {_DECISION_TABLE} "
                f"WHERE {trainable}"
            )
            connection.execute(
                "CREATE OR REPLACE VIEW serving_license_admissions AS "
                f"SELECT * FROM {_ADMISSION_TABLE}"
            )
        finally:
            connection.close()

    def _consumer_group(self, connection: Any) -> str:
        instance = str(
            connection.execute(
                "SELECT value FROM _serving_metadata WHERE key = 'instance_id'"
            ).fetchone()[0]
        )
        return f"s2p-serving-index-{instance}"

    def _consume(self) -> None:
        import duckdb  # type: ignore[import-untyped]
        from confluent_kafka import Consumer, KafkaError  # type: ignore[import-untyped]

        connection: Any = duckdb.connect(self.database_path, read_only=False)
        consumer = Consumer(
            {
                "bootstrap.servers": self.brokers,
                "group.id": self._consumer_group(connection),
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
                "fetch.message.max.bytes": int(
                    os.environ.get("S2P_KAFKA_MESSAGE_MAX_BYTES", "67108864")
                ),
                "max.partition.fetch.bytes": int(
                    os.environ.get("S2P_KAFKA_MESSAGE_MAX_BYTES", "67108864")
                ),
            }
        )
        consumer.subscribe([self.decisions_topic, self.admissions_topic])
        self._running.set()
        try:
            while not self._stop.is_set():
                message = consumer.poll(1.0)
                if message is None:
                    continue
                error = message.error()
                if error is not None:
                    if error.code() == KafkaError._PARTITION_EOF:
                        continue
                    raise RuntimeError(str(error))
                payload = message.value()
                if payload is None:
                    consumer.commit(message=message, asynchronous=False)
                    continue
                try:
                    decision = (
                        GoldRecord.model_validate_json(payload)
                        if message.topic() == self.decisions_topic
                        else None
                    )
                    admission = (
                        LicenseAdmissionDecision.model_validate_json(payload)
                        if message.topic() == self.admissions_topic
                        else None
                    )
                except ValueError:
                    _LOG.exception(
                        "serving_index_invalid_record",
                        extra={"topic": message.topic(), "partition": message.partition()},
                    )
                    consumer.commit(message=message, asynchronous=False)
                    continue
                if decision is not None:
                    self.apply_decision(connection, decision)
                elif admission is not None:
                    self.apply_admission(connection, admission)
                consumer.commit(message=message, asynchronous=False)
        except Exception:
            _LOG.exception("serving_index_consumer_failed")
        finally:
            self._running.clear()
            consumer.close()
            connection.close()

    @staticmethod
    def _columns(connection: Any, table: str) -> list[str]:
        return [
            str(row[1]) for row in connection.execute(f"PRAGMA table_info('{table}')").fetchall()
        ]


def wait_until_running(index: ServingIndex, timeout_seconds: float = 10.0) -> None:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        if index.running:
            return
        time.sleep(0.05)
    raise RuntimeError("serving index consumer did not start")
