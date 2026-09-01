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
from collections.abc import Sequence
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
        self._threads: dict[str, threading.Thread] = {}
        self._running_topics: set[str] = set()
        self._running_lock = threading.Lock()
        self._write_lock = threading.Lock()
        self._caught_up = {
            self.decisions_topic: threading.Event(),
            self.admissions_topic: threading.Event(),
        }
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
        with self._running_lock:
            running = set(self._running_topics)
        return running == {self.decisions_topic, self.admissions_topic} and all(
            thread.is_alive() for thread in self._threads.values()
        )

    @property
    def ready(self) -> bool:
        return self.running and all(event.is_set() for event in self._caught_up.values())

    def start(self) -> None:
        if self._threads:
            return
        for topic, kind in (
            (self.decisions_topic, "decision"),
            (self.admissions_topic, "admission"),
        ):
            thread = threading.Thread(
                target=self._consume_topic,
                kwargs={"topic": topic, "kind": kind},
                name=f"serving-index-{kind}",
                daemon=True,
            )
            self._threads[topic] = thread
            thread.start()

    def close(self) -> None:
        self._stop.set()
        for thread in self._threads.values():
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
        self.apply_decisions(connection, [record])

    def apply_decisions(self, connection: Any, records: Sequence[GoldRecord]) -> None:
        if not records:
            return
        columns = self._columns(connection, _DECISION_TABLE)
        rows: list[list[Any]] = []
        for record in records:
            row = _decision_values(record)
            missing = sorted(set(columns) - set(row))
            if missing:
                raise ValueError(f"decision is missing serving columns: {missing}")
            rows.append([row[column] for column in columns])
        placeholders = ", ".join("?" for _ in columns)
        names = ", ".join(columns)
        assignments = ", ".join(
            f"{column} = excluded.{column}"
            for column in columns
            if column not in {"doc_id", "scoring_version", "classifier_revision", "policy_revision"}
        )
        statement = (
            f"INSERT INTO {_DECISION_TABLE} ({names}) VALUES ({placeholders}) "
            "ON CONFLICT (doc_id, scoring_version, classifier_revision, policy_revision) "
            f"DO UPDATE SET {assignments} WHERE excluded.trace_id < {_DECISION_TABLE}.trace_id"
        )
        with self._write_lock:
            connection.execute("BEGIN TRANSACTION")
            try:
                connection.executemany(statement, rows)
                connection.execute("COMMIT")
            except Exception:
                connection.execute("ROLLBACK")
                raise

    def apply_admission(self, connection: Any, record: LicenseAdmissionDecision) -> None:
        self.apply_admissions(connection, [record])

    def apply_admissions(
        self, connection: Any, records: Sequence[LicenseAdmissionDecision]
    ) -> None:
        if not records:
            return
        columns = self._columns(connection, _ADMISSION_TABLE)
        rows: list[list[Any]] = []
        for record in records:
            row = _admission_values(record)
            missing = sorted(set(columns) - set(row))
            if missing:
                raise ValueError(f"admission is missing serving columns: {missing}")
            rows.append([row[column] for column in columns])
        placeholders = ", ".join("?" for _ in columns)
        names = ", ".join(columns)
        assignments = ", ".join(
            f"{column} = excluded.{column}" for column in columns if column != "decision_id"
        )
        statement = (
            f"INSERT INTO {_ADMISSION_TABLE} ({names}) VALUES ({placeholders}) "
            f"ON CONFLICT (decision_id) DO UPDATE SET {assignments}"
        )
        with self._write_lock:
            connection.execute("BEGIN TRANSACTION")
            try:
                connection.executemany(statement, rows)
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

    def _consume_topic(self, *, topic: str, kind: str) -> None:
        import duckdb  # type: ignore[import-untyped]
        from confluent_kafka import Consumer, KafkaError  # type: ignore[import-untyped]

        connection: Any = duckdb.connect(self.database_path, read_only=False)
        batch_size = max(1, int(os.environ.get("S2P_SERVING_INDEX_BATCH_SIZE", "1000")))
        target_offsets: dict[int, int] = {}
        progress_offsets: dict[int, int] = {}
        consumer = Consumer(
            {
                "bootstrap.servers": self.brokers,
                "group.id": f"{self._consumer_group(connection)}-{kind}",
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

        def assigned(active_consumer: Any, partitions: list[Any]) -> None:
            committed = active_consumer.committed(partitions, timeout=10)
            committed_by_partition = {item.partition: item.offset for item in committed}
            for partition in partitions:
                low, high = active_consumer.get_watermark_offsets(partition, timeout=10)
                target_offsets[partition.partition] = high
                offset = committed_by_partition.get(partition.partition, -1)
                progress_offsets[partition.partition] = offset if offset >= 0 else low
            active_consumer.assign(partitions)
            self._mark_caught_up(topic, target_offsets, progress_offsets)

        consumer.subscribe([topic], on_assign=assigned)
        with self._running_lock:
            self._running_topics.add(topic)
        try:
            while not self._stop.is_set():
                messages = consumer.consume(num_messages=batch_size, timeout=1.0)
                if not messages:
                    continue
                decisions: list[GoldRecord] = []
                admissions: list[LicenseAdmissionDecision] = []
                invalid = 0
                handled: list[Any] = []
                for message in messages:
                    error = message.error()
                    if error is not None:
                        if error.code() == KafkaError._PARTITION_EOF:
                            continue
                        raise RuntimeError(str(error))
                    handled.append(message)
                    payload = message.value()
                    if payload is None:
                        continue
                    try:
                        if kind == "decision":
                            decisions.append(GoldRecord.model_validate_json(payload))
                        else:
                            admissions.append(LicenseAdmissionDecision.model_validate_json(payload))
                    except ValueError:
                        invalid += 1
                self.apply_decisions(connection, decisions)
                self.apply_admissions(connection, admissions)
                if handled:
                    consumer.commit(asynchronous=False)
                    for message in handled:
                        partition = message.partition()
                        progress_offsets[partition] = max(
                            progress_offsets.get(partition, 0), message.offset() + 1
                        )
                    self._mark_caught_up(topic, target_offsets, progress_offsets)
                if invalid:
                    _LOG.warning(
                        "serving_index_skipped_invalid_records",
                        extra={"topic": topic, "count": invalid},
                    )
        except Exception:
            _LOG.exception("serving_index_consumer_failed", extra={"topic": topic})
        finally:
            with self._running_lock:
                self._running_topics.discard(topic)
            consumer.close()
            connection.close()

    def _mark_caught_up(
        self,
        topic: str,
        target_offsets: dict[int, int],
        progress_offsets: dict[int, int],
    ) -> None:
        if target_offsets and all(
            progress_offsets.get(partition, -1) >= high
            for partition, high in target_offsets.items()
        ):
            self._caught_up[topic].set()

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
