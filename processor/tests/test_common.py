"""Tests for :mod:`processor.common` serde + config."""

from __future__ import annotations

import json
import sys
from datetime import UTC, datetime
from types import ModuleType
from typing import ClassVar

import pytest

from processor.common import (
    BytewaxRuntimeStatus,
    DurableProcessingFailureWriter,
    bronze_loads,
    gold_dumps,
    gold_loads,
    kafka_consumer_config,
    kafka_payload_max_bytes,
    kafka_producer_config,
    kafka_source_batch_size,
    kafka_starting_offset,
    load_config,
    new_trace_id,
    run_bytewax_flow,
    silver_dumps,
    silver_loads,
    tracked_kafka_source,
)
from schemas.bronze import BronzeRecord
from schemas.gold import GoldRecord
from schemas.silver import SilverRecord


def test_new_trace_id_format() -> None:
    tid = new_trace_id()
    assert len(tid) == 32
    assert all(c in "0123456789abcdef" for c in tid)


def test_load_config_reads_kubernetes_env_contract(monkeypatch) -> None:
    monkeypatch.setenv("S2P_ENV", "prod")
    monkeypatch.setenv("LOG_LEVEL", "DEBUG")
    monkeypatch.setenv("REDPANDA_BROKERS", "redpanda:9092")
    monkeypatch.setenv("S2P_CONSUMER_GROUP", "s2p-test")
    monkeypatch.setenv("S2P_RAW_TOPIC", "raw.prod")
    monkeypatch.setenv("S2P_NORMALIZED_TOPIC", "normalized.prod")
    monkeypatch.setenv("S2P_CURATED_TOPIC", "curated.prod")
    monkeypatch.setenv("MINIO_ENDPOINT", "http://minio:9000")
    monkeypatch.setenv("MINIO_ACCESS_KEY", "access")
    monkeypatch.setenv("MINIO_SECRET_KEY", "secret")
    monkeypatch.setenv("MINIO_BRONZE_BUCKET", "bronze-prod")
    monkeypatch.setenv("MINIO_SILVER_BUCKET", "silver-prod")
    monkeypatch.setenv("MINIO_GOLD_BUCKET", "gold-prod")
    monkeypatch.setenv("POLARIS_URI", "http://polaris:8181/api/catalog")
    monkeypatch.setenv("POLARIS_WAREHOUSE", "s3://gold/warehouse")

    cfg = load_config()

    assert cfg.env == "prod"
    assert cfg.log_level == "DEBUG"
    assert cfg.redpanda_brokers == "redpanda:9092"
    assert cfg.consumer_group == "s2p-test"
    assert cfg.raw_topic == "raw.prod"
    assert cfg.normalized_topic == "normalized.prod"
    assert cfg.curated_topic == "curated.prod"
    assert cfg.minio_endpoint == "http://minio:9000"
    assert cfg.minio_access_key == "access"
    assert cfg.minio_secret_key == "secret"
    assert cfg.bronze_bucket == "bronze-prod"
    assert cfg.silver_bucket == "silver-prod"
    assert cfg.gold_bucket == "gold-prod"
    assert cfg.polaris_uri == "http://polaris:8181/api/catalog"
    assert cfg.polaris_warehouse == "s3://gold/warehouse"


def test_kafka_starting_offset_maps_names(monkeypatch) -> None:
    monkeypatch.delenv("S2P_KAFKA_START_OFFSET", raising=False)
    assert kafka_starting_offset() == -2

    monkeypatch.setenv("S2P_KAFKA_START_OFFSET", "latest")
    assert kafka_starting_offset() == -1

    monkeypatch.setenv("S2P_KAFKA_START_OFFSET", "stored")
    assert kafka_starting_offset() == -1000

    monkeypatch.setenv("S2P_KAFKA_START_OFFSET", "42")
    assert kafka_starting_offset() == 42


def test_kafka_consumer_config_sets_group_id() -> None:
    assert kafka_consumer_config("s2p-fetcher") == {
        "group.id": "s2p-fetcher",
        "auto.offset.reset": "earliest",
        "fetch.message.max.bytes": "1048576",
    }


def test_kafka_message_configs_follow_environment(monkeypatch) -> None:
    monkeypatch.setenv("S2P_KAFKA_MESSAGE_MAX_BYTES", "2097152")

    assert kafka_consumer_config("curator") == {
        "group.id": "curator",
        "auto.offset.reset": "earliest",
        "fetch.message.max.bytes": "2097152",
    }
    assert kafka_producer_config() == {"message.max.bytes": "2097152"}
    assert kafka_payload_max_bytes() == 2031616


def test_kafka_payload_limit_requires_framing_headroom(monkeypatch) -> None:
    monkeypatch.setenv("S2P_KAFKA_MESSAGE_MAX_BYTES", "2097152")
    monkeypatch.setenv("S2P_KAFKA_PAYLOAD_MAX_BYTES", "2097152")

    with pytest.raises(RuntimeError, match="must be positive and smaller"):
        kafka_payload_max_bytes()


def test_kafka_source_batch_size_defaults_to_one_and_requires_positive(monkeypatch) -> None:
    monkeypatch.delenv("S2P_BYTEWAX_SOURCE_BATCH_SIZE", raising=False)
    assert kafka_source_batch_size() == 1

    monkeypatch.setenv("S2P_BYTEWAX_SOURCE_BATCH_SIZE", "4")
    assert kafka_source_batch_size() == 4

    monkeypatch.setenv("S2P_BYTEWAX_SOURCE_BATCH_SIZE", "0")
    with pytest.raises(RuntimeError, match="must be positive"):
        kafka_source_batch_size()


def test_tracked_kafka_source_forwards_explicit_batch_bound(monkeypatch) -> None:
    captured: dict[str, object] = {}

    class FakeKafkaSource:
        def __init__(self, **kwargs: object) -> None:
            captured.update(kwargs)

    bytewax_package = ModuleType("bytewax")
    bytewax_package.__path__ = []  # type: ignore[attr-defined]
    connectors_package = ModuleType("bytewax.connectors")
    connectors_package.__path__ = []  # type: ignore[attr-defined]
    kafka_module = ModuleType("bytewax.connectors.kafka")
    kafka_module.KafkaSource = FakeKafkaSource  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "bytewax", bytewax_package)
    monkeypatch.setitem(sys.modules, "bytewax.connectors", connectors_package)
    monkeypatch.setitem(sys.modules, "bytewax.connectors.kafka", kafka_module)

    tracked_kafka_source(
        runtime_status=None,
        source_name="raw_fetched",
        brokers=["redpanda:9092"],
        topics=["raw.fetched"],
        starting_offset=-1000,
        add_config={"group.id": "fetcher"},
        batch_size=1,
    )

    assert captured["batch_size"] == 1
    assert captured["topics"] == ["raw.fetched"]


def test_run_bytewax_flow_initializes_and_reuses_recovery(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("S2P_STATE_DIR", str(tmp_path))
    monkeypatch.setenv("S2P_BYTEWAX_SNAPSHOT_SECONDS", "2.5")
    cfg = load_config()
    calls: dict[str, object] = {"init_count": 0}

    class FakeRecoveryConfig:
        def __init__(self, path) -> None:
            self.path = path

    def fake_init_db_dir(path, part_count) -> None:
        calls["init_count"] = int(calls["init_count"]) + 1
        calls["part_count"] = part_count
        (path / "part-0.sqlite3").touch()

    def fake_cli_main(flow, *, epoch_interval, recovery_config) -> None:
        calls["flow"] = flow
        calls["epoch_interval"] = epoch_interval
        calls["recovery_path"] = recovery_config.path

    bytewax_package = ModuleType("bytewax")
    bytewax_package.__path__ = []  # type: ignore[attr-defined]
    recovery_module = ModuleType("bytewax.recovery")
    recovery_module.RecoveryConfig = FakeRecoveryConfig  # type: ignore[attr-defined]
    recovery_module.init_db_dir = fake_init_db_dir  # type: ignore[attr-defined]
    run_module = ModuleType("bytewax.run")
    run_module.cli_main = fake_cli_main  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "bytewax", bytewax_package)
    monkeypatch.setitem(sys.modules, "bytewax.recovery", recovery_module)
    monkeypatch.setitem(sys.modules, "bytewax.run", run_module)

    flow = object()
    run_bytewax_flow(flow, cfg, "fetcher")
    run_bytewax_flow(flow, cfg, "fetcher")

    expected = tmp_path / "bytewax" / "fetcher"
    assert calls["init_count"] == 1
    assert calls["part_count"] == 1
    assert calls["flow"] is flow
    assert calls["epoch_interval"].total_seconds() == 2.5  # type: ignore[union-attr]
    assert calls["recovery_path"] == expected


def test_runtime_status_requires_runtime_and_every_source() -> None:
    status = BytewaxRuntimeStatus()
    status.register_source("decisions")
    status.register_source("admissions")
    status.mark_runtime_started()
    status.mark_source_assigned("decisions")
    assert status.is_ready() is False
    status.mark_source_assigned("admissions")
    assert status.is_ready() is True
    status.mark_runtime_stopped()
    assert status.is_ready() is False


def test_run_bytewax_flow_rejects_recovery_partition_drift(monkeypatch, tmp_path) -> None:
    recovery_dir = tmp_path / "bytewax" / "fetcher-v2"
    recovery_dir.mkdir(parents=True)
    (recovery_dir / "part-0.sqlite3").touch()
    monkeypatch.setenv("S2P_STATE_DIR", str(tmp_path))
    monkeypatch.setenv("S2P_BYTEWAX_RECOVERY_PARTITIONS", "2")

    bytewax_package = ModuleType("bytewax")
    bytewax_package.__path__ = []  # type: ignore[attr-defined]
    recovery_module = ModuleType("bytewax.recovery")
    recovery_module.RecoveryConfig = object  # type: ignore[attr-defined]
    recovery_module.init_db_dir = lambda *_args: None  # type: ignore[attr-defined]
    run_module = ModuleType("bytewax.run")
    run_module.cli_main = lambda *_args, **_kwargs: None  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "bytewax", bytewax_package)
    monkeypatch.setitem(sys.modules, "bytewax.recovery", recovery_module)
    monkeypatch.setitem(sys.modules, "bytewax.run", run_module)

    with pytest.raises(RuntimeError, match="recovery partition mismatch"):
        run_bytewax_flow(object(), load_config(), "fetcher-v2")


def test_durable_processing_failure_is_idempotent_and_auditable() -> None:
    class FakeS3:
        def __init__(self) -> None:
            self.writes: list[dict[str, object]] = []

        def put_object(self, **kwargs) -> None:
            self.writes.append(kwargs)

    class Message:
        topic = "docs.normalized"
        partition = 2
        offset = 41
        key = b"sha256:key-fallback"
        headers: ClassVar[list[tuple[str, bytes]]] = []
        value = b'{"doc_id":"sha256:test","trace_id":"0123456789abcdef0123456789abcdef"}'

    s3 = FakeS3()
    writer = DurableProcessingFailureWriter(s3=s3, bucket="gold")
    first = writer.record(stage="curate", message=Message(), reason="ValidationError")
    second = writer.record(stage="curate", message=Message(), reason="ValidationError")

    assert first == second
    assert s3.writes[0]["Key"] == s3.writes[1]["Key"]
    body = json.loads(s3.writes[0]["Body"])
    assert body["doc_id"] == "sha256:test"
    assert body["topic"] == "docs.normalized"
    assert body["partition"] == 2
    assert body["offset"] == 41
    assert body["retry_classification"] == "deterministic"
    assert body["error_revision"] == "processing-failure-v1"


def test_durable_processing_failure_recovers_audit_identity_from_message() -> None:
    class FakeS3:
        def __init__(self) -> None:
            self.body = b""

        def put_object(self, **kwargs) -> None:
            self.body = kwargs["Body"]

    class Message:
        topic = "curation.decisions"
        partition = 0
        offset = 9
        key = b"sha256:key-fallback"
        headers: ClassVar[list[tuple[str, bytes]]] = [
            ("trace_id", b"fedcba9876543210fedcba9876543210")
        ]
        value = b"malformed"

    s3 = FakeS3()
    DurableProcessingFailureWriter(s3=s3, bucket="gold").record(
        stage="iceberg-gold", message=Message(), reason="ValidationError"
    )

    body = json.loads(s3.body)
    assert body["doc_id"] == "sha256:key-fallback"
    assert body["trace_id"] == "fedcba9876543210fedcba9876543210"


def test_bronze_roundtrip() -> None:
    rec = BronzeRecord(
        doc_id="sha256:" + "0" * 64,
        url="https://example.com/x",
        fetched_at=datetime(2026, 6, 15, tzinfo=UTC),
        http_status=200,
        http_last_modified=None,
        content_type="text/html",
        raw_html_s3_uri="s3://bronze/x.html",
        source_feed="rss-test",
        trace_id="0" * 32,
    )
    payload = rec.model_dump_json().encode("utf-8")
    parsed = bronze_loads(payload)
    assert parsed.doc_id == rec.doc_id
    assert parsed.source_feed == rec.source_feed


def test_silver_roundtrip(silver_record: SilverRecord) -> None:
    silver_record = silver_record.model_copy(update={"minhash_sig": b"\x00\xff\x80\x01" * 112})
    payload = silver_dumps(silver_record)
    parsed = silver_loads(payload)
    assert parsed.doc_id == silver_record.doc_id
    assert parsed.lang == silver_record.lang
    assert parsed.minhash_sig == silver_record.minhash_sig


def test_gold_roundtrip() -> None:
    rec = GoldRecord(
        doc_id="sha256:" + "f" * 64,
        text="hello",
        lang="en",
        tokens=1,
        quality_score=4.0,
        edu_score=4.0,
        license="unknown",
        license_source="unknown",
        risk_tier=1,
        valid_from=datetime(2026, 6, 15, tzinfo=UTC),
        scoring_version="v0.1.0",
        classifier_revision="proxy-heuristic-0.1",
        policy_revision="git:test",
        trace_id="0" * 32,
    )
    payload = gold_dumps(rec)
    parsed = gold_loads(payload)
    assert parsed.doc_id == rec.doc_id
