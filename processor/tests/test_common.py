"""Tests for :mod:`processor.common` serde + config."""

from __future__ import annotations

from datetime import UTC, datetime

from processor.common import (
    bronze_loads,
    decon_dumps,
    decon_loads,
    gold_dumps,
    gold_loads,
    kafka_consumer_config,
    kafka_starting_offset,
    load_config,
    new_trace_id,
    silver_dumps,
    silver_loads,
)
from schemas.bronze import BronzeRecord
from schemas.decon import DeconAttestation
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
    monkeypatch.setenv("S2P_DECON_TOPIC", "decon.prod")
    monkeypatch.setenv("MINIO_ENDPOINT", "http://minio:9000")
    monkeypatch.setenv("MINIO_ACCESS_KEY", "access")
    monkeypatch.setenv("MINIO_SECRET_KEY", "secret")
    monkeypatch.setenv("MINIO_BRONZE_BUCKET", "bronze-prod")
    monkeypatch.setenv("MINIO_SILVER_BUCKET", "silver-prod")
    monkeypatch.setenv("MINIO_GOLD_BUCKET", "gold-prod")
    monkeypatch.setenv("MINIO_DECON_BUCKET", "decon-prod")
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
    assert cfg.decon_attest_topic == "decon.prod"
    assert cfg.minio_endpoint == "http://minio:9000"
    assert cfg.minio_access_key == "access"
    assert cfg.minio_secret_key == "secret"
    assert cfg.bronze_bucket == "bronze-prod"
    assert cfg.silver_bucket == "silver-prod"
    assert cfg.gold_bucket == "gold-prod"
    assert cfg.decon_bucket == "decon-prod"
    assert cfg.polaris_uri == "http://polaris:8181/api/catalog"
    assert cfg.polaris_warehouse == "s3://gold/warehouse"


def test_kafka_starting_offset_maps_names(monkeypatch) -> None:
    monkeypatch.delenv("S2P_KAFKA_START_OFFSET", raising=False)
    assert kafka_starting_offset() == -2

    monkeypatch.setenv("S2P_KAFKA_START_OFFSET", "latest")
    assert kafka_starting_offset() == -1

    monkeypatch.setenv("S2P_KAFKA_START_OFFSET", "42")
    assert kafka_starting_offset() == 42


def test_kafka_consumer_config_sets_group_id() -> None:
    assert kafka_consumer_config("s2p-fetcher") == {
        "group.id": "s2p-fetcher",
        "auto.offset.reset": "earliest",
    }


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
    silver_record = silver_record.model_copy(
        update={"minhash_sig": b"\x00\xff\x80\x01" * 112}
    )
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


def test_decon_roundtrip_canonical_bytes() -> None:
    rec = DeconAttestation(
        snapshot_id=1,
        committed_at=datetime(2026, 6, 15, tzinfo=UTC),
        benchmark_set_version="v-test",
        benchmarks=["MMLU", "GSM8K", "HumanEval", "MATH", "GPQA"],
        tokens_scanned=10,
        tokens_flagged=0,
        rejected_doc_hashes=[],
        per_benchmark_hits={"MMLU": 0, "GSM8K": 0, "HumanEval": 0, "MATH": 0, "GPQA": 0},
        signature="sig",
        signer_cert="cert",
    )
    payload = decon_dumps(rec)
    # Canonical (sorted-keys) bytes are deterministic across calls.
    assert payload == decon_dumps(rec)
    parsed = decon_loads(payload)
    assert parsed.snapshot_id == rec.snapshot_id
