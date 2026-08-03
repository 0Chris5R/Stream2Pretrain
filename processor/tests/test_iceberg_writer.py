"""Tests for :mod:`processor.iceberg_writer`."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from processor import common
from processor.decon_gate import DeconGate
from processor.iceberg_writer import AttestationSink, IcebergWriter, gold_identifier
from schemas.decon import DeconAttestation
from schemas.gold import GoldRecord


def _gold() -> GoldRecord:
    return GoldRecord(
        doc_id="sha256:" + "a" * 64,
        text="def train_model(x): return x",
        lang="en",
        tokens=6,
        quality_score=4.0,
        edu_score=4.0,
        license="Apache-2.0",
        license_source="unknown",
        risk_tier=1,
        valid_from=datetime(2026, 6, 15, tzinfo=UTC),
        scoring_version="v-test",
        classifier_revision="classifier-test",
        policy_revision="git:test",
        trace_id="0" * 32,
        source_format="code",
        extraction_pipeline="github-release-tarball-2026-06",
        spdx_license="Apache-2.0",
        spdx_license_source="github_api",
    )


def test_to_arrow_includes_v2_provenance_columns() -> None:
    writer = IcebergWriter(
        catalog=object(),
        decon=DeconGate(benchmark_set_version="v-test"),
        scoring_version="v-test",
        classifier_revision="classifier-test",
        policy_revision="git:test",
    )

    table = writer._to_arrow([_gold()])

    assert table.column("source_format").to_pylist() == ["code"]
    assert table.column("extraction_pipeline").to_pylist() == [
        "github-release-tarball-2026-06"
    ]
    assert table.column("spdx_license").to_pylist() == ["Apache-2.0"]
    assert table.column("spdx_license_source").to_pylist() == ["github_api"]


class _FakeS3:
    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], bytes] = {}

    def put_object(self, **kwargs: object) -> None:
        assert kwargs["ContentType"] == "application/json"
        body = kwargs["Body"]
        assert isinstance(body, bytes)
        self.objects[(str(kwargs["Bucket"]), str(kwargs["Key"]))] = body


class _FakeProducer:
    def __init__(self) -> None:
        self.messages: list[tuple[str, bytes, bytes]] = []
        self.flushed = False

    def produce(self, topic: str, *, key: bytes, value: bytes) -> None:
        self.messages.append((topic, key, value))

    def flush(self) -> None:
        self.flushed = True


def test_attestation_sink_writes_decon_bucket_and_topic() -> None:
    s3 = _FakeS3()
    producer = _FakeProducer()
    sink = AttestationSink(
        s3_client=s3,
        bucket="s2p-decon",
        kafka_producer=producer,
        topic="decon.attest",
    )
    attestation = DeconAttestation(
        snapshot_id=7,
        committed_at=datetime(2026, 6, 15, tzinfo=UTC),
        benchmark_set_version="v-test",
        benchmarks=["MMLU", "GSM8K", "HumanEval", "MATH", "GPQA"],
        tokens_scanned=12,
        tokens_flagged=1,
        rejected_doc_hashes=["sha256:" + "b" * 64],
        per_benchmark_hits={"MMLU": 1, "GSM8K": 0, "HumanEval": 0, "MATH": 0, "GPQA": 0},
        signature="sig",
        signer_cert="cert",
    )

    uri = sink.write(attestation)

    key = "decon/v-test/00000000000000000007.json"
    payload = s3.objects[("s2p-decon", key)]
    assert uri == f"s3://s2p-decon/{key}"
    assert common.decon_loads(payload).snapshot_id == 7
    assert producer.messages == [("decon.attest", b"7", payload)]
    assert producer.flushed


def test_writer_drops_non_trainable_rows_before_buffering() -> None:
    writer = IcebergWriter(
        catalog=object(),
        decon=DeconGate(benchmark_set_version="v-test"),
        scoring_version="v-test",
        classifier_revision="classifier-test",
        policy_revision="git:test",
    )
    rejected = _gold().model_copy(update={"risk_tier": 2, "reject_reasons": ["license_excluded"]})

    assert writer.add(rejected) is None
    assert writer.flush().rows_committed == 0


def test_gold_identifier_follows_helm_namespace_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("S2P_ICEBERG_NAMESPACE", raising=False)
    monkeypatch.delenv("S2P_ICEBERG_GOLD_TABLE", raising=False)
    monkeypatch.setenv("ICEBERG_NAMESPACE", "gold")

    assert gold_identifier() == ("gold", "curated")

    monkeypatch.setenv("S2P_ICEBERG_NAMESPACE", "research")
    monkeypatch.setenv("S2P_ICEBERG_GOLD_TABLE", "trainable")

    assert gold_identifier() == ("research", "trainable")
