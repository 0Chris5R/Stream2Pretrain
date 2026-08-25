"""Tests for :mod:`processor.iceberg_writer`."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from processor import common
from processor.decon_gate import DeconGate
from processor.iceberg_writer import (
    AttestationSink,
    IcebergWriter,
    LicenseAdmissionWriter,
    gold_identifier,
)
from schemas.decon import DeconAttestation
from schemas.gold import GoldRecord
from schemas.license_admission import LicenseAdmissionDecision


def _gold() -> GoldRecord:
    return GoldRecord(
        doc_id="sha256:" + "a" * 64,
        text="A compact training-data document.",
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
        source_format="web",
        extraction_pipeline="hf-model-card-markdown-v1",
        spdx_license="Apache-2.0",
        spdx_license_source="source_terms",
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

    assert table.column("source_format").to_pylist() == ["web"]
    assert table.column("extraction_pipeline").to_pylist() == ["hf-model-card-markdown-v1"]
    assert table.column("spdx_license").to_pylist() == ["Apache-2.0"]
    assert table.column("spdx_license_source").to_pylist() == ["source_terms"]


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


class _Snapshot:
    snapshot_id = 11


class _MemoryTable:
    def __init__(self) -> None:
        self.rows = 0
        self.tables: list[object] = []
        self.properties: dict[str, str] = {}

    def append(self, table: object) -> None:
        self.rows += int(table.num_rows)
        self.tables.append(table)

    def current_snapshot(self) -> _Snapshot:
        return _Snapshot()

    def transaction(self) -> object:
        table = self

        class _Transaction:
            def __enter__(self) -> _Transaction:
                return self

            def __exit__(self, *_args: object) -> None:
                return None

            def set_properties(self, **properties: str) -> None:
                table.properties.update(properties)

        return _Transaction()

    def scan(self, *, selected_fields: tuple[str, ...]) -> object:
        import pyarrow as pa

        selected = [table.select(selected_fields) for table in self.tables]
        arrow = (
            pa.concat_tables(selected)
            if selected
            else pa.table({name: pa.array([], type=pa.string()) for name in selected_fields})
        )

        class _Scan:
            def to_arrow(self) -> object:
                return arrow

        return _Scan()


class _MemoryWriter(IcebergWriter):
    def __init__(self, **kwargs: object) -> None:
        super().__init__(**kwargs)  # type: ignore[arg-type]
        self.decisions = _MemoryTable()
        self.gold = _MemoryTable()

    def _ensure_decisions_table(self) -> _MemoryTable:
        return self.decisions

    def _ensure_table(self) -> _MemoryTable:
        return self.gold

    def _set_snapshot_props(self, *_args: object) -> None:
        return None


class _AdmissionCatalog:
    def __init__(self) -> None:
        self.table = _MemoryTable()

    def load_table(self, _identifier: object) -> _MemoryTable:
        return self.table


class _MemoryLicenseAdmissionWriter(LicenseAdmissionWriter):
    def _ensure_table(self) -> _MemoryTable:
        return self._catalog.table  # type: ignore[attr-defined]


def test_license_admission_writer_batches_and_deduplicates_decisions() -> None:
    catalog = _AdmissionCatalog()
    writer = _MemoryLicenseAdmissionWriter(catalog)  # type: ignore[arg-type]
    decision = LicenseAdmissionDecision(
        decision_id="sha256:" + "b" * 64,
        doc_id="sha256:" + "a" * 64,
        source_feed="arxiv-cs-ai",
        source_url="https://arxiv.org/abs/2608.00001",
        observed_at=datetime(2026, 8, 19, tzinfo=UTC),
        status="admitted",
        license_id="CC-BY-4.0",
        license_source="rss_entry",
        reason="CC-BY-4.0 is on the training allowlist",
        trace_id="0" * 32,
    )

    second = decision.model_copy(
        update={
            "decision_id": "sha256:" + "c" * 64,
            "doc_id": "sha256:" + "d" * 64,
        }
    )

    assert writer.add_batch([decision, second, decision]) == 2
    assert writer.add_batch([decision, second]) == 0
    assert catalog.table.rows == 2
    assert len(catalog.table.tables) == 1


def test_writer_persists_rejected_decision_without_adding_it_to_gold() -> None:
    writer = _MemoryWriter(
        catalog=object(),
        decon=DeconGate(benchmark_set_version="v-test"),
        scoring_version="v-test",
        classifier_revision="classifier-test",
        policy_revision="git:test",
    )
    rejected = _gold().model_copy(update={"risk_tier": 2, "reject_reasons": ["license_excluded"]})

    assert writer.add(rejected) is None
    stats = writer.flush()
    assert stats.rows_committed == 0
    assert stats.decisions_committed == 1
    assert writer.decisions.rows == 1
    assert writer.gold.rows == 0


def test_writer_does_not_materialize_legacy_benchmark_candidate() -> None:
    writer = _MemoryWriter(
        catalog=object(),
        decon=DeconGate(benchmark_set_version="v-test"),
        scoring_version="v-test",
        classifier_revision="classifier-test",
        policy_revision="git:test",
    )
    candidate = _gold().model_copy(
        update={
            "route": "benchmark_candidate",
            "eligible_routes": [
                "broad_pretraining",
                "reasoning_candidate",
                "benchmark_candidate",
            ],
        }
    )

    assert writer.add(candidate) is None
    stats = writer.flush()

    assert stats.rows_committed == 0
    assert stats.benchmark_candidates_committed == 0
    assert writer.decisions.rows == 1
    assert writer.gold.rows == 0


def test_writer_ignores_replayed_decision_recipe() -> None:
    writer = _MemoryWriter(
        catalog=object(),
        decon=DeconGate(benchmark_set_version="v-test"),
        scoring_version="v-test",
        classifier_revision="classifier-test",
        policy_revision="git:test",
    )
    record = _gold().model_copy(update={"route": "broad_pretraining"})

    writer.add(record)
    first = writer.flush()
    writer.add(record)
    replay = writer.flush()

    assert first.decisions_committed == 1
    assert replay.decisions_committed == 0
    assert replay.rows_committed == 0
    assert writer.decisions.rows == 1
    assert writer.gold.rows == 1


def test_writer_ignores_replay_after_restart_by_scanning_iceberg_keys() -> None:
    first_writer = _MemoryWriter(
        catalog=object(),
        decon=DeconGate(benchmark_set_version="v-test"),
        scoring_version="v-test",
        classifier_revision="classifier-test",
        policy_revision="git:test",
    )
    record = _gold().model_copy(update={"route": "broad_pretraining"})
    first_writer.add(record)
    first_writer.flush()

    restarted_writer = _MemoryWriter(
        catalog=object(),
        decon=DeconGate(benchmark_set_version="v-test"),
        scoring_version="v-test",
        classifier_revision="classifier-test",
        policy_revision="git:test",
    )
    restarted_writer.decisions = first_writer.decisions
    restarted_writer.gold = first_writer.gold
    restarted_writer.add(record)
    replay = restarted_writer.flush()

    assert replay.decisions_committed == 0
    assert replay.rows_committed == 0
    assert restarted_writer.decisions.rows == 1
    assert restarted_writer.gold.rows == 1


def test_gold_identifier_follows_helm_namespace_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("S2P_ICEBERG_NAMESPACE", raising=False)
    monkeypatch.delenv("S2P_ICEBERG_GOLD_TABLE", raising=False)
    monkeypatch.setenv("ICEBERG_NAMESPACE", "gold")

    assert gold_identifier() == ("gold", "curated")

    monkeypatch.setenv("S2P_ICEBERG_NAMESPACE", "research")
    monkeypatch.setenv("S2P_ICEBERG_GOLD_TABLE", "trainable")

    assert gold_identifier() == ("research", "trainable")
