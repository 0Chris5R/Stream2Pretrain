"""Tests for :mod:`processor.common` serde + config."""

from __future__ import annotations

from datetime import datetime, timezone

from processor.common import (
    bronze_loads,
    decon_dumps,
    decon_loads,
    gold_dumps,
    gold_loads,
    new_trace_id,
    silver_dumps,
    silver_loads,
)
from schemas.bronze import BronzeRecord
from schemas.decon import DeconAttestation
from schemas.gold import GoldRecord
from schemas.silver import SilverRecord, SilverTags


def test_new_trace_id_format() -> None:
    tid = new_trace_id()
    assert len(tid) == 32
    assert all(c in "0123456789abcdef" for c in tid)


def test_bronze_roundtrip() -> None:
    rec = BronzeRecord(
        doc_id="sha256:" + "0" * 64,
        url="https://example.com/x",
        fetched_at=datetime(2026, 6, 15, tzinfo=timezone.utc),
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
    payload = silver_dumps(silver_record)
    parsed = silver_loads(payload)
    assert parsed.doc_id == silver_record.doc_id
    assert parsed.lang == silver_record.lang


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
        valid_from=datetime(2026, 6, 15, tzinfo=timezone.utc),
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
        committed_at=datetime(2026, 6, 15, tzinfo=timezone.utc),
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
