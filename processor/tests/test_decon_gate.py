"""Tests for :mod:`processor.decon_gate`."""

from __future__ import annotations

from datetime import UTC, datetime

from processor.decon_gate import DeconGate, shingle_ngrams
from processor.sign import AttestationSigner, verify_signature
from schemas.gold import GoldRecord


def _gold(text: str, doc_id_suffix: str = "0") -> GoldRecord:
    return GoldRecord(
        doc_id="sha256:" + doc_id_suffix.ljust(64, "0")[:64],
        text=text,
        lang="en",
        tokens=len(text.split()),
        quality_score=3.0,
        edu_score=3.0,
        license="unknown",
        license_source="unknown",
        risk_tier=1,
        pii_flags=[],
        contaminated_with=[],
        valid_from=datetime(2026, 6, 15, tzinfo=UTC),
        valid_to=None,
        reject_reasons=[],
        scoring_version="v0.1.0",
        classifier_revision="proxy-heuristic-0.1",
        policy_revision="git:test",
        snapshot_id=None,
        trace_id="0123456789abcdef0123456789abcdef",
    )


def test_shingle_ngrams_size() -> None:
    text = "the quick brown fox jumps over the lazy dog"
    out = list(shingle_ngrams(text, n=3))
    assert "the quick brown" in out
    assert len(out) == len(text.split()) - 3 + 1


def test_decon_gate_no_hits_on_clean_text() -> None:
    gate = DeconGate(benchmark_set_version="v-test", signer=AttestationSigner())
    record, hits = gate.scan(
        _gold("totally innocuous prose about cats and dogs in spring time and summer time")
    )
    assert hits == []
    assert record.contaminated_with == []


def test_decon_gate_flags_overlapping_text() -> None:
    bench_text = "what is the capital of france and germany and italy"
    gate = DeconGate(
        benchmark_set_version="v-test",
        benchmark_corpus={"MMLU": [bench_text]},
        signer=AttestationSigner(),
    )
    record, hits = gate.scan(_gold(bench_text + " plus extra trailing words to fill"))
    assert "MMLU" in hits
    assert "MMLU" in record.contaminated_with


def test_attestation_is_signed_and_verifiable() -> None:
    signer = AttestationSigner()
    gate = DeconGate(benchmark_set_version="v-test", signer=signer)
    gate.scan(_gold("clean prose without any benchmark overlap"))
    att = gate.flush_attestation(snapshot_id=42, committed_at=datetime(2026, 6, 15, tzinfo=UTC))
    assert att.snapshot_id == 42
    # Reconstruct canonical payload and verify the embedded signature.
    import json

    payload = {
        "snapshot_id": att.snapshot_id,
        "committed_at": att.committed_at.isoformat().replace("+00:00", "Z"),
        "benchmark_set_version": att.benchmark_set_version,
        "benchmarks": list(att.benchmarks),
        "tokens_scanned": att.tokens_scanned,
        "tokens_flagged": att.tokens_flagged,
        "rejected_doc_hashes": list(att.rejected_doc_hashes),
        "per_benchmark_hits": dict(att.per_benchmark_hits),
    }
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    assert verify_signature(canonical, att.signature, att.signer_cert)


def test_attestation_resets_state_between_flushes() -> None:
    gate = DeconGate(
        benchmark_set_version="v-test",
        benchmark_corpus={"MMLU": ["foo bar baz qux quux corge grault garply"]},
    )
    gate.scan(_gold("foo bar baz qux quux corge grault garply"))
    first = gate.flush_attestation(snapshot_id=1)
    assert first.tokens_flagged > 0
    second = gate.flush_attestation(snapshot_id=2)
    # No new scans -> counters are zero again.
    assert second.tokens_scanned == 0
    assert second.tokens_flagged == 0
