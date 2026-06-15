"""Tests for :mod:`processor.curate` end-to-end pipeline."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from processor.common import ProcessorConfig
from processor.curate import build_state, curate_one, process_silver_payload
from processor.common import silver_dumps
from schemas.silver import SilverRecord, SilverTags


def _silver(text: str, doc_id: str = "sha256:" + "a" * 64) -> SilverRecord:
    return SilverRecord(
        doc_id=doc_id,
        url="https://example.com/curate",
        title="Test",
        text=text,
        lang="en",
        lang_score=0.9,
        extracted_with="resiliparse-0.14",
        tags=SilverTags(
            gopher_pass=True,
            c4_nopunc_pass=True,
            perplexity=120.0,
            perplexity_bucket="head",
        ),
        minhash_sig=bytes(112 * 4),
        near_dup_cluster_id=None,
        valid_from=datetime(2026, 6, 15, tzinfo=timezone.utc),
        valid_to=None,
        valid_from_source="http_last_modified",
        trace_id="0123456789abcdef0123456789abcdef",
    )


def test_curate_clean_text_passes(cfg: ProcessorConfig, long_english_text: str) -> None:
    state = build_state(cfg)
    try:
        gold = curate_one(state, _silver(long_english_text))
        assert gold.lang == "en"
        assert gold.tokens > 0
        assert gold.risk_tier == 1
        assert gold.reject_reasons == []
    finally:
        state.close()


def test_curate_flags_pii(cfg: ProcessorConfig, long_english_text: str) -> None:
    state = build_state(cfg)
    try:
        text = long_english_text + " contact me at john.doe@example.com please."
        gold = curate_one(state, _silver(text, doc_id="sha256:" + "b" * 64))
        assert "email" in gold.pii_flags
        assert "pii_detected" in gold.reject_reasons
        assert gold.risk_tier == 3
    finally:
        state.close()


def test_curate_flags_curly_brace(cfg: ProcessorConfig) -> None:
    state = build_state(cfg)
    try:
        text = "Function { return 42; } and a sentence. " * 30
        gold = curate_one(state, _silver(text, doc_id="sha256:" + "c" * 64))
        assert "c4_nopunc_filter" in gold.reject_reasons
        assert gold.risk_tier >= 2
    finally:
        state.close()


def test_curate_marks_duplicates(cfg: ProcessorConfig, long_english_text: str) -> None:
    state = build_state(cfg)
    try:
        first = curate_one(state, _silver(long_english_text, doc_id="sha256:" + "1" * 64))
        second = curate_one(state, _silver(long_english_text, doc_id="sha256:" + "2" * 64))
        assert first.reject_reasons == []
        assert "near_duplicate" in second.reject_reasons
    finally:
        state.close()


def test_process_silver_payload_returns_bytes(cfg: ProcessorConfig, long_english_text: str) -> None:
    state = build_state(cfg)
    try:
        payload = silver_dumps(_silver(long_english_text))
        out = process_silver_payload(state, payload)
        assert out is not None
        assert b"doc_id" in out
    finally:
        state.close()
