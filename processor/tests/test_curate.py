"""Tests for :mod:`processor.curate` end-to-end pipeline."""

from __future__ import annotations

from datetime import UTC, datetime

from processor.common import ProcessorConfig, gold_loads, silver_dumps
from processor.curate import build_state, curate_one, is_trainable_gold, process_silver_payload
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
        valid_from=datetime(2026, 6, 15, tzinfo=UTC),
        valid_to=None,
        valid_from_source="http_last_modified",
        trace_id="0123456789abcdef0123456789abcdef",
        spdx_license="Apache-2.0",
        spdx_license_source="manual_override",
    )


def test_curate_clean_text_passes(cfg: ProcessorConfig, long_english_text: str) -> None:
    state = build_state(cfg)
    try:
        gold = curate_one(state, _silver(long_english_text))
        assert gold.lang == "en"
        assert gold.tokens > 0
        assert gold.risk_tier == 1
        assert gold.reject_reasons == []
        assert is_trainable_gold(gold)
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
        assert not is_trainable_gold(gold)
    finally:
        state.close()


def test_curate_flags_curly_brace(cfg: ProcessorConfig) -> None:
    state = build_state(cfg)
    try:
        text = "Function { return 42; } and a sentence. " * 30
        gold = curate_one(state, _silver(text, doc_id="sha256:" + "c" * 64))
        assert "c4_nopunc_filter" in gold.reject_reasons
        assert gold.risk_tier >= 2
        assert not is_trainable_gold(gold)
    finally:
        state.close()


def test_curate_marks_duplicates(cfg: ProcessorConfig, long_english_text: str) -> None:
    state = build_state(cfg)
    try:
        first = curate_one(state, _silver(long_english_text, doc_id="sha256:" + "1" * 64))
        second = curate_one(state, _silver(long_english_text, doc_id="sha256:" + "2" * 64))
        assert first.reject_reasons == []
        assert "near_duplicate" in second.reject_reasons
        assert is_trainable_gold(first)
        assert not is_trainable_gold(second)
    finally:
        state.close()


def test_curate_recomputes_placeholder_seed_minhash(
    cfg: ProcessorConfig, long_english_text: str
) -> None:
    state = build_state(cfg)
    try:
        silver = _silver(long_english_text, doc_id="sha256:" + "3" * 64).model_copy(
            update={"minhash_backend": "placeholder"}
        )
        gold = curate_one(state, silver)
        assert "minhash_backend_mismatch" not in gold.reject_reasons
        assert gold.risk_tier == 1
    finally:
        state.close()


def test_curate_propagates_source_format_and_spdx(
    cfg: ProcessorConfig, long_english_text: str
) -> None:
    state = build_state(cfg)
    try:
        silver = _silver(long_english_text, doc_id="sha256:" + "4" * 64).model_copy(
            update={
                "source_format": "code",
                "extraction_pipeline": "github-release-tarball-2026-06",
                "spdx_license": "Apache-2.0",
                "spdx_license_source": "github_api",
            }
        )
        gold = curate_one(state, silver)
        assert gold.source_format == "code"
        assert gold.extraction_pipeline == "github-release-tarball-2026-06"
        assert gold.license == "Apache-2.0"
        assert gold.spdx_license == "Apache-2.0"
        assert gold.spdx_license_source == "github_api"
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


def test_process_silver_payload_drops_rejected_rows(
    cfg: ProcessorConfig, long_english_text: str
) -> None:
    state = build_state(cfg)
    try:
        text = long_english_text + " contact me at john.doe@example.com please."
        payload = silver_dumps(_silver(text, doc_id="sha256:" + "5" * 64))
        assert process_silver_payload(state, payload) is None
    finally:
        state.close()


def test_missing_license_is_trainable_for_non_code(
    cfg: ProcessorConfig, long_english_text: str
) -> None:
    state = build_state(cfg)
    try:
        silver = _silver(long_english_text, doc_id="sha256:" + "6" * 64).model_copy(
            update={"spdx_license": None, "spdx_license_source": "unknown"}
        )
        payload = process_silver_payload(state, silver_dumps(silver))
        assert payload is not None
        gold = gold_loads(payload)
        assert "license_excluded" not in gold.reject_reasons
        assert gold.license == "unknown"
        assert gold.risk_tier == 1
        assert is_trainable_gold(gold)
    finally:
        state.close()


def test_missing_code_license_is_not_trainable(
    cfg: ProcessorConfig, long_english_text: str
) -> None:
    state = build_state(cfg)
    try:
        silver = _silver(long_english_text, doc_id="sha256:" + "8" * 64).model_copy(
            update={
                "source_format": "code",
                "spdx_license": None,
                "spdx_license_source": "unknown",
            }
        )
        gold = curate_one(state, silver)
        assert "license_excluded" in gold.reject_reasons
        assert not is_trainable_gold(gold)
    finally:
        state.close()


def test_code_license_must_be_permissive_whitelist(
    cfg: ProcessorConfig, long_english_text: str
) -> None:
    state = build_state(cfg)
    try:
        silver = _silver(long_english_text, doc_id="sha256:" + "7" * 64).model_copy(
            update={
                "source_format": "code",
                "spdx_license": "GPL-3.0-only",
                "spdx_license_source": "github_api",
            }
        )
        gold = curate_one(state, silver)
        assert "license_excluded" in gold.reject_reasons
        assert not is_trainable_gold(gold)
    finally:
        state.close()
