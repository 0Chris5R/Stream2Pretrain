"""Tests for :mod:`processor.curate` end-to-end pipeline."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import ClassVar

import pytest

from processor import common
from processor.common import ProcessorConfig, gold_loads, silver_dumps
from processor.curate import (
    build_state,
    curate_one,
    is_trainable_gold,
    process_silver_decision_payload,
    process_silver_payload,
)
from processor.operators.kenlm_score import PerplexityResult
from processor.operators.quality import QualityScore
from schemas.silver import SilverRecord, SilverSegment, SilverTags


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


class _SplitModelClient:
    closed: ClassVar[list[str]] = []

    def __init__(self, base_url: str) -> None:
        self.base_url = base_url
        if "quality" in base_url:
            self.metadata = {
                "ready": True,
                "quality": {
                    "finepdfs-edu-v2": {
                        "backend": "transformers-cpu",
                        "revision": "finepdfs@pinned",
                    },
                    "fineweb-edu": {
                        "backend": "transformers-cpu",
                        "revision": "fineweb@pinned",
                    },
                },
            }
        elif "kenlm" in base_url:
            self.metadata = {
                "ready": True,
                "kenlm": {
                    "backend": "kenlm-sentencepiece",
                    "scorer": "kenlm-sentencepiece:en.arpa.bin",
                },
            }
        else:
            self.metadata = {
                "ready": True,
                "embedding": {
                    "backend": "onnxruntime-cpu",
                    "revision": "e5@pinned",
                },
            }

    def quality(self, model_family: str, _text: str) -> QualityScore:
        return QualityScore(4.0, str(self.metadata["quality"][model_family]["revision"]))  # type: ignore[index]

    def perplexity(self, _text: str) -> PerplexityResult:
        return PerplexityResult(42.0, "head", "kenlm-sentencepiece:en.arpa.bin")

    def embed(self, _text: str) -> list[float]:
        return [1.0, 0.0]

    def close(self) -> None:
        self.closed.append(self.base_url)


class _LowQualityScorer:
    revision = "test-low-quality"
    backend = "test"

    def score(self, _text: str) -> QualityScore:
        return QualityScore(1.0, self.revision)


def test_split_model_services_are_all_required_and_closed(
    cfg: ProcessorConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("S2P_MODEL_SERVICE_URL", raising=False)
    monkeypatch.setenv("S2P_QUALITY_MODEL_SERVICE_URL", "http://quality")
    monkeypatch.setenv("S2P_KENLM_MODEL_SERVICE_URL", "http://kenlm")
    monkeypatch.setenv("S2P_EMBEDDING_MODEL_SERVICE_URL", "http://embedding")
    monkeypatch.setattr("processor.curate.CuratorModelClient", _SplitModelClient)
    _SplitModelClient.closed.clear()

    state = build_state(cfg)
    assert len(state.model_clients) == 3
    state.close()
    assert set(_SplitModelClient.closed) == {
        "http://quality",
        "http://kenlm",
        "http://embedding",
    }


def test_partial_split_model_service_configuration_fails(
    cfg: ProcessorConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("S2P_MODEL_SERVICE_URL", raising=False)
    monkeypatch.setenv("S2P_QUALITY_MODEL_SERVICE_URL", "http://quality")
    monkeypatch.delenv("S2P_KENLM_MODEL_SERVICE_URL", raising=False)
    monkeypatch.delenv("S2P_EMBEDDING_MODEL_SERVICE_URL", raising=False)

    with pytest.raises(RuntimeError, match="all split curator model service URLs"):
        build_state(cfg)


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


def test_cluster_smoke_observes_but_does_not_gate_on_fineweb_score(
    cfg: ProcessorConfig, long_english_text: str
) -> None:
    state = build_state(cfg)
    try:
        state.fineweb_quality = _LowQualityScorer()
        silver = _silver(long_english_text).model_copy(
            update={
                "source_feed": "cluster-smoke",
                "source_format": "html",
                "extraction_pipeline": "cluster-smoke-1.0",
            }
        )

        gold = curate_one(state, silver)

        assert gold.segment_scores[0].fineweb_edu_score == 1.0
        assert "low_quality_score" not in gold.reject_reasons
        assert is_trainable_gold(gold)
    finally:
        state.close()


def test_curate_flags_pii(cfg: ProcessorConfig, long_english_text: str) -> None:
    state = build_state(cfg)
    try:
        text = long_english_text + " contact me at john.doe@example.com please."
        gold = curate_one(state, _silver(text, doc_id="sha256:" + "b" * 64))
        assert "email" in gold.removed_body_pii_flags
        assert "email" not in gold.pii_flags
        assert "john.doe@example.com" not in gold.text
        assert gold.pii_action == "body_quarantine"
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


def test_curate_removes_only_the_sensitive_section(
    cfg: ProcessorConfig, long_english_text: str
) -> None:
    state = build_state(cfg)
    try:
        silver = _silver(long_english_text, doc_id="sha256:" + "d" * 64).model_copy(
            update={
                "title": "Segment-aware paper",
                "model_text": long_english_text,
                "segments": [
                    SilverSegment(
                        segment_id="methods",
                        title="Methods",
                        role="methods",
                        text=long_english_text,
                        word_count=len(long_english_text.split()),
                    ),
                    SilverSegment(
                        segment_id="contact",
                        title="Contact details",
                        role="other",
                        text="Correspondence should be sent to author@example.invalid.",
                        word_count=7,
                    ),
                ],
                "source_word_count": len(long_english_text.split()) + 7,
                "training_word_count": len(long_english_text.split()) + 7,
                "included_section_count": 2,
            }
        )

        gold = curate_one(state, silver)

        assert "author@example.invalid" not in gold.text
        assert "email" in gold.removed_body_pii_flags
        assert gold.pii_action == "segments_removed"
        assert "pii_detected" not in gold.reject_reasons
        assert any(
            score.segment_id == "contact" and score.decision == "excluded"
            for score in gold.segment_scores
        )
        assert any(
            score.segment_id == "methods" and score.decision == "included"
            for score in gold.segment_scores
        )
        assert is_trainable_gold(gold)
    finally:
        state.close()


def test_author_email_is_metadata_not_a_body_reject(
    cfg: ProcessorConfig, long_english_text: str
) -> None:
    state = build_state(cfg)
    try:
        silver = _silver(long_english_text, doc_id="sha256:" + "e" * 64).model_copy(
            update={"source_metadata_text": "Ada Researcher ada@example.invalid"}
        )

        gold = curate_one(state, silver)

        assert "email" in gold.metadata_pii_flags
        assert gold.pii_action == "metadata_removed"
        assert "pii_detected" not in gold.reject_reasons
        assert is_trainable_gold(gold)
    finally:
        state.close()


def test_scientific_curly_braces_are_a_visible_nonblocking_signal(
    cfg: ProcessorConfig, long_english_text: str
) -> None:
    state = build_state(cfg)
    try:
        text = long_english_text + " The shard set is S = {n-k, n} for every experiment."
        silver = _silver(text, doc_id="sha256:" + "f" * 64).model_copy(
            update={
                "scientific_artifact_s3_uri": "s3://silver/scientific/f/document.json",
                "source_feed": "arxiv-html",
                "model_text": text,
                "segments": [
                    SilverSegment(
                        segment_id="methods",
                        title="Methods",
                        role="methods",
                        text=text,
                        word_count=len(text.split()),
                    )
                ],
                "included_section_count": 1,
            }
        )

        gold = curate_one(state, silver)

        assert not gold.c4_curly_brace_pass
        assert "c4_nopunc_filter" not in gold.reject_reasons
        assert gold.segment_scores[0].decision == "included"
        assert is_trainable_gold(gold)
    finally:
        state.close()


def test_scientific_lorem_signal_is_diagnostic_not_a_web_filter(cfg: ProcessorConfig) -> None:
    state = build_state(cfg)
    try:
        text = "Lorem ipsum dolor sit amet consectetur adipiscing elit sed do eiusmod tempor. " * 20
        silver = _silver(text, doc_id="sha256:" + "e" * 64).model_copy(
            update={
                "scientific_artifact_s3_uri": "s3://silver/scientific/e/document.json",
                "source_feed": "arxiv-html-fetcher",
                "source_format": "html",
                "extraction_pipeline": "arxiv-html-scientific-v1",
                "model_text": text,
                "segments": [
                    SilverSegment(
                        segment_id="methods",
                        title="Methods",
                        role="methods",
                        text=text,
                        word_count=len(text.split()),
                    )
                ],
                "included_section_count": 1,
            }
        )

        gold = curate_one(state, silver)

        assert not gold.c4_lorem_ipsum_pass
        assert "c4_nopunc_filter" not in gold.reject_reasons
    finally:
        state.close()


def test_structured_evidence_with_email_is_removed_before_final_projection(
    cfg: ProcessorConfig, long_english_text: str
) -> None:
    state = build_state(cfg)
    try:
        silver = _silver(long_english_text, doc_id="sha256:" + "7" * 64).model_copy(
            update={
                "model_text": long_english_text,
                "segments": [
                    SilverSegment(
                        segment_id="results",
                        title="Results",
                        role="results",
                        text=long_english_text,
                        word_count=len(long_english_text.split()),
                    )
                ],
                "structured_text": "[FIGURE] Visible text: contact author@example.invalid [/FIGURE]",
                "scientific_artifact_s3_uri": "s3://silver/scientific/7/document.json",
                "included_section_count": 1,
            }
        )

        gold = curate_one(state, silver)

        assert "author@example.invalid" not in gold.text
        assert "email" in gold.removed_body_pii_flags
        assert "pii_detected" not in gold.reject_reasons
        assert gold.pii_action == "segments_removed"
        assert is_trainable_gold(gold)
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


def test_decision_replay_returns_identical_cached_result(
    cfg: ProcessorConfig, long_english_text: str
) -> None:
    state = build_state(cfg)
    try:
        payload = silver_dumps(_silver(long_english_text))

        first = process_silver_decision_payload(state, payload)
        replay = process_silver_decision_payload(state, payload)

        assert replay == first
        assert "near_duplicate" not in common.gold_loads(replay[0]).reject_reasons
    finally:
        state.close()


def test_decision_replay_after_worker_restart_uses_durable_cache(
    cfg: ProcessorConfig, long_english_text: str
) -> None:
    payload = silver_dumps(_silver(long_english_text))
    first_state = build_state(cfg)
    try:
        first = process_silver_decision_payload(first_state, payload)
    finally:
        first_state.close()

    replay_state = build_state(cfg)
    try:
        replay = process_silver_decision_payload(replay_state, payload)
    finally:
        replay_state.close()

    assert replay == first
    assert "near_duplicate" not in common.gold_loads(replay[0]).reject_reasons


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


def test_transform_only_scientific_artifact_reaches_paper_foundry(
    cfg: ProcessorConfig,
    long_english_text: str,
) -> None:
    state = build_state(cfg)
    try:
        silver = _silver(long_english_text, doc_id="sha256:" + "5" * 64).model_copy(
            update={
                "source_feed": "arxiv-html-fetcher",
                "source_format": "html",
                "extraction_pipeline": "arxiv-html-scientific-v1",
                "training_usage": "posttrain_transform_only",
                "spdx_license": "arxiv-non-exclusive-distribution",
                "spdx_license_source": "arxiv_api",
                "scientific_artifact_s3_uri": "s3://silver/scientific/5/document.json",
                "segments": [
                    SilverSegment(
                        segment_id="methods",
                        title="Methods",
                        role="methods",
                        text=long_english_text,
                        word_count=len(long_english_text.split()),
                    )
                ],
                "included_section_count": 1,
            }
        )

        gold = curate_one(state, silver)

        assert gold.route == "posttrain_candidate"
        assert gold.eligible_routes == ["posttrain_candidate"]
        assert "verbatim pretraining export is forbidden" in gold.route_reasons[0]
        assert is_trainable_gold(gold)
    finally:
        state.close()


def test_metadata_is_discovery_only_and_never_trainable(cfg: ProcessorConfig) -> None:
    state = build_state(cfg)
    try:
        text = (
            "model release version 2 benchmark evaluation dataset license Apache 2.0 "
            "training architecture results repository 2026 task paper abstract"
        )
        silver = _silver(text, doc_id="sha256:" + "0" * 64).model_copy(
            update={"source_format": "metadata", "source_feed": "hf-models"}
        )

        gold = curate_one(state, silver)

        assert gold.classifier_revision == "metadata-discovery-only-v1"
        assert gold.segment_scores[0].finepdfs_edu_score is None
        assert gold.segment_scores[0].fineweb_edu_score is None
        assert "discovery_metadata" in gold.content_tags
        assert "metadata_only" in gold.reject_reasons
        assert gold.route == "quarantine"
    finally:
        state.close()


def test_short_web_page_is_quarantined_instead_of_retried(cfg: ProcessorConfig) -> None:
    state = build_state(cfg)
    try:
        silver = _silver("brief page", doc_id="sha256:" + "b" * 64).model_copy(
            update={"source_format": "html", "source_feed": "rss-openai-news"}
        )

        gold = curate_one(state, silver)

        assert "insufficient_body" in gold.reject_reasons
        assert gold.route == "quarantine"
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


def test_missing_paper_license_is_quarantined(cfg: ProcessorConfig, long_english_text: str) -> None:
    state = build_state(cfg)
    try:
        silver = _silver(long_english_text, doc_id="sha256:" + "6" * 64).model_copy(
            update={"spdx_license": None, "spdx_license_source": "unknown"}
        )
        payload = process_silver_payload(state, silver_dumps(silver))
        assert payload is None
        decision_payload, trainable = process_silver_decision_payload(state, silver_dumps(silver))
        gold = gold_loads(decision_payload)
        assert "license_excluded" in gold.reject_reasons
        assert gold.license == "unknown"
        assert not trainable
    finally:
        state.close()
