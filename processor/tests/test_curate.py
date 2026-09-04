"""Tests for :mod:`processor.curate` end-to-end pipeline."""

from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import ClassVar

import pytest

from processor import common
from processor import curate as curate_module
from processor.common import ProcessorConfig, gold_loads, silver_dumps
from processor.curate import (
    _score_quality_texts,
    _score_segment_models,
    _training_projection,
    build_state,
    curate_one,
    extraction_retry_payload,
    is_trainable_gold,
    process_silver_decision_payload,
    process_silver_decision_payloads,
    process_silver_payload,
)
from processor.operators.kenlm_score import PerplexityResult
from processor.operators.quality import QualityScore
from processor.work_cutoff import WorkCutoff
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

    def __init__(self, base_url: str, **_kwargs: object) -> None:
        self.base_url = base_url
        if any(value in base_url for value in ("quality", "finepdfs")):
            self.metadata = {
                "ready": True,
                "quality": {
                    "source-pretrain-quality": {
                        "backend": "transformers-cpu",
                        "revision": "finepdfs@pinned",
                    },
                    "source-arxiv-posttrain": {
                        "backend": "transformers-cpu",
                        "revision": "finepdfs@pinned",
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

    def quality(self, model_family: str, _text: str) -> QualityScore:
        return QualityScore(4.0, str(self.metadata["quality"][model_family]["revision"]))  # type: ignore[index]

    def perplexity(self, _text: str) -> PerplexityResult:
        return PerplexityResult(42.0, "head", "kenlm-sentencepiece:en.arpa.bin")

    def close(self) -> None:
        self.closed.append(self.base_url)


class _LowQualityScorer:
    revision = "test-low-quality"
    backend = "test"

    def score(self, _text: str) -> QualityScore:
        return QualityScore(1.0, self.revision)


class _BatchQualityScorer:
    revision = "batch-test"
    backend = "test"

    def __init__(self) -> None:
        self.batches: list[list[str]] = []

    def score(self, text: str) -> QualityScore:
        return QualityScore(min(5.0, float(len(text))), self.revision)

    def score_many(self, texts: list[str]) -> list[QualityScore]:
        self.batches.append(list(texts))
        return [self.score(text) for text in texts]


class _BatchKenLM:
    scorer = "batch-test"

    def score(self, text: str) -> PerplexityResult:
        return PerplexityResult(float(len(text)), "head", self.scorer)


class _BlockingBatchQualityScorer(_BatchQualityScorer):
    def __init__(self, started: threading.Event, release: threading.Event) -> None:
        super().__init__()
        self.started = started
        self.release = release

    def score_many(self, texts: list[str]) -> list[QualityScore]:
        self.started.set()
        assert self.release.wait(timeout=2)
        return super().score_many(texts)


class _ConcurrentBatchQualityScorer(_BatchQualityScorer):
    def __init__(self, expected_active: int, release: threading.Event) -> None:
        super().__init__()
        self.expected_active = expected_active
        self.release = release
        self.all_active = threading.Event()
        self._lock = threading.Lock()
        self.active = 0
        self.max_active = 0

    def score_many(self, texts: list[str]) -> list[QualityScore]:
        with self._lock:
            self.active += 1
            self.max_active = max(self.max_active, self.active)
            if self.active == self.expected_active:
                self.all_active.set()
        try:
            assert self.release.wait(timeout=2)
            return super().score_many(texts)
        finally:
            with self._lock:
                self.active -= 1


def test_segment_models_batch_the_sole_quality_classifier(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("S2P_CURATOR_CLASSIFIER_BATCH_SIZE", "2")
    monkeypatch.setenv("S2P_CURATOR_CLASSIFIER_CONCURRENCY", "6")
    quality = _BatchQualityScorer()
    segments = [
        SilverSegment(
            segment_id=f"section-{index}",
            title=f"Section {index}",
            text="x" * index,
            word_count=1,
        )
        for index in range(1, 6)
    ]

    results = _score_segment_models(
        segments,
        quality=quality,
        kenlm=_BatchKenLM(),
        use_kenlm=True,
    )

    assert sorted(len(batch) for batch in quality.batches) == [1, 2, 2]
    assert sorted(text for batch in quality.batches for text in batch) == sorted(
        segment.text for segment in segments
    )
    assert set(results) == {segment.segment_id for segment in segments}
    for segment in segments:
        result = results[segment.segment_id]
        assert result.quality is not None
        assert result.quality.edu_score == float(len(segment.text))
        assert result.perplexity is not None
        assert result.perplexity.perplexity == float(len(segment.text))


def test_segment_models_can_skip_quality_for_a_deterministic_reject(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("S2P_CURATOR_CLASSIFIER_BATCH_SIZE", "2")
    monkeypatch.setenv("S2P_CURATOR_CLASSIFIER_CONCURRENCY", "6")
    quality = _BatchQualityScorer()
    segments = [
        SilverSegment(
            segment_id=f"section-{index}",
            title=f"Section {index}",
            text="x" * index,
            word_count=1,
        )
        for index in range(1, 5)
    ]

    results = _score_segment_models(
        segments,
        quality=None,
        kenlm=_BatchKenLM(),
        use_kenlm=False,
    )

    assert list(results) == [segment.segment_id for segment in segments]
    assert quality.batches == []
    for segment in segments:
        result = results[segment.segment_id]
        assert result.quality is None
        assert result.perplexity is None


def test_document_prefetch_fills_all_six_family_request_lanes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("S2P_CURATOR_CLASSIFIER_BATCH_SIZE", "2")
    monkeypatch.setenv("S2P_CURATOR_CLASSIFIER_CONCURRENCY", "12")
    release = threading.Event()
    scorer = _ConcurrentBatchQualityScorer(expected_active=6, release=release)
    texts = [f"text-{index}" for index in range(12)]

    with ThreadPoolExecutor(max_workers=1) as caller:
        pending = caller.submit(_score_quality_texts, scorer, texts)
        assert scorer.all_active.wait(timeout=2)
        release.set()
        results = pending.result(timeout=2)

    assert scorer.max_active == 6
    assert list(results) == texts
    assert all(result.revision == scorer.revision for result in results.values())


def test_document_micro_batch_matches_exact_serial_decisions(
    cfg: ProcessorConfig,
    long_english_text: str,
) -> None:
    documents = [
        _silver(long_english_text, doc_id="sha256:" + "1" * 64),
        _silver(long_english_text, doc_id="sha256:" + "2" * 64),
        _silver(long_english_text, doc_id="sha256:" + "3" * 64).model_copy(
            update={
                "source_feed": "arxiv-html-fetcher",
                "source_format": "html",
                "extraction_pipeline": "arxiv-html-scientific-v1",
                "model_text": long_english_text,
                "scientific_artifact_s3_uri": "s3://silver/scientific/3/document.json",
            }
        ),
        _silver(long_english_text, doc_id="sha256:" + "4" * 64).model_copy(
            update={
                "source_feed": "hf-models",
                "source_format": "web",
                "extraction_pipeline": "hf-model-card-markdown-v1",
                "title": "Measured model",
                "segments": [
                    SilverSegment(
                        segment_id="description",
                        title="Model description",
                        text=(
                            long_english_text
                            + " Transformer architecture evaluation reaches 84.2% accuracy."
                        ),
                        word_count=len(long_english_text.split()) + 6,
                    ),
                    SilverSegment(
                        segment_id="placeholder",
                        title="Limitations",
                        text="More information needed.",
                        word_count=3,
                    ),
                ],
            }
        ),
        _silver(long_english_text, doc_id="sha256:" + "5" * 64).model_copy(
            update={"lang": "de", "lang_score": 0.99}
        ),
        _silver(long_english_text, doc_id="sha256:" + "6" * 64).model_copy(
            update={"source_format": "metadata", "source_feed": "hf-models"}
        ),
        _silver("brief page", doc_id="sha256:" + "7" * 64),
        _silver(
            long_english_text + " api_key = abcdefghijklmnopqrstuvwxyz123456",
            doc_id="sha256:" + "8" * 64,
        ),
        _silver(long_english_text + " unique licence case", doc_id="sha256:" + "9" * 64).model_copy(
            update={"spdx_license": None, "spdx_license_source": "unknown"}
        ),
        _silver(
            "Function { return 42; } and a sentence. " * 30,
            doc_id="sha256:" + "a" * 64,
        ),
        _silver(
            long_english_text + " Dataset structure has 1200 rows over three splits.",
            doc_id="sha256:" + "b" * 64,
        ).model_copy(
            update={
                "source_feed": "hf-datasets",
                "source_format": "web",
                "extraction_pipeline": "hf-dataset-card-markdown-v1",
                "title": "Measured dataset",
            }
        ),
        _silver(
            long_english_text + " incomplete figure extraction case",
            doc_id="sha256:" + "c" * 64,
        ).model_copy(
            update={
                "source_feed": "arxiv-html-fetcher",
                "source_format": "html",
                "extraction_pipeline": "arxiv-html-scientific-v1",
                "model_text": long_english_text + " incomplete figure extraction case",
                "extraction_warnings": ["figure_enrichment_failed:ocr"],
                "scientific_artifact_s3_uri": "s3://silver/scientific/c/document.json",
            }
        ),
        _silver(
            long_english_text + " transform-only scientific case",
            doc_id="sha256:" + "d" * 64,
        ).model_copy(
            update={
                "source_feed": "arxiv-html-fetcher",
                "source_format": "html",
                "extraction_pipeline": "arxiv-html-scientific-v1",
                "model_text": long_english_text + " transform-only scientific case",
                "training_usage": "posttrain_transform_only",
                "spdx_license": "arxiv-non-exclusive-distribution",
                "spdx_license_source": "arxiv_api",
                "scientific_artifact_s3_uri": "s3://silver/scientific/d/document.json",
            }
        ),
    ]
    serial_state = build_state(replace(cfg, state_dir=f"{cfg.state_dir}-serial"))
    batch_state = build_state(replace(cfg, state_dir=f"{cfg.state_dir}-batch"))
    try:
        serial = []
        for silver in documents:
            gold = curate_one(serial_state, silver)
            serial.append((common.gold_dumps(gold), is_trainable_gold(gold)))

        outcomes = process_silver_decision_payloads(
            batch_state,
            [silver_dumps(silver) for silver in documents],
        )
        assert all(outcome.error is None for outcome in outcomes)
        assert [outcome.value for outcome in outcomes] == serial
    finally:
        serial_state.close()
        batch_state.close()


def test_document_micro_batch_fills_finepdfs_without_skipping_eligible_segments(
    cfg: ProcessorConfig,
    long_english_text: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("S2P_CURATOR_CLASSIFIER_BATCH_SIZE", "2")
    monkeypatch.setenv("S2P_CURATOR_CLASSIFIER_CONCURRENCY", "12")
    state = build_state(cfg)
    finepdfs = _BatchQualityScorer()
    state.source_quality = finepdfs
    state.kenlm = _BatchKenLM()
    documents = []
    expected_texts = []
    for index in range(12):
        text = (
            f"Batch document {index}. {long_english_text} "
            "The architecture has 12 attention layers and was trained for 20 epochs. "
            "Evaluation accuracy is 91.5 percent on research/example-dataset."
        )
        expected_texts.append(text)
        documents.append(
            _silver(text, doc_id=f"sha256:{index + 1:064x}").model_copy(
                update={
                    "source_feed": "hf-models",
                    "source_format": "web",
                    "extraction_pipeline": "hf-model-card-markdown-v1",
                    "title": f"Measured model {index}",
                    "segments": [
                        SilverSegment(
                            segment_id="description",
                            title="Model description",
                            text=text,
                            word_count=len(text.split()),
                        )
                    ],
                }
            )
        )
    try:
        outcomes = process_silver_decision_payloads(
            state,
            [silver_dumps(silver) for silver in documents],
        )

        assert all(outcome.error is None for outcome in outcomes)
        assert sorted(len(batch) for batch in finepdfs.batches) == [2] * 6
        assert sorted(text for batch in finepdfs.batches for text in batch) == sorted(
            [
                f"[SOURCE=hf] [SECTION_TYPE=summary] [SECTION_TITLE=Model description]\n{text}"
                for text in expected_texts
            ]
        )
        for outcome in outcomes:
            assert outcome.value is not None
            gold = common.gold_loads(outcome.value[0])
            assert gold.quality_diagnostics["sections"][0]["score"] == 5.0
    finally:
        state.close()


def test_hf_card_deterministic_reject_skips_finepdfs_inference(
    cfg: ProcessorConfig,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("S2P_CURATOR_CLASSIFIER_BATCH_SIZE", "2")
    state = build_state(cfg)
    finepdfs = _BatchQualityScorer()
    state.source_quality = finepdfs
    rejected_text = "This is a model card. More information needed. " * 20
    rejected = _silver(rejected_text).model_copy(
        update={
            "source_feed": "hf-models",
            "source_format": "web",
            "extraction_pipeline": "hf-model-card-markdown-v2",
            "title": "Automatic model card",
            "segments": [
                SilverSegment(
                    segment_id="description",
                    title="Model description",
                    text=rejected_text,
                    word_count=len(rejected_text.split()),
                )
            ],
        }
    )
    try:
        outcomes = process_silver_decision_payloads(state, [silver_dumps(rejected)])

        assert finepdfs.batches == []
        assert outcomes[0].value is not None
        gold = common.gold_loads(outcomes[0].value[0])
        assert "hf_card_quality_filter" in gold.reject_reasons
        assert gold.segment_scores[0].finepdfs_edu_score is None
        assert gold.classifier_revision == "not-run:deterministic-reject"
    finally:
        state.close()


def test_known_near_duplicate_skips_finepdfs_before_durable_rejection(
    cfg: ProcessorConfig,
    long_english_text: str,
) -> None:
    state = build_state(cfg)
    finepdfs = _BatchQualityScorer()
    state.source_quality = finepdfs
    first = _silver(long_english_text, doc_id="sha256:" + "a" * 64)
    duplicate = _silver(long_english_text, doc_id="sha256:" + "b" * 64)
    try:
        first_outcome = process_silver_decision_payloads(state, [silver_dumps(first)])[0]
        assert first_outcome.value is not None
        finepdfs.batches.clear()

        duplicate_outcome = process_silver_decision_payloads(state, [silver_dumps(duplicate)])[0]

        assert finepdfs.batches == []
        assert duplicate_outcome.value is not None
        gold = common.gold_loads(duplicate_outcome.value[0])
        assert "near_duplicate" in gold.reject_reasons
        assert gold.segment_scores[0].finepdfs_edu_score is None
    finally:
        state.close()


def test_split_model_services_are_all_required_and_closed(
    cfg: ProcessorConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("S2P_MODEL_SERVICE_URL", raising=False)
    monkeypatch.setenv("S2P_QUALITY_MODEL_SERVICE_URL", "http://quality")
    monkeypatch.setenv("S2P_KENLM_MODEL_SERVICE_URL", "http://kenlm")
    monkeypatch.setattr("processor.curate.CuratorModelClient", _SplitModelClient)
    _SplitModelClient.closed.clear()

    state = build_state(cfg)
    assert len(state.model_clients) == 2
    state.close()
    assert set(_SplitModelClient.closed) == {
        "http://quality",
        "http://kenlm",
    }


def test_independent_model_services_are_all_required_and_closed(
    cfg: ProcessorConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("S2P_MODEL_SERVICE_URL", raising=False)
    monkeypatch.delenv("S2P_QUALITY_MODEL_SERVICE_URL", raising=False)
    monkeypatch.setenv("S2P_QUALITY_MODEL_SERVICE_URL", "http://quality")
    monkeypatch.setenv("S2P_KENLM_MODEL_SERVICE_URL", "http://kenlm")
    monkeypatch.setattr("processor.curate.CuratorModelClient", _SplitModelClient)
    _SplitModelClient.closed.clear()

    state = build_state(cfg)
    assert len(state.model_clients) == 2
    state.close()
    assert set(_SplitModelClient.closed) == {
        "http://quality",
        "http://kenlm",
    }


def test_partial_split_model_service_configuration_fails(
    cfg: ProcessorConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("S2P_MODEL_SERVICE_URL", raising=False)
    monkeypatch.setenv("S2P_QUALITY_MODEL_SERVICE_URL", "http://quality")
    monkeypatch.delenv("S2P_KENLM_MODEL_SERVICE_URL", raising=False)

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


def test_cluster_smoke_obeys_the_real_quality_gate(
    cfg: ProcessorConfig, long_english_text: str
) -> None:
    state = build_state(cfg)
    try:
        state.source_quality = _LowQualityScorer()
        silver = _silver(long_english_text).model_copy(
            update={
                "source_feed": "cluster-smoke",
                "source_format": "html",
                "extraction_pipeline": "cluster-smoke-1.0",
            }
        )

        gold = curate_one(state, silver)

        assert gold.quality_diagnostics["score"] == 1.0
        assert "low_quality_score" in gold.reject_reasons
        assert not is_trainable_gold(gold)
    finally:
        state.close()


def test_training_projection_does_not_duplicate_matching_first_heading() -> None:
    silver = _silver("technical documentation").model_copy(update={"title": "Useful Model"})
    segment = SilverSegment(
        segment_id="overview",
        title="Useful Model",
        text="Technical documentation with evaluation evidence.",
        word_count=5,
    )
    projection = _training_projection(silver, [segment])

    assert projection.count("Useful Model") == 1
    assert segment.text in projection


def test_hf_placeholder_section_is_excluded_without_rejecting_substantive_card(
    cfg: ProcessorConfig,
    long_english_text: str,
) -> None:
    state = build_state(cfg)
    try:
        silver = _silver(long_english_text).model_copy(
            update={
                "source_feed": "hf-models",
                "source_format": "web",
                "extraction_pipeline": "hf-model-card-markdown-v1",
                "title": "Measured classifier",
                "segments": [
                    SilverSegment(
                        segment_id="description",
                        title="Model description",
                        text=(
                            long_english_text
                            + " The transformer architecture reaches 84.2% accuracy on NamedBench."
                        ),
                        word_count=len(long_english_text.split()) + 11,
                    ),
                    SilverSegment(
                        segment_id="uses",
                        title="Intended uses and limitations",
                        text="More information needed.",
                        word_count=3,
                    ),
                ],
            }
        )

        gold = curate_one(state, silver)

        assert "hf_card_quality_filter" not in gold.reject_reasons
        assert "More information needed" not in gold.text
        assert any(
            score.segment_id == "uses" and score.decision == "excluded"
            for score in gold.segment_scores
        )
        assert "hf_model_documentation" in gold.content_tags
        assert "educational_web" not in gold.content_tags
    finally:
        state.close()


def test_hf_compact_documentation_counts_its_document_title(
    cfg: ProcessorConfig,
) -> None:
    state = build_state(cfg)
    try:
        body = (
            "This artifact stores one shared set of SafeTensors with dedicated Hugging Face "
            "projection keys. It loads through AutoModelForCausalLM and through the ConceptLM "
            "vLLM backend without a native weight copy. A conversion manifest records source "
            "and lossless key-conversion evidence for the checkpoint. The documented runtime "
            "supports deterministic loading."
        )
        title = "ConceptLM NCP Olmo Stage One Checkpoint"
        assert len(body.split()) < 50
        assert len(f"# {title}\n\n{body}".split()) >= 50
        silver = _silver(body).model_copy(
            update={
                "source_feed": "hf-models",
                "source_format": "web",
                "extraction_pipeline": "hf-model-card-markdown-v1",
                "title": title,
                "segments": [
                    SilverSegment(
                        segment_id="overview",
                        title=title,
                        text=body,
                        word_count=len(body.split()),
                    )
                ],
            }
        )

        gold = curate_one(state, silver)

        assert "insufficient_body" not in gold.reject_reasons
        assert "hf_card_quality_filter" not in gold.reject_reasons
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
        assert gold.pii_action == "body_redacted"
        assert "pii_detected" not in gold.reject_reasons
        assert gold.risk_tier == 1
        assert is_trainable_gold(gold)
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


def test_curate_redacts_contact_details_without_removing_the_section(
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
        assert gold.pii_action == "body_redacted"
        assert "pii_detected" not in gold.reject_reasons
        assert any(
            score.segment_id == "contact" and score.decision == "included"
            for score in gold.segment_scores
        )
        assert any(
            score.segment_id == "methods" and score.decision == "included"
            for score in gold.segment_scores
        )
        assert is_trainable_gold(gold)
    finally:
        state.close()


def test_scientific_curation_excludes_pre_abstract_segments(
    cfg: ProcessorConfig, long_english_text: str
) -> None:
    state = build_state(cfg)
    try:
        silver = _silver(long_english_text).model_copy(
            update={
                "source_feed": "arxiv-html-fetcher",
                "source_format": "pdf",
                "segments": [
                    SilverSegment(
                        segment_id="author",
                        title="Ada Researcher",
                        text="Example University",
                        role="other",
                        word_count=2,
                    ),
                    SilverSegment(
                        segment_id="abstract",
                        title="Abstract",
                        text=long_english_text,
                        role="abstract",
                        word_count=len(long_english_text.split()),
                    ),
                ],
            }
        )
        gold = curate_one(state, silver)
        assert "Ada Researcher" not in gold.text
        assert "Example University" not in gold.text
        assert long_english_text.strip() in gold.text
        assert not any(score.segment_id == "author" for score in gold.segment_scores)
        assert any("Ada Researcher: front matter" in value for value in gold.excluded_sections)
    finally:
        state.close()


def test_expired_silver_skips_decision_and_classifier_work(
    cfg: ProcessorConfig, long_english_text: str
) -> None:
    state = build_state(cfg)
    now = datetime(2026, 9, 4, tzinfo=UTC)
    try:
        silver = _silver(long_english_text).model_copy(
            update={"source_fetched_at": now - timedelta(days=2)}
        )
        [outcome] = process_silver_decision_payloads(
            state,
            [common.silver_dumps(silver)],
            work_cutoff=WorkCutoff(clock=lambda: now),
        )
        assert outcome.expired
        assert outcome.value is None
        assert outcome.error is None
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
                "url": "https://arxiv.org/html/2608.12345",
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


def test_structured_evidence_with_email_is_redacted_before_final_projection(
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
        assert "[EMAIL]" in gold.text
        assert gold.pii_action == "body_redacted"
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


def test_same_payload_twice_in_one_micro_batch_uses_ordered_cache(
    cfg: ProcessorConfig, long_english_text: str
) -> None:
    state = build_state(cfg)
    try:
        payload = silver_dumps(_silver(long_english_text))

        outcomes = process_silver_decision_payloads(state, [payload, payload])

        assert outcomes[0].error is None
        assert outcomes[1].error is None
        assert outcomes[1].value == outcomes[0].value
        assert outcomes[0].value is not None
        assert "near_duplicate" not in common.gold_loads(outcomes[0].value[0]).reject_reasons
    finally:
        state.close()


def test_transient_batch_failure_replays_cached_prefix_without_second_lsh_mutation(
    cfg: ProcessorConfig,
    long_english_text: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = build_state(cfg)
    first = _silver(long_english_text, doc_id="sha256:" + "1" * 64)
    second = _silver(
        (
            "Independent compiler scheduling measurements compare latency, "
            "throughput, memory locality, and deterministic execution. "
        )
        * 25,
        doc_id="sha256:" + "2" * 64,
    )
    payloads = [silver_dumps(first), silver_dumps(second)]
    real_curate_one = curate_module.curate_one

    def fail_on_second(scoring_state: object, silver: SilverRecord) -> object:
        if silver.doc_id == second.doc_id:
            raise RuntimeError("transient second-document failure")
        return real_curate_one(scoring_state, silver)  # type: ignore[arg-type]

    try:
        monkeypatch.setattr(curate_module, "curate_one", fail_on_second)
        with pytest.raises(RuntimeError, match="transient second-document failure"):
            process_silver_decision_payloads(state, payloads)

        first_cache_key = curate_module._decision_cache_key(state, payloads[0])
        cached_first = state.decision_cache.get(first_cache_key)
        assert cached_first is not None

        monkeypatch.setattr(curate_module, "curate_one", real_curate_one)
        replayed = process_silver_decision_payloads(state, payloads)

        assert replayed[0].value == cached_first
        assert replayed[0].error is None
        assert replayed[1].error is None
        assert replayed[1].value is not None
        assert "near_duplicate" not in gold_loads(replayed[1].value[0]).reject_reasons
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
                "url": "https://arxiv.org/html/2608.54321",
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

        assert gold.classifier_revision == "not-run:deterministic-reject"
        assert gold.segment_scores[0].finepdfs_edu_score is None
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


def test_scientific_authoring_template_is_quarantined(cfg: ProcessorConfig) -> None:
    state = build_state(cfg)
    try:
        template_text = (
            "This starter file demonstrates IEEEtran.cls for conference papers. "
            "Subsection text here. Subsubsection text here. The conclusion goes here. "
        ) * 20
        segments = [
            SilverSegment(
                segment_id="introduction",
                title="Introduction",
                role="introduction",
                text=template_text,
                word_count=len(template_text.split()),
            )
        ]
        silver = _silver(template_text, doc_id="sha256:" + "8" * 64).model_copy(
            update={
                "title": "Bare Demo of IEEEtran.cls for Conferences",
                "source_feed": "arxiv-html-fetcher",
                "url": "https://arxiv.org/html/2608.88888",
                "source_format": "html",
                "extraction_pipeline": "arxiv-html-scientific-v1",
                "segments": segments,
                "model_text": template_text,
                "training_word_count": len(template_text.split()),
                "included_section_count": 1,
                "scientific_artifact_s3_uri": "s3://silver/scientific/8/document.json",
            }
        )

        gold = curate_one(state, silver)

        assert "document_template" in gold.reject_reasons
        assert gold.route == "quarantine"
        assert not is_trainable_gold(gold)
    finally:
        state.close()


def test_short_real_scientific_note_keeps_structured_math(cfg: ProcessorConfig) -> None:
    state = build_state(cfg)
    try:
        body = (
            "We derive a compact estimator for streaming gradients under bounded delay. "
            "The proof applies convexity to each update and sums the residual terms. "
            "On 18 controlled trials the estimator reduces median error by 12 percent. "
            "Ablations isolate the delayed update and regularization terms. "
            "The method remains limited to stationary observation noise and future work "
            "must test adversarial delays and non-convex objectives."
        )
        silver = _silver(body, doc_id="sha256:" + "9" * 64).model_copy(
            update={
                "title": "A short note on delayed streaming gradients",
                "source_feed": "arxiv-html-fetcher",
                "url": "https://arxiv.org/html/2608.99999",
                "source_format": "html",
                "extraction_pipeline": "arxiv-html-scientific-v1",
                "segments": [
                    SilverSegment(
                        segment_id="result",
                        title="Derivation and result",
                        role="results",
                        text=body,
                        word_count=len(body.split()),
                    )
                ],
                "model_text": body,
                "structured_text": (
                    "[EQUATION id=eq-1 latex=E_t\\leq E_0+\\sum_i r_i] "
                    "E_t <= E_0 + sum_i r_i [/EQUATION]"
                ),
                "training_word_count": len(body.split()),
                "included_section_count": 1,
                "equation_count": 1,
                "scientific_artifact_s3_uri": "s3://silver/scientific/9/document.json",
            }
        )

        gold = curate_one(state, silver)

        assert "document_template" not in gold.reject_reasons
        assert "[EQUATION" in gold.text
        assert "sum_i r_i" in gold.text
    finally:
        state.close()


def test_scientific_extraction_retry_reuses_admitted_body_and_is_bounded(
    cfg: ProcessorConfig,
) -> None:
    state = build_state(cfg)
    try:
        silver = _silver("brief paper body", doc_id="sha256:" + "c" * 64).model_copy(
            update={
                "source_feed": "arxiv-html-fetcher",
                "url": "https://arxiv.org/html/2608.99999",
                "source_format": "html",
                "extraction_pipeline": "arxiv-html-scientific-v1",
                "raw_html_s3_uri": "s3://bronze/arxiv/paper.html",
                "source_content_type": "text/html",
                "source_fetched_at": datetime(2026, 8, 26, tzinfo=UTC),
            }
        )

        first = curate_one(state, silver)
        assert first.route == "retry"
        payload = extraction_retry_payload(silver, first)
        assert payload is not None
        retry = common.bronze_loads(payload)
        assert retry.raw_html_s3_uri == "s3://bronze/arxiv/paper.html"
        assert retry.source_feed == "arxiv-extraction-retry"
        assert retry.source_format == "metadata"
        assert retry.extraction_pipeline == "arxiv-pdf-retry-v1|curation-retry=1"

        exhausted = curate_one(
            state,
            silver.model_copy(
                update={"extraction_pipeline": "arxiv-html-scientific-v1|curation-retry=2"}
            ),
        )
        assert exhausted.route == "quarantine"
        assert "bounded alternate retry" in exhausted.route_reasons[0]
        assert extraction_retry_payload(silver, exhausted) is None
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


def test_process_silver_payload_redacts_contact_details_without_dropping_document(
    cfg: ProcessorConfig, long_english_text: str
) -> None:
    state = build_state(cfg)
    try:
        text = long_english_text + " contact me at john.doe@example.com please."
        payload = silver_dumps(_silver(text, doc_id="sha256:" + "5" * 64))
        output = process_silver_payload(state, payload)
        assert output is not None
        gold = gold_loads(output)
        assert "john.doe@example.com" not in gold.text
        assert "[EMAIL]" in gold.text
        assert gold.pii_action == "body_redacted"
        assert gold.removed_body_pii_flags == ["email"]
        assert "pii_detected" not in gold.reject_reasons
    finally:
        state.close()


def test_process_silver_payload_quarantines_secret_bearing_document(
    cfg: ProcessorConfig, long_english_text: str
) -> None:
    state = build_state(cfg)
    try:
        text = long_english_text + " api_key = abcdefghijklmnopqrstuvwxyz123456"
        payload = silver_dumps(_silver(text, doc_id="sha256:" + "7" * 64))
        decision_payload, trainable = process_silver_decision_payload(state, payload)
        gold = gold_loads(decision_payload)
        assert not trainable
        assert "abcdefghijklmnopqrstuvwxyz" not in gold.text
        assert "secret" in gold.pii_flags
        assert "pii_detected" in gold.reject_reasons
        assert gold.pii_action == "body_quarantine"
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
