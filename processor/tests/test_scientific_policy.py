"""Tests for explainable scientific routing policy."""

from __future__ import annotations

from processor.scientific_policy import (
    composite_quality_score,
    route_document,
)
from processor.tests.test_curate import _silver
from schemas.silver import SilverSegment


def _segment(segment_id: str, role: str, words: int) -> SilverSegment:
    return SilverSegment(
        segment_id=segment_id,
        title=segment_id,
        role=role,  # type: ignore[arg-type]
        text="word " * words,
        word_count=words,
    )


def test_evidence_rich_paper_is_pretrain_and_posttrain_eligible() -> None:
    silver = _silver("scientific body " * 100).model_copy(
        update={
            "source_feed": "arxiv-html-live",
            "url": "https://arxiv.org/html/2608.00001",
            "scientific_artifact_s3_uri": "s3://silver/scientific/a/document.json",
            "segments": [_segment("methods", "methods", 100)],
        }
    )

    decision = route_document(
        silver=silver,
        reject_reasons=[],
        reasoning_score=0.8,
    )

    assert decision.route == "posttrain_candidate"
    assert decision.eligible_routes == ["pretrain", "posttrain_candidate"]


def test_lower_reasoning_document_is_pretrain_only() -> None:
    silver = _silver("scientific body " * 100).model_copy(
        update={
            "source_feed": "local-controlled-fixtures-v3",
        }
    )

    decision = route_document(
        silver=silver,
        reject_reasons=[],
        reasoning_score=0.4,
    )

    assert decision.route == "pretrain"
    assert decision.eligible_routes == ["pretrain"]


def test_upstream_curation_never_allocates_a_benchmark_split() -> None:
    silver = _silver("scientific body " * 100).model_copy(
        update={
            "source_feed": "arxiv-html-live",
            "scientific_artifact_s3_uri": "s3://silver/scientific/a/document.json",
            "segments": [_segment("results", "results", 100)],
        }
    )

    decision = route_document(
        silver=silver,
        reject_reasons=[],
        reasoning_score=0.8,
    )

    assert decision.route == "posttrain_candidate"


def test_non_scientific_high_reasoning_record_is_never_a_paper_candidate() -> None:
    silver = _silver("model card body " * 100).model_copy(
        update={
            "source_feed": "hf-models",
            "source_format": "web",
            "extraction_pipeline": "hf-model-card-markdown-v1",
            "segments": [_segment("model-card", "other", 100)],
        }
    )

    decision = route_document(silver=silver, reject_reasons=[], reasoning_score=1.0)

    assert decision.route == "pretrain"
    assert decision.eligible_routes == ["pretrain"]


def test_scientific_record_without_durable_artifact_is_not_a_candidate() -> None:
    silver = _silver("scientific body " * 100).model_copy(
        update={
            "source_feed": "arxiv-html-live",
            "segments": [_segment("methods", "methods", 100)],
        }
    )

    decision = route_document(silver=silver, reject_reasons=[], reasoning_score=1.0)

    assert decision.route == "pretrain"
    assert decision.eligible_routes == ["pretrain"]


def test_composite_does_not_substitute_non_applicable_web_signals() -> None:
    score = composite_quality_score(
        edu_score=5.0,
        structural_quality_score=5.0,
        lang_score=1.0,
        gopher_pass=False,
        c4_pass=False,
        perplexity_bucket="tail",
        language_applicable=False,
        web_heuristics_applicable=False,
        perplexity_applicable=False,
    )

    assert score == 5.0
