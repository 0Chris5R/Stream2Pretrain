"""Tests for explainable scientific routing policy."""

from __future__ import annotations

from datetime import UTC, datetime

from processor.scientific_policy import representative_segments, route_document
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


def test_representative_segments_reserve_role_families() -> None:
    segments = [
        _segment("abstract", "abstract", 80),
        _segment("intro", "introduction", 400),
        *[_segment(f"method-{index}", "methods", 1_000 - index) for index in range(5)],
        *[_segment(f"result-{index}", "results", 900 - index) for index in range(5)],
        _segment("conclusion", "conclusion", 120),
        _segment("appendix", "appendix", 700),
    ]

    sampled = representative_segments(segments, limit=6)

    assert {segment.role for segment in sampled} == {
        "abstract",
        "introduction",
        "methods",
        "results",
        "conclusion",
        "appendix",
    }


def test_representative_segments_fill_spare_capacity_by_information() -> None:
    segments = [
        _segment("abstract", "abstract", 80),
        _segment("method-short", "methods", 100),
        _segment("method-long", "methods", 600),
        _segment("result", "results", 300),
    ]

    sampled = representative_segments(segments, limit=3)

    assert [segment.segment_id for segment in sampled] == ["abstract", "method-long", "result"]


def test_representative_segments_zero_limit_is_empty() -> None:
    assert representative_segments([_segment("abstract", "abstract", 10)], limit=0) == []


def test_recent_evidence_rich_paper_is_reserved_from_training() -> None:
    silver = _silver("scientific body " * 100).model_copy(
        update={
            "source_feed": "arxiv-html-live",
            "url": "https://arxiv.org/html/2608.00001",
            "valid_from": datetime(2026, 8, 15, tzinfo=UTC),
        }
    )

    decision = route_document(
        silver=silver,
        reject_reasons=[],
        reasoning_score=0.8,
        benchmark_score=0.9,
        benchmark_cutoff=datetime(2025, 1, 1, tzinfo=UTC),
    )

    assert decision.route == "benchmark_candidate"
    assert decision.eligible_routes == [
        "broad_pretraining",
        "reasoning_candidate",
        "benchmark_candidate",
    ]


def test_controlled_local_fixture_never_enters_benchmark_reserve() -> None:
    silver = _silver("scientific body " * 100).model_copy(
        update={
            "source_feed": "local-controlled-fixtures-v3",
            "valid_from": datetime(2026, 8, 15, tzinfo=UTC),
        }
    )

    decision = route_document(
        silver=silver,
        reject_reasons=[],
        reasoning_score=0.8,
        benchmark_score=0.9,
        benchmark_cutoff=datetime(2025, 1, 1, tzinfo=UTC),
    )

    assert decision.route == "reasoning_candidate"
    assert "benchmark_candidate" not in decision.eligible_routes


def test_explicit_reserve_canary_proves_benchmark_route() -> None:
    silver = _silver("scientific body " * 100).model_copy(
        update={
            "source_feed": "local-benchmark-reserve-canary",
            "valid_from": datetime(2026, 8, 15, tzinfo=UTC),
        }
    )

    decision = route_document(
        silver=silver,
        reject_reasons=[],
        reasoning_score=0.8,
        benchmark_score=0.9,
        benchmark_cutoff=datetime(2025, 1, 1, tzinfo=UTC),
    )

    assert decision.route == "benchmark_candidate"
    assert "canary" in decision.reasons[-1]
