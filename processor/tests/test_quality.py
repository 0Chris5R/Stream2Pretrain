"""Tests for :mod:`processor.operators.quality`."""

from __future__ import annotations

from processor.operators.quality import QualityClassifier


def test_proxy_revision_string() -> None:
    q = QualityClassifier(None)
    assert q.revision == "proxy-heuristic-0.1"


def test_score_in_range_for_clean_text(long_english_text: str) -> None:
    q = QualityClassifier(None)
    out = q.score(long_english_text)
    assert 0.0 <= out.quality_score <= 5.0
    assert 0.0 <= out.edu_score <= 5.0


def test_empty_text_is_zero() -> None:
    out = QualityClassifier(None).score("")
    assert out.quality_score == 0.0
    assert out.edu_score == 0.0


def test_short_text_penalised() -> None:
    short = "Just a few words here."
    long_text = " ".join(["The streaming pipeline curates documents." for _ in range(80)])
    q = QualityClassifier(None)
    assert q.score(short).quality_score <= q.score(long_text).quality_score
