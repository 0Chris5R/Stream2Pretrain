"""Tests for :mod:`processor.operators.gopher`."""

from __future__ import annotations

from processor.operators.gopher import GopherFilter


def test_gopher_passes_clean_text(long_english_text: str) -> None:
    assert GopherFilter().passes(long_english_text)


def test_gopher_rejects_too_short() -> None:
    assert not GopherFilter().passes("Two words.")


def test_gopher_rejects_too_long_word_avg() -> None:
    long_words = " ".join(["antidisestablishmentarianism"] * 200)
    assert not GopherFilter().passes(long_words)


def test_gopher_rejects_bullet_dump() -> None:
    bullets = "\n".join("- bullet item " + str(i) for i in range(120))
    assert not GopherFilter().passes(bullets)


def test_gopher_rejects_ellipsis_dominated() -> None:
    lines = ["This sentence ends in ellipsis..."] * 120
    text = "\n".join(lines)
    assert not GopherFilter().passes(text)


def test_gopher_stats_word_count(long_english_text: str) -> None:
    s = GopherFilter().stats(long_english_text)
    assert s.word_count > 50
    assert s.alpha_word_ratio > 0.8
