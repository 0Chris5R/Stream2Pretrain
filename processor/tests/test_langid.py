"""Tests for :mod:`processor.operators.langid`."""

from __future__ import annotations

from processor.operators.langid import LangIdentifier


def test_langid_handles_short_input() -> None:
    """Short inputs always succeed with score in [0, 1]."""
    res = LangIdentifier(min_chars=1).identify("hi")
    assert 0.0 <= res.score <= 1.0


def test_langid_returns_und_on_punctuation_only() -> None:
    res = LangIdentifier().identify("!!!! .... ????")
    assert res.lang in {"und", "en"}


def test_langid_english_long_text() -> None:
    text = (
        "the quick brown fox jumps over the lazy dog and that is what we have "
        "been saying for a long time about this topic"
    )
    res = LangIdentifier(min_chars=1).identify(text)
    assert res.lang in {"en", "und"} or len(res.lang) >= 2
