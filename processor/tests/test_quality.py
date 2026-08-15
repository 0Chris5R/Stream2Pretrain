"""Tests for :mod:`processor.operators.quality`."""

from __future__ import annotations

from processor.operators.quality import QualityClassifier


def test_proxy_revision_string() -> None:
    q = QualityClassifier(None)
    assert q.revision == "proxy-heuristic-0.1"


def test_score_in_range_for_clean_text(long_english_text: str) -> None:
    q = QualityClassifier(None)
    out = q.score(long_english_text)
    assert 0.0 <= out.edu_score <= 5.0


def test_empty_text_is_zero() -> None:
    out = QualityClassifier(None).score("")
    assert out.edu_score == 0.0


def test_short_text_penalised() -> None:
    short = "Just a few words here."
    long_text = " ".join(["The streaming pipeline curates documents." for _ in range(80)])
    q = QualityClassifier(None)
    assert q.score(short).edu_score <= q.score(long_text).edu_score


class _Tokenizer:
    def encode(self, text: str, **_: object) -> list[int]:
        return [ord(char) for char in text]

    def decode(self, values: list[int], **_: object) -> str:
        return "".join(chr(value) for value in values)


def test_finepdfs_long_document_scores_top_and_bottom() -> None:
    classifier = QualityClassifier(None, model_family="finepdfs-edu-v2")
    classifier._tokenizer = _Tokenizer()  # type: ignore[assignment]
    text = "a" * 10_001 + "BOTTOM"

    chunks = classifier._text_chunks(text)

    assert len(chunks) == 2
    assert chunks[0].startswith("a")
    assert chunks[1].endswith("BOTTOM")
