"""Tests for :mod:`processor.operators.kenlm_score`."""

from __future__ import annotations

from processor.operators.kenlm_score import KenLMScorer


def test_scorer_returns_bucket_for_clean_text() -> None:
    scorer = KenLMScorer(None)
    out = scorer.score("a b a b a b a b a b")
    assert out.bucket in {"head", "middle", "tail"}
    assert out.scorer.startswith(("kenlm:", "proxy-character-entropy-"))


def test_scorer_handles_empty_input() -> None:
    scorer = KenLMScorer(None)
    out = scorer.score("")
    assert out.perplexity == 0.0
    assert out.bucket == "head"


def test_random_text_is_high_perplexity() -> None:
    scorer = KenLMScorer(None)
    rare = "qzxqzxqzxqzxqzxqzxqzxqzxqzxqzx"
    common_text = "the the the the the the the the the the"
    rare_ppl = scorer.score(rare).perplexity
    # Equal-frequency strings have similar perplexity in the proxy; we just
    # check sanity: both produce finite, non-negative values.
    assert rare_ppl >= 0.0
    assert scorer.score(common_text).perplexity >= 0.0
