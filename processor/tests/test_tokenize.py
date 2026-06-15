"""Tests for :mod:`processor.tokenize`."""

from __future__ import annotations

from processor.tokenize import Tokenizer


def test_token_count_nonzero_on_text() -> None:
    out = Tokenizer().count("The quick brown fox jumps over the lazy dog")
    assert out.tokens > 0


def test_empty_text_zero_tokens() -> None:
    assert Tokenizer().count("").tokens == 0


def test_backend_is_one_of_known() -> None:
    assert Tokenizer().backend in {"tiktoken", "sentencepiece", "proxy-whitespace"}
