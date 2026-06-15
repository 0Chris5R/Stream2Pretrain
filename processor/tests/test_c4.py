"""Tests for :mod:`processor.operators.c4`."""

from __future__ import annotations

from processor.operators.c4 import (
    C4Filter,
    fraction_lines_with_punct,
    has_curly_braces,
    looks_like_lorem_ipsum,
)


def test_fraction_punct_full() -> None:
    text = "Hello world.\nGoodbye world.\nAnother line!"
    assert fraction_lines_with_punct(text) == 1.0


def test_fraction_punct_zero() -> None:
    text = "no punct here\nstill no punct"
    assert fraction_lines_with_punct(text) == 0.0


def test_curly_brace_detection() -> None:
    assert has_curly_braces("function(){return}")
    assert not has_curly_braces("plain prose without braces")


def test_lorem_ipsum_detection() -> None:
    assert looks_like_lorem_ipsum("Lorem ipsum dolor sit amet")
    assert not looks_like_lorem_ipsum("a normal blog post about cats")


def test_c4_filter_passes_clean_prose(long_english_text: str) -> None:
    assert C4Filter().passes(long_english_text)


def test_c4_filter_rejects_curly_brace_dump() -> None:
    text = "Some sentences. {curly brace} appears here. Another sentence."
    assert not C4Filter().passes(text)


def test_c4_filter_rejects_lorem_ipsum() -> None:
    assert not C4Filter().passes("Lorem ipsum dolor sit amet, consectetur adipiscing elit.")
