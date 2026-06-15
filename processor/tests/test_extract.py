"""Tests for :mod:`processor.operators.extract`."""

from __future__ import annotations

from processor.operators.extract import ResiliparseExtractor


def test_extractor_returns_text_and_title() -> None:
    html = b"<html><head><title>Hello</title></head><body><p>World text here.</p></body></html>"
    out = ResiliparseExtractor().extract(html)
    assert "World" in out.text
    assert out.extracted_with.startswith(("resiliparse-", "fallback-regex-"))
    if out.title is not None:
        assert "Hello" in out.title


def test_empty_html_returns_empty_text() -> None:
    out = ResiliparseExtractor().extract("")
    assert out.text == ""


def test_strips_tags_in_fallback_path() -> None:
    """The regex fallback always strips HTML tags."""
    extractor = ResiliparseExtractor()
    html = "<div><span>alpha</span><span>beta</span></div>"
    out = extractor.extract(html)
    assert "alpha" in out.text and "beta" in out.text
    assert "<" not in out.text and ">" not in out.text
