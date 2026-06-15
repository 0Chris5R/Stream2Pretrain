"""Unit tests for the URL canonicalization + hashing helpers."""

from __future__ import annotations

import pytest

from ingest.common.hashing import canonical_url, content_sha256, doc_id_for_url


class TestCanonicalUrl:
    def test_lowercases_scheme_and_host(self) -> None:
        assert (
            canonical_url("HTTPS://Example.COM/Path?B=2&A=1")
            == "https://example.com/Path?A=1&B=2"
        )

    def test_drops_default_ports(self) -> None:
        assert canonical_url("https://example.com:443/p") == "https://example.com/p"
        assert canonical_url("http://example.com:80/p") == "http://example.com/p"

    def test_keeps_nondefault_ports(self) -> None:
        assert canonical_url("http://example.com:8080/p") == "http://example.com:8080/p"

    def test_strips_tracking_params(self) -> None:
        url = "https://x.io/?utm_source=tw&id=42&fbclid=abc"
        assert canonical_url(url) == "https://x.io/?id=42"

    def test_drops_fragment(self) -> None:
        assert canonical_url("https://x.io/p#anchor") == "https://x.io/p"

    def test_empty_path_normalizes_to_root(self) -> None:
        assert canonical_url("https://x.io") == "https://x.io/"

    def test_rejects_non_http(self) -> None:
        with pytest.raises(ValueError):
            canonical_url("ftp://example.com/")
        with pytest.raises(ValueError):
            canonical_url("")


class TestDocIdForUrl:
    def test_idempotent(self) -> None:
        a = doc_id_for_url("https://Example.com/foo?utm_source=x&id=1")
        b = doc_id_for_url("HTTPS://example.com:443/foo?id=1#frag")
        assert a == b
        assert a.startswith("sha256:")
        assert len(a) == len("sha256:") + 64

    def test_different_urls_yield_different_ids(self) -> None:
        assert doc_id_for_url("https://x.io/a") != doc_id_for_url("https://x.io/b")


class TestContentSha256:
    def test_empty_string(self) -> None:
        # Pre-computed sha256("") for sanity.
        expected = (
            "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        )
        assert content_sha256(b"") == expected
