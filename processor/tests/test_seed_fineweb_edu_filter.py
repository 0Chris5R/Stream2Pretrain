"""Tests for :mod:`processor.seed.fineweb_edu_filter`."""

from __future__ import annotations

from datetime import UTC, datetime

from processor.seed import fineweb_edu_filter as ff
from processor.seed.cursor import SeedCursor


def test_url_matches_allowlist_exact_host() -> None:
    assert ff.url_matches_allowlist("https://arxiv.org/abs/2402.01234", ["arxiv.org"])


def test_url_matches_allowlist_subdomain_suffix() -> None:
    assert ff.url_matches_allowlist(
        "https://magazine.sebastianraschka.com/p/training",
        ["sebastianraschka.com"],
    )


def test_url_matches_allowlist_no_match() -> None:
    assert not ff.url_matches_allowlist(
        "https://example.com/post",
        ["arxiv.org", "openai.com"],
    )


def test_url_matches_allowlist_handles_invalid_url() -> None:
    assert not ff.url_matches_allowlist("", ff.DEFAULT_URL_ALLOWLIST)
    assert not ff.url_matches_allowlist("not-a-url", ["arxiv.org"])


def test_derive_valid_from_iso_date() -> None:
    row = {"date": "2024-09-15T00:00:00Z"}
    assert ff.derive_valid_from(row) == datetime(2024, 9, 15, tzinfo=UTC)


def test_derive_valid_from_cc_dump() -> None:
    row = {"dump": "CC-MAIN-2024-30"}
    dt = ff.derive_valid_from(row)
    assert dt.year == 2024
    assert dt.tzinfo is not None


def test_derive_valid_from_falls_back_to_cutoff() -> None:
    row: dict[str, object] = {}
    dt = ff.derive_valid_from(row)
    assert dt == datetime(2024, 4, 1, tzinfo=UTC)


def test_to_seed_document_keeps_arxiv_url() -> None:
    row = {
        "id": "abc-123",
        "url": "https://arxiv.org/abs/2402.01234",
        "text": "abstract text",
        "title": "On a thing",
        "date": "2024-05-01T00:00:00Z",
        "score": 4.2,
        "license": "CC-BY-4.0",
    }
    doc = ff.to_seed_document(row, allowlist=("arxiv.org",))
    assert doc is not None
    assert doc.repo_id == ff.REPO_ID
    assert doc.url == "https://arxiv.org/abs/2402.01234"
    assert doc.source_format == "html"
    assert doc.spdx_license == "CC-BY-4.0"
    assert doc.extra["fineweb_edu_score"] == "4.200"


def test_to_seed_document_does_not_inherit_dataset_wrapper_license() -> None:
    doc = ff.to_seed_document(
        {
            "id": "abc-124",
            "url": "https://arxiv.org/abs/2402.01235",
            "text": "abstract text",
        },
        allowlist=("arxiv.org",),
    )

    assert doc is not None
    assert doc.spdx_license is None
    assert doc.spdx_license_source == "unknown"


def test_to_seed_document_drops_offdomain_url() -> None:
    row = {
        "id": "abc-123",
        "url": "https://example.com/post",
        "text": "x",
    }
    assert ff.to_seed_document(row, allowlist=("arxiv.org",)) is None


def test_to_seed_document_drops_missing_text() -> None:
    row = {
        "id": "abc-123",
        "url": "https://arxiv.org/abs/x",
        "text": "",
    }
    assert ff.to_seed_document(row, allowlist=("arxiv.org",)) is None


def test_iter_documents_filters_and_skips() -> None:
    rows = [
        {"id": "a", "url": "https://arxiv.org/abs/1", "text": "..."},
        {"id": "b", "url": "https://example.com/2", "text": "..."},
        {"id": "c", "url": "https://huggingface.co/blog/post", "text": "..."},
    ]
    cursor = SeedCursor(repo_id=ff.REPO_ID)
    cursor.last_native_id = "a"
    out = list(
        ff.iter_documents(
            cursor,
            rows=rows,
            allowlist=("arxiv.org", "huggingface.co"),
        )
    )
    assert [d.native_id for d in out] == ["c"]
