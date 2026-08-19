"""Tests for :mod:`processor.seed.redpajama_arxiv`."""

from __future__ import annotations

import json
from datetime import UTC, datetime

from processor.seed import redpajama_arxiv as rpa
from processor.seed.cursor import SeedCursor


def test_parse_meta_timestamp_dict() -> None:
    dt = rpa.parse_meta_timestamp({"timestamp": "2024-01-04T10:00:00Z"})
    assert dt == datetime(2024, 1, 4, 10, 0, 0, tzinfo=UTC)


def test_parse_meta_timestamp_string() -> None:
    payload = json.dumps({"timestamp": "2023-04-17T00:00:00+00:00"})
    dt = rpa.parse_meta_timestamp(payload)
    assert dt == datetime(2023, 4, 17, tzinfo=UTC)


def test_parse_meta_timestamp_invalid_returns_none() -> None:
    assert rpa.parse_meta_timestamp("not-json") is None
    assert rpa.parse_meta_timestamp({"other": "x"}) is None
    assert rpa.parse_meta_timestamp(None) is None


def test_native_id_from_url_meta() -> None:
    row = {"meta": {"url": "https://arxiv.org/abs/2402.01234"}}
    assert rpa.native_id_for(row) == "2402.01234"


def test_native_id_falls_back_to_text_hash() -> None:
    row = {"meta": {"other": "x"}, "text": "some-deterministic-input"}
    nid = rpa.native_id_for(row)
    assert nid.startswith("sha:")
    assert len(nid) == len("sha:") + 16


def test_derive_valid_from_uses_meta_timestamp() -> None:
    row = {"meta": {"timestamp": "2022-07-04T00:00:00Z"}}
    assert rpa.derive_valid_from(row) == datetime(2022, 7, 4, tzinfo=UTC)


def test_derive_valid_from_falls_back_to_release_cutoff() -> None:
    row = {"meta": {"other": "x"}}
    assert rpa.derive_valid_from(row).year == 2023


def test_to_seed_document_arxiv_url() -> None:
    row = {
        "text": "abstract...",
        "meta": {
            "url": "https://arxiv.org/abs/2308.05670",
            "timestamp": "2023-08-10T00:00:00Z",
        },
    }
    doc = rpa.to_seed_document(row)
    assert doc is not None
    assert doc.repo_id == rpa.REPO_ID
    assert doc.native_id == "2308.05670"
    assert doc.url == "https://arxiv.org/abs/2308.05670"
    assert doc.source_format == "latex"
    assert doc.extraction_pipeline == "redpajama-arxiv-2023-04"
    assert doc.spdx_license is None
    assert doc.spdx_license_source == "unknown"
    assert doc.valid_from == datetime(2023, 8, 10, tzinfo=UTC)


def test_to_seed_document_drops_empty_text() -> None:
    row = {"text": "", "meta": {"url": "https://arxiv.org/abs/2"}}
    assert rpa.to_seed_document(row) is None


def test_iter_documents_skip_via_cursor() -> None:
    rows = [
        {
            "text": "row a",
            "meta": {
                "url": "https://arxiv.org/abs/2402.01000",
                "timestamp": "2024-02-15T00:00:00Z",
            },
        },
        {
            "text": "row b",
            "meta": {
                "url": "https://arxiv.org/abs/2402.01100",
                "timestamp": "2024-02-16T00:00:00Z",
            },
        },
    ]
    cursor = SeedCursor(repo_id=rpa.REPO_ID)
    cursor.last_native_id = "2402.01000"
    out = list(rpa.iter_documents(cursor, rows=rows))
    assert [d.native_id for d in out] == ["2402.01100"]


def test_iter_documents_respects_max_docs() -> None:
    rows = [
        {
            "text": f"row {i}",
            "meta": {
                "url": f"https://arxiv.org/abs/2402.0{i}",
                "timestamp": "2024-02-15T00:00:00Z",
            },
        }
        for i in range(5)
    ]
    cursor = SeedCursor(repo_id=rpa.REPO_ID)
    out = list(rpa.iter_documents(cursor, rows=rows, max_docs=2))
    assert len(out) == 2
