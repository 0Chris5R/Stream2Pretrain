"""Tests for :mod:`processor.seed.pes2o`."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from processor.seed import pes2o
from processor.seed.cursor import SeedCursor


def test_is_cs_row_via_field_of_study() -> None:
    assert pes2o.is_cs_row({"s2_fields_of_study": ["Computer Science", "Mathematics"]})
    assert not pes2o.is_cs_row({"s2_fields_of_study": ["Biology"]})


def test_is_cs_row_via_arxiv_categories_string() -> None:
    assert pes2o.is_cs_row({"categories": "cs.CL cs.LG"})
    assert pes2o.is_cs_row({"categories": "stat.ML"})
    assert not pes2o.is_cs_row({"categories": "math.AG"})


def test_is_cs_row_via_arxiv_categories_list() -> None:
    assert pes2o.is_cs_row({"categories": ["cs.AI", "math.PR"]})


def test_is_cs_row_falls_back_to_keep_when_no_metadata() -> None:
    assert pes2o.is_cs_row({"id": 1, "text": "..."})


def test_derive_valid_from_iso_created() -> None:
    row = {"created": "2024-09-15T00:00:00Z"}
    dt = pes2o.derive_valid_from(row)
    assert dt == datetime(2024, 9, 15, tzinfo=UTC)


def test_derive_valid_from_year_month() -> None:
    row = {"year": 2025, "month": 3}
    assert pes2o.derive_valid_from(row) == datetime(2025, 3, 1, tzinfo=UTC)


def test_derive_valid_from_year_only() -> None:
    row = {"year": "2025"}
    assert pes2o.derive_valid_from(row) == datetime(2025, 1, 1, tzinfo=UTC)


def test_derive_valid_from_falls_back_to_v2_cutoff() -> None:
    row: dict[str, Any] = {}
    assert pes2o.derive_valid_from(row) == datetime(2023, 1, 3, tzinfo=UTC)


def test_native_id_for_zero_pads_numeric_id() -> None:
    assert pes2o.native_id_for({"id": 42}) == "0000000000000042"


def test_native_id_for_passes_string_through() -> None:
    assert pes2o.native_id_for({"id": "doi:10.1/abc"}) == "doi:10.1/abc"


def test_to_seed_document_basic() -> None:
    row = {
        "id": 7,
        "text": "neural ranking systems for retrieval",
        "title": "On Neural Ranking",
        "year": 2024,
        "month": 9,
        "s2_fields_of_study": ["Computer Science"],
    }
    doc = pes2o.to_seed_document(row)
    assert doc is not None
    assert doc.repo_id == pes2o.REPO_ID
    assert doc.native_id == "0000000000000007"
    assert doc.title == "On Neural Ranking"
    assert doc.text.startswith("neural ranking")
    assert doc.lang == "en"
    assert doc.source_format == "latex"
    assert doc.spdx_license == "ODC-By-1.0"
    assert doc.spdx_license_source == "dataset_metadata"
    assert doc.valid_from == datetime(2024, 9, 1, tzinfo=UTC)


def test_to_seed_document_drops_non_cs_rows() -> None:
    row = {"id": 7, "text": "x", "categories": "math.AG"}
    assert pes2o.to_seed_document(row) is None


def test_to_seed_document_drops_empty_text() -> None:
    row = {"id": 7, "text": "   ", "s2_fields_of_study": ["Computer Science"]}
    assert pes2o.to_seed_document(row) is None


def test_iter_documents_respects_cursor_skip() -> None:
    rows = [
        {"id": 1, "text": "a", "s2_fields_of_study": ["Computer Science"]},
        {"id": 2, "text": "b", "s2_fields_of_study": ["Computer Science"]},
        {"id": 3, "text": "c", "s2_fields_of_study": ["Computer Science"]},
    ]
    cursor = SeedCursor(repo_id=pes2o.REPO_ID)
    cursor.last_native_id = "0000000000000002"
    docs = list(pes2o.iter_documents(cursor, rows=rows))
    assert len(docs) == 1
    assert docs[0].native_id == "0000000000000003"


def test_iter_documents_max_docs() -> None:
    rows = [
        {"id": i, "text": f"row {i}", "s2_fields_of_study": ["Computer Science"]}
        for i in range(1, 6)
    ]
    cursor = SeedCursor(repo_id=pes2o.REPO_ID)
    docs = list(pes2o.iter_documents(cursor, rows=rows, max_docs=2))
    assert len(docs) == 2
