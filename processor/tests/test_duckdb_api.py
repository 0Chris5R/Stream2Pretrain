from __future__ import annotations

from collections.abc import Sequence
from typing import Any

import pytest

from processor.duckdb_api import (
    DuckDBQueryService,
    _create_empty_gold_relation,
    _optional_bool,
    _register_iceberg_relation,
)


class _FakeConnection:
    def __init__(self) -> None:
        self.description: Sequence[tuple[Any, ...]] | None = None
        self.rows: list[tuple[Any, ...]] = []
        self.calls: list[tuple[str, Sequence[Any] | None]] = []

    def execute(self, sql: str, parameters: Sequence[Any] | None = None) -> _FakeConnection:
        self.calls.append((sql, parameters))
        if "GROUP BY source_feed" in sql:
            self.description = [("source_feed",), ("tokens",), ("documents",)]
            self.rows = [("arxiv", 10, 2)]
        elif "quality_score" in sql or "edu_score" in sql:
            self.description = [("score",), ("count",)]
            self.rows = [(3.5, 7)]
        else:
            self.description = [("answer",)]
            self.rows = [(42,)]
        return self

    def fetchall(self) -> list[tuple[Any, ...]]:
        return self.rows


class _OverviewConnection(_FakeConnection):
    def execute(self, sql: str, parameters: Sequence[Any] | None = None) -> _OverviewConnection:
        self.calls.append((sql, parameters))
        if "durable_decisions" in sql:
            self.description = [("durable_decisions",)]
            self.rows = [(9,)]
        elif "training_export_documents" in sql:
            self.description = [("training_export_documents",)]
            self.rows = [(4,)]
        elif "UNNEST(reject_reasons)" in sql:
            self.description = [("reason",), ("count",)]
            self.rows = [("near_duplicate", 1), ("pii_detected", 1)]
        elif "AS total" in sql:
            self.description = [("source",), ("total",)]
            self.rows = [("arxiv-live", 3), ("fixtures", 6)]
        elif "AS accepted" in sql:
            self.description = [("source",), ("accepted",)]
            self.rows = [("arxiv-live", 3), ("fixtures", 1)]
        else:
            raise AssertionError(f"unexpected SQL: {sql}")
        return self


class _DatasetConnection(_FakeConnection):
    def execute(self, sql: str, parameters: Sequence[Any] | None = None) -> _DatasetConnection:
        self.calls.append((sql, parameters))
        if "COUNT(DISTINCT source_feed)" in sql:
            self.description = [
                ("documents",),
                ("tokens",),
                ("source_words",),
                ("projection_words",),
                ("source_count",),
            ]
            self.rows = [(3, 1200, 900, 800, 1)]
        elif "SELECT DISTINCT" in sql:
            keys = [
                "policy_revision",
                "scoring_version",
                "classifier_revision",
                "classifier_backend",
                "projection_version",
                "extraction_pipeline",
                "benchmark_set_version",
                "decon_embedding_revision",
                "pii_scanner_revision",
                "lang_detector_revision",
                "tokenizer_revision",
                "perplexity_scorer",
                "minhash_backend",
                "lsh_backend",
            ]
            self.description = [(key,) for key in keys]
            self.rows = [tuple(f"{key}-v1" for key in keys)]
        else:
            raise AssertionError(f"unexpected SQL: {sql}")
        return self


def test_as_of_uses_half_open_validity_predicate() -> None:
    conn = _FakeConnection()
    service = DuckDBQueryService(conn)

    rows = service.as_of("2026-06-17T10:00:00Z")

    assert rows == [{"source_feed": "arxiv", "tokens": 10, "documents": 2}]
    sql, params = conn.calls[-1]
    assert "valid_from <= CAST(? AS TIMESTAMP)" in sql
    assert "valid_to IS NULL OR valid_to > CAST(? AS TIMESTAMP)" in sql
    assert params == ["2026-06-17T10:00:00Z", "2026-06-17T10:00:00Z"]


def test_quality_histogram_shape() -> None:
    service = DuckDBQueryService(_FakeConnection())

    assert service.quality_histogram() == {
        "buckets": [{"score": 3.5, "count": 7}],
        "edu_buckets": [{"score": 3.5, "count": 7}],
    }


def test_corpus_overview_uses_durable_decision_and_gold_counts() -> None:
    service = DuckDBQueryService(_OverviewConnection())

    assert service.corpus_overview() == {
        "durable_decisions": 9,
        "training_export_documents": 4,
        "rejected_by_reason": {"near_duplicate": 1, "pii_detected": 1},
        "per_source_acceptance": [
            {"source": "arxiv-live", "accepted": 3, "total": 3},
            {"source": "fixtures", "accepted": 1, "total": 6},
        ],
    }


def test_safe_query_rejects_writes_and_multiple_statements() -> None:
    service = DuckDBQueryService(_FakeConnection())

    with pytest.raises(ValueError, match="only SELECT"):
        service.safe_query("DELETE FROM gold", [])
    with pytest.raises(ValueError, match="multiple"):
        service.safe_query("SELECT 1; SELECT 2", [])


def test_safe_query_returns_rows_and_duration() -> None:
    service = DuckDBQueryService(_FakeConnection())

    payload = service.safe_query("SELECT 42 AS answer", [])

    assert payload["rows"] == [{"answer": 42}]
    assert payload["durationMs"] >= 0


def test_dataset_summary_contains_reproducible_revision_manifest() -> None:
    service = DuckDBQueryService(_DatasetConnection())

    result = service.dataset_summary(
        date_from="2026-08-01T00:00:00Z",
        date_to="2026-08-15T23:59:59Z",
        routes=["reasoning_candidate"],
        include_structured=False,
    )

    assert result["documents"] == 3
    assert result["selection"]["include_structured"] is False
    assert result["manifest"]["revisions"]["classifier_revision"] == ["classifier_revision-v1"]
    assert result["manifest"]["decision_table"]["table"] == "curation_decisions"
    assert result["manifest"]["export_limit"] == 5_000


def test_gold_relation_is_validated() -> None:
    with pytest.raises(ValueError, match="gold_relation"):
        DuckDBQueryService(_FakeConnection(), gold_relation="gold; DROP TABLE gold")


def test_empty_gold_relation_is_gold_shaped() -> None:
    conn = _FakeConnection()

    _create_empty_gold_relation(conn, "gold")

    sql, _params = conn.calls[-1]
    assert "CREATE OR REPLACE VIEW gold" in sql
    assert "source_feed" in sql
    assert "valid_from" in sql
    assert "quality_score" in sql
    assert "WHERE FALSE" in sql


def test_iceberg_relation_defensively_deduplicates_recipe_rows(monkeypatch) -> None:
    conn = _FakeConnection()
    monkeypatch.setattr("processor.duckdb_api._load_table_location", lambda _name: "/warehouse/t")

    _register_iceberg_relation(conn, "decisions", "curation_decisions")

    sql, _params = conn.calls[-1]
    assert "ROW_NUMBER() OVER" in sql
    assert "PARTITION BY doc_id, scoring_version, classifier_revision, policy_revision" in sql


def test_document_filters_are_parameterized_and_hide_fixtures() -> None:
    service = DuckDBQueryService(_FakeConnection())

    where, params = service._document_where(
        search="method",
        routes=["reasoning_candidate"],
        tags=["empirical_evidence"],
        has_figures=True,
        min_edu=3.0,
    )

    assert "source_feed NOT LIKE 'local-%'" in where
    assert "LIST_CONTAINS(content_tags, ?)" in where
    assert "figure_count > 0" in where
    assert "edu_score >= ?" in where
    assert "method" not in where
    assert params == [
        "%method%",
        "%method%",
        "%method%",
        "reasoning_candidate",
        "empirical_evidence",
        3.0,
    ]


def test_optional_boolean_parser_rejects_ambiguous_values() -> None:
    assert _optional_bool(None) is None
    assert _optional_bool("true") is True
    assert _optional_bool("0") is False
    with pytest.raises(ValueError, match="boolean"):
        _optional_bool("sometimes")
