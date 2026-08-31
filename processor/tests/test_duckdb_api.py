from __future__ import annotations

from collections.abc import Sequence
from typing import Any

import pytest

from processor.duckdb_api import (
    DuckDBQueryService,
    _configure_runtime_limits,
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
        if "admission.license_id" in sql:
            self.description = [("reason",), ("count",)]
            self.rows = [("license_missing", 2)]
        elif "admission.source_feed" in sql:
            self.description = [("source",), ("total",)]
            self.rows = [("arxiv-live", 2)]
        elif "durable_decisions" in sql:
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
    assert "scoring_version = 'pretrain-content-v2'" in sql
    assert params == ["2026-06-17T10:00:00Z", "2026-06-17T10:00:00Z"]


def test_quality_histogram_shape() -> None:
    service = DuckDBQueryService(_FakeConnection())

    assert service.quality_histogram() == {
        "buckets": [{"score": 3.5, "count": 7}],
        "edu_buckets": [{"score": 3.5, "count": 7}],
    }


def test_corpus_overview_uses_durable_decision_and_gold_counts() -> None:
    connection = _OverviewConnection()
    service = DuckDBQueryService(connection)

    assert service.corpus_overview() == {
        "durable_decisions": 11,
        "training_export_documents": 4,
        "rejected_by_reason": {
            "near_duplicate": 1,
            "pii_detected": 1,
            "license_missing": 2,
        },
        "per_source_acceptance": [
            {"source": "arxiv-live", "accepted": 3, "total": 5},
            {"source": "fixtures", "accepted": 1, "total": 6},
        ],
    }
    accepted_query = next(sql for sql, _ in connection.calls if "AS accepted" in sql)
    assert "scoring_version = 'pretrain-content-v2'" in accepted_query


class _LicenseAdmissionsConnection:
    def __init__(self) -> None:
        self.description: list[tuple[str]] = []
        self.rows: list[tuple[object, ...]] = []
        self.calls: list[tuple[str, list[object]]] = []

    def execute(self, sql: str, params: list[object]) -> _LicenseAdmissionsConnection:
        self.calls.append((sql, params))
        if "GROUP BY status" in sql:
            self.description = [("status",), ("count",)]
            self.rows = [
                ("admitted", 4),
                ("posttrain_transform_only", 3),
                ("quarantined", 6),
            ]
        elif "GROUP BY license_id, status" in sql:
            self.description = [("license_id",), ("status",), ("count",)]
            self.rows = [("CC-BY-4.0", "admitted", 4), ("unknown", "quarantined", 6)]
        elif "ORDER BY observed_at DESC" in sql:
            self.description = [
                ("decision_id",),
                ("doc_id",),
                ("source_feed",),
                ("source_url",),
                ("observed_at",),
                ("status",),
                ("license_id",),
                ("license_source",),
                ("reason",),
                ("content_fetch_started",),
            ]
            self.rows = [
                (
                    "decision-1",
                    "doc-1",
                    "rss-arxiv-cs-ai",
                    "https://example.test/1",
                    "2026-08-22 12:00:00",
                    "admitted",
                    "CC-BY-4.0",
                    "rss_entry",
                    "allowed",
                    False,
                )
            ]
        elif "GROUP BY 1, license_id, status" in sql:
            self.description = [
                ("source_feed",),
                ("license_id",),
                ("status",),
                ("count",),
            ]
            self.rows = [("rss-arxiv-cs-ai", "CC-BY-4.0", "admitted", 2)]
        elif "GROUP BY 1, license_source" in sql:
            self.description = [("source_feed",), ("license_source",), ("count",)]
            self.rows = [("rss-arxiv-cs-ai", "rss_entry", 4)]
        elif "GROUP BY 1" in sql:
            self.description = [
                ("source_feed",),
                ("documents",),
                ("admitted",),
                ("posttrain_transform_only",),
                ("quarantined",),
                ("last_observed_at",),
            ]
            self.rows = [("rss-arxiv-cs-ai", 4, 2, 1, 1, "2026-08-22 12:00:00")]
        else:
            raise AssertionError(f"unexpected SQL: {sql}")
        return self

    def fetchall(self) -> list[tuple[object, ...]]:
        return self.rows


class _AdmissionOnlyDetailConnection:
    def __init__(self) -> None:
        self.description: list[tuple[str]] = []
        self.rows: list[tuple[object, ...]] = []

    def execute(self, sql: str, _params: list[object]) -> _AdmissionOnlyDetailConnection:
        if "FROM decisions" in sql:
            self.description = [("doc_id",)]
            self.rows = []
        elif "status = 'quarantined'" in sql:
            self.description = [
                ("doc_id",),
                ("title",),
                ("source_url",),
                ("source_feed",),
                ("source_format",),
                ("valid_from",),
                ("license_id",),
                ("license_source",),
                ("reason",),
                ("raw_license",),
                ("normalized_license",),
                ("resolver",),
                ("evidence_url",),
                ("evidence_revision",),
                ("evidence_scope",),
                ("policy_revision",),
                ("resolved_at",),
            ]
            self.rows = [
                (
                    "sha256:" + "a" * 64,
                    "https://example.test/paper",
                    "https://example.test/paper",
                    "rss-example",
                    "html",
                    "2026-08-23 10:00:00",
                    "unknown",
                    "unknown",
                    "item-level machine-readable licence is unresolved",
                    None,
                    "unknown",
                    "web-page-license-probe",
                    "https://example.test/paper",
                    None,
                    "unknown",
                    "license-policy-2026-08-23",
                    "2026-08-23 10:00:00",
                )
            ]
        else:
            raise AssertionError(f"unexpected SQL: {sql}")
        return self

    def fetchall(self) -> list[tuple[object, ...]]:
        return self.rows


def test_license_admissions_exposes_durable_24h_source_activity() -> None:
    service = DuckDBQueryService(_LicenseAdmissionsConnection())

    admissions = service.license_admissions(recent_limit=5)
    activity = service.source_activity(window_hours=24)

    assert admissions["admitted"] == 4
    assert admissions["posttrain_transform_only"] == 3
    assert admissions["quarantined"] == 6
    assert activity["window_hours"] == 24
    assert activity["sources"] == [
        {
            "source_feed": "rss-arxiv-cs-ai",
            "documents": 4,
            "admitted": 2,
            "posttrain_transform_only": 1,
            "quarantined": 1,
            "last_observed_at": "2026-08-22 12:00:00",
            "license_distribution": [{"license_id": "CC-BY-4.0", "status": "admitted", "count": 2}],
            "license_provenance": [{"license_source": "rss_entry", "count": 4}],
        }
    ]
    activity_sql, activity_params = next(
        (sql, params)
        for sql, params in service._conn.calls
        if "GROUP BY 1" in sql and "last_observed_at" in sql
    )
    assert "FROM license_admissions" in activity_sql
    assert len(activity_params) == 1


def test_source_activity_bounds_requested_window() -> None:
    service = DuckDBQueryService(_LicenseAdmissionsConnection())

    assert service.source_activity(window_hours=0)["window_hours"] == 1
    assert service.source_activity(window_hours=10_000)["window_hours"] == 168


def test_document_exposes_prefetch_license_quarantine_for_audit() -> None:
    service = DuckDBQueryService(
        _AdmissionOnlyDetailConnection(),
        decisions_relation="decisions",
        license_admissions_relation="license_admissions",
    )

    document = service.document("sha256:" + "a" * 64)

    assert document is not None
    assert document["admission_only"] is True
    assert document["route"] == "quarantine"
    assert document["training_usage"] == "quarantined"
    assert document["reject_reasons"] == ["license_missing"]
    assert document["license_admission"]["resolver"] == "web-page-license-probe"


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
    assert result["selection"]["license_policy"] == "strict_allowlist"
    assert "CC-BY-4.0" in result["selection"]["allowed_licenses"]
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
    monkeypatch.setattr(
        "processor.duckdb_api._load_table_reference",
        lambda _name: ("/warehouse/t", "00001-test"),
    )

    _register_iceberg_relation(conn, "decisions", "curation_decisions")

    sql, _params = conn.calls[-1]
    assert "version = '00001-test'" in sql
    assert "ROW_NUMBER() OVER" in sql
    assert "PARTITION BY doc_id, scoring_version, classifier_revision, policy_revision" in sql


def test_iceberg_relation_refresh_is_cached_between_aggregate_queries(monkeypatch) -> None:
    conn = _FakeConnection()
    registrations: list[tuple[str, str]] = []
    monkeypatch.setattr(
        "processor.duckdb_api._register_iceberg_relation",
        lambda _conn, relation, table: registrations.append((relation, table)),
    )
    service = DuckDBQueryService(
        conn,
        refresh_iceberg=True,
        catalog_refresh_seconds=30,
    )

    service.quality_histogram()

    assert registrations == [("gold", "curated")]


def test_duckdb_runtime_limits_enable_bounded_spilling(monkeypatch, tmp_path) -> None:
    conn = _FakeConnection()
    spill = tmp_path / "spill"
    monkeypatch.setenv("S2P_DUCKDB_TEMP_DIRECTORY", str(spill))
    monkeypatch.setenv("S2P_DUCKDB_MEMORY_LIMIT", "384MB")

    _configure_runtime_limits(conn)

    assert spill.is_dir()
    statements = [sql for sql, _params in conn.calls]
    assert "SET memory_limit='384MB'" in statements
    assert "SET threads='1'" in statements
    assert f"SET temp_directory='{spill}'" in statements
    assert "SET max_temp_directory_size='3GB'" in statements


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
    assert "scoring_version = 'pretrain-content-v2'" in where
    assert "LIST_CONTAINS(content_tags, ?)" in where
    assert "route = ?" in where
    assert "eligible_routes" not in where
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
