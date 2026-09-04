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
        self.description = [("scope",), ("kind",), ("key",), ("count",)]
        if "WITH all_decisions AS MATERIALIZED" not in sql:
            raise AssertionError(f"unexpected SQL: {sql}")
        self.rows = [
            ("decision", "total", "", 9),
            ("decision", "source", "arxiv-live", 3),
            ("decision", "source", "fixtures", 6),
            ("decision", "reason", "near_duplicate", 1),
            ("decision", "reason", "pii_detected", 1),
            ("gold", "total", "", 4),
            ("gold", "source", "arxiv-live", 3),
            ("gold", "source", "fixtures", 1),
            ("license", "total", "", 2),
            ("license", "source", "arxiv-live", 2),
            ("license", "reason", "license_missing", 2),
        ]
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
        elif "SELECT DISTINCT tag AS value" in sql:
            self.description = [("value",)]
            self.rows = [("scientific_reasoning",), ("tables",)]
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


class _EmptyDocumentConnection(_FakeConnection):
    def execute(
        self, sql: str, parameters: Sequence[Any] | None = None
    ) -> _EmptyDocumentConnection:
        self.calls.append((sql, parameters))
        if "CAST(COUNT(*) AS BIGINT) AS count" in sql:
            self.description = [("count",)]
            self.rows = [(0,)]
        else:
            self.description = [("doc_id",)]
            self.rows = []
        return self


class _EmptyFacetConnection(_FakeConnection):
    def execute(self, sql: str, parameters: Sequence[Any] | None = None) -> _EmptyFacetConnection:
        self.calls.append((sql, parameters))
        self.description = [("value",)]
        self.rows = []
        return self


def test_as_of_uses_half_open_validity_predicate() -> None:
    conn = _FakeConnection()
    service = DuckDBQueryService(conn)

    rows = service.as_of("2026-06-17T10:00:00Z")

    assert rows == [{"source_feed": "arxiv", "tokens": 10, "documents": 2}]
    sql, params = conn.calls[-1]
    assert "valid_from <= CAST(? AS TIMESTAMP)" in sql
    assert "valid_to IS NULL OR valid_to > CAST(? AS TIMESTAMP)" in sql
    assert "PARTITION BY doc_id" in sql
    assert "revision_rank = 1" in sql
    assert "scoring_version = 'pretrain-content-v3'" not in sql
    assert params == ["2026-06-17T10:00:00Z", "2026-06-17T10:00:00Z"]


def test_as_of_collapses_policy_generations_to_latest_document() -> None:
    duckdb = pytest.importorskip("duckdb")
    connection = duckdb.connect(":memory:")
    connection.execute(
        """
        CREATE TABLE gold (
          doc_id VARCHAR,
          source_feed VARCHAR,
          reject_reasons VARCHAR[],
          scoring_version VARCHAR,
          policy_revision VARCHAR,
          trace_id VARCHAR,
          valid_from TIMESTAMP,
          valid_to TIMESTAMP,
          tokens BIGINT,
          risk_tier INTEGER DEFAULT 1,
          route VARCHAR DEFAULT 'pretrain',
          pii_flags VARCHAR[] DEFAULT []
        );
        INSERT INTO gold (doc_id, source_feed, reject_reasons, scoring_version,
          policy_revision, trace_id, valid_from, valid_to, tokens) VALUES
          ('d1', 'arxiv-html', [], 'pretrain-content-v2', 'p2', 't2',
           '2026-08-01', NULL, 100),
          ('d1', 'arxiv-html', [], 'pretrain-content-v3', 'p3', 't3',
           '2026-09-01', NULL, 120),
          ('d2', 'hf-models', [], 'pretrain-content-v2', 'p2', 't4',
           '2026-08-15', NULL, 40),
          ('d3', 'hf-datasets', ['c4_nopunc_filter'], 'pretrain-content-v3', 'p3', 't5',
           '2026-08-20', NULL, 60);
        """
    )
    service = DuckDBQueryService(connection, gold_relation="gold", decisions_relation="gold")

    assert service.as_of("2026-09-02T00:00:00Z") == [
        {"source_feed": "arxiv-html", "tokens": 120, "documents": 1},
        {"source_feed": "hf-models", "tokens": 40, "documents": 1},
    ]


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
    assert len(connection.calls) == 1
    overview_query = connection.calls[0][0]
    assert "PARTITION BY doc_id" in overview_query
    assert "scoring_version = 'pretrain-content-v3'" not in overview_query


def test_corpus_overview_scans_each_durable_relation_once() -> None:
    connection = _OverviewConnection()
    service = DuckDBQueryService(connection)

    service.corpus_overview()

    sql = connection.calls[0][0]
    assert sql.count("FROM decisions") == 1
    assert sql.count("FROM gold") == 1
    assert sql.count("FROM license_admissions AS admission") == 1
    assert "FROM all_decisions AS decision" in sql


def test_corpus_overview_prepares_all_snapshots_before_single_statement(monkeypatch) -> None:
    connection = _OverviewConnection()
    iceberg_registrations: list[tuple[str, str]] = []
    license_registrations: list[str] = []
    monkeypatch.setattr(
        "processor.duckdb_api._register_iceberg_relation",
        lambda _conn, relation, table: iceberg_registrations.append((relation, table)),
    )
    monkeypatch.setattr(
        "processor.duckdb_api._register_license_relation",
        lambda _conn, relation: license_registrations.append(relation),
    )
    service = DuckDBQueryService(
        connection,
        refresh_iceberg=True,
        catalog_refresh_seconds=30,
    )

    service.corpus_overview()

    assert iceberg_registrations == [
        ("decisions", "curation_decisions"),
        ("gold", "curated"),
    ]
    assert license_registrations == ["license_admissions"]
    assert len(connection.calls) == 1


def test_corpus_overview_one_pass_preserves_filter_and_anti_join_contract() -> None:
    duckdb = pytest.importorskip("duckdb")
    connection = duckdb.connect(":memory:")
    connection.execute(
        """
        CREATE TABLE decisions (
          doc_id VARCHAR,
          source_feed VARCHAR,
          reject_reasons VARCHAR[],
          scoring_version VARCHAR,
          classifier_revision VARCHAR,
          policy_revision VARCHAR,
          trace_id VARCHAR,
          valid_from TIMESTAMP
        );
        INSERT INTO decisions VALUES
          ('d1', 'arxiv-html', [], 'pretrain-content-v2', 'c2', 'p2', 't2', '2026-08-01'),
          ('d1', 'arxiv-html', [], 'pretrain-content-v3', 'c3', 'p3', 't3', '2026-09-01'),
          ('d2', 'hf-models', ['near_duplicate'], 'pretrain-content-v3', 'c3', 'p3', 't3', '2026-09-01'),
          ('d3', 'oai-arxiv-cs', [], 'pretrain-content-v3', 'c3', 'p3', 't3', '2026-09-01'),
          ('d4', 'arxiv-html', ['c4_nopunc_filter'], 'pretrain-content-v3', 'c3', 'p3', 't3', '2026-09-01'),
          ('d5', 'arxiv-html', [], 'old-policy', 'c1', 'p1', 't1', '2026-07-01'),
          ('d6', 'local-smoke', [], 'pretrain-content-v3', 'c3', 'p3', 't3', '2026-09-01'),
          ('d7', 'hf-datasets', [], 'pretrain-content-v3', 'c3', 'p3', 't3', '2026-09-01');

        CREATE TABLE gold (
          doc_id VARCHAR,
          source_feed VARCHAR,
          reject_reasons VARCHAR[],
          scoring_version VARCHAR,
          classifier_revision VARCHAR,
          policy_revision VARCHAR,
          trace_id VARCHAR,
          valid_from TIMESTAMP
        );
        INSERT INTO gold VALUES
          ('d1', 'arxiv-html', [], 'pretrain-content-v2', 'c2', 'p2', 't2', '2026-08-01'),
          ('d1', 'arxiv-html', [], 'pretrain-content-v3', 'c3', 'p3', 't3', '2026-09-01'),
          ('d5', 'arxiv-html', [], 'old-policy', 'c1', 'p1', 't1', '2026-07-01'),
          ('d7', 'hf-datasets', [], 'pretrain-content-v3', 'c3', 'p3', 't3', '2026-09-01'),
          ('d3', 'oai-arxiv-cs', [], 'pretrain-content-v3', 'c3', 'p3', 't3', '2026-09-01');

        CREATE TABLE license_admissions (
          doc_id VARCHAR,
          source_feed VARCHAR,
          license_id VARCHAR,
          status VARCHAR,
          policy_revision VARCHAR,
          source_format VARCHAR
        );
        INSERT INTO license_admissions VALUES
          ('l1', 'arxiv-html', 'unknown', 'quarantined',
           'license-policy-2026-08-25', 'html'),
          ('l2', 'hf-models', 'CC-BY-ND-4.0', 'quarantined',
           'license-policy-2026-08-25', 'html'),
          ('d1', 'arxiv-html', 'unknown', 'quarantined',
           'license-policy-2026-08-25', 'html'),
          ('l3', 'local-smoke', 'unknown', 'quarantined',
           'license-policy-2026-08-25', 'html'),
          ('l4', 'arxiv-html', 'unknown', 'quarantined',
           'license-policy-2026-08-25', 'metadata');
        """
    )
    service = DuckDBQueryService(connection)

    assert service.corpus_overview() == {
        "durable_decisions": 6,
        "training_export_documents": 3,
        "rejected_by_reason": {
            "near_duplicate": 1,
            "license_missing": 1,
            "license_not_permitted": 1,
        },
        "per_source_acceptance": [
            {"source": "arxiv-html", "accepted": 2, "total": 3},
            {"source": "hf-datasets", "accepted": 1, "total": 1},
            {"source": "hf-models", "accepted": 0, "total": 2},
        ],
    }


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
        self.calls: list[tuple[str, list[object]]] = []

    def execute(self, sql: str, _params: list[object]) -> _AdmissionOnlyDetailConnection:
        self.calls.append((sql, _params))
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
    decision_sql = service._conn.calls[0][0]
    assert "ROW_NUMBER() OVER" in decision_sql
    assert "PARTITION BY doc_id" in decision_sql
    assert "scoring_version = 'pretrain-content-v3'" not in decision_sql


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
    assert result["available_content_tags"] == ["scientific_reasoning", "tables"]
    assert result["selection"]["include_structured"] is False
    assert result["selection"]["license_policy"] == "strict_allowlist"
    assert "CC-BY-4.0" in result["selection"]["allowed_licenses"]
    assert result["manifest"]["revisions"]["classifier_revision"] == ["classifier_revision-v1"]
    assert result["manifest"]["decision_table"]["table"] == "curation_decisions"
    assert result["manifest"]["export_limit"] == 5_000


def test_dataset_summary_uses_latest_document_decisions_and_exportable_tags() -> None:
    connection = _DatasetConnection()
    service = DuckDBQueryService(connection)

    service.dataset_summary(
        date_from="2026-08-01T00:00:00Z",
        date_to="2026-09-01T23:59:59Z",
        routes=["pretrain"],
        tags=["scientific_reasoning"],
    )

    aggregate_sql = connection.calls[0][0]
    tag_sql = connection.calls[2][0]
    assert "ROW_NUMBER() OVER" in aggregate_sql
    assert "PARTITION BY doc_id" in aggregate_sql
    assert "scoring_version = 'pretrain-content-v3'" not in aggregate_sql
    assert "LIST_CONTAINS(content_tags, ?)" not in tag_sql
    assert "COALESCE(spdx_license, license) IN" in tag_sql
    assert "risk_tier = 1" in tag_sql


def test_documents_collection_uses_latest_decision_across_policy_generations() -> None:
    connection = _EmptyDocumentConnection()
    service = DuckDBQueryService(connection)

    result = service.documents()

    assert result["items"] == []
    page_sql = connection.calls[0][0]
    count_sql = connection.calls[1][0]
    for sql in (page_sql, count_sql):
        assert "ROW_NUMBER() OVER" in sql
        assert "PARTITION BY doc_id" in sql
        assert "scoring_version = 'pretrain-content-v3'" not in sql


def test_document_facets_use_latest_decisions_across_policy_generations() -> None:
    connection = _EmptyFacetConnection()
    service = DuckDBQueryService(connection)

    assert service.document_facets() == {
        "sources": [],
        "source_formats": [],
        "content_tags": [],
        "rejection_reasons": [],
    }
    assert len(connection.calls) == 4
    for sql, _params in connection.calls:
        assert "ROW_NUMBER() OVER" in sql
        assert "PARTITION BY doc_id" in sql
        assert "scoring_version = 'pretrain-content-v3'" not in sql


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

    assert registrations == [("decisions", "curation_decisions")]


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
    assert "scoring_version = 'pretrain-content-v3'" not in where
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
