"""DuckDB HTTP API for cockpit lakehouse queries.

The service keeps Iceberg/httpfs extensions warm in one in-cluster process and
exposes only the narrow routes the UI needs. Arbitrary SQL is restricted to
read-only ``SELECT`` statements; dashboards should prefer the typed endpoints.
"""

from __future__ import annotations

import os
import re
import time
from collections.abc import Sequence
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from functools import partial
from typing import Any, Protocol
from urllib.parse import unquote, urlparse

from ingest.common.license_admission import LICENSE_POLICY_REVISION, PERMISSIVE_TRAINING_LICENSES

_RELATION_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_.$]*$")
_TRAINING_LICENSE_SQL = ", ".join(
    f"'{value.replace(chr(39), chr(39) * 2)}'" for value in sorted(PERMISSIVE_TRAINING_LICENSES)
)
_LICENSE_POLICY_SQL = LICENSE_POLICY_REVISION.replace("'", "''")
_REMOVED_CORPUS_SOURCES = (
    "github-events",
    "github-releases",
    "github-release-tarballs",
    "hf-daily-papers",
    "oai-arxiv-cs",
    "rss-openai-news",
    "rss-deepmind-blog",
    "rss-hf-blog",
    "rss-bair-blog",
    "rss-eleuther-blog",
)
_LEGACY_DISPLAY_REJECTIONS = (
    "metadata_only",
    "c4_nopunc_filter",
    "gopher_filter",
)


def _visible_source_predicate(
    column: str = "source_feed", *, include_fixtures: bool = False
) -> str:
    """Exclude fixtures, internal discovery, removed sources, and backfills."""
    removed = ", ".join(f"'{value}'" for value in _REMOVED_CORPUS_SOURCES)
    clauses: list[str] = []
    if not include_fixtures:
        clauses.extend(
            (
                f"{column} NOT LIKE 'local-%'",
                f"{column} <> 'cluster-smoke'",
            )
        )
    clauses.extend(
        (
            f"{column} NOT LIKE 'rss-arxiv-%'",
            f"{column} NOT LIKE 'seed:%'",
            f"{column} NOT IN ({removed})",
        )
    )
    return " AND ".join(clauses)


def _current_decision_predicate(
    source_column: str = "source_feed",
    reject_column: str = "reject_reasons",
    *,
    include_fixtures: bool = False,
) -> str:
    """Hide superseded policy rows from every normal product view."""
    historical = " OR ".join(
        f"LIST_CONTAINS({reject_column}, '{reason}')" for reason in _LEGACY_DISPLAY_REJECTIONS
    )
    return (
        f"{_visible_source_predicate(source_column, include_fixtures=include_fixtures)} "
        f"AND NOT ({historical})"
    )


class DuckDBConnection(Protocol):
    description: Sequence[tuple[Any, ...]] | None

    def execute(self, sql: str, parameters: Sequence[Any] | None = None) -> DuckDBConnection: ...
    def fetchall(self) -> list[tuple[Any, ...]]: ...


class DuckDBQueryService:
    """Typed query helper around a DuckDB connection."""

    def __init__(
        self,
        connection: DuckDBConnection,
        *,
        gold_relation: str = "gold",
        decisions_relation: str = "decisions",
        license_admissions_relation: str = "license_admissions",
        refresh_iceberg: bool = False,
        catalog_refresh_seconds: float = 30.0,
        artifact_store: ScientificArtifactStore | None = None,
    ) -> None:
        if not _RELATION_RE.fullmatch(gold_relation):
            raise ValueError("gold_relation must be a simple DuckDB relation name")
        if not _RELATION_RE.fullmatch(decisions_relation):
            raise ValueError("decisions_relation must be a simple relation name")
        if not _RELATION_RE.fullmatch(license_admissions_relation):
            raise ValueError("license_admissions_relation must be a simple relation name")
        self._conn = connection
        self._gold = gold_relation
        self._decisions = decisions_relation
        self._license_admissions = license_admissions_relation
        self._refresh_iceberg = refresh_iceberg
        self._catalog_refresh_seconds = max(0.0, catalog_refresh_seconds)
        self._relation_refreshed_at: dict[str, float] = {}
        self._artifact_store = artifact_store

    @classmethod
    def from_env(cls) -> DuckDBQueryService:
        import duckdb  # type: ignore[import-untyped]

        db_path = os.environ.get("S2P_DUCKDB_DATABASE", ":memory:")
        gold_relation = os.environ.get("S2P_DUCKDB_GOLD_RELATION", "gold")
        decisions_relation = os.environ.get("S2P_DUCKDB_DECISIONS_RELATION", "decisions")
        license_admissions_relation = os.environ.get(
            "S2P_DUCKDB_LICENSE_ADMISSIONS_RELATION", "license_admissions"
        )
        conn = duckdb.connect(db_path, read_only=False)
        _load_extensions(conn)
        if os.environ.get("S2P_DUCKDB_UNSAFE_VERSION_GUESSING") == "1":
            # The laptop profile uses a single PyIceberg SQLite-catalog writer,
            # which does not maintain DuckDB's optional version-hint.text.
            conn.execute("SET unsafe_enable_version_guessing = true")
        _configure_s3(conn)
        _configure_runtime_limits(conn)
        return cls(
            conn,
            gold_relation=gold_relation,
            decisions_relation=decisions_relation,
            license_admissions_relation=license_admissions_relation,
            refresh_iceberg=True,
            catalog_refresh_seconds=float(
                os.environ.get("S2P_DUCKDB_CATALOG_REFRESH_SECONDS", "30")
            ),
            artifact_store=ScientificArtifactStore.from_env(),
        )

    def as_of(self, ts: str) -> list[dict[str, Any]]:
        sql = f"""
        SELECT
          source_feed,
          CAST(COALESCE(SUM(tokens), 0) AS BIGINT) AS tokens,
          CAST(COUNT(*) AS BIGINT) AS documents
        FROM {self._gold}
        WHERE valid_from <= CAST(? AS TIMESTAMP)
          AND (valid_to IS NULL OR valid_to > CAST(? AS TIMESTAMP))
          AND {_visible_source_predicate()}
        GROUP BY source_feed
        ORDER BY tokens DESC, source_feed ASC
        """
        return self._rows(sql, [ts, ts], relation=self._gold)

    def quality_histogram(self) -> dict[str, list[dict[str, Any]]]:
        composite_sql = f"""
        SELECT
          CAST(FLOOR(quality_score * 2) / 2 AS DOUBLE) AS score,
          CAST(COUNT(*) AS BIGINT) AS count
        FROM {self._gold}
        WHERE {_current_decision_predicate()}
        GROUP BY score
        ORDER BY score ASC
        """
        edu_sql = f"""
        SELECT
          CAST(FLOOR(edu_score * 2) / 2 AS DOUBLE) AS score,
          CAST(COUNT(*) AS BIGINT) AS count
        FROM {self._gold}
        WHERE {_current_decision_predicate()}
        GROUP BY score
        ORDER BY score ASC
        """
        return {
            "buckets": self._rows(composite_sql, [], relation=self._gold),
            "edu_buckets": self._rows(edu_sql, [], relation=self._gold),
        }

    def curation_summary(self) -> list[dict[str, Any]]:
        """Aggregate the durable decision stream by final corpus route."""
        sql = f"""
        SELECT
          route,
          CAST(COUNT(*) AS BIGINT) AS documents,
          CAST(COALESCE(SUM(source_word_count), 0) AS BIGINT) AS source_words,
          CAST(COALESCE(SUM(training_word_count), 0) AS BIGINT) AS training_words,
          CAST(COALESCE(AVG(quality_score), 0) AS DOUBLE) AS mean_quality,
          CAST(COALESCE(AVG(edu_score), 0) AS DOUBLE) AS mean_edu
        FROM {self._decisions}
        WHERE {_current_decision_predicate()}
        GROUP BY route
        ORDER BY documents DESC, route ASC
        """
        rows = self._rows(sql, [], relation=self._decisions)
        early_quarantines = self._rows(
            f"""
            SELECT CAST(COUNT(*) AS BIGINT) AS documents
            FROM {self._license_admissions} AS admission
            WHERE admission.status = 'quarantined'
              AND admission.policy_revision = '{_LICENSE_POLICY_SQL}'
              AND admission.source_format IS DISTINCT FROM 'metadata'
              AND {_visible_source_predicate("admission.source_feed")}
              AND NOT EXISTS (
                SELECT 1 FROM {self._decisions} AS decision
                WHERE decision.doc_id = admission.doc_id
              )
            """,
            [],
            relation=self._license_admissions,
        )
        count = int(early_quarantines[0]["documents"]) if early_quarantines else 0
        if count:
            quarantine = next((row for row in rows if row["route"] == "quarantine"), None)
            if quarantine is None:
                rows.append(
                    {
                        "route": "quarantine",
                        "documents": count,
                        "source_words": 0,
                        "training_words": 0,
                        "mean_quality": 0.0,
                        "mean_edu": 0.0,
                    }
                )
            else:
                quarantine["documents"] = int(quarantine["documents"]) + count
            rows.sort(key=lambda row: (-int(row["documents"]), str(row["route"])))
        return rows

    def corpus_overview(self) -> dict[str, Any]:
        """Return restart-safe headline counts from durable Iceberg tables.

        Prometheus process counters correctly describe activity since a worker
        started, but they cannot describe the current corpus after recovery.
        Dashboard totals therefore come from the decision and Gold tables.
        """
        decision_totals = self._rows(
            f"""
            SELECT CAST(COUNT(*) AS BIGINT) AS durable_decisions
            FROM {self._decisions}
            WHERE {_current_decision_predicate()}
            """,
            [],
            relation=self._decisions,
        )
        training_totals = self._rows(
            f"""
            SELECT CAST(COUNT(*) AS BIGINT) AS training_export_documents
            FROM {self._gold}
            WHERE {_current_decision_predicate()}
            """,
            [],
            relation=self._gold,
        )
        reasons = self._rows(
            f"""
            SELECT reason, CAST(COUNT(*) AS BIGINT) AS count
            FROM {self._decisions}, UNNEST(reject_reasons) AS rejected(reason)
            WHERE {_current_decision_predicate()}
            GROUP BY 1
            ORDER BY count DESC, reason ASC
            """,
            [],
            relation=self._decisions,
        )
        decision_sources = self._rows(
            f"""
            SELECT source_feed AS source, CAST(COUNT(*) AS BIGINT) AS total
            FROM {self._decisions}
            WHERE {_current_decision_predicate()}
            GROUP BY source_feed
            ORDER BY source_feed ASC
            """,
            [],
            relation=self._decisions,
        )
        accepted_sources = self._rows(
            f"""
            SELECT source_feed AS source, CAST(COUNT(*) AS BIGINT) AS accepted
            FROM {self._gold}
            WHERE {_visible_source_predicate()}
            GROUP BY source_feed
            ORDER BY source_feed ASC
            """,
            [],
            relation=self._gold,
        )
        early_license_reasons = self._rows(
            f"""
            SELECT
              CASE WHEN admission.license_id = 'unknown'
                   THEN 'license_missing'
                   ELSE 'license_not_permitted'
              END AS reason,
              CAST(COUNT(*) AS BIGINT) AS count
            FROM {self._license_admissions} AS admission
            WHERE admission.status = 'quarantined'
              AND admission.policy_revision = '{_LICENSE_POLICY_SQL}'
              AND admission.source_format IS DISTINCT FROM 'metadata'
              AND {_visible_source_predicate("admission.source_feed")}
              AND NOT EXISTS (
                SELECT 1 FROM {self._decisions} AS decision
                WHERE decision.doc_id = admission.doc_id
              )
            GROUP BY 1
            ORDER BY count DESC, reason ASC
            """,
            [],
            relation=self._license_admissions,
        )
        early_license_sources = self._rows(
            f"""
            SELECT admission.source_feed AS source, CAST(COUNT(*) AS BIGINT) AS total
            FROM {self._license_admissions} AS admission
            WHERE admission.status = 'quarantined'
              AND admission.policy_revision = '{_LICENSE_POLICY_SQL}'
              AND admission.source_format IS DISTINCT FROM 'metadata'
              AND {_visible_source_predicate("admission.source_feed")}
              AND NOT EXISTS (
                SELECT 1 FROM {self._decisions} AS decision
                WHERE decision.doc_id = admission.doc_id
              )
            GROUP BY admission.source_feed
            ORDER BY admission.source_feed ASC
            """,
            [],
            relation=self._license_admissions,
        )
        accepted_by_source = {str(row["source"]): int(row["accepted"]) for row in accepted_sources}
        totals_by_source = {str(row["source"]): int(row["total"]) for row in decision_sources}
        for row in early_license_sources:
            source = str(row["source"])
            totals_by_source[source] = totals_by_source.get(source, 0) + int(row["total"])
        per_source = [
            {
                "source": source,
                "accepted": accepted_by_source.get(source, 0),
                "total": total,
            }
            for source, total in sorted(totals_by_source.items())
        ]
        rejection_counts = {str(row["reason"]): int(row["count"]) for row in reasons}
        for row in early_license_reasons:
            reason = str(row["reason"])
            rejection_counts[reason] = rejection_counts.get(reason, 0) + int(row["count"])
        early_total = sum(int(row["count"]) for row in early_license_reasons)
        return {
            "durable_decisions": int(decision_totals[0]["durable_decisions"]) + early_total,
            "training_export_documents": int(training_totals[0]["training_export_documents"]),
            "rejected_by_reason": rejection_counts,
            "per_source_acceptance": per_source,
        }

    def license_admissions(self, *, recent_limit: int = 20) -> dict[str, Any]:
        """Summarize and expose the immutable pre-fetch licence ledger."""
        totals = self._rows(
            f"""
            SELECT status, CAST(COUNT(*) AS BIGINT) AS count
            FROM {self._license_admissions}
            WHERE policy_revision = '{_LICENSE_POLICY_SQL}'
              AND source_format IS DISTINCT FROM 'metadata'
              AND {_visible_source_predicate()}
            GROUP BY status
            ORDER BY status
            """,
            [],
            relation=self._license_admissions,
        )
        by_license = self._rows(
            f"""
            SELECT license_id, status, CAST(COUNT(*) AS BIGINT) AS count
            FROM {self._license_admissions}
            WHERE policy_revision = '{_LICENSE_POLICY_SQL}'
              AND source_format IS DISTINCT FROM 'metadata'
              AND {_visible_source_predicate()}
            GROUP BY license_id, status
            ORDER BY count DESC, license_id
            """,
            [],
            relation=self._license_admissions,
        )
        recent = self._rows(
            f"""
            SELECT decision_id, doc_id, source_feed, source_url,
                   CAST(observed_at AS VARCHAR) AS observed_at,
                   status, license_id, license_source, reason,
                   content_fetch_started
            FROM {self._license_admissions}
            WHERE policy_revision = '{_LICENSE_POLICY_SQL}'
              AND source_format IS DISTINCT FROM 'metadata'
              AND {_visible_source_predicate()}
            ORDER BY observed_at DESC, decision_id
            LIMIT ?
            """,
            [max(1, min(recent_limit, 100))],
            relation=self._license_admissions,
        )
        counts = {str(row["status"]): int(row["count"]) for row in totals}
        return {
            "admitted": counts.get("admitted", 0),
            "posttrain_transform_only": counts.get("posttrain_transform_only", 0),
            "quarantined": counts.get("quarantined", 0),
            "by_license": by_license,
            "recent": recent,
        }

    def source_activity(self, *, window_hours: int = 24) -> dict[str, Any]:
        """Return durable per-source decisions and their licence evidence."""
        bounded_hours = max(1, min(window_hours, 24 * 7))
        cutoff = datetime.now(UTC) - timedelta(hours=bounded_hours)
        sources = self._rows(
            f"""
            SELECT
              CASE
                WHEN source_feed LIKE 'seed:wayback:%' THEN 'seed:wayback'
                ELSE source_feed
              END AS source_feed,
              CAST(COUNT(*) AS BIGINT) AS documents,
              CAST(COUNT(*) FILTER (WHERE status = 'admitted') AS BIGINT) AS admitted,
              CAST(COUNT(*) FILTER (
                WHERE status = 'posttrain_transform_only'
              ) AS BIGINT) AS posttrain_transform_only,
              CAST(COUNT(*) FILTER (WHERE status = 'quarantined') AS BIGINT) AS quarantined,
              CAST(MAX(observed_at) AS VARCHAR) AS last_observed_at
            FROM {self._license_admissions}
            WHERE observed_at >= CAST(? AS TIMESTAMP)
              AND policy_revision = '{_LICENSE_POLICY_SQL}'
              AND source_format IS DISTINCT FROM 'metadata'
              AND {_visible_source_predicate()}
            GROUP BY 1
            ORDER BY source_feed
            """,
            [cutoff.isoformat()],
            relation=self._license_admissions,
        )
        license_rows = self._rows(
            f"""
            SELECT
              CASE
                WHEN source_feed LIKE 'seed:wayback:%' THEN 'seed:wayback'
                ELSE source_feed
              END AS source_feed,
              license_id,
              status,
              CAST(COUNT(*) AS BIGINT) AS count
            FROM {self._license_admissions}
            WHERE observed_at >= CAST(? AS TIMESTAMP)
              AND policy_revision = '{_LICENSE_POLICY_SQL}'
              AND source_format IS DISTINCT FROM 'metadata'
              AND {_visible_source_predicate()}
            GROUP BY 1, license_id, status
            ORDER BY source_feed, count DESC, license_id, status
            """,
            [cutoff.isoformat()],
            relation=self._license_admissions,
        )
        provenance_rows = self._rows(
            f"""
            SELECT
              CASE
                WHEN source_feed LIKE 'seed:wayback:%' THEN 'seed:wayback'
                ELSE source_feed
              END AS source_feed,
              license_source,
              CAST(COUNT(*) AS BIGINT) AS count
            FROM {self._license_admissions}
            WHERE observed_at >= CAST(? AS TIMESTAMP)
              AND policy_revision = '{_LICENSE_POLICY_SQL}'
              AND source_format IS DISTINCT FROM 'metadata'
              AND {_visible_source_predicate()}
            GROUP BY 1, license_source
            ORDER BY source_feed, count DESC, license_source
            """,
            [cutoff.isoformat()],
            relation=self._license_admissions,
        )
        by_source: dict[str, dict[str, Any]] = {str(row["source_feed"]): row for row in sources}
        for row in sources:
            row["license_distribution"] = []
            row["license_provenance"] = []
        for row in license_rows:
            source = by_source.get(str(row["source_feed"]))
            if source is not None:
                source["license_distribution"].append(
                    {
                        "license_id": row["license_id"],
                        "status": row["status"],
                        "count": row["count"],
                    }
                )
        for row in provenance_rows:
            source = by_source.get(str(row["source_feed"]))
            if source is not None:
                source["license_provenance"].append(
                    {
                        "license_source": row["license_source"],
                        "count": row["count"],
                    }
                )
        return {"window_hours": bounded_hours, "sources": sources}

    def documents(
        self,
        *,
        page: int = 1,
        page_size: int = 25,
        search: str | None = None,
        routes: Sequence[str] = (),
        sources: Sequence[str] = (),
        source_formats: Sequence[str] = (),
        date_from: str | None = None,
        date_to: str | None = None,
        tags: Sequence[str] = (),
        rejection_reasons: Sequence[str] = (),
        has_figures: bool | None = None,
        has_tables: bool | None = None,
        has_equations: bool | None = None,
        include_fixtures: bool = False,
        min_edu: float | None = None,
        max_edu: float | None = None,
        min_quality: float | None = None,
        max_quality: float | None = None,
        sort: str = "newest",
    ) -> dict[str, Any]:
        """Return a paginated, server-filtered collection of durable decisions."""
        bounded_page = max(1, page)
        bounded_size = max(1, min(page_size, 100))
        where, params = self._document_where(
            search=search,
            routes=routes,
            sources=sources,
            source_formats=source_formats,
            date_from=date_from,
            date_to=date_to,
            tags=tags,
            rejection_reasons=rejection_reasons,
            has_figures=has_figures,
            has_tables=has_tables,
            has_equations=has_equations,
            include_fixtures=include_fixtures,
            min_edu=min_edu,
            max_edu=max_edu,
            min_quality=min_quality,
            max_quality=max_quality,
        )
        order_by = {
            "newest": "valid_from DESC, doc_id ASC",
            "oldest": "valid_from ASC, doc_id ASC",
            "quality_desc": "quality_score DESC, valid_from DESC",
            "edu_desc": "edu_score DESC, valid_from DESC",
            "perplexity_asc": "perplexity ASC, valid_from DESC",
        }.get(sort, "valid_from DESC, doc_id ASC")
        sql = f"""
        WITH document_rows AS (
          SELECT
            doc_id, text, source_feed, source_format, lang, valid_from,
            quality_score, edu_score, structural_quality_score, reasoning_score,
            benchmark_score, perplexity, risk_tier, route,
            COALESCE(training_usage, 'pretrain_and_posttrain') AS training_usage,
            content_tags, reject_reasons, source_word_count, training_word_count,
            included_section_count, excluded_section_count, figure_count,
            table_count, equation_count, citation_count,
            scientific_artifact_s3_uri,
            FALSE AS admission_only
          FROM {self._decisions}
          WHERE {_current_decision_predicate()}
          UNION ALL
          SELECT
            admission.doc_id,
            CAST(admission.source_url AS VARCHAR) AS text,
            admission.source_feed,
            COALESCE(admission.source_format, 'unfetched') AS source_format,
            'not_applicable' AS lang,
            admission.observed_at AS valid_from,
            0.0 AS quality_score,
            0.0 AS edu_score,
            0.0 AS structural_quality_score,
            0.0 AS reasoning_score,
            0.0 AS benchmark_score,
            0.0 AS perplexity,
            3 AS risk_tier,
            'quarantine' AS route,
            'quarantined' AS training_usage,
            CAST([] AS VARCHAR[]) AS content_tags,
            CASE WHEN admission.license_id = 'unknown'
                 THEN CAST(['license_missing'] AS VARCHAR[])
                 ELSE CAST(['license_not_permitted'] AS VARCHAR[])
            END AS reject_reasons,
            0 AS source_word_count,
            0 AS training_word_count,
            0 AS included_section_count,
            0 AS excluded_section_count,
            0 AS figure_count,
            0 AS table_count,
            0 AS equation_count,
            0 AS citation_count,
            CAST(NULL AS VARCHAR) AS scientific_artifact_s3_uri,
            TRUE AS admission_only
          FROM {self._license_admissions} AS admission
          WHERE admission.status = 'quarantined'
            AND admission.policy_revision = '{_LICENSE_POLICY_SQL}'
            AND admission.source_format IS DISTINCT FROM 'metadata'
            AND {_visible_source_predicate("admission.source_feed")}
            AND NOT EXISTS (
              SELECT 1 FROM {self._decisions} AS decision
              WHERE decision.doc_id = admission.doc_id
            )
        )
        SELECT
          doc_id,
          TRIM(LEADING '# ' FROM SPLIT_PART(text, '\n', 1)) AS title,
          source_feed,
          source_format,
          lang,
          CAST(valid_from AS VARCHAR) AS valid_from,
          quality_score,
          edu_score,
          structural_quality_score,
          reasoning_score,
          benchmark_score,
          perplexity,
          risk_tier,
          route,
          COALESCE(training_usage, 'pretrain_and_posttrain') AS training_usage,
          content_tags,
          reject_reasons,
          source_word_count,
          training_word_count,
          included_section_count,
          excluded_section_count,
          figure_count,
          table_count,
          equation_count,
          citation_count,
          scientific_artifact_s3_uri,
          admission_only,
          SUBSTR(text, 1, 320) AS text_preview,
          CAST(COUNT(*) OVER () AS BIGINT) AS _total
        FROM document_rows
        {where}
        ORDER BY {order_by}
        LIMIT ? OFFSET ?
        """
        if self._refresh_iceberg:
            self._prepare_relation(self._decisions)
            self._prepare_relation(self._license_admissions)
        rows = self._rows(
            sql,
            [*params, bounded_size, (bounded_page - 1) * bounded_size],
            relation=None,
        )
        total = int(rows[0].pop("_total")) if rows else 0
        return {
            "items": rows,
            "total": total,
            "page": bounded_page,
            "page_size": bounded_size,
            "pages": (total + bounded_size - 1) // bounded_size,
        }

    def document_facets(self, *, include_fixtures: bool = False) -> dict[str, list[str]]:
        """Return collection values used by compact filter controls."""
        if self._refresh_iceberg:
            self._prepare_relation(self._decisions)
            self._prepare_relation(self._license_admissions)
        fixture_clause = f"WHERE {_visible_source_predicate(include_fixtures=include_fixtures)}"
        current_decisions = _current_decision_predicate(include_fixtures=include_fixtures)
        admission_rows = f"""
          SELECT admission.source_feed,
                 COALESCE(admission.source_format, 'unfetched') AS source_format,
                 CASE WHEN admission.license_id = 'unknown'
                      THEN 'license_missing' ELSE 'license_not_permitted' END AS reason
          FROM {self._license_admissions} AS admission
          WHERE admission.status = 'quarantined'
            AND admission.policy_revision = '{_LICENSE_POLICY_SQL}'
            AND admission.source_format IS DISTINCT FROM 'metadata'
            AND {_visible_source_predicate("admission.source_feed")}
            AND NOT EXISTS (
              SELECT 1 FROM {self._decisions} AS decision
              WHERE decision.doc_id = admission.doc_id
            )
        """
        sources = self._rows(
            f"""
            SELECT DISTINCT source_feed AS value
            FROM (
              SELECT source_feed FROM {self._decisions} WHERE {current_decisions}
              UNION ALL
              SELECT source_feed FROM ({admission_rows}) AS early
            ) AS source_rows
            {fixture_clause}
            ORDER BY value
            """,
            [],
            relation=None,
        )
        formats = self._rows(
            f"""
            SELECT DISTINCT source_format AS value
            FROM (
              SELECT source_feed, source_format FROM {self._decisions}
              WHERE {current_decisions}
              UNION ALL
              SELECT source_feed, source_format FROM ({admission_rows}) AS early
            ) AS format_rows
            {fixture_clause}
            ORDER BY value
            """,
            [],
            relation=None,
        )
        conjunction = "WHERE" if not fixture_clause else "AND"
        tags = self._rows(
            f"SELECT DISTINCT tag AS value FROM {self._decisions}, "
            f"UNNEST(content_tags) AS values(tag) WHERE {current_decisions} "
            f"AND tag IS NOT NULL ORDER BY value",
            [],
            relation=self._decisions,
        )
        reasons = self._rows(
            f"""
            SELECT DISTINCT reason AS value
            FROM (
              SELECT source_feed, reason
              FROM {self._decisions}, UNNEST(reject_reasons) AS values(reason)
              WHERE {current_decisions}
              UNION ALL
              SELECT source_feed, reason FROM ({admission_rows}) AS early
            ) AS reason_rows
            {fixture_clause}
            {conjunction} reason IS NOT NULL
            ORDER BY value
            """,
            [],
            relation=None,
        )
        return {
            "sources": [str(row["value"]) for row in sources],
            "source_formats": [str(row["value"]) for row in formats],
            "content_tags": [str(row["value"]) for row in tags],
            "rejection_reasons": [str(row["value"]) for row in reasons],
        }

    def dataset_rows(
        self,
        *,
        date_from: str,
        date_to: str,
        routes: Sequence[str],
        sources: Sequence[str] = (),
        source_formats: Sequence[str] = (),
        tags: Sequence[str] = (),
        min_edu: float | None = None,
        min_quality: float | None = None,
        include_structured: bool = True,
        limit: int = 5_000,
    ) -> list[dict[str, Any]]:
        """Return a bounded, reproducible JSONL-ready training export."""
        where, params = self._document_where(
            routes=routes,
            sources=sources,
            source_formats=source_formats,
            date_from=date_from,
            date_to=date_to,
            tags=tags,
            min_edu=min_edu,
            min_quality=min_quality,
            include_fixtures=False,
        )
        sql = f"""
        SELECT
          doc_id, text, source_feed, source_format, CAST(valid_from AS VARCHAR) AS valid_from,
          route, content_tags, quality_score, edu_score, structural_quality_score,
          reasoning_score, benchmark_score, tokens, policy_revision, scoring_version,
          classifier_revision, projection_version, scientific_artifact_s3_uri
          , spdx_license, spdx_license_source
        FROM {self._decisions}
        {where}
          AND COALESCE(spdx_license, license) IN ({_TRAINING_LICENSE_SQL})
          AND risk_tier = 1
          AND ARRAY_LENGTH(reject_reasons) = 0
        ORDER BY valid_from ASC, doc_id ASC
        LIMIT ?
        """
        rows = self._rows(sql, [*params, max(1, min(limit, 5_000))], relation=self._decisions)
        if not include_structured:
            for row in rows:
                row["text"] = _without_structured_surrogates(str(row["text"]))
        return rows

    def dataset_summary(
        self,
        *,
        date_from: str,
        date_to: str,
        routes: Sequence[str],
        sources: Sequence[str] = (),
        source_formats: Sequence[str] = (),
        tags: Sequence[str] = (),
        min_edu: float | None = None,
        min_quality: float | None = None,
        include_structured: bool = True,
    ) -> dict[str, Any]:
        """Return reproducible selection metadata before an export is created."""
        where, params = self._document_where(
            routes=routes,
            sources=sources,
            source_formats=source_formats,
            date_from=date_from,
            date_to=date_to,
            tags=tags,
            min_edu=min_edu,
            min_quality=min_quality,
            include_fixtures=False,
        )
        rows = self._rows(
            f"""
            SELECT
              CAST(COUNT(*) AS BIGINT) AS documents,
              CAST(COALESCE(SUM(tokens), 0) AS BIGINT) AS tokens,
              CAST(COALESCE(SUM(source_word_count), 0) AS BIGINT) AS source_words,
              CAST(COALESCE(SUM(training_word_count), 0) AS BIGINT) AS projection_words,
              CAST(COUNT(DISTINCT source_feed) AS BIGINT) AS source_count
            FROM {self._decisions}
            {where}
              AND COALESCE(spdx_license, license) IN ({_TRAINING_LICENSE_SQL})
              AND risk_tier = 1
              AND ARRAY_LENGTH(reject_reasons) = 0
            """,
            params,
            relation=self._decisions,
        )[0]
        revisions = self._rows(
            f"""
            SELECT DISTINCT
              policy_revision, scoring_version, classifier_revision, classifier_backend,
              projection_version, extraction_pipeline, benchmark_set_version,
              decon_embedding_revision, pii_scanner_revision, lang_detector_revision,
              tokenizer_revision, perplexity_scorer, minhash_backend, lsh_backend
            FROM {self._decisions}
            {where}
              AND COALESCE(spdx_license, license) IN ({_TRAINING_LICENSE_SQL})
              AND risk_tier = 1
              AND ARRAY_LENGTH(reject_reasons) = 0
            ORDER BY policy_revision, classifier_revision
            """,
            params,
            relation=self._decisions,
        )
        revision_keys = (
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
        )
        decisions_table = os.environ.get("S2P_ICEBERG_DECISIONS_TABLE", "curation_decisions")
        return {
            **rows,
            "selection": {
                "date_from": date_from,
                "date_to": date_to,
                "routes": list(routes),
                "sources": list(sources),
                "source_formats": list(source_formats),
                "content_tags": list(tags),
                "min_edu": min_edu,
                "min_quality": min_quality,
                "include_structured": include_structured,
                "license_policy": "strict_allowlist",
                "allowed_licenses": sorted(PERMISSIVE_TRAINING_LICENSES),
                "fixtures_included": False,
            },
            "manifest": {
                "revisions": {
                    key: sorted(
                        {str(row[key]) for row in revisions if row.get(key) not in {None, ""}}
                    )
                    for key in revision_keys
                },
                "decision_table": _table_snapshot_manifest(decisions_table),
                "export_limit": 5_000,
            },
        }

    def _document_where(
        self,
        *,
        search: str | None = None,
        routes: Sequence[str] = (),
        sources: Sequence[str] = (),
        source_formats: Sequence[str] = (),
        date_from: str | None = None,
        date_to: str | None = None,
        tags: Sequence[str] = (),
        rejection_reasons: Sequence[str] = (),
        has_figures: bool | None = None,
        has_tables: bool | None = None,
        has_equations: bool | None = None,
        include_fixtures: bool = False,
        min_edu: float | None = None,
        max_edu: float | None = None,
        min_quality: float | None = None,
        max_quality: float | None = None,
    ) -> tuple[str, list[Any]]:
        clauses: list[str] = [_current_decision_predicate(include_fixtures=include_fixtures)]
        params: list[Any] = []
        if search and search.strip():
            clauses.append(
                "(LOWER(text) LIKE ? OR LOWER(doc_id) LIKE ? OR LOWER(source_feed) LIKE ?)"
            )
            needle = f"%{search.strip().lower()}%"
            params.extend([needle, needle, needle])
        if routes:
            clauses.append("(" + " OR ".join("route = ?" for _ in routes) + ")")
            params.extend(routes)
        for column, values in (
            ("source_feed", sources),
            ("source_format", source_formats),
        ):
            if values:
                clauses.append("(" + " OR ".join(f"{column} = ?" for _ in values) + ")")
                params.extend(values)
        for column, values in (("content_tags", tags), ("reject_reasons", rejection_reasons)):
            for value in values:
                clauses.append(f"LIST_CONTAINS({column}, ?)")
                params.append(value)
        if date_from:
            clauses.append("valid_from >= CAST(? AS TIMESTAMP)")
            params.append(date_from)
        if date_to:
            clauses.append("valid_from <= CAST(? AS TIMESTAMP)")
            params.append(date_to)
        for presence_column, present in (
            ("figure_count", has_figures),
            ("table_count", has_tables),
            ("equation_count", has_equations),
        ):
            if present is not None:
                clauses.append(f"{presence_column} {'>' if present else '='} 0")
        for score_column, threshold_operator, threshold in (
            ("edu_score", ">=", min_edu),
            ("edu_score", "<=", max_edu),
            ("quality_score", ">=", min_quality),
            ("quality_score", "<=", max_quality),
        ):
            if threshold is not None:
                clauses.append(f"{score_column} {threshold_operator} ?")
                params.append(threshold)
        return ("WHERE " + " AND ".join(clauses) if clauses else "", params)

    def document(self, doc_id: str) -> dict[str, Any] | None:
        """Return one full decision with its structured scientific artifact."""
        sql = f"""
        SELECT
          doc_id, TRIM(LEADING '# ' FROM SPLIT_PART(text, '\n', 1)) AS title,
          text, source_feed, source_format, lang, CAST(valid_from AS VARCHAR) AS valid_from,
          quality_score, edu_score, risk_tier, reject_reasons, pii_flags,
          contaminated_with, extraction_pipeline, classifier_revision, classifier_backend,
          scoring_version, policy_revision, license, license_source,
          spdx_license, spdx_license_source,
          COALESCE(training_usage, 'pretrain_and_posttrain') AS training_usage,
          scientific_artifact_s3_uri, figure_count, table_count, equation_count,
          citation_count, extraction_warnings, lang_score, gopher_pass,
          c4_nopunc_pass, c4_curly_brace_pass, c4_lorem_ipsum_pass,
          c4_fraction_lines_with_punct, perplexity, perplexity_bucket,
          perplexity_scorer, near_duplicate, near_dup_cluster_id,
          minhash_backend, minhash_num_perms, lsh_backend,
          structural_quality_score, extraction_completeness, reasoning_score,
          benchmark_score, route, eligible_routes, route_reasons, content_tags,
          segment_scores_json, projection_version, source_word_count,
          training_word_count, included_section_count, excluded_section_count,
          excluded_sections, metadata_pii_flags, removed_body_pii_flags,
          pii_action, pii_scanner_revision, lang_detector_revision,
          tokenizer_revision, gopher_word_count, gopher_mean_word_len,
          gopher_stopword_ratio, gopher_bullet_line_ratio,
          gopher_ellipsis_line_ratio, gopher_symbol_word_ratio,
          gopher_alpha_word_ratio, decon_exact_matches,
          decon_semantic_matches, decon_max_similarity, decon_ngram_size,
          decon_embedding_revision, benchmark_set_version
        FROM {self._decisions}
        WHERE doc_id = ? AND {_current_decision_predicate()}
        ORDER BY valid_from DESC
        LIMIT 1
        """
        rows = self._rows(sql, [doc_id], relation=self._decisions)
        if not rows:
            admission_only = self._rows(
                f"""
                SELECT
                  doc_id,
                  CAST(source_url AS VARCHAR) AS title,
                  CAST(source_url AS VARCHAR) AS source_url,
                  source_feed,
                  COALESCE(source_format, 'unfetched') AS source_format,
                  CAST(observed_at AS VARCHAR) AS valid_from,
                  license_id,
                  license_source,
                  reason,
                  raw_license,
                  normalized_license,
                  resolver,
                  CAST(evidence_url AS VARCHAR) AS evidence_url,
                  evidence_revision,
                  evidence_scope,
                  policy_revision,
                  CAST(resolved_at AS VARCHAR) AS resolved_at
                FROM {self._license_admissions}
                WHERE doc_id = ? AND status = 'quarantined'
                ORDER BY observed_at DESC, decision_id DESC
                LIMIT 1
                """,
                [doc_id],
                relation=self._license_admissions,
            )
            if not admission_only:
                return None
            admission = admission_only[0]
            reject_reason = (
                "license_missing"
                if str(admission.get("license_id")) == "unknown"
                else "license_not_permitted"
            )
            return {
                "admission_only": True,
                "doc_id": admission["doc_id"],
                "title": admission["title"],
                "source_url": admission["source_url"],
                "source_feed": admission["source_feed"],
                "source_format": admission["source_format"],
                "valid_from": admission["valid_from"],
                "route": "quarantine",
                "training_usage": "quarantined",
                "content_tags": [],
                "reject_reasons": [reject_reason],
                "license_admission": {
                    "status": "quarantined",
                    "license_id": admission["license_id"],
                    "license_source": admission["license_source"],
                    "reason": admission["reason"],
                    "raw_license": admission["raw_license"],
                    "normalized_license": admission["normalized_license"],
                    "resolver": admission["resolver"],
                    "evidence_url": admission["evidence_url"],
                    "evidence_revision": admission["evidence_revision"],
                    "evidence_scope": admission["evidence_scope"],
                    "policy_revision": admission["policy_revision"],
                    "resolved_at": admission["resolved_at"],
                },
            }
        row = rows[0]
        row["admission_only"] = False
        admission_rows = self._rows(
            f"""
            SELECT status, license_id, license_source, reason, raw_license,
                   normalized_license, resolver, evidence_url, evidence_revision,
                   evidence_scope, policy_revision,
                   CAST(resolved_at AS VARCHAR) AS resolved_at
            FROM {self._license_admissions}
            WHERE doc_id = ?
            ORDER BY observed_at DESC, decision_id DESC
            LIMIT 1
            """,
            [doc_id],
            relation=self._license_admissions,
        )
        row["license_admission"] = admission_rows[0] if admission_rows else None
        try:
            import orjson

            parsed_scores = orjson.loads(str(row.pop("segment_scores_json", "[]")))
            row["segment_scores"] = parsed_scores if isinstance(parsed_scores, list) else []
        except Exception:
            row["segment_scores"] = []
        uri = row.get("scientific_artifact_s3_uri")
        row["scientific_artifact"] = (
            self._artifact_store.read_json(str(uri))
            if uri and self._artifact_store is not None
            else None
        )
        return row

    def figure(self, doc_id: str, figure_id: str) -> tuple[bytes, str] | None:
        """Load one figure through its structured artifact."""
        document = self.document(doc_id)
        if not document or self._artifact_store is None:
            return None
        artifact = document.get("scientific_artifact")
        if not isinstance(artifact, dict):
            return None
        for figure in artifact.get("figures", []):
            if not isinstance(figure, dict) or figure.get("figure_id") != figure_id:
                continue
            uri = figure.get("asset_s3_uri")
            if not uri:
                return None
            return self._artifact_store.read_bytes(str(uri))
        return None

    def safe_query(self, sql: str, params: Sequence[Any]) -> dict[str, Any]:
        stripped = sql.strip().rstrip(";")
        if not stripped.lower().startswith("select"):
            raise ValueError("only SELECT statements are allowed")
        if ";" in stripped:
            raise ValueError("multiple SQL statements are not allowed")
        started = time.perf_counter()
        if self._refresh_iceberg:
            self._prepare_relation(self._gold)
            self._prepare_relation(self._decisions)
        rows = self._rows(stripped, params, relation=None)
        return {"rows": rows, "durationMs": (time.perf_counter() - started) * 1000.0}

    def _rows(
        self,
        sql: str,
        params: Sequence[Any],
        *,
        relation: str | None = None,
    ) -> list[dict[str, Any]]:
        if self._refresh_iceberg and relation is not None:
            self._prepare_relation(relation)
        result = self._conn.execute(sql, params)
        names = [str(col[0]) for col in (result.description or [])]
        return [dict(zip(names, row, strict=True)) for row in result.fetchall()]

    def _prepare_relation(self, relation: str) -> None:
        """Refresh an Iceberg view at most once per configured cache window.

        A typed endpoint often runs several aggregate queries over the same
        relation. Reloading Polaris metadata and recreating the DuckDB view for
        every aggregate serialised all UI requests behind redundant catalog
        calls. The API still observes new snapshots within the short bounded
        refresh interval.
        """
        now = time.monotonic()
        refreshed_at = self._relation_refreshed_at.get(relation)
        if refreshed_at is not None and now - refreshed_at < self._catalog_refresh_seconds:
            return
        if relation == self._license_admissions:
            _register_license_relation(self._conn, relation)
        else:
            table_name = (
                os.environ.get("S2P_ICEBERG_GOLD_TABLE", "curated")
                if relation == self._gold
                else os.environ.get("S2P_ICEBERG_DECISIONS_TABLE", "curation_decisions")
            )
            _register_iceberg_relation(self._conn, relation, table_name)
        self._relation_refreshed_at[relation] = time.monotonic()


def _load_extensions(conn: DuckDBConnection) -> None:
    for extension in ("httpfs", "iceberg"):
        try:
            conn.execute(f"INSTALL {extension}")
            conn.execute(f"LOAD {extension}")
        except Exception:
            # Some images bake extensions in or run without network. Query
            # failures are surfaced by the routes with a 503.
            pass


def _configure_s3(conn: DuckDBConnection) -> None:
    """Configure DuckDB httpfs for the in-cluster MinIO endpoint."""
    endpoint = os.environ.get("MINIO_ENDPOINT")
    access_key = os.environ.get("MINIO_ACCESS_KEY") or os.environ.get("AWS_ACCESS_KEY_ID")
    secret_key = os.environ.get("MINIO_SECRET_KEY") or os.environ.get("AWS_SECRET_ACCESS_KEY")
    if not endpoint or not access_key or not secret_key:
        return
    parsed = urlparse(endpoint)
    host = parsed.netloc or parsed.path
    use_ssl = "true" if parsed.scheme == "https" else "false"
    settings = {
        "s3_endpoint": host,
        "s3_access_key_id": access_key,
        "s3_secret_access_key": secret_key,
        "s3_region": os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
        "s3_url_style": "path",
        "s3_use_ssl": use_ssl,
    }
    for key, value in settings.items():
        conn.execute(f"SET {key}={_sql_string(value)}")


def _configure_runtime_limits(conn: DuckDBConnection) -> None:
    """Keep analytical scans inside the container and spill to bounded disk."""
    settings = {
        "memory_limit": os.environ.get("S2P_DUCKDB_MEMORY_LIMIT", "512MB"),
        "threads": os.environ.get("S2P_DUCKDB_THREADS", "1"),
        "temp_directory": os.environ.get("S2P_DUCKDB_TEMP_DIRECTORY", "/tmp/duckdb-spill"),
        "max_temp_directory_size": os.environ.get("S2P_DUCKDB_MAX_TEMP_DIRECTORY_SIZE", "3GB"),
    }
    os.makedirs(settings["temp_directory"], exist_ok=True)
    for key, value in settings.items():
        conn.execute(f"SET {key}={_sql_string(value)}")


def _register_gold_relation(conn: DuckDBConnection, relation: str) -> None:
    """Expose the Polaris Iceberg Gold table as the local DuckDB relation."""
    _register_iceberg_relation(conn, relation, os.environ.get("S2P_ICEBERG_GOLD_TABLE", "curated"))


def _register_iceberg_relation(conn: DuckDBConnection, relation: str, table_name: str) -> None:
    """Expose one configured Iceberg table as a DuckDB view."""
    if not _RELATION_RE.fullmatch(relation):
        raise ValueError("gold_relation must be a simple DuckDB relation name")
    reference = _load_table_reference(table_name)
    if reference is None:
        _create_empty_gold_relation(conn, relation)
        return
    location, version = reference
    version_arg = f", version = {_sql_string(version)}" if version else ""
    scan = f"iceberg_scan({_sql_string(location)}{version_arg}, allow_moved_paths = true)"
    conn.execute(
        f"CREATE OR REPLACE VIEW {relation} AS "
        f"SELECT * FROM {scan} "
        "QUALIFY ROW_NUMBER() OVER ("
        "PARTITION BY doc_id, scoring_version, classifier_revision, policy_revision "
        "ORDER BY trace_id ASC"
        ") = 1"
    )


def _register_license_relation(conn: DuckDBConnection, relation: str) -> None:
    """Expose the pre-fetch admission ledger, de-duplicated by decision id."""
    table_name = os.environ.get("S2P_ICEBERG_LICENSE_ADMISSIONS_TABLE", "license_admissions")
    reference = _load_table_reference(table_name)
    if reference is None:
        _create_empty_license_relation(conn, relation)
        return
    location, version = reference
    version_arg = f", version = {_sql_string(version)}" if version else ""
    scan = f"iceberg_scan({_sql_string(location)}{version_arg}, allow_moved_paths = true)"
    conn.execute(
        f"CREATE OR REPLACE VIEW {relation} AS "
        f"SELECT * FROM {scan} "
        "QUALIFY ROW_NUMBER() OVER (PARTITION BY decision_id ORDER BY observed_at ASC) = 1"
    )


def _create_empty_license_relation(conn: DuckDBConnection, relation: str) -> None:
    conn.execute(
        f"""
        CREATE OR REPLACE VIEW {relation} AS
        SELECT
          CAST(NULL AS VARCHAR) AS decision_id,
          CAST(NULL AS VARCHAR) AS doc_id,
          CAST(NULL AS VARCHAR) AS source_feed,
          CAST(NULL AS VARCHAR) AS source_url,
          CAST(NULL AS VARCHAR) AS source_format,
          CAST(NULL AS TIMESTAMP) AS observed_at,
          CAST(NULL AS VARCHAR) AS status,
          CAST(NULL AS VARCHAR) AS license_id,
          CAST(NULL AS VARCHAR) AS license_source,
          CAST(NULL AS VARCHAR) AS raw_license,
          CAST(NULL AS VARCHAR) AS normalized_license,
          CAST(NULL AS VARCHAR) AS resolver,
          CAST(NULL AS VARCHAR) AS evidence_url,
          CAST(NULL AS VARCHAR) AS evidence_revision,
          CAST(NULL AS VARCHAR) AS evidence_scope,
          CAST(NULL AS VARCHAR) AS policy_revision,
          CAST(NULL AS TIMESTAMP) AS resolved_at,
          CAST(NULL AS VARCHAR) AS reason,
          CAST(NULL AS VARCHAR) AS trace_id,
          CAST(FALSE AS BOOLEAN) AS content_fetch_started
        WHERE FALSE
        """
    )


def _load_gold_table_location() -> str | None:
    """Resolve the Gold location through the configured runtime catalog."""
    return _load_table_location(os.environ.get("S2P_ICEBERG_GOLD_TABLE", "curated"))


def _load_table_location(table_name: str) -> str | None:
    """Resolve one table location through the configured runtime catalog."""
    reference = _load_table_reference(table_name)
    return reference[0] if reference is not None else None


def _load_table_reference(table_name: str) -> tuple[str, str | None] | None:
    """Resolve the table root and exact metadata version from the catalog."""
    from processor.iceberg_catalog import load_runtime_catalog

    catalog_type = os.environ.get("S2P_ICEBERG_CATALOG_TYPE", "rest").strip().lower()
    if catalog_type != "sql" and not (
        os.environ.get("POLARIS_URI") and os.environ.get("POLARIS_WAREHOUSE")
    ):
        return None
    namespace = os.environ.get("S2P_ICEBERG_NAMESPACE") or os.environ.get(
        "ICEBERG_NAMESPACE", "gold"
    )
    try:
        table = load_runtime_catalog().load_table((namespace, table_name))
    except Exception as exc:
        if _is_missing_iceberg_table(exc):
            return None
        raise
    location = getattr(table, "location", None)
    root = str(location()) if callable(location) else str(location)
    metadata_location = str(getattr(table, "metadata_location", "") or "")
    metadata_name = metadata_location.rsplit("/", 1)[-1]
    suffix = ".metadata.json"
    version = metadata_name[: -len(suffix)] if metadata_name.endswith(suffix) else None
    return root, version


def _table_snapshot_manifest(table_name: str) -> dict[str, Any]:
    """Return the current Iceberg identity without pretending it is immutable."""
    from processor.iceberg_catalog import load_runtime_catalog

    namespace = os.environ.get("S2P_ICEBERG_NAMESPACE") or os.environ.get(
        "ICEBERG_NAMESPACE", "gold"
    )
    result: dict[str, Any] = {
        "namespace": namespace,
        "table": table_name,
        "snapshot_id": None,
        "metadata_location": None,
    }
    catalog_type = os.environ.get("S2P_ICEBERG_CATALOG_TYPE", "rest").strip().lower()
    if catalog_type != "sql" and not (
        os.environ.get("POLARIS_URI") and os.environ.get("POLARIS_WAREHOUSE")
    ):
        return result
    try:
        table = load_runtime_catalog().load_table((namespace, table_name))
    except Exception as exc:
        if _is_missing_iceberg_table(exc):
            return result
        raise
    snapshot = table.current_snapshot()
    # Iceberg snapshot IDs are 64-bit integers and commonly exceed JavaScript's
    # exact integer range. Preserve the identifier byte-for-byte over JSON.
    result["snapshot_id"] = str(snapshot.snapshot_id) if snapshot is not None else None
    result["metadata_location"] = getattr(table, "metadata_location", None) or getattr(
        table.metadata, "metadata_location", None
    )
    return result


def _is_missing_iceberg_table(exc: Exception) -> bool:
    name = exc.__class__.__name__.lower()
    message = str(exc).lower()
    return "nosuch" in name or "not found" in message or "does not exist" in message


def _create_empty_gold_relation(conn: DuckDBConnection, relation: str) -> None:
    """Create a zero-row Gold-shaped view until the first Iceberg commit exists."""
    conn.execute(
        f"""
        CREATE OR REPLACE VIEW {relation} AS
        SELECT
          CAST(NULL AS VARCHAR) AS doc_id,
          CAST(NULL AS VARCHAR) AS text,
          CAST(NULL AS VARCHAR) AS lang,
          CAST(NULL AS INTEGER) AS tokens,
          CAST(NULL AS DOUBLE) AS quality_score,
          CAST(NULL AS DOUBLE) AS edu_score,
          CAST(NULL AS VARCHAR) AS license,
          CAST(NULL AS VARCHAR) AS license_source,
          CAST(NULL AS INTEGER) AS risk_tier,
          CAST([] AS VARCHAR[]) AS pii_flags,
          CAST([] AS VARCHAR[]) AS contaminated_with,
          CAST(NULL AS TIMESTAMP) AS valid_from,
          CAST(NULL AS TIMESTAMP) AS valid_to,
          CAST([] AS VARCHAR[]) AS reject_reasons,
          CAST(NULL AS VARCHAR) AS scoring_version,
          CAST(NULL AS VARCHAR) AS classifier_revision,
          CAST(NULL AS VARCHAR) AS policy_revision,
          CAST(NULL AS BIGINT) AS snapshot_id,
          CAST(NULL AS VARCHAR) AS trace_id,
          CAST(NULL AS VARCHAR) AS source_feed,
          CAST(NULL AS VARCHAR) AS source_format,
          CAST(NULL AS VARCHAR) AS extraction_pipeline,
          CAST(NULL AS VARCHAR) AS spdx_license,
          CAST(NULL AS VARCHAR) AS spdx_license_source,
          CAST(NULL AS VARCHAR) AS scientific_artifact_s3_uri,
          CAST(0 AS INTEGER) AS figure_count,
          CAST(0 AS INTEGER) AS table_count,
          CAST(0 AS INTEGER) AS equation_count,
          CAST(0 AS INTEGER) AS citation_count,
          CAST([] AS VARCHAR[]) AS extraction_warnings
          , CAST(0 AS DOUBLE) AS lang_score
          , CAST(TRUE AS BOOLEAN) AS gopher_pass
          , CAST(TRUE AS BOOLEAN) AS c4_nopunc_pass
          , CAST(TRUE AS BOOLEAN) AS c4_curly_brace_pass
          , CAST(TRUE AS BOOLEAN) AS c4_lorem_ipsum_pass
          , CAST(1 AS DOUBLE) AS c4_fraction_lines_with_punct
          , CAST(0 AS DOUBLE) AS perplexity
          , CAST('head' AS VARCHAR) AS perplexity_bucket
          , CAST('unknown' AS VARCHAR) AS perplexity_scorer
          , CAST(FALSE AS BOOLEAN) AS near_duplicate
          , CAST(NULL AS VARCHAR) AS near_dup_cluster_id
          , CAST('unknown' AS VARCHAR) AS minhash_backend
          , CAST('unknown' AS VARCHAR) AS lsh_backend
          , CAST(0 AS INTEGER) AS minhash_num_perms
          , CAST(0 AS DOUBLE) AS structural_quality_score
          , CAST(0 AS DOUBLE) AS extraction_completeness
          , CAST(0 AS DOUBLE) AS reasoning_score
          , CAST(0 AS DOUBLE) AS benchmark_score
          , CAST('quarantine' AS VARCHAR) AS route
          , CAST([] AS VARCHAR[]) AS eligible_routes
          , CAST([] AS VARCHAR[]) AS route_reasons
          , CAST([] AS VARCHAR[]) AS content_tags
          , CAST('[]' AS VARCHAR) AS segment_scores_json
          , CAST('document-v1' AS VARCHAR) AS projection_version
          , CAST(0 AS INTEGER) AS source_word_count
          , CAST(0 AS INTEGER) AS training_word_count
          , CAST(0 AS INTEGER) AS included_section_count
          , CAST(0 AS INTEGER) AS excluded_section_count
          , CAST([] AS VARCHAR[]) AS excluded_sections
          , CAST([] AS VARCHAR[]) AS metadata_pii_flags
          , CAST([] AS VARCHAR[]) AS removed_body_pii_flags
          , CAST('none' AS VARCHAR) AS pii_action
          , CAST('unknown' AS VARCHAR) AS pii_scanner_revision
          , CAST('unknown' AS VARCHAR) AS lang_detector_revision
          , CAST('unknown' AS VARCHAR) AS tokenizer_revision
          , CAST(0 AS INTEGER) AS gopher_word_count
          , CAST(0 AS DOUBLE) AS gopher_mean_word_len
          , CAST(0 AS DOUBLE) AS gopher_stopword_ratio
          , CAST(0 AS DOUBLE) AS gopher_bullet_line_ratio
          , CAST(0 AS DOUBLE) AS gopher_ellipsis_line_ratio
          , CAST(0 AS DOUBLE) AS gopher_symbol_word_ratio
          , CAST(0 AS DOUBLE) AS gopher_alpha_word_ratio
          , CAST([] AS VARCHAR[]) AS decon_exact_matches
          , CAST([] AS VARCHAR[]) AS decon_semantic_matches
          , CAST(0 AS DOUBLE) AS decon_max_similarity
          , CAST(13 AS INTEGER) AS decon_ngram_size
          , CAST('unknown' AS VARCHAR) AS decon_embedding_revision
          , CAST('unknown' AS VARCHAR) AS benchmark_set_version
          , CAST('unknown' AS VARCHAR) AS classifier_backend
          , CAST('pretrain_and_posttrain' AS VARCHAR) AS training_usage
        WHERE FALSE
        """
    )


class ScientificArtifactStore:
    """Restricted MinIO reader for scientific JSON and image artifacts."""

    def __init__(self, *, s3_client: Any, allowed_bucket: str) -> None:
        self._s3 = s3_client
        self._allowed_bucket = allowed_bucket

    @classmethod
    def from_env(cls) -> ScientificArtifactStore:
        import boto3

        return cls(
            s3_client=boto3.client(
                "s3",
                endpoint_url=os.environ.get("MINIO_ENDPOINT", "http://minio:9000"),
                aws_access_key_id=os.environ.get("MINIO_ACCESS_KEY", "minioadmin"),
                aws_secret_access_key=os.environ.get("MINIO_SECRET_KEY", "minioadmin"),
                region_name="us-east-1",
            ),
            allowed_bucket=os.environ.get("MINIO_SILVER_BUCKET", "silver"),
        )

    def read_json(self, uri: str) -> dict[str, Any] | None:
        try:
            payload, _ = self.read_bytes(uri)
            import orjson

            value = orjson.loads(payload)
            return value if isinstance(value, dict) else None
        except Exception:
            return None

    def read_bytes(self, uri: str) -> tuple[bytes, str]:
        parsed = urlparse(uri)
        if parsed.scheme != "s3" or parsed.netloc != self._allowed_bucket:
            raise ValueError("artifact URI is outside the configured Silver bucket")
        key = parsed.path.lstrip("/")
        if not key.startswith("scientific/"):
            raise ValueError("artifact URI is outside the scientific prefix")
        response = self._s3.get_object(  # type: ignore[union-attr]
            Bucket=self._allowed_bucket, Key=key
        )
        return response["Body"].read(), str(
            response.get("ContentType") or "application/octet-stream"
        )


def _sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


async def serve(service: DuckDBQueryService, *, port: int = 8090) -> None:
    import asyncio

    from aiohttp import web  # type: ignore[import-untyped]

    query_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="duckdb-query")

    async def run_query(function: Any, /, *args: Any, **kwargs: Any) -> Any:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(query_executor, partial(function, *args, **kwargs))

    async def stop_query_executor(_: web.Application) -> None:
        query_executor.shutdown(wait=False, cancel_futures=True)

    async def probe(_: web.Request) -> web.Response:
        return web.Response(text="ok\n", content_type="text/plain")

    async def as_of(request: web.Request) -> web.Response:
        ts = request.query.get("ts")
        if not ts:
            return web.json_response({"detail": "missing ts"}, status=400)
        try:
            return web.json_response(await run_query(service.as_of, ts))
        except Exception as exc:
            return web.json_response({"detail": str(exc)}, status=503)

    async def quality(_: web.Request) -> web.Response:
        try:
            return web.json_response(await run_query(service.quality_histogram))
        except Exception as exc:
            return web.json_response({"detail": str(exc)}, status=503)

    async def curation_summary(_: web.Request) -> web.Response:
        try:
            return web.json_response(await run_query(service.curation_summary))
        except Exception as exc:
            return web.json_response({"detail": str(exc)}, status=503)

    async def corpus_overview(_: web.Request) -> web.Response:
        try:
            return web.json_response(await run_query(service.corpus_overview))
        except Exception as exc:
            return web.json_response({"detail": str(exc)}, status=503)

    async def license_admissions(request: web.Request) -> web.Response:
        try:
            return web.json_response(
                await run_query(
                    service.license_admissions,
                    recent_limit=int(request.query.get("recent_limit", "20")),
                )
            )
        except (TypeError, ValueError):
            return web.json_response({"detail": "invalid recent_limit"}, status=400)
        except Exception as exc:
            return web.json_response({"detail": str(exc)}, status=503)

    async def source_activity(request: web.Request) -> web.Response:
        try:
            return web.json_response(
                await run_query(
                    service.source_activity,
                    window_hours=int(request.query.get("window_hours", "24")),
                )
            )
        except (TypeError, ValueError):
            return web.json_response({"detail": "invalid window_hours"}, status=400)
        except Exception as exc:
            return web.json_response({"detail": str(exc)}, status=503)

    async def documents(request: web.Request) -> web.Response:
        try:
            return web.json_response(
                await run_query(
                    service.documents,
                    page=int(request.query.get("page", "1")),
                    page_size=int(request.query.get("page_size", "25")),
                    search=request.query.get("search"),
                    routes=request.query.getall("route", []),
                    sources=request.query.getall("source", []),
                    source_formats=request.query.getall("source_format", []),
                    date_from=request.query.get("date_from"),
                    date_to=request.query.get("date_to"),
                    tags=request.query.getall("tag", []),
                    rejection_reasons=request.query.getall("rejection_reason", []),
                    has_figures=_optional_bool(request.query.get("has_figures")),
                    has_tables=_optional_bool(request.query.get("has_tables")),
                    has_equations=_optional_bool(request.query.get("has_equations")),
                    include_fixtures=_optional_bool(request.query.get("include_fixtures")) is True,
                    min_edu=_optional_float(request.query.get("min_edu")),
                    max_edu=_optional_float(request.query.get("max_edu")),
                    min_quality=_optional_float(request.query.get("min_quality")),
                    max_quality=_optional_float(request.query.get("max_quality")),
                    sort=request.query.get("sort", "newest"),
                )
            )
        except (TypeError, ValueError):
            return web.json_response({"detail": "invalid document filters"}, status=400)
        except Exception as exc:
            return web.json_response({"detail": str(exc)}, status=503)

    async def document_facets(request: web.Request) -> web.Response:
        try:
            return web.json_response(
                await run_query(
                    service.document_facets,
                    include_fixtures=_optional_bool(request.query.get("include_fixtures")) is True,
                )
            )
        except Exception as exc:
            return web.json_response({"detail": str(exc)}, status=503)

    async def dataset_export(request: web.Request) -> web.Response:
        date_from = request.query.get("date_from")
        date_to = request.query.get("date_to")
        routes = request.query.getall("route", ["pretrain", "posttrain_candidate"])
        if not date_from or not date_to:
            return web.json_response({"detail": "date_from and date_to are required"}, status=400)
        try:
            rows = await run_query(
                service.dataset_rows,
                date_from=date_from,
                date_to=date_to,
                routes=routes,
                sources=request.query.getall("source", []),
                source_formats=request.query.getall("source_format", []),
                tags=request.query.getall("tag", []),
                min_edu=_optional_float(request.query.get("min_edu")),
                min_quality=_optional_float(request.query.get("min_quality")),
                include_structured=_optional_bool(request.query.get("include_structured"))
                is not False,
                limit=int(request.query.get("limit", "5000")),
            )
            output_format = request.query.get("format", "jsonl")
            if output_format == "parquet":
                import io

                import pyarrow as pa
                import pyarrow.parquet as pq

                target = io.BytesIO()
                pq.write_table(  # type: ignore[no-untyped-call]
                    pa.Table.from_pylist(rows), target, compression="zstd"
                )
                return web.Response(
                    body=target.getvalue(),
                    content_type="application/vnd.apache.parquet",
                    headers={
                        "Content-Disposition": 'attachment; filename="stream2pretrain.parquet"'
                    },
                )
            if output_format != "jsonl":
                return web.json_response({"detail": "format must be jsonl or parquet"}, status=400)
            import orjson

            payload = b"".join(orjson.dumps(row) + b"\n" for row in rows)
            return web.Response(
                body=payload,
                content_type="application/x-ndjson",
                headers={"Content-Disposition": 'attachment; filename="stream2pretrain.jsonl"'},
            )
        except (TypeError, ValueError):
            return web.json_response({"detail": "invalid export filters"}, status=400)
        except Exception as exc:
            return web.json_response({"detail": str(exc)}, status=503)

    async def dataset_summary(request: web.Request) -> web.Response:
        date_from = request.query.get("date_from")
        date_to = request.query.get("date_to")
        routes = request.query.getall("route", ["pretrain", "posttrain_candidate"])
        if not date_from or not date_to:
            return web.json_response({"detail": "date_from and date_to are required"}, status=400)
        try:
            return web.json_response(
                await run_query(
                    service.dataset_summary,
                    date_from=date_from,
                    date_to=date_to,
                    routes=routes,
                    sources=request.query.getall("source", []),
                    source_formats=request.query.getall("source_format", []),
                    tags=request.query.getall("tag", []),
                    min_edu=_optional_float(request.query.get("min_edu")),
                    min_quality=_optional_float(request.query.get("min_quality")),
                    include_structured=_optional_bool(request.query.get("include_structured"))
                    is not False,
                )
            )
        except Exception as exc:
            return web.json_response({"detail": str(exc)}, status=503)

    async def document(request: web.Request) -> web.Response:
        try:
            value = await run_query(service.document, unquote(request.match_info["doc_id"]))
        except Exception as exc:
            return web.json_response({"detail": str(exc)}, status=503)
        if value is None:
            return web.json_response({"detail": "document not found"}, status=404)
        return web.json_response(value)

    async def figure(request: web.Request) -> web.Response:
        try:
            value = await run_query(
                service.figure,
                unquote(request.match_info["doc_id"]),
                unquote(request.match_info["figure_id"]),
            )
        except Exception as exc:
            return web.json_response({"detail": str(exc)}, status=503)
        if value is None:
            return web.json_response({"detail": "figure not found"}, status=404)
        payload, content_type = value
        return web.Response(body=payload, content_type=content_type)

    async def query(request: web.Request) -> web.Response:
        body = await request.json()
        try:
            return web.json_response(
                await run_query(
                    service.safe_query,
                    str(body.get("sql", "")),
                    body.get("params", []),
                )
            )
        except ValueError as exc:
            return web.json_response({"detail": str(exc)}, status=400)
        except Exception as exc:
            return web.json_response({"detail": str(exc)}, status=503)

    app = web.Application()
    app.router.add_get("/healthz", probe)
    app.router.add_get("/readyz", probe)
    app.router.add_get("/as-of", as_of)
    app.router.add_get("/quality-histogram", quality)
    app.router.add_get("/curation-summary", curation_summary)
    app.router.add_get("/corpus-overview", corpus_overview)
    app.router.add_get("/license-admissions", license_admissions)
    app.router.add_get("/source-activity", source_activity)
    app.router.add_get("/documents", documents)
    app.router.add_get("/document-facets", document_facets)
    app.router.add_get("/documents/{doc_id}", document)
    app.router.add_get("/documents/{doc_id}/figures/{figure_id}", figure)
    app.router.add_get("/datasets/export", dataset_export)
    app.router.add_get("/datasets/summary", dataset_summary)
    app.router.add_post("/query", query)
    app.on_cleanup.append(stop_query_executor)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, os.environ.get("S2P_BIND_HOST", "::"), port)
    await site.start()
    while True:
        await asyncio.sleep(3600)


def main() -> None:
    import asyncio

    service = DuckDBQueryService.from_env()
    asyncio.run(serve(service, port=int(os.environ.get("S2P_DUCKDB_API_PORT", "8090"))))


def _optional_bool(value: str | None) -> bool | None:
    if value is None or value == "":
        return None
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes"}:
        return True
    if normalized in {"0", "false", "no"}:
        return False
    raise ValueError("expected a boolean")


def _optional_float(value: str | None) -> float | None:
    if value is None or value == "":
        return None
    return float(value)


def _without_structured_surrogates(text: str) -> str:
    """Remove bounded generated blocks while retaining the scientific body."""
    cleaned = re.sub(
        r"\n*\[(?:TABLE|FIGURE)\].*?\[/(?:TABLE|FIGURE)\]\n*",
        "\n\n",
        text,
        flags=re.DOTALL,
    )
    cleaned = re.sub(r"\n*\[EQUATION\].*?\[/EQUATION\]\n*", "\n\n", cleaned, flags=re.DOTALL)
    return cleaned.strip()


if __name__ == "__main__":
    main()
