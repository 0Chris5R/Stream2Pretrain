"""Iceberg writer: ``curation.decisions`` -> audit and clean Iceberg tables.

Loads / creates the gold table via the Polaris REST catalog (pyiceberg
``RestCatalog``), collects incoming records into bounded time/size batches,
and commits those batches as Iceberg snapshots. Per-record provenance stays in
the table rows.

The Bytewax sink wraps :class:`IcebergWriter` so the same class can be
exercised by unit tests without spinning up a Bytewax dataflow.

Schema
------
Partitioning follows RESEARCH.md section 6:

    silver.PARTITION BY lang, bucket(16, doc_id)
    gold.PARTITION BY lang, risk_tier, month(valid_from)
The partition spec is set up on first ``ensure_table`` call so a fresh
deployment is one-shot bootstrappable.
"""

from __future__ import annotations

import os
import threading
import time
from contextlib import suppress
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any

import orjson

from processor import common
from processor.iceberg_catalog import (
    ensure_iceberg_maintenance_properties,
    iceberg_maintenance_properties,
)
from processor.metrics import PROCESSOR_METRICS, ProcessorMetrics
from processor.probes import start_probe_server
from schemas.gold import GoldRecord
from schemas.license_admission import LicenseAdmissionDecision

if TYPE_CHECKING:
    import pyarrow as pa
    from pyiceberg.catalog import Catalog
    from pyiceberg.table import Table


DEFAULT_GOLD_NAMESPACE: str = "gold"
DEFAULT_GOLD_TABLE: str = "curated"
DEFAULT_BATCH_SIZE: int = 256
DEFAULT_FLUSH_INTERVAL_SECONDS: int = 60
DecisionKey = tuple[str, str, str, str]


def _positive_int_env(name: str, default: int) -> int:
    value = int(os.environ.get(name, default))
    if value < 1:
        raise ValueError(f"{name} must be at least 1")
    return value


def _maintenance_properties() -> dict[str, str]:
    return iceberg_maintenance_properties()


def _ensure_maintenance_properties(table: Table) -> None:
    ensure_iceberg_maintenance_properties(table)


def _is_missing_catalog_table(exc: Exception) -> bool:
    name = exc.__class__.__name__.lower()
    message = str(exc).lower()
    return "nosuchtable" in name or "not found" in message or "does not exist" in message


def _ensure_optional_columns(table: Table, columns: tuple[tuple[str, object], ...]) -> None:
    """Evolve an existing Iceberg table with backward-compatible columns."""
    missing: list[tuple[str, object]] = []
    schema = table.schema()
    for name, field_type in columns:
        try:
            schema.find_field(name)
        except ValueError:
            missing.append((name, field_type))
    if not missing:
        return
    update = table.update_schema()
    for name, field_type in missing:
        update.add_column(name, field_type, required=False)
    update.commit()


class LicenseAdmissionWriter:
    """Append deduplicated pre-fetch licence decisions in Iceberg batches."""

    def __init__(self, catalog: Catalog) -> None:
        self._catalog = catalog
        self._known_ids: set[str] | None = None

    def add(self, decision: LicenseAdmissionDecision) -> bool:
        """Compatibility wrapper for callers that submit one decision."""
        return self.add_batch([decision]) == 1

    def add_batch(self, decisions: list[LicenseAdmissionDecision]) -> int:
        """Append one data file and snapshot for all new decisions in the batch."""
        if not decisions:
            return 0
        table = self._ensure_table()
        _ensure_maintenance_properties(table)
        if self._known_ids is None:
            self._known_ids = self._load_ids(table)
        pending: list[LicenseAdmissionDecision] = []
        pending_ids: set[str] = set()
        for decision in decisions:
            if decision.decision_id in self._known_ids or decision.decision_id in pending_ids:
                continue
            pending.append(decision)
            pending_ids.add(decision.decision_id)
        if not pending:
            return 0
        table.append(self._to_arrow(pending))
        self._known_ids.update(pending_ids)
        return len(pending)

    def _load_ids(self, table: Table) -> set[str]:
        return {
            str(value)
            for value in table.scan(selected_fields=("decision_id",))
            .to_arrow()
            .column("decision_id")
            .to_pylist()
        }

    def _ensure_table(self) -> Table:
        identifier = (
            os.environ.get("S2P_ICEBERG_NAMESPACE", DEFAULT_GOLD_NAMESPACE),
            os.environ.get("S2P_ICEBERG_LICENSE_ADMISSIONS_TABLE", "license_admissions"),
        )
        from pyiceberg.types import StringType, TimestamptzType

        try:
            table = self._catalog.load_table(identifier)
        except Exception as exc:
            if not _is_missing_catalog_table(exc):
                raise
        else:
            _ensure_optional_columns(
                table,
                (
                    ("raw_license", StringType()),
                    ("normalized_license", StringType()),
                    ("resolver", StringType()),
                    ("evidence_url", StringType()),
                    ("evidence_revision", StringType()),
                    ("evidence_scope", StringType()),
                    ("policy_revision", StringType()),
                    ("resolved_at", TimestamptzType()),
                    ("source_format", StringType()),
                ),
            )
            return table
        from pyiceberg.partitioning import PartitionField, PartitionSpec
        from pyiceberg.schema import Schema
        from pyiceberg.transforms import IdentityTransform, MonthTransform
        from pyiceberg.types import BooleanType, NestedField, StringType, TimestamptzType

        with suppress(Exception):
            self._catalog.create_namespace((identifier[0],))
        schema = Schema(
            NestedField(1, "decision_id", StringType(), required=True),
            NestedField(2, "doc_id", StringType(), required=True),
            NestedField(3, "source_feed", StringType(), required=True),
            NestedField(4, "source_url", StringType(), required=True),
            NestedField(5, "observed_at", TimestamptzType(), required=True),
            NestedField(6, "status", StringType(), required=True),
            NestedField(7, "license_id", StringType(), required=True),
            NestedField(8, "license_source", StringType(), required=True),
            NestedField(9, "reason", StringType(), required=True),
            NestedField(10, "trace_id", StringType(), required=True),
            NestedField(11, "content_fetch_started", BooleanType(), required=True),
            NestedField(12, "raw_license", StringType(), required=False),
            NestedField(13, "normalized_license", StringType(), required=False),
            NestedField(14, "resolver", StringType(), required=False),
            NestedField(15, "evidence_url", StringType(), required=False),
            NestedField(16, "evidence_revision", StringType(), required=False),
            NestedField(17, "evidence_scope", StringType(), required=False),
            NestedField(18, "policy_revision", StringType(), required=False),
            NestedField(19, "resolved_at", TimestamptzType(), required=False),
            NestedField(20, "source_format", StringType(), required=False),
        )
        spec = PartitionSpec(
            PartitionField(3, 1000, IdentityTransform(), "source_feed"),
            PartitionField(6, 1001, IdentityTransform(), "status"),
            PartitionField(5, 1002, MonthTransform(), "observed_month"),
        )
        return self._catalog.create_table(
            identifier,
            schema=schema,
            partition_spec=spec,
            properties=_maintenance_properties(),
        )

    @staticmethod
    def _to_arrow(decisions: list[LicenseAdmissionDecision]) -> pa.Table:
        import pyarrow as pa

        schema = pa.schema(
            [
                pa.field("decision_id", pa.string(), nullable=False),
                pa.field("doc_id", pa.string(), nullable=False),
                pa.field("source_feed", pa.string(), nullable=False),
                pa.field("source_url", pa.string(), nullable=False),
                pa.field("observed_at", pa.timestamp("us", tz="UTC"), nullable=False),
                pa.field("status", pa.string(), nullable=False),
                pa.field("license_id", pa.string(), nullable=False),
                pa.field("license_source", pa.string(), nullable=False),
                pa.field("reason", pa.string(), nullable=False),
                pa.field("trace_id", pa.string(), nullable=False),
                pa.field("content_fetch_started", pa.bool_(), nullable=False),
                pa.field("raw_license", pa.string(), nullable=True),
                pa.field("normalized_license", pa.string(), nullable=True),
                pa.field("resolver", pa.string(), nullable=True),
                pa.field("evidence_url", pa.string(), nullable=True),
                pa.field("evidence_revision", pa.string(), nullable=True),
                pa.field("evidence_scope", pa.string(), nullable=True),
                pa.field("policy_revision", pa.string(), nullable=True),
                pa.field("resolved_at", pa.timestamp("us", tz="UTC"), nullable=True),
                pa.field("source_format", pa.string(), nullable=True),
            ]
        )
        return pa.Table.from_pydict(
            {
                "decision_id": [decision.decision_id for decision in decisions],
                "doc_id": [decision.doc_id for decision in decisions],
                "source_feed": [decision.source_feed for decision in decisions],
                "source_url": [str(decision.source_url) for decision in decisions],
                "observed_at": [decision.observed_at for decision in decisions],
                "status": [decision.status for decision in decisions],
                "license_id": [decision.license_id for decision in decisions],
                "license_source": [decision.license_source for decision in decisions],
                "reason": [decision.reason for decision in decisions],
                "trace_id": [decision.trace_id for decision in decisions],
                "content_fetch_started": [decision.content_fetch_started for decision in decisions],
                "raw_license": [decision.raw_license for decision in decisions],
                "normalized_license": [decision.normalized_license for decision in decisions],
                "resolver": [decision.resolver for decision in decisions],
                "evidence_url": [
                    str(decision.evidence_url) if decision.evidence_url is not None else None
                    for decision in decisions
                ],
                "evidence_revision": [decision.evidence_revision for decision in decisions],
                "evidence_scope": [decision.evidence_scope for decision in decisions],
                "policy_revision": [decision.policy_revision for decision in decisions],
                "resolved_at": [decision.resolved_at for decision in decisions],
                "source_format": [decision.source_format for decision in decisions],
            },
            schema=schema,
        )


def gold_identifier() -> tuple[str, str]:
    """Return the Iceberg identifier used for the curated Gold table."""
    namespace = os.environ.get("S2P_ICEBERG_NAMESPACE") or os.environ.get(
        "ICEBERG_NAMESPACE", DEFAULT_GOLD_NAMESPACE
    )
    table = os.environ.get("S2P_ICEBERG_GOLD_TABLE", DEFAULT_GOLD_TABLE)
    return (namespace, table)


def decisions_identifier() -> tuple[str, str]:
    """Return the Iceberg identifier for every accepted/rejected decision."""
    namespace = os.environ.get("S2P_ICEBERG_NAMESPACE") or os.environ.get(
        "ICEBERG_NAMESPACE", DEFAULT_GOLD_NAMESPACE
    )
    table = os.environ.get("S2P_ICEBERG_DECISIONS_TABLE", "curation_decisions")
    return (namespace, table)


@dataclass(slots=True)
class WriterStats:
    """Diagnostics returned by :meth:`IcebergWriter.flush`."""

    rows_committed: int = 0
    decisions_committed: int = 0
    snapshot_id: int | None = None
    watermark: datetime | None = None


@dataclass
class _Buffer:
    """Internal pending-rows buffer."""

    rows: list[GoldRecord] = field(default_factory=list)
    watermark: datetime | None = None

    def add(self, record: GoldRecord) -> None:
        self.rows.append(record)
        ts = record.valid_from
        if self.watermark is None or ts > self.watermark:
            self.watermark = ts

    def __len__(self) -> int:
        return len(self.rows)

    def reset(self) -> None:
        self.rows.clear()
        self.watermark = None


class IcebergWriter:
    """Buffer-and-commit writer for the decisions and clean Gold tables.

    Parameters
    ----------
    catalog
        A pyiceberg ``Catalog`` (constructed once per pod).
    batch_size
        Soft trigger for committing. ``flush`` can also be called
        directly for time-based triggers.
    """

    def __init__(
        self,
        *,
        catalog: Catalog,
        metrics: ProcessorMetrics | None = None,
        batch_size: int = DEFAULT_BATCH_SIZE,
    ) -> None:
        self._catalog = catalog
        self._metrics = metrics
        self._batch_size = batch_size
        self._buffer = _Buffer()
        self._lock = threading.Lock()
        self._known_keys: dict[str, set[DecisionKey]] = {}

    @classmethod
    def from_config(
        cls,
        cfg: common.ProcessorConfig,
        *,
        metrics: ProcessorMetrics | None = None,
    ) -> IcebergWriter:
        """Build a writer from a :class:`ProcessorConfig`.

        Constructs the Polaris REST catalog client lazily so importing the
        module costs nothing in tests.
        """
        from processor.iceberg_catalog import load_runtime_catalog

        catalog = load_runtime_catalog(cfg)
        return cls(
            catalog=catalog,
            metrics=metrics,
            batch_size=_positive_int_env("S2P_FLUSH_RECORDS", DEFAULT_BATCH_SIZE),
        )

    def add(self, record: GoldRecord) -> WriterStats | None:
        """Buffer one scored decision; auto-flush at ``batch_size``."""
        with self._lock:
            self._buffer.add(record)
            if len(self._buffer) >= self._batch_size:
                return self._flush_locked()
        return None

    def flush(self) -> WriterStats:
        """Force a commit of all buffered rows."""
        with self._lock:
            if not self._buffer.rows:
                return WriterStats(rows_committed=0)
            return self._flush_locked()

    def _flush_locked(self) -> WriterStats:
        """Internal commit path - caller holds ``self._lock``.

        Order is buffer-snapshot, attempt-append, then-on-success reset +
        commit bookkeeping. Failures re-raise so the calling Bytewax operator
        retries with the same buffer (rather than dropping rows on a
        transient catalog/MinIO outage).
        """
        rows = list(self._buffer.rows)
        watermark = self._buffer.watermark
        accepted_rows = [row for row in rows if _is_trainable_gold(row)]
        decisions_table = self._ensure_decisions_table()
        _ensure_maintenance_properties(decisions_table)
        decision_rows = self._uncommitted_rows("decisions", decisions_table, rows)
        gold_table = self._ensure_table() if accepted_rows else None
        if gold_table is not None:
            _ensure_maintenance_properties(gold_table)
        gold_rows = (
            self._uncommitted_rows("gold", gold_table, accepted_rows)
            if gold_table is not None
            else []
        )
        started = time.perf_counter()
        decision_snapshot_id: int | None = None
        if decision_rows:
            decision_snapshot_id = self._append(
                decisions_table,
                self._to_arrow(decision_rows),
                _rows_watermark(decision_rows),
            )
            if decision_snapshot_id is None:
                raise RuntimeError(
                    f"iceberg decision append failed for {len(decision_rows)} rows; buffer preserved"
                )
            self._remember_rows("decisions", decision_rows)

        if gold_rows and gold_table is not None:
            gold_snapshot_id = self._append(
                gold_table,
                self._to_arrow(gold_rows),
                _rows_watermark(gold_rows),
            )
            if gold_snapshot_id is None:
                raise RuntimeError(
                    "gold append failed after the durable decision commit; replay is safe"
                )
            self._remember_rows("gold", gold_rows)
        elapsed = time.perf_counter() - started
        self._buffer.reset()
        if self._metrics is not None:
            self._metrics.record_iceberg_flush(
                rows=len(gold_rows),
                decisions=len(decision_rows),
                seconds=elapsed,
            )
        return WriterStats(
            rows_committed=len(gold_rows),
            decisions_committed=len(decision_rows),
            snapshot_id=decision_snapshot_id,
            watermark=watermark,
        )

    def _uncommitted_rows(
        self,
        cache_name: str,
        table: Table,
        rows: list[GoldRecord],
    ) -> list[GoldRecord]:
        """Return one row per recipe key that the target table does not contain."""
        existing = self._known_keys.get(cache_name)
        if existing is None:
            existing = self._load_existing_keys(table)
            self._known_keys[cache_name] = existing
        pending: set[DecisionKey] = set()
        output: list[GoldRecord] = []
        for row in rows:
            key = _decision_key(row)
            if key in existing or key in pending:
                continue
            pending.add(key)
            output.append(row)
        return output

    def _load_existing_keys(self, table: Table) -> set[DecisionKey]:
        """Load committed recipe keys after a writer restart.

        Unit-test table doubles without a scan API start empty. Real Iceberg
        tables must scan successfully; silently treating an unreadable table
        as empty would recreate the duplicate-row bug this guard prevents.
        """
        scan = getattr(table, "scan", None)
        if scan is None:
            return set()
        refresh = getattr(table, "refresh", None)
        if callable(refresh):
            refresh()
        try:
            arrow = scan(
                selected_fields=(
                    "doc_id",
                    "scoring_version",
                    "classifier_revision",
                    "policy_revision",
                )
            ).to_arrow()
            columns = [
                arrow.column(name).to_pylist()
                for name in (
                    "doc_id",
                    "scoring_version",
                    "classifier_revision",
                    "policy_revision",
                )
            ]
            return {
                (str(doc_id), str(scoring), str(classifier), str(policy))
                for doc_id, scoring, classifier, policy in zip(*columns, strict=True)
            }
        except Exception as exc:
            raise RuntimeError("failed to read committed Iceberg decision keys") from exc

    def _remember_rows(self, cache_name: str, rows: list[GoldRecord]) -> None:
        self._known_keys.setdefault(cache_name, set()).update(_decision_key(row) for row in rows)

    def _ensure_table(self) -> Table:
        """Create the accepted Gold table if missing."""
        return self._ensure_table_at(gold_identifier())

    def _ensure_decisions_table(self) -> Table:
        """Create the authoritative accepted/rejected decision table."""
        return self._ensure_table_at(decisions_identifier())

    def _ensure_table_at(self, identifier: tuple[str, str]) -> Table:
        """Create one Gold-shaped Iceberg table if missing."""
        from pyiceberg.partitioning import PartitionField, PartitionSpec
        from pyiceberg.schema import Schema
        from pyiceberg.transforms import IdentityTransform, MonthTransform
        from pyiceberg.types import (
            BooleanType,
            DoubleType,
            IntegerType,
            ListType,
            LongType,
            NestedField,
            StringType,
            TimestamptzType,
        )

        try:
            table = self._catalog.load_table(identifier)
        except Exception as exc:
            if not _is_missing_catalog_table(exc):
                raise
        else:
            _ensure_optional_columns(
                table,
                (
                    ("training_usage", StringType()),
                    ("quality_diagnostics_json", StringType()),
                ),
            )
            return table
        # Bring up an empty namespace if it does not yet exist.
        with suppress(Exception):
            self._catalog.create_namespace((identifier[0],))
        schema = Schema(
            NestedField(1, "doc_id", StringType(), required=True),
            NestedField(2, "text", StringType(), required=True),
            NestedField(3, "lang", StringType(), required=True),
            NestedField(4, "tokens", IntegerType(), required=True),
            NestedField(5, "quality_score", DoubleType(), required=True),
            NestedField(6, "edu_score", DoubleType(), required=True),
            NestedField(7, "license", StringType(), required=True),
            NestedField(8, "license_source", StringType(), required=True),
            NestedField(9, "risk_tier", IntegerType(), required=True),
            NestedField(
                10,
                "pii_flags",
                ListType(11, StringType(), element_required=False),
                required=False,
            ),
            NestedField(14, "valid_from", TimestamptzType(), required=True),
            NestedField(15, "valid_to", TimestamptzType(), required=False),
            NestedField(
                16,
                "reject_reasons",
                ListType(17, StringType(), element_required=False),
                required=False,
            ),
            NestedField(18, "scoring_version", StringType(), required=True),
            NestedField(19, "classifier_revision", StringType(), required=True),
            NestedField(20, "policy_revision", StringType(), required=True),
            NestedField(21, "snapshot_id", LongType(), required=False),
            NestedField(22, "trace_id", StringType(), required=True),
            NestedField(23, "source_feed", StringType(), required=True),
            NestedField(24, "source_format", StringType(), required=True),
            NestedField(25, "extraction_pipeline", StringType(), required=True),
            NestedField(26, "spdx_license", StringType(), required=False),
            NestedField(27, "spdx_license_source", StringType(), required=True),
            NestedField(28, "scientific_artifact_s3_uri", StringType(), required=False),
            NestedField(29, "figure_count", IntegerType(), required=True),
            NestedField(30, "table_count", IntegerType(), required=True),
            NestedField(31, "equation_count", IntegerType(), required=True),
            NestedField(32, "citation_count", IntegerType(), required=True),
            NestedField(
                33,
                "extraction_warnings",
                ListType(34, StringType(), element_required=False),
                required=False,
            ),
            NestedField(35, "lang_score", DoubleType(), required=True),
            NestedField(36, "gopher_pass", BooleanType(), required=True),
            NestedField(37, "c4_nopunc_pass", BooleanType(), required=True),
            NestedField(38, "c4_curly_brace_pass", BooleanType(), required=True),
            NestedField(39, "c4_lorem_ipsum_pass", BooleanType(), required=True),
            NestedField(40, "c4_fraction_lines_with_punct", DoubleType(), required=True),
            NestedField(41, "perplexity", DoubleType(), required=True),
            NestedField(42, "perplexity_bucket", StringType(), required=True),
            NestedField(43, "perplexity_scorer", StringType(), required=True),
            NestedField(44, "near_duplicate", BooleanType(), required=True),
            NestedField(45, "near_dup_cluster_id", StringType(), required=False),
            NestedField(46, "minhash_backend", StringType(), required=True),
            NestedField(47, "lsh_backend", StringType(), required=True),
            NestedField(48, "minhash_num_perms", IntegerType(), required=True),
            NestedField(49, "structural_quality_score", DoubleType(), required=True),
            NestedField(50, "extraction_completeness", DoubleType(), required=True),
            NestedField(51, "reasoning_score", DoubleType(), required=True),
            NestedField(53, "route", StringType(), required=True),
            NestedField(
                54,
                "eligible_routes",
                ListType(55, StringType(), element_required=False),
                required=False,
            ),
            NestedField(
                56,
                "route_reasons",
                ListType(57, StringType(), element_required=False),
                required=False,
            ),
            NestedField(
                58,
                "content_tags",
                ListType(59, StringType(), element_required=False),
                required=False,
            ),
            NestedField(60, "segment_scores_json", StringType(), required=True),
            NestedField(61, "projection_version", StringType(), required=True),
            NestedField(62, "source_word_count", IntegerType(), required=True),
            NestedField(63, "training_word_count", IntegerType(), required=True),
            NestedField(64, "included_section_count", IntegerType(), required=True),
            NestedField(65, "excluded_section_count", IntegerType(), required=True),
            NestedField(
                66,
                "excluded_sections",
                ListType(67, StringType(), element_required=False),
                required=False,
            ),
            NestedField(
                68,
                "metadata_pii_flags",
                ListType(69, StringType(), element_required=False),
                required=False,
            ),
            NestedField(
                70,
                "removed_body_pii_flags",
                ListType(71, StringType(), element_required=False),
                required=False,
            ),
            NestedField(72, "pii_action", StringType(), required=True),
            NestedField(73, "pii_scanner_revision", StringType(), required=True),
            NestedField(74, "lang_detector_revision", StringType(), required=True),
            NestedField(75, "tokenizer_revision", StringType(), required=True),
            NestedField(76, "gopher_word_count", IntegerType(), required=True),
            NestedField(77, "gopher_mean_word_len", DoubleType(), required=True),
            NestedField(78, "gopher_stopword_ratio", DoubleType(), required=True),
            NestedField(79, "gopher_bullet_line_ratio", DoubleType(), required=True),
            NestedField(80, "gopher_ellipsis_line_ratio", DoubleType(), required=True),
            NestedField(81, "gopher_symbol_word_ratio", DoubleType(), required=True),
            NestedField(82, "gopher_alpha_word_ratio", DoubleType(), required=True),
            NestedField(91, "classifier_backend", StringType(), required=True),
            NestedField(92, "training_usage", StringType(), required=False),
            NestedField(93, "quality_diagnostics_json", StringType(), required=False),
        )
        partition_spec = PartitionSpec(
            PartitionField(source_id=3, field_id=1000, transform=IdentityTransform(), name="lang"),
            PartitionField(
                source_id=9, field_id=1001, transform=IdentityTransform(), name="risk_tier"
            ),
            PartitionField(
                source_id=14, field_id=1002, transform=MonthTransform(), name="valid_from_month"
            ),
        )
        return self._catalog.create_table(
            identifier=identifier,
            schema=schema,
            partition_spec=partition_spec,
            properties={
                "format-version": "2",
                "write.format.default": "parquet",
                **_maintenance_properties(),
            },
        )

    def _to_arrow(self, rows: list[GoldRecord]) -> pa.Table:
        import pyarrow as pa

        cols: dict[str, list[Any]] = {
            "doc_id": [r.doc_id for r in rows],
            "text": [r.text for r in rows],
            "lang": [r.lang for r in rows],
            "tokens": [r.tokens for r in rows],
            "quality_score": [float(r.quality_score) for r in rows],
            "edu_score": [float(r.edu_score) for r in rows],
            "license": [r.license for r in rows],
            "license_source": [r.license_source for r in rows],
            "risk_tier": [int(r.risk_tier) for r in rows],
            "pii_flags": [list(r.pii_flags) for r in rows],
            "valid_from": [r.valid_from for r in rows],
            "valid_to": [r.valid_to for r in rows],
            "reject_reasons": [list(r.reject_reasons) for r in rows],
            "scoring_version": [r.scoring_version for r in rows],
            "classifier_revision": [r.classifier_revision for r in rows],
            "quality_diagnostics_json": [
                orjson.dumps(r.quality_diagnostics).decode() if r.quality_diagnostics else None
                for r in rows
            ],
            "policy_revision": [r.policy_revision for r in rows],
            "snapshot_id": [r.snapshot_id for r in rows],
            "trace_id": [r.trace_id for r in rows],
            "source_feed": [r.source_feed for r in rows],
            "source_format": [r.source_format for r in rows],
            "extraction_pipeline": [r.extraction_pipeline for r in rows],
            "spdx_license": [r.spdx_license for r in rows],
            "spdx_license_source": [r.spdx_license_source for r in rows],
            "scientific_artifact_s3_uri": [r.scientific_artifact_s3_uri for r in rows],
            "figure_count": [r.figure_count for r in rows],
            "table_count": [r.table_count for r in rows],
            "equation_count": [r.equation_count for r in rows],
            "citation_count": [r.citation_count for r in rows],
            "extraction_warnings": [list(r.extraction_warnings) for r in rows],
            "lang_score": [float(r.lang_score) for r in rows],
            "gopher_pass": [r.gopher_pass for r in rows],
            "c4_nopunc_pass": [r.c4_nopunc_pass for r in rows],
            "c4_curly_brace_pass": [r.c4_curly_brace_pass for r in rows],
            "c4_lorem_ipsum_pass": [r.c4_lorem_ipsum_pass for r in rows],
            "c4_fraction_lines_with_punct": [float(r.c4_fraction_lines_with_punct) for r in rows],
            "perplexity": [float(r.perplexity) for r in rows],
            "perplexity_bucket": [r.perplexity_bucket for r in rows],
            "perplexity_scorer": [r.perplexity_scorer for r in rows],
            "near_duplicate": [r.near_duplicate for r in rows],
            "near_dup_cluster_id": [r.near_dup_cluster_id for r in rows],
            "minhash_backend": [r.minhash_backend for r in rows],
            "lsh_backend": [r.lsh_backend for r in rows],
            "minhash_num_perms": [r.minhash_num_perms for r in rows],
            "structural_quality_score": [float(r.structural_quality_score) for r in rows],
            "extraction_completeness": [float(r.extraction_completeness) for r in rows],
            "reasoning_score": [float(r.reasoning_score) for r in rows],
            "route": [r.route for r in rows],
            "eligible_routes": [list(r.eligible_routes) for r in rows],
            "route_reasons": [list(r.route_reasons) for r in rows],
            "content_tags": [list(r.content_tags) for r in rows],
            "segment_scores_json": [
                orjson.dumps([score.model_dump(mode="json") for score in r.segment_scores]).decode(
                    "utf-8"
                )
                for r in rows
            ],
            "projection_version": [r.projection_version for r in rows],
            "source_word_count": [r.source_word_count for r in rows],
            "training_word_count": [r.training_word_count for r in rows],
            "included_section_count": [r.included_section_count for r in rows],
            "excluded_section_count": [r.excluded_section_count for r in rows],
            "excluded_sections": [list(r.excluded_sections) for r in rows],
            "metadata_pii_flags": [list(r.metadata_pii_flags) for r in rows],
            "removed_body_pii_flags": [list(r.removed_body_pii_flags) for r in rows],
            "pii_action": [r.pii_action for r in rows],
            "pii_scanner_revision": [r.pii_scanner_revision for r in rows],
            "lang_detector_revision": [r.lang_detector_revision for r in rows],
            "tokenizer_revision": [r.tokenizer_revision for r in rows],
            "gopher_word_count": [r.gopher_word_count for r in rows],
            "gopher_mean_word_len": [float(r.gopher_mean_word_len) for r in rows],
            "gopher_stopword_ratio": [float(r.gopher_stopword_ratio) for r in rows],
            "gopher_bullet_line_ratio": [float(r.gopher_bullet_line_ratio) for r in rows],
            "gopher_ellipsis_line_ratio": [float(r.gopher_ellipsis_line_ratio) for r in rows],
            "gopher_symbol_word_ratio": [float(r.gopher_symbol_word_ratio) for r in rows],
            "gopher_alpha_word_ratio": [float(r.gopher_alpha_word_ratio) for r in rows],
            "classifier_backend": [r.classifier_backend for r in rows],
            "training_usage": [r.training_usage for r in rows],
        }
        # PyIceberg validates Arrow nullability and timestamp timezone against
        # the declared table schema.  Inferred Arrow schemas mark every field
        # nullable and would make a fresh local table append fail even when all
        # values are present, so keep the producer schema explicit.
        arrow_schema = pa.schema(
            [
                pa.field("doc_id", pa.string(), nullable=False),
                pa.field("text", pa.string(), nullable=False),
                pa.field("lang", pa.string(), nullable=False),
                pa.field("tokens", pa.int32(), nullable=False),
                pa.field("quality_score", pa.float64(), nullable=False),
                pa.field("edu_score", pa.float64(), nullable=False),
                pa.field("license", pa.string(), nullable=False),
                pa.field("license_source", pa.string(), nullable=False),
                pa.field("risk_tier", pa.int32(), nullable=False),
                pa.field("pii_flags", pa.list_(pa.string()), nullable=True),
                pa.field("valid_from", pa.timestamp("us", tz="UTC"), nullable=False),
                pa.field("valid_to", pa.timestamp("us", tz="UTC"), nullable=True),
                pa.field("reject_reasons", pa.list_(pa.string()), nullable=True),
                pa.field("scoring_version", pa.string(), nullable=False),
                pa.field("classifier_revision", pa.string(), nullable=False),
                pa.field("quality_diagnostics_json", pa.string(), nullable=True),
                pa.field("policy_revision", pa.string(), nullable=False),
                pa.field("snapshot_id", pa.int64(), nullable=True),
                pa.field("trace_id", pa.string(), nullable=False),
                pa.field("source_feed", pa.string(), nullable=False),
                pa.field("source_format", pa.string(), nullable=False),
                pa.field("extraction_pipeline", pa.string(), nullable=False),
                pa.field("spdx_license", pa.string(), nullable=True),
                pa.field("spdx_license_source", pa.string(), nullable=False),
                pa.field("scientific_artifact_s3_uri", pa.string(), nullable=True),
                pa.field("figure_count", pa.int32(), nullable=False),
                pa.field("table_count", pa.int32(), nullable=False),
                pa.field("equation_count", pa.int32(), nullable=False),
                pa.field("citation_count", pa.int32(), nullable=False),
                pa.field("extraction_warnings", pa.list_(pa.string()), nullable=True),
                pa.field("lang_score", pa.float64(), nullable=False),
                pa.field("gopher_pass", pa.bool_(), nullable=False),
                pa.field("c4_nopunc_pass", pa.bool_(), nullable=False),
                pa.field("c4_curly_brace_pass", pa.bool_(), nullable=False),
                pa.field("c4_lorem_ipsum_pass", pa.bool_(), nullable=False),
                pa.field("c4_fraction_lines_with_punct", pa.float64(), nullable=False),
                pa.field("perplexity", pa.float64(), nullable=False),
                pa.field("perplexity_bucket", pa.string(), nullable=False),
                pa.field("perplexity_scorer", pa.string(), nullable=False),
                pa.field("near_duplicate", pa.bool_(), nullable=False),
                pa.field("near_dup_cluster_id", pa.string(), nullable=True),
                pa.field("minhash_backend", pa.string(), nullable=False),
                pa.field("lsh_backend", pa.string(), nullable=False),
                pa.field("minhash_num_perms", pa.int32(), nullable=False),
                pa.field("structural_quality_score", pa.float64(), nullable=False),
                pa.field("extraction_completeness", pa.float64(), nullable=False),
                pa.field("reasoning_score", pa.float64(), nullable=False),
                pa.field("route", pa.string(), nullable=False),
                pa.field("eligible_routes", pa.list_(pa.string()), nullable=True),
                pa.field("route_reasons", pa.list_(pa.string()), nullable=True),
                pa.field("content_tags", pa.list_(pa.string()), nullable=True),
                pa.field("segment_scores_json", pa.string(), nullable=False),
                pa.field("projection_version", pa.string(), nullable=False),
                pa.field("source_word_count", pa.int32(), nullable=False),
                pa.field("training_word_count", pa.int32(), nullable=False),
                pa.field("included_section_count", pa.int32(), nullable=False),
                pa.field("excluded_section_count", pa.int32(), nullable=False),
                pa.field("excluded_sections", pa.list_(pa.string()), nullable=True),
                pa.field("metadata_pii_flags", pa.list_(pa.string()), nullable=True),
                pa.field("removed_body_pii_flags", pa.list_(pa.string()), nullable=True),
                pa.field("pii_action", pa.string(), nullable=False),
                pa.field("pii_scanner_revision", pa.string(), nullable=False),
                pa.field("lang_detector_revision", pa.string(), nullable=False),
                pa.field("tokenizer_revision", pa.string(), nullable=False),
                pa.field("gopher_word_count", pa.int32(), nullable=False),
                pa.field("gopher_mean_word_len", pa.float64(), nullable=False),
                pa.field("gopher_stopword_ratio", pa.float64(), nullable=False),
                pa.field("gopher_bullet_line_ratio", pa.float64(), nullable=False),
                pa.field("gopher_ellipsis_line_ratio", pa.float64(), nullable=False),
                pa.field("gopher_symbol_word_ratio", pa.float64(), nullable=False),
                pa.field("gopher_alpha_word_ratio", pa.float64(), nullable=False),
                pa.field("classifier_backend", pa.string(), nullable=False),
                pa.field("training_usage", pa.string(), nullable=True),
            ]
        )
        return pa.table(cols, schema=arrow_schema)

    def _append(
        self, table: Table, arrow_table: pa.Table, watermark: datetime | None
    ) -> int | None:
        """Atomic micro-batch append; returns new snapshot id.

        Errors propagate to the caller so the in-flight buffer can be
        preserved and retried. ``None`` is reserved for the rare case where
        the append succeeded but the catalog could not surface a snapshot
        id (treated as failure to keep the contract simple).
        """
        table.append(arrow_table)
        snapshot = table.current_snapshot()
        return int(snapshot.snapshot_id) if snapshot else None


def _decision_key(record: GoldRecord) -> DecisionKey:
    """Stable identity of one deterministic decision recipe for a document."""
    return (
        record.doc_id,
        record.scoring_version,
        record.classifier_revision,
        record.policy_revision,
    )


def _rows_watermark(rows: list[GoldRecord]) -> datetime | None:
    return max((row.valid_from for row in rows), default=None)


def _is_trainable_gold(record: GoldRecord) -> bool:
    """Defensive writer-side guard for the clean-only Gold contract."""
    return (
        record.risk_tier == 1
        and record.route
        in {"pretrain", "broad_pretraining", "posttrain_candidate", "reasoning_candidate"}
        and not record.reject_reasons
        and not record.pii_flags
    )


def build_dataflow(
    cfg: common.ProcessorConfig,
    *,
    runtime_status: common.BytewaxRuntimeStatus | None = None,
) -> object:
    """Persist the authoritative decision stream and accepted Gold subset."""
    from bytewax import operators as op
    from bytewax.dataflow import Dataflow

    tracer = common.init_tracer("s2p-iceberg-writer", cfg)
    writer = IcebergWriter.from_config(cfg, metrics=PROCESSOR_METRICS)
    from processor.iceberg_catalog import load_runtime_catalog

    admission_writer = LicenseAdmissionWriter(load_runtime_catalog(cfg))
    failure_writer = common.DurableProcessingFailureWriter.from_config(cfg)
    flush_records = _positive_int_env("S2P_FLUSH_RECORDS", DEFAULT_BATCH_SIZE)
    flush_interval = timedelta(
        seconds=_positive_int_env(
            "S2P_FLUSH_INTERVAL_SECONDS",
            DEFAULT_FLUSH_INTERVAL_SECONDS,
        )
    )
    flow = Dataflow(os.environ.get("S2P_BYTEWAX_FLOW_NAME", "s2p-iceberg-writer-live-v2"))
    # The configured offset is only the bootstrap frontier for a new recovery
    # database. Once snapshots exist, Bytewax recovery owns progress.
    start_offset = common.kafka_starting_offset()
    source = common.tracked_kafka_source(
        runtime_status=runtime_status,
        source_name="curation_decisions",
        brokers=cfg.redpanda_brokers.split(","),
        topics=[cfg.decisions_topic],
        starting_offset=start_offset,
        add_config=common.kafka_consumer_config(cfg.consumer_group),
        batch_size=common.kafka_source_batch_size(),
    )
    inp = op.input("docs_curated", flow, source)

    def _decode_gold(msg: object) -> GoldRecord | None:
        payload = getattr(msg, "value", None)
        if payload is None:
            failure_writer.record(stage="iceberg-gold", message=msg, reason="kafka_tombstone")
            PROCESSOR_METRICS.record_failure(stage="iceberg", reason="kafka_tombstone")
            return None
        try:
            return common.gold_loads(payload)
        except Exception as exc:
            with tracer.start_as_current_span("iceberg.decode") as span:
                span.record_exception(exc)
            failure_writer.record(
                stage="iceberg-gold",
                message=msg,
                reason=type(exc).__name__,
            )
            PROCESSOR_METRICS.record_failure(stage="iceberg", reason=type(exc).__name__)
            return None

    def _ingest(batch: tuple[str, list[GoldRecord]]) -> None:
        with tracer.start_as_current_span("iceberg.append") as span:
            _key, rows = batch
            stats = None
            for row in rows:
                result = writer.add(row)
                if result is not None:
                    stats = result
            tail = writer.flush()
            if tail.decisions_committed or tail.rows_committed:
                stats = tail
            if stats is None:
                return
            span.set_attribute("batch_records", len(rows))
            span.set_attribute("rows_committed", stats.rows_committed)
            span.set_attribute("decisions_committed", stats.decisions_committed)
            span.set_attribute(
                "snapshot_id",
                stats.snapshot_id if stats.snapshot_id is not None else -1,
            )

    decoded = op.filter_map("decode_curated", inp, _decode_gold)
    keyed = op.key_on("key_curated", decoded, lambda _record: "iceberg")
    batches = op.collect(
        "batch_curated",
        keyed,
        timeout=flush_interval,
        max_size=flush_records,
    )
    op.inspect("iceberg_write", batches, lambda _step, batch: _ingest(batch))

    admission_source = common.tracked_kafka_source(
        runtime_status=runtime_status,
        source_name="license_admissions",
        brokers=cfg.redpanda_brokers.split(","),
        topics=[cfg.license_admissions_topic],
        starting_offset=start_offset,
        add_config=common.kafka_consumer_config(f"{cfg.consumer_group}-licenses"),
        batch_size=common.kafka_source_batch_size(),
    )
    admission_inp = op.input("license_admissions", flow, admission_source)

    def _decode_admission(msg: object) -> LicenseAdmissionDecision | None:
        payload = getattr(msg, "value", None)
        if payload is None:
            failure_writer.record(
                stage="iceberg-license-admission",
                message=msg,
                reason="kafka_tombstone",
            )
            PROCESSOR_METRICS.record_failure(stage="iceberg", reason="kafka_tombstone")
            return None
        try:
            return LicenseAdmissionDecision.model_validate_json(payload)
        except ValueError as exc:
            failure_writer.record(
                stage="iceberg-license-admission",
                message=msg,
                reason=type(exc).__name__,
            )
            PROCESSOR_METRICS.record_failure(stage="iceberg", reason=type(exc).__name__)
            return None

    def _ingest_admission(batch: tuple[str, list[LicenseAdmissionDecision]]) -> None:
        with tracer.start_as_current_span("iceberg.license_admission") as span:
            try:
                _key, decisions = batch
                committed = admission_writer.add_batch(decisions)
                span.set_attribute("batch_records", len(decisions))
                span.set_attribute("committed", committed)
            except Exception as exc:
                span.record_exception(exc)
                # Never acknowledge a Kafka admission record that failed to
                # reach the durable ledger. Bytewax must retry it.
                raise

    decoded_admissions = op.filter_map(
        "decode_license_admissions", admission_inp, _decode_admission
    )
    keyed_admissions = op.key_on(
        "key_license_admissions",
        decoded_admissions,
        lambda _decision: "iceberg",
    )
    admission_batches = op.collect(
        "batch_license_admissions",
        keyed_admissions,
        timeout=flush_interval,
        max_size=flush_records,
    )
    op.inspect(
        "iceberg_license_admission_write",
        admission_batches,
        lambda _step, batch: _ingest_admission(batch),
    )
    return flow


def main() -> None:
    """Entrypoint for the ``s2p-iceberg-writer`` console script."""
    cfg = common.load_config()
    common.configure_logging(cfg.log_level, json_output=not cfg.is_dev)
    log = common.get_logger("s2p.iceberg")
    log.info(
        "starting iceberg writer",
        decisions_topic=cfg.decisions_topic,
        license_admissions_topic=cfg.license_admissions_topic,
    )
    runtime_status = common.BytewaxRuntimeStatus()
    flow = build_dataflow(cfg, runtime_status=runtime_status)
    start_probe_server(
        metrics_provider=PROCESSOR_METRICS.render_prometheus,
        readiness_provider=runtime_status.is_ready,
    )
    common.run_bytewax_flow(
        flow,
        cfg,
        os.environ.get("S2P_BYTEWAX_RECOVERY_NAME", "iceberg-writer-live-v2"),
        runtime_status=runtime_status,
    )
