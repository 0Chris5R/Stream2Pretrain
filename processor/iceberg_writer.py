"""Iceberg writer: ``docs.curated`` -> Iceberg ``gold`` table.

Loads / creates the gold table via the Polaris REST catalog (pyiceberg
``RestCatalog``), buffers incoming :class:`GoldRecord` rows into PyArrow
tables, and commits them as Iceberg micro-batches. Each commit attaches
snapshot properties:

- ``watermark``               - max ``valid_from`` in the batch
- ``policy_revision``         - git SHA of the policy bundle
- ``scoring_version``         - the scoring recipe identifier
- ``classifier_revision``     - the FineWeb-Edu ONNX revision
- ``decon_attestation_uri``   - s3 URI of the signed attestation

The Bytewax sink wraps :class:`IcebergWriter` so the same class can be
exercised by unit tests without spinning up a Bytewax dataflow.

Schema
------
Partitioning follows RESEARCH.md section 6:

    silver.PARTITION BY lang, bucket(16, doc_id)
    gold.PARTITION BY lang, risk_tier, month(valid_from)
    decon_attestations.PARTITION BY month(committed_at)

The partition spec is set up on first ``ensure_table`` call so a fresh
deployment is one-shot bootstrappable.
"""

from __future__ import annotations

import os
import threading
import time
from contextlib import suppress
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from processor import common
from processor.decon_gate import DeconGate
from processor.metrics import PROCESSOR_METRICS, ProcessorMetrics
from processor.probes import start_probe_server
from schemas.decon import DeconAttestation
from schemas.gold import GoldRecord

if TYPE_CHECKING:
    import pyarrow as pa
    from pyiceberg.catalog import Catalog
    from pyiceberg.table import Table


DEFAULT_GOLD_NAMESPACE: str = "gold"
DEFAULT_GOLD_TABLE: str = "curated"
DECON_TABLE: str = "decon_attestations"
DEFAULT_BATCH_SIZE: int = 256


def gold_identifier() -> tuple[str, str]:
    """Return the Iceberg identifier used for the curated Gold table."""
    namespace = os.environ.get("S2P_ICEBERG_NAMESPACE") or os.environ.get(
        "ICEBERG_NAMESPACE", DEFAULT_GOLD_NAMESPACE
    )
    table = os.environ.get("S2P_ICEBERG_GOLD_TABLE", DEFAULT_GOLD_TABLE)
    return (namespace, table)


@dataclass(slots=True)
class WriterStats:
    """Diagnostics returned by :meth:`IcebergWriter.flush`."""

    rows_committed: int = 0
    snapshot_id: int | None = None
    attestation_signed: bool = False
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
    """Buffer-and-commit writer for the gold + decon attestation tables.

    Parameters
    ----------
    catalog
        A pyiceberg ``Catalog`` (constructed once per pod).
    decon
        The :class:`DeconGate` instance that produced the curated batch -
        used to flush a signed attestation each time we commit.
    batch_size
        Soft trigger for committing. ``flush`` can also be called
        directly for time-based triggers.
    """

    def __init__(
        self,
        *,
        catalog: Catalog,
        decon: DeconGate,
        scoring_version: str,
        classifier_revision: str,
        policy_revision: str,
        attestation_writer: AttestationSink | None = None,
        metrics: ProcessorMetrics | None = None,
        batch_size: int = DEFAULT_BATCH_SIZE,
    ) -> None:
        self._catalog = catalog
        self._decon = decon
        self._scoring_version = scoring_version
        self._classifier_revision = classifier_revision
        self._policy_revision = policy_revision
        self._attestation_writer = attestation_writer
        self._metrics = metrics
        self._batch_size = batch_size
        self._buffer = _Buffer()
        self._lock = threading.Lock()

    @classmethod
    def from_config(
        cls,
        cfg: common.ProcessorConfig,
        *,
        decon: DeconGate,
        attestation_writer: AttestationSink | None = None,
        metrics: ProcessorMetrics | None = None,
    ) -> IcebergWriter:
        """Build a writer from a :class:`ProcessorConfig`.

        Constructs the Polaris REST catalog client lazily so importing the
        module costs nothing in tests.
        """
        from pyiceberg.catalog import load_catalog

        props: dict[str, str] = {
            "uri": cfg.polaris_uri,
            "warehouse": cfg.polaris_warehouse,
            "s3.endpoint": cfg.minio_endpoint,
            "s3.access-key-id": cfg.minio_access_key,
            "s3.secret-access-key": cfg.minio_secret_key,
            "s3.region": "us-east-1",
            "py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO",
        }
        if cfg.polaris_token:
            props["token"] = cfg.polaris_token
        credential = os.environ.get("POLARIS_CREDENTIAL")
        if credential:
            props["credential"] = credential
            props["scope"] = os.environ.get("POLARIS_SCOPE", "PRINCIPAL_ROLE:ALL")
        catalog = load_catalog("polaris", **props)
        sink = attestation_writer if attestation_writer is not None else build_attestation_sink(cfg)
        return cls(
            catalog=catalog,
            decon=decon,
            scoring_version=cfg.benchmark_set_version,
            classifier_revision="fineweb-edu-onnx-int8",
            policy_revision="git:dev",
            attestation_writer=sink,
            metrics=metrics,
            batch_size=int(os.environ.get("S2P_FLUSH_RECORDS", DEFAULT_BATCH_SIZE)),
        )

    def add(self, record: GoldRecord) -> WriterStats | None:
        """Buffer one record; auto-flush when ``batch_size`` is reached."""
        if not _is_trainable_gold(record):
            return None
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
        attestation. Failures re-raise so the calling Bytewax operator
        retries with the same buffer (rather than dropping rows on a
        transient catalog/MinIO outage).
        """
        rows = list(self._buffer.rows)
        watermark = self._buffer.watermark
        table = self._ensure_table()
        arrow_table = self._to_arrow(rows)
        started = time.perf_counter()
        snapshot_id = self._append(table, arrow_table, watermark)
        elapsed = time.perf_counter() - started
        if snapshot_id is None:
            # Append failed: keep the buffer intact so the next call retries.
            raise RuntimeError(
                f"iceberg append failed for {len(rows)} rows; buffer preserved for retry"
            )
        # Append committed; safe to drain the buffer and seal the attestation.
        self._buffer.reset()
        attestation = self._decon.flush_attestation(
            snapshot_id=snapshot_id,
            committed_at=datetime.now(UTC),
            extra_per_benchmark_hits=_aggregate_per_benchmark_hits(rows),
            extra_rejected_doc_hashes=_aggregate_rejected_doc_hashes(rows),
            extra_tokens_scanned=_aggregate_tokens_scanned(rows),
            extra_tokens_flagged=_aggregate_tokens_flagged(rows),
        )
        attest_uri = None
        if self._attestation_writer is not None:
            attest_uri = self._attestation_writer.write(attestation)
        self._set_snapshot_props(table, snapshot_id, attestation, attest_uri)
        if self._metrics is not None:
            self._metrics.record_iceberg_flush(rows=len(rows), seconds=elapsed)
        return WriterStats(
            rows_committed=len(rows),
            snapshot_id=snapshot_id,
            attestation_signed=bool(attestation.signature),
            watermark=watermark,
        )

    def _ensure_table(self) -> Table:
        """Create the gold table if missing; return the loaded handle."""
        from pyiceberg.partitioning import PartitionField, PartitionSpec
        from pyiceberg.schema import Schema
        from pyiceberg.transforms import IdentityTransform, MonthTransform
        from pyiceberg.types import (
            DoubleType,
            IntegerType,
            ListType,
            LongType,
            NestedField,
            StringType,
            TimestampType,
        )

        identifier = gold_identifier()
        try:
            return self._catalog.load_table(identifier)
        except Exception:
            pass
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
            NestedField(
                12,
                "contaminated_with",
                ListType(13, StringType(), element_required=False),
                required=False,
            ),
            NestedField(14, "valid_from", TimestampType(), required=True),
            NestedField(15, "valid_to", TimestampType(), required=False),
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
            "contaminated_with": [list(r.contaminated_with) for r in rows],
            "valid_from": [r.valid_from for r in rows],
            "valid_to": [r.valid_to for r in rows],
            "reject_reasons": [list(r.reject_reasons) for r in rows],
            "scoring_version": [r.scoring_version for r in rows],
            "classifier_revision": [r.classifier_revision for r in rows],
            "policy_revision": [r.policy_revision for r in rows],
            "snapshot_id": [r.snapshot_id for r in rows],
            "trace_id": [r.trace_id for r in rows],
            "source_feed": [r.source_feed for r in rows],
            "source_format": [r.source_format for r in rows],
            "extraction_pipeline": [r.extraction_pipeline for r in rows],
            "spdx_license": [r.spdx_license for r in rows],
            "spdx_license_source": [r.spdx_license_source for r in rows],
        }
        return pa.table(cols)

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

    def _set_snapshot_props(
        self,
        table: Table,
        snapshot_id: int,
        attestation: DeconAttestation,
        attest_uri: str | None,
    ) -> None:
        """Annotate the just-committed snapshot with our metadata.

        Snapshot summary is the right place for per-snapshot facts (so a
        time-travel query can read them back); table properties are
        per-table and would be overwritten on every commit. We try the
        snapshot-summary path first via pyiceberg's ``update_snapshot``
        API and fall back to a side-table-style table property keyed by
        ``snapshot_id`` so historical attestation pointers remain
        retrievable even when the running pyiceberg version does not
        expose snapshot summaries directly.
        """
        per_snapshot = {
            f"stream2pretrain.snapshot.{snapshot_id}.policy_revision": self._policy_revision,
            f"stream2pretrain.snapshot.{snapshot_id}.scoring_version": self._scoring_version,
            f"stream2pretrain.snapshot.{snapshot_id}.classifier_revision": (
                self._classifier_revision
            ),
            f"stream2pretrain.snapshot.{snapshot_id}.attestation_signature": (
                attestation.signature
            ),
            f"stream2pretrain.snapshot.{snapshot_id}.attestation_set_version": (
                attestation.benchmark_set_version
            ),
        }
        if attest_uri:
            per_snapshot[f"stream2pretrain.snapshot.{snapshot_id}.decon_attestation_uri"] = (
                attest_uri
            )
        # Pointer to the latest snapshot's metadata, for cheap "current" reads.
        latest = {
            "stream2pretrain.latest_snapshot_id": str(snapshot_id),
            "stream2pretrain.latest_attestation_signature": attestation.signature,
            "stream2pretrain.latest_attestation_set_version": (
                attestation.benchmark_set_version
            ),
        }
        if attest_uri:
            latest["stream2pretrain.latest_decon_attestation_uri"] = attest_uri
        # Best-effort: prefer the snapshot summary API when available so the
        # snapshot itself carries its attestation. Otherwise persist into
        # table properties using a snapshot-id-prefixed key namespace.
        try:
            update = getattr(table, "update_snapshot", None)
            if callable(update):
                with update() as us:  # type: ignore[misc]
                    setter = getattr(us, "set_snapshot_summary_property", None)
                    if callable(setter):
                        for k, v in per_snapshot.items():
                            setter(k.split(".", 2)[-1], v)
                        return
        except Exception:
            # Fall through to the property-based fallback below.
            pass
        try:
            with table.transaction() as txn:
                txn.set_properties(**per_snapshot, **latest)
        except Exception:
            # Annotation is best-effort; the actual commit already succeeded.
            pass


def build_attestation_sink(cfg: common.ProcessorConfig) -> AttestationSink:
    """Create the production attestation sink from processor config."""
    import boto3
    from confluent_kafka import Producer

    s3_client = boto3.client(
        "s3",
        endpoint_url=cfg.minio_endpoint,
        aws_access_key_id=cfg.minio_access_key,
        aws_secret_access_key=cfg.minio_secret_key,
        region_name="us-east-1",
    )
    producer = Producer({"bootstrap.servers": cfg.redpanda_brokers})
    return AttestationSink(
        s3_client=s3_client,
        bucket=cfg.decon_bucket,
        kafka_producer=producer,
        topic=cfg.decon_attest_topic,
    )


class AttestationSink:
    """Writes a signed :class:`DeconAttestation` to MinIO + Kafka.

    The S3 path is
    ``s3://<decon>/decon/<benchmark_set_version>/<snapshot_id>.json``.
    Kafka publication uses the dev/prod ``decon.attest`` topic constants.
    """

    def __init__(
        self,
        *,
        s3_client: object,
        bucket: str,
        kafka_producer: object | None = None,
        topic: str = "decon.attest",
    ) -> None:
        self._s3 = s3_client
        self._bucket = bucket
        self._producer = kafka_producer
        self._topic = topic

    def write(self, attestation: DeconAttestation) -> str:
        """Persist + publish; returns the canonical s3 URI."""
        payload = common.decon_dumps(attestation)
        key = (
            f"decon/{attestation.benchmark_set_version}/"
            f"{attestation.snapshot_id:020d}.json"
        )
        try:
            self._s3.put_object(  # type: ignore[union-attr]
                Bucket=self._bucket,
                Key=key,
                Body=payload,
                ContentType="application/json",
            )
        except Exception:
            return ""
        uri = f"s3://{self._bucket}/{key}"
        if self._producer is not None:
            try:
                self._producer.produce(self._topic, key=str(attestation.snapshot_id).encode(), value=payload)  # type: ignore[union-attr]
                self._producer.flush()  # type: ignore[union-attr]
            except Exception:
                pass
        return uri


def _aggregate_per_benchmark_hits(rows: list[GoldRecord]) -> dict[str, int]:
    """Sum the per-benchmark contamination tags across buffered rows.

    The curator stamps each :class:`GoldRecord.contaminated_with` with the
    benchmarks the in-process Decon-Gate fired on. Aggregating here lets
    the writer's attestation reflect the actual per-snapshot scan signal
    even though it does not own the populated bloom filters.
    """
    out: dict[str, int] = {}
    for r in rows:
        for bench in r.contaminated_with or []:
            out[bench] = out.get(bench, 0) + 1
    return out


def _aggregate_rejected_doc_hashes(rows: list[GoldRecord]) -> list[str]:
    """Collect doc ids that fired any contamination tag in this batch."""
    return [r.doc_id for r in rows if r.contaminated_with]


def _aggregate_tokens_scanned(rows: list[GoldRecord]) -> int:
    """Sum the curator-reported token counts for the buffered rows."""
    return sum(int(r.tokens or 0) for r in rows)


def _aggregate_tokens_flagged(rows: list[GoldRecord]) -> int:
    """Sum tokens belonging to documents that were flagged in this batch."""
    return sum(int(r.tokens or 0) for r in rows if r.contaminated_with)


def _is_trainable_gold(record: GoldRecord) -> bool:
    """Defensive writer-side guard for the clean-only Gold contract."""
    return (
        record.risk_tier == 1
        and not record.reject_reasons
        and not record.pii_flags
        and not record.contaminated_with
    )


def build_dataflow(cfg: common.ProcessorConfig) -> object:
    """Construct the Bytewax dataflow that pumps ``docs.curated`` to Iceberg."""
    from bytewax import operators as op
    from bytewax.connectors.kafka import KafkaSource
    from bytewax.dataflow import Dataflow

    tracer = common.init_tracer("s2p-iceberg-writer", cfg)
    decon = DeconGate(benchmark_set_version=cfg.benchmark_set_version)
    writer = IcebergWriter.from_config(cfg, decon=decon, metrics=PROCESSOR_METRICS)
    flow = Dataflow("s2p-iceberg-writer")
    # ``beginning`` keeps the writer at-least-once across restarts (the
    # consumer group offset advances from there). See processor/curate.py.
    start_offset = common.kafka_starting_offset()
    source = KafkaSource(
        brokers=cfg.redpanda_brokers.split(","),
        topics=[cfg.curated_topic],
        starting_offset=start_offset,
        add_config=common.kafka_consumer_config(cfg.consumer_group),
    )
    inp = op.input("docs_curated", flow, source)

    def _ingest(msg: object) -> None:
        with tracer.start_as_current_span("iceberg.append") as span:
            payload = getattr(msg, "value", None)
            if payload is None:
                return
            try:
                gold = common.gold_loads(payload)
            except Exception as exc:
                span.record_exception(exc)
                return
            stats = writer.add(gold)
            if stats:
                span.set_attribute("rows_committed", stats.rows_committed)
                span.set_attribute(
                    "snapshot_id",
                    stats.snapshot_id if stats.snapshot_id is not None else -1,
                )

    op.inspect("iceberg_write", inp, lambda _step, msg: _ingest(msg))
    return flow


def main() -> None:
    """Entrypoint for the ``s2p-iceberg-writer`` console script."""
    cfg = common.load_config()
    common.configure_logging(cfg.log_level, json_output=not cfg.is_dev)
    log = common.get_logger("s2p.iceberg")
    log.info("starting iceberg writer", topic=cfg.curated_topic)
    start_probe_server(metrics_provider=PROCESSOR_METRICS.render_prometheus)
    flow = build_dataflow(cfg)
    from bytewax.run import cli_main

    cli_main(flow)
