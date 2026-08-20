"""Bytewax one-shot Job: stream the v0.2.0 seed mixture into ``docs.normalized``.

Composes the five seed-corpus components from :mod:`processor.seed`:

1. ``allenai/peS2o``                       (cs.* slice)
2. ``togethercomputer/RedPajama-Data-1T``  (config ``arxiv``)
3. ``HuggingFaceFW/fineweb-edu``           (URL-allowlisted)
4. ``HuggingFaceTB/stack-edu``             (Python+ML)
5. Custom Wayback backfill                 (24-month RSS/Atom replay)

Each :class:`processor.seed.types.SeedDocument` is mapped onto a
:class:`schemas.silver.SilverRecord` and emitted to ``docs.normalized``.
The ``source_feed = "seed:<repo_id>"`` Kafka header is a transport-only
debug aid; ``SilverRecord`` does not carry ``source_feed``, so curate.py
(which reads ``msg.value`` only) cannot dispatch on it. Downstream
seed-vs-live discrimination is done via :attr:`SilverRecord.extraction_pipeline`
instead - every seed component stamps a distinct id (``pes2o-seed-2026-06``,
``redpajama-arxiv-2023-04``, ``fineweb-edu-2024``, ``stack-edu-2024``,
``wayback-backfill-2026-06``) that no live extractor produces. Iceberg
``as_of(timestamp)`` queries should filter on
``extraction_pipeline LIKE '%-seed-%' OR extraction_pipeline IN
('redpajama-arxiv-2023-04', 'fineweb-edu-2024', 'stack-edu-2024',
'wayback-backfill-2026-06')`` to scope to seed-derived rows.

Run modes
---------
- ``S2P_SEED_COMPONENTS=pes2o,redpajama-arxiv,fineweb-edu,stack-edu,wayback``
  selects which components to ingest. Default: all five.
- ``S2P_SEED_MAX_DOCS_PER_COMPONENT`` caps each component for smoke runs.
- ``S2P_SEED_DRY_RUN=1`` prints SilverRecord stats without emitting.

Determinism
-----------
On rerun the loader reads ``s3://<state_bucket>/seed-loader/<repo_id>.cursor.json``
via :class:`processor.seed.cursor.CursorStore` and skips rows whose
``native_id`` sorts <= the cursor. The cursor advances every
``CURSOR_FLUSH_INTERVAL`` rows so a kill -9 mid-Job never replays more than
that many rows.

Honest scope notes
------------------
- HuggingFace ``datasets`` cache directory size is unbounded for the full
  ingest. PVC sizing is documented in
  :file:`charts/stream2pretrain/values.yaml` (``seedLoader.hf_cache_pvc``).
- We never propagate ``valid_from = fetched_at`` for seed records. If a
  per-row publication date is missing, the per-component loader picks the
  dataset's release / cutoff date and tags ``valid_from_source`` with
  ``dataset_metadata`` to stay honest.
"""

from __future__ import annotations

import hashlib
import os
import secrets
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import boto3

from ingest.common.license_admission import decide_license_admission
from processor import common
from processor.seed import (
    fineweb_edu_filter,
    pes2o,
    redpajama_arxiv,
    stack_edu_filter,
    wayback_backfill,
)
from processor.seed.cursor import CursorStore, SeedCursor
from processor.seed.types import SeedDocument
from schemas.license_admission import LicenseAdmissionDecision
from schemas.silver import SilverRecord, SilverTags

CURSOR_FLUSH_INTERVAL: int = 200
"""Save the cursor every N emitted rows. Keeps the worst-case replay
small without paying the MinIO PutObject cost on every record."""

DEFAULT_COMPONENTS: tuple[str, ...] = (
    "pes2o",
    "redpajama-arxiv",
    "fineweb-edu",
    "stack-edu",
    "wayback",
)


@dataclass(frozen=True, slots=True)
class SeedLoaderConfig:
    """Runtime config for the seed loader Job.

    Distinct from :class:`processor.common.ProcessorConfig` so the seed
    loader can run with a smaller surface (no decon-gate, no benchmark
    paths). The processor config is still used for Redpanda + MinIO + OTel.
    """

    components: tuple[str, ...]
    max_docs_per_component: int | None
    dry_run: bool
    state_bucket: str
    fineweb_url_allowlist: tuple[str, ...]
    wayback_months: int


def load_seed_config() -> SeedLoaderConfig:
    """Build the seed-specific config from the process environment."""
    raw_components = os.environ.get("S2P_SEED_COMPONENTS", ",".join(DEFAULT_COMPONENTS))
    components = tuple(c.strip() for c in raw_components.split(",") if c.strip())
    max_raw = os.environ.get("S2P_SEED_MAX_DOCS_PER_COMPONENT")
    max_docs = int(max_raw) if max_raw and max_raw.isdigit() else None
    dry_run = os.environ.get("S2P_SEED_DRY_RUN", "0") == "1"
    state_bucket = os.environ.get("S2P_STATE_BUCKET", "state")
    raw_allow = os.environ.get("S2P_SEED_FINEWEB_ALLOWLIST", "")
    if raw_allow:
        allowlist = tuple(x.strip() for x in raw_allow.split(",") if x.strip())
    else:
        allowlist = fineweb_edu_filter.DEFAULT_URL_ALLOWLIST
    months_raw = os.environ.get("S2P_SEED_WAYBACK_MONTHS", "24")
    try:
        months = int(months_raw)
    except ValueError:
        months = 24
    return SeedLoaderConfig(
        components=components,
        max_docs_per_component=max_docs,
        dry_run=dry_run,
        state_bucket=state_bucket,
        fineweb_url_allowlist=allowlist,
        wayback_months=months,
    )


# ---------------------------------------------------------------------------
# SeedDocument -> SilverRecord
# ---------------------------------------------------------------------------


def _doc_id_for(repo_id: str, native_id: str) -> str:
    """sha256 of ``<repo_id>:<native_id>`` matched to schemas.bronze.DocId."""
    payload = f"{repo_id}:{native_id}".encode()
    return "sha256:" + hashlib.sha256(payload).hexdigest()


def _new_trace_id() -> str:
    """Fresh 32-char hex trace id for one row."""
    return secrets.token_hex(16)


def _normalize_seed_url(repo_id: str, raw_url: str, native_id: str) -> str:
    """Rewrite ``hf://<repo_id>/<native_id>`` synthetic URIs to the canonical
    HuggingFace dataset viewer URL so they pass :class:`pydantic.HttpUrl`
    validation on :class:`SilverRecord.url`.

    Per-component loaders intentionally fall back to ``hf://...`` when the
    upstream row carries no real URL (peS2o has no per-row URL at all;
    RedPajama-arxiv falls back to ``hf://`` for ``sha:`` ids; Stack-Edu
    falls back when repo+path are missing). Without this rewrite,
    SilverRecord construction inside the dataflow throws ValidationError
    and the entire component partition tears down silently. The rewrite is
    idempotent for already-https URLs.
    """
    if not raw_url:
        # Final fallback: a viewer URL keyed on repo_id alone. native_id is
        # appended as a fragment so the URL stays unique per row.
        safe = native_id.replace("#", "_")
        return f"https://huggingface.co/datasets/{repo_id}#{safe}"
    if raw_url.startswith("hf://"):
        # ``hf://<repo_id>/<native_id>`` -> dataset viewer URL with the
        # native id encoded as a fragment.
        tail = raw_url[len("hf://") :]
        if "/" in tail:
            repo_part, _, nid_part = tail.partition("/")
            safe = nid_part.replace("#", "_")
            return f"https://huggingface.co/datasets/{repo_part}#{safe}"
        return f"https://huggingface.co/datasets/{tail}"
    return raw_url


def to_silver(doc: SeedDocument, *, trace_id: str | None = None) -> SilverRecord:
    """Map a :class:`SeedDocument` onto a :class:`SilverRecord`.

    The downstream ``curate`` dataflow re-runs Gopher / C4 / KenLM / MinHash
    so this function emits placeholder tags and a zero-byte MinHash
    signature. The signature byte length stays at the schema-default 112
    permutations x 4 bytes = 448 bytes; the ``rensa`` backend below will
    overwrite it.
    """
    title = doc.title
    if title is not None and len(title) > 2048:
        title = title[:2048]
    sig = bytes(112 * 4)
    silver_url = _normalize_seed_url(doc.repo_id, doc.url, doc.native_id)
    return SilverRecord(
        doc_id=_doc_id_for(doc.repo_id, doc.native_id),
        url=silver_url,
        title=title,
        text=doc.text,
        lang=doc.lang,
        lang_score=1.0,
        extracted_with=doc.extraction_pipeline,
        tags=SilverTags(
            gopher_pass=True,
            c4_nopunc_pass=True,
            perplexity=0.0,
            perplexity_bucket="head",
        ),
        minhash_sig=sig,
        minhash_backend="placeholder",
        minhash_num_perms=112,
        near_dup_cluster_id=None,
        valid_from=doc.valid_from.astimezone(UTC),
        valid_to=None,
        valid_from_source="dataset_metadata",
        trace_id=trace_id or _new_trace_id(),
        source_format=doc.source_format,
        extraction_pipeline=doc.extraction_pipeline,
        spdx_license=doc.spdx_license,
        spdx_license_source=doc.spdx_license_source,
    )


def seed_admission(doc: SeedDocument) -> LicenseAdmissionDecision:
    """Decide a seed row before it can enter downstream model processing."""
    source_url = _normalize_seed_url(doc.repo_id, doc.url, doc.native_id)
    return decide_license_admission(
        source_url=source_url,
        source_feed=f"seed:{doc.repo_id}",
        license_value=doc.spdx_license,
        license_source=doc.spdx_license_source,
        source_format=doc.source_format,
    ).decision


# ---------------------------------------------------------------------------
# Component dispatch
# ---------------------------------------------------------------------------


# Registry: component name -> (repo_id, iter_documents factory).
ComponentFactory = Callable[[SeedCursor, SeedLoaderConfig], Iterator[SeedDocument]]


def _pes2o_factory(cursor: SeedCursor, cfg: SeedLoaderConfig) -> Iterator[SeedDocument]:
    return pes2o.iter_documents(cursor, max_docs=cfg.max_docs_per_component)


def _redpajama_factory(cursor: SeedCursor, cfg: SeedLoaderConfig) -> Iterator[SeedDocument]:
    return redpajama_arxiv.iter_documents(cursor, max_docs=cfg.max_docs_per_component)


def _fineweb_factory(cursor: SeedCursor, cfg: SeedLoaderConfig) -> Iterator[SeedDocument]:
    return fineweb_edu_filter.iter_documents(
        cursor,
        allowlist=cfg.fineweb_url_allowlist,
        max_docs=cfg.max_docs_per_component,
    )


def _stack_factory(cursor: SeedCursor, cfg: SeedLoaderConfig) -> Iterator[SeedDocument]:
    return stack_edu_filter.iter_documents(cursor, max_docs=cfg.max_docs_per_component)


def _wayback_factory(cursor: SeedCursor, cfg: SeedLoaderConfig) -> Iterator[SeedDocument]:
    return wayback_backfill.iter_documents(
        cursor,
        months=cfg.wayback_months,
        max_docs=cfg.max_docs_per_component,
    )


COMPONENTS: dict[str, tuple[str, ComponentFactory]] = {
    "pes2o": (pes2o.REPO_ID, _pes2o_factory),
    "redpajama-arxiv": (redpajama_arxiv.REPO_ID, _redpajama_factory),
    "fineweb-edu": (fineweb_edu_filter.REPO_ID, _fineweb_factory),
    "stack-edu": (stack_edu_filter.REPO_ID, _stack_factory),
    "wayback": ("wayback:multi-feed", _wayback_factory),
}


# ---------------------------------------------------------------------------
# Cursor-aware streaming pipeline
# ---------------------------------------------------------------------------


def stream_component(
    name: str,
    *,
    cursor_store: CursorStore,
    cfg: SeedLoaderConfig,
    on_record: Callable[[str, SilverRecord], None],
    on_admission: Callable[[LicenseAdmissionDecision], None] | None = None,
) -> SeedCursor:
    """Stream one component end-to-end; return the final cursor.

    ``on_record`` is the sink: in production it produces to Redpanda, in
    tests it appends to a list. The cursor is flushed every
    ``CURSOR_FLUSH_INTERVAL`` rows AND once at the end.
    """
    if name not in COMPONENTS:
        raise ValueError(f"unknown seed component: {name!r}")
    repo_id, factory = COMPONENTS[name]
    cursor = cursor_store.load(repo_id)
    rows_since_flush = 0
    for doc in factory(cursor, cfg):
        admission = seed_admission(doc)
        if on_admission is not None:
            on_admission(admission)
        if admission.status == "admitted":
            record = to_silver(doc, trace_id=admission.trace_id)
            on_record(repo_id, record)
        cursor.advance(doc.native_id)
        rows_since_flush += 1
        if rows_since_flush >= CURSOR_FLUSH_INTERVAL:
            cursor_store.save(cursor)
            rows_since_flush = 0
    cursor_store.save(cursor)
    return cursor


# ---------------------------------------------------------------------------
# Bytewax dataflow + main
# ---------------------------------------------------------------------------


def build_dataflow(cfg: common.ProcessorConfig, seed_cfg: SeedLoaderConfig) -> object:
    """Build a Bytewax dataflow whose source is the chained component iter
    and whose sink is the ``docs.normalized`` Kafka topic.

    Imports of :mod:`bytewax.*` are deferred inside this function so that
    unit tests can import :mod:`processor.seed_loader` on a CI image
    without the runtime extra installed.
    """
    from bytewax import operators as op
    from bytewax.connectors.kafka import KafkaSink, KafkaSinkMessage
    from bytewax.dataflow import Dataflow
    from bytewax.inputs import FixedPartitionedSource, StatelessSourcePartition

    s3 = boto3.client(
        "s3",
        endpoint_url=cfg.minio_endpoint,
        aws_access_key_id=cfg.minio_access_key,
        aws_secret_access_key=cfg.minio_secret_key,
        region_name="us-east-1",
    )
    cursor_store = CursorStore(s3, bucket=seed_cfg.state_bucket)

    class _Source(FixedPartitionedSource):
        def list_parts(self) -> list[str]:
            # One partition per component so workers parallelize naturally.
            return list(seed_cfg.components)

        def build_part(
            self,
            step_id: str,
            for_part: str,
            resume_state: object | None,
        ) -> StatelessSourcePartition:
            return _ComponentPartition(for_part, cursor_store, seed_cfg)

    class _ComponentPartition(StatelessSourcePartition):
        def __init__(
            self,
            component: str,
            cursor_store: CursorStore,
            seed_cfg: SeedLoaderConfig,
        ) -> None:
            self._component = component
            self._cursor_store = cursor_store
            self._cfg = seed_cfg
            if component not in COMPONENTS:
                self._iter: Iterator[tuple[str, SilverRecord]] = iter(())
                self._repo_id = component
                return
            self._repo_id, factory = COMPONENTS[component]
            cursor = cursor_store.load(self._repo_id)
            self._cursor = cursor
            self._rows_since_flush = 0
            self._docs = factory(cursor, seed_cfg)

        def next_batch(
            self,
        ) -> list[tuple[str, SilverRecord | None, LicenseAdmissionDecision]]:
            try:
                doc = next(self._docs)
            except StopIteration:
                # Final cursor flush before signalling completion.
                self._cursor_store.save(self._cursor)
                raise
            admission = seed_admission(doc)
            record = (
                to_silver(doc, trace_id=admission.trace_id)
                if admission.status == "admitted"
                else None
            )
            self._cursor.advance(doc.native_id)
            self._rows_since_flush += 1
            if self._rows_since_flush >= CURSOR_FLUSH_INTERVAL:
                self._cursor_store.save(self._cursor)
                self._rows_since_flush = 0
            return [(self._repo_id, record, admission)]

    flow = Dataflow("s2p-seed-loader")
    inp = op.input("seed.source", flow, _Source())

    def _to_kafka(
        item: tuple[str, SilverRecord | None, LicenseAdmissionDecision],
    ) -> KafkaSinkMessage:
        repo_id, rec, _ = item
        assert rec is not None
        headers = [
            ("trace_id", rec.trace_id.encode("ascii")),
            ("source_feed", f"seed:{repo_id}".encode()),
        ]
        return KafkaSinkMessage(
            key=rec.doc_id.encode("utf-8"),
            value=common.silver_dumps(rec),
            headers=headers,
        )

    admitted = op.filter("seed.license_admitted", inp, lambda item: item[1] is not None)
    mapped = op.map("seed.to_kafka", admitted, _to_kafka)
    if not seed_cfg.dry_run:
        sink = KafkaSink(
            brokers=cfg.redpanda_brokers.split(","),
            topic=cfg.normalized_topic,
            add_config=common.kafka_producer_config(),
        )
        op.output("seed.sink", mapped, sink)
        admission_sink = KafkaSink(
            brokers=cfg.redpanda_brokers.split(","),
            topic=cfg.license_admissions_topic,
            add_config=common.kafka_producer_config(),
        )

        def _admission_to_kafka(
            item: tuple[str, SilverRecord | None, LicenseAdmissionDecision],
        ) -> KafkaSinkMessage:
            decision = item[2]
            return KafkaSinkMessage(
                key=decision.decision_id.encode("ascii"),
                value=decision.model_dump_json().encode("utf-8"),
                headers=[
                    ("trace_id", decision.trace_id.encode("ascii")),
                    ("schema", b"LicenseAdmissionDecision/v1"),
                ],
            )

        admission_rows = op.map("seed.admission_to_kafka", inp, _admission_to_kafka)
        op.output("seed.admission_sink", admission_rows, admission_sink)
    else:
        op.inspect("seed.dry_run", mapped)
    return flow


def run_inprocess(cfg: common.ProcessorConfig, seed_cfg: SeedLoaderConfig) -> dict[str, Any]:
    """Synchronous fallback runner (no Bytewax) for the smoke job.

    Returns per-component stats. Used by :func:`main` when the
    ``S2P_SEED_INPROCESS=1`` env var is set, and by the unit tests.
    """
    s3 = boto3.client(
        "s3",
        endpoint_url=cfg.minio_endpoint,
        aws_access_key_id=cfg.minio_access_key,
        aws_secret_access_key=cfg.minio_secret_key,
        region_name="us-east-1",
    )
    cursor_store = CursorStore(s3, bucket=seed_cfg.state_bucket)
    sink = _build_sink(cfg, seed_cfg)
    admission_sink = _build_admission_sink(cfg, seed_cfg)
    stats: dict[str, int] = {}
    for name in seed_cfg.components:
        cursor = stream_component(
            name,
            cursor_store=cursor_store,
            cfg=seed_cfg,
            on_record=sink,
            on_admission=admission_sink,
        )
        stats[name] = cursor.rows_emitted
    return {
        "started_at": datetime.now(tz=UTC).isoformat(),
        "components": stats,
    }


def _build_sink(
    cfg: common.ProcessorConfig, seed_cfg: SeedLoaderConfig
) -> Callable[[str, SilverRecord], None]:
    """Build an ``on_record`` callable that targets Redpanda or stdout."""
    if seed_cfg.dry_run:
        log = common.get_logger("s2p.seed_loader")

        def _print(repo_id: str, rec: SilverRecord) -> None:
            log.info("seed.dry_run", repo_id=repo_id, doc_id=rec.doc_id, lang=rec.lang)

        return _print

    from confluent_kafka import Producer  # type: ignore[import-not-found]

    producer = Producer(
        {
            "bootstrap.servers": cfg.redpanda_brokers,
            "client.id": "s2p-seed-loader",
            "compression.type": "zstd",
            "linger.ms": "20",
            "enable.idempotence": "true",
        }
    )

    def _produce(repo_id: str, rec: SilverRecord) -> None:
        headers = [
            ("trace_id", rec.trace_id.encode("ascii")),
            ("source_feed", f"seed:{repo_id}".encode()),
        ]
        producer.produce(
            cfg.normalized_topic,
            key=rec.doc_id.encode("utf-8"),
            value=common.silver_dumps(rec),
            headers=headers,
        )
        producer.poll(0)

    return _produce


def _build_admission_sink(
    cfg: common.ProcessorConfig, seed_cfg: SeedLoaderConfig
) -> Callable[[LicenseAdmissionDecision], None]:
    if seed_cfg.dry_run:
        log = common.get_logger("s2p.seed_loader")

        def _print(decision: LicenseAdmissionDecision) -> None:
            log.info(
                "seed.license_admission",
                doc_id=decision.doc_id,
                status=decision.status,
                license=decision.license_id,
            )

        return _print

    from confluent_kafka import Producer  # type: ignore[import-not-found]

    producer = Producer(
        {
            "bootstrap.servers": cfg.redpanda_brokers,
            "client.id": "s2p-seed-license-admission",
            "compression.type": "zstd",
            "linger.ms": "20",
            "enable.idempotence": "true",
        }
    )

    def _produce(decision: LicenseAdmissionDecision) -> None:
        producer.produce(
            cfg.license_admissions_topic,
            key=decision.decision_id.encode("ascii"),
            value=decision.model_dump_json().encode("utf-8"),
            headers=[
                ("trace_id", decision.trace_id.encode("ascii")),
                ("schema", b"LicenseAdmissionDecision/v1"),
            ],
        )
        producer.poll(0)

    return _produce


def main() -> None:
    """Entry point for the seed loader Job container."""
    cfg = common.load_config()
    common.configure_logging(cfg.log_level, json_output=not cfg.is_dev)
    log = common.get_logger("s2p.seed_loader")
    seed_cfg = load_seed_config()
    log.info(
        "starting seed loader",
        components=list(seed_cfg.components),
        dry_run=seed_cfg.dry_run,
        max_docs_per_component=seed_cfg.max_docs_per_component,
    )
    inprocess = os.environ.get("S2P_SEED_INPROCESS", "0") == "1"
    if inprocess:
        stats = run_inprocess(cfg, seed_cfg)
        log.info("seed loader finished (inprocess)", **stats)
        return
    flow = build_dataflow(cfg, seed_cfg)
    from bytewax.run import cli_main  # type: ignore[import-not-found]

    cli_main(flow)


__all__ = [
    "COMPONENTS",
    "CURSOR_FLUSH_INTERVAL",
    "DEFAULT_COMPONENTS",
    "SeedLoaderConfig",
    "build_dataflow",
    "load_seed_config",
    "main",
    "run_inprocess",
    "seed_admission",
    "stream_component",
    "to_silver",
]
