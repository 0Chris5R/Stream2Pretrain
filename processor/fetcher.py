"""Bytewax dataflow: ``raw.fetched`` -> ``docs.normalized``.

Per record the dataflow:

1. Reads the BronzeRecord pointer + raw HTML from MinIO (the bronze object
   key is in ``raw_html_s3_uri``).
2. Runs the Resiliparse extractor.
3. Detects the language with fastlangid (proxy fallback in CI).
4. Builds a SilverRecord with a placeholder MinHash signature and an
   ``open-ended`` validity interval seeded by the validity-interval
   enricher.
5. Emits the SilverRecord on ``docs.normalized``.

The dataflow is deterministic per offset, so a Bytewax recovery from the
last RocksDB checkpoint replays identical SilverRecords.
"""

from __future__ import annotations

import gzip
import os
from dataclasses import dataclass
from typing import Any

import boto3
import orjson
from botocore.exceptions import BotoCoreError, ClientError

from processor import common
from processor.operators.extract import ResiliparseExtractor
from processor.operators.langid import LangIdentifier
from processor.operators.minhash import MinHasher
from processor.operators.validity import ValidityEnricher, WaybackLookup
from schemas.bronze import BronzeRecord
from schemas.silver import SilverRecord, SilverTags


@dataclass(slots=True)
class FetcherState:
    """Per-worker state for the fetcher dataflow.

    The members are eagerly constructed at module load so each Bytewax
    worker pays the model-load cost exactly once.
    """

    extractor: ResiliparseExtractor
    lang_id: LangIdentifier
    minhasher: MinHasher
    validity: ValidityEnricher
    s3: Any
    bucket: str


def build_state(cfg: common.ProcessorConfig, *, with_wayback: bool = True) -> FetcherState:
    """Construct a :class:`FetcherState` from the runtime config."""
    s3 = boto3.client(
        "s3",
        endpoint_url=cfg.minio_endpoint,
        aws_access_key_id=cfg.minio_access_key,
        aws_secret_access_key=cfg.minio_secret_key,
        region_name="us-east-1",
    )
    wayback = WaybackLookup() if with_wayback else None
    return FetcherState(
        extractor=ResiliparseExtractor(),
        lang_id=LangIdentifier(),
        minhasher=MinHasher(),
        validity=ValidityEnricher(wayback_lookup=wayback),
        s3=s3,
        bucket=cfg.bronze_bucket,
    )


def fetch_raw_bytes(state: FetcherState, bronze: BronzeRecord) -> bytes:
    """Read the raw HTML pointed at by ``bronze.raw_html_s3_uri``.

    Strips the ``s3://<bucket>/`` prefix and uses the configured MinIO
    client for the actual GET. On any S3 error returns an empty bytes
    object so downstream operators degrade gracefully.
    """
    uri = bronze.raw_html_s3_uri
    if not uri.startswith("s3://"):
        return b""
    rest = uri[len("s3://") :]
    bucket, _, key = rest.partition("/")
    if not bucket or not key:
        return b""
    try:
        resp = state.s3.get_object(Bucket=bucket, Key=key)
        body = resp["Body"].read()
    except (BotoCoreError, ClientError):
        return b""
    if uri.endswith(".gz") or resp.get("ContentEncoding") == "gzip":
        try:
            return gzip.decompress(body)
        except Exception:
            return body
    return body


def normalize(state: FetcherState, bronze: BronzeRecord, raw_html: bytes) -> SilverRecord | None:
    """Turn one (BronzeRecord + raw HTML) into a SilverRecord."""
    extracted = state.extractor.extract(raw_html)
    text = extracted.text.strip()
    if not text:
        return None
    lang_result = state.lang_id.identify(text)
    sig = state.minhasher.signature(text)
    html_text = raw_html.decode("utf-8", errors="replace") if raw_html else ""
    interval = state.validity.enrich(
        url=str(bronze.url),
        fetched_at=bronze.fetched_at,
        http_last_modified=bronze.http_last_modified,
        html=html_text,
    )
    return SilverRecord(
        doc_id=bronze.doc_id,
        url=bronze.url,
        title=extracted.title,
        text=text,
        lang=lang_result.lang,
        lang_score=lang_result.score,
        extracted_with=extracted.extracted_with,
        tags=SilverTags(
            gopher_pass=True,  # populated by curate.py
            c4_nopunc_pass=True,
            perplexity=0.0,
            perplexity_bucket="head",
        ),
        minhash_sig=sig.digest,
        minhash_backend=state.minhasher.backend,
        minhash_num_perms=state.minhasher.num_perms,
        near_dup_cluster_id=None,
        valid_from=interval.valid_from,
        valid_to=interval.valid_to,
        valid_from_source=interval.valid_from_source,
        trace_id=bronze.trace_id,
    )


def process_bronze_payload(state: FetcherState, payload: bytes) -> SilverRecord | None:
    """Deserialize a Kafka payload, run the pipeline, return the silver row."""
    bronze = common.bronze_loads(payload)
    raw_html = fetch_raw_bytes(state, bronze)
    return normalize(state, bronze, raw_html)


def build_dataflow(cfg: common.ProcessorConfig) -> object:
    """Construct the Bytewax dataflow.

    Imports of ``bytewax.*`` happen inside this function so unit tests can
    import :mod:`processor.fetcher` without paying the runtime dependency.
    Bytewax is only required when actually running the dataflow.
    """
    from bytewax.connectors.kafka import KafkaSink, KafkaSource, KafkaSinkMessage
    from bytewax.dataflow import Dataflow
    from bytewax import operators as op

    tracer = common.init_tracer("s2p-fetcher", cfg)
    state = build_state(cfg)
    flow = Dataflow("s2p-fetcher")
    # ``beginning`` ensures a fresh deploy or offset reset replays from the
    # topic's retention window (at-least-once). Override via env if a debug
    # run needs to skip backlog: ``S2P_KAFKA_START_OFFSET=end``.
    start_offset = os.environ.get("S2P_KAFKA_START_OFFSET", "beginning")
    source = KafkaSource(
        brokers=cfg.redpanda_brokers.split(","),
        topics=[cfg.raw_topic],
        consumer_group=cfg.consumer_group + "-fetcher",
        starting_offset=start_offset,
    )
    inp = op.input("raw_fetched", flow, source)

    def _step(msg: object) -> KafkaSinkMessage | None:
        with tracer.start_as_current_span("fetcher.process") as span:
            payload = getattr(msg, "value", None)
            if payload is None:
                return None
            try:
                silver = process_bronze_payload(state, payload)
            except Exception as exc:  # noqa: BLE001
                span.record_exception(exc)
                return None
            if silver is None:
                return None
            span.set_attribute("doc_id", silver.doc_id)
            span.set_attribute("lang", silver.lang)
            return KafkaSinkMessage(
                key=silver.doc_id.encode("utf-8"),
                value=common.silver_dumps(silver),
                headers=[("trace_id", silver.trace_id.encode("ascii"))],
            )

    mapped = op.map("fetcher.normalize", inp, _step)
    filtered = op.filter("fetcher.drop_none", mapped, lambda m: m is not None)
    sink = KafkaSink(
        brokers=cfg.redpanda_brokers.split(","),
        topic=cfg.normalized_topic,
    )
    op.output("fetcher.sink", filtered, sink)
    return flow


def main() -> None:
    """Entrypoint: ``s2p-fetcher`` console script."""
    cfg = common.load_config()
    common.configure_logging(cfg.log_level, json_output=not cfg.is_dev)
    log = common.get_logger("s2p.fetcher")
    log.info("starting fetcher dataflow", brokers=cfg.redpanda_brokers, topic=cfg.raw_topic)
    flow = build_dataflow(cfg)
    # ``bytewax.run`` is deferred to avoid importing the runtime in tests.
    from bytewax.run import cli_main

    cli_main(flow)


def serialize_for_kafka(record: SilverRecord) -> tuple[bytes, bytes]:
    """Pure helper used in tests: returns (key, value) for a SilverRecord."""
    return record.doc_id.encode("utf-8"), common.silver_dumps(record)


def deserialize_for_test(payload: bytes) -> dict[str, Any]:
    """Round-trip helper for tests."""
    return orjson.loads(payload)
