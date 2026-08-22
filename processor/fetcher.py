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
import io
import os
from dataclasses import dataclass
from typing import Any, cast

import boto3
import orjson
from botocore.exceptions import BotoCoreError, ClientError

from ingest.common.license_admission import is_training_permitted
from processor import common
from processor.metrics import PROCESSOR_METRICS, ProcessorMetrics
from processor.operators.extract import ResiliparseExtractor
from processor.operators.langid import LangIdentifier
from processor.operators.minhash import MinHasher
from processor.operators.validity import ValidityEnricher, WaybackLookup
from processor.probes import start_probe_server
from processor.scientific import ScientificProcessingResult, ScientificProcessor
from schemas.bronze import BronzeRecord
from schemas.silver import SilverRecord, SilverSegment, SilverTags


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
    scientific: ScientificProcessor | None = None


def build_state(cfg: common.ProcessorConfig, *, with_wayback: bool = True) -> FetcherState:
    """Construct a :class:`FetcherState` from the runtime config."""
    require_real_models = os.environ.get("S2P_REQUIRE_REAL_MODELS") == "1"
    s3 = boto3.client(
        "s3",
        endpoint_url=cfg.minio_endpoint,
        aws_access_key_id=cfg.minio_access_key,
        aws_secret_access_key=cfg.minio_secret_key,
        region_name="us-east-1",
    )
    wayback = WaybackLookup() if with_wayback else None
    extractor = ResiliparseExtractor(allow_fallback=not require_real_models)
    lang_id = LangIdentifier(allow_fallback=not require_real_models)
    minhasher = MinHasher()
    if require_real_models and minhasher.backend == "fallback-pyhash":
        raise RuntimeError("datasketch or rensa MinHash is required")
    scientific = ScientificProcessor(
        s3_client=s3,
        bucket=cfg.silver_bucket,
        models_dir=cfg.models_dir,
        user_agent=cfg.user_agent,
        require_real_models=require_real_models,
    )
    return FetcherState(
        extractor=extractor,
        lang_id=lang_id,
        minhasher=minhasher,
        validity=ValidityEnricher(wayback_lookup=wayback),
        s3=s3,
        bucket=cfg.bronze_bucket,
        scientific=scientific,
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
    max_object_bytes = int(os.environ.get("S2P_MAX_RAW_OBJECT_BYTES", str(64 * 1024 * 1024)))
    max_expanded_bytes = int(os.environ.get("S2P_MAX_EXPANDED_OBJECT_BYTES", str(64 * 1024 * 1024)))
    if max_object_bytes <= 0 or max_expanded_bytes <= 0:
        raise ValueError("raw object byte limits must be positive")
    if bronze.bytes_size is not None and bronze.bytes_size > max_object_bytes:
        return b""
    try:
        resp = state.s3.get_object(Bucket=bucket, Key=key)
        content_length = resp.get("ContentLength")
        if isinstance(content_length, int) and content_length > max_object_bytes:
            return b""
        body = cast(bytes, resp["Body"].read(max_object_bytes + 1))
    except (BotoCoreError, ClientError):
        return b""
    if len(body) > max_object_bytes:
        return b""
    if uri.endswith(".gz") or resp.get("ContentEncoding") == "gzip":
        try:
            with gzip.GzipFile(fileobj=io.BytesIO(body)) as compressed:
                expanded = compressed.read(max_expanded_bytes + 1)
            return expanded if len(expanded) <= max_expanded_bytes else b""
        except (EOFError, OSError):
            return body
    return body if len(body) <= max_expanded_bytes else b""


def _structured_payload_text(payload: bytes) -> tuple[str, str | None]:
    """Project JSON metadata/review payloads into deterministic plain text."""
    try:
        value = orjson.loads(payload)
    except orjson.JSONDecodeError:
        text = payload.decode("utf-8", errors="replace").strip()
        return text, None

    title: str | None = None
    if isinstance(value, dict):
        for key in ("title", "name", "id", "modelId"):
            candidate = value.get(key)
            if isinstance(candidate, str) and candidate.strip():
                title = candidate.strip()
                break
        paper = value.get("paper")
        if title is None and isinstance(paper, dict):
            candidate = paper.get("title") or paper.get("id")
            if isinstance(candidate, str) and candidate.strip():
                title = candidate.strip()

    strings: list[str] = []

    def _collect(item: object) -> None:
        if isinstance(item, str):
            cleaned = item.strip()
            if cleaned and not cleaned.startswith(("http://", "https://")):
                strings.append(cleaned)
            return
        if isinstance(item, dict):
            for child in item.values():
                _collect(child)
            return
        if isinstance(item, list):
            for child in item:
                _collect(child)

    _collect(value)
    text = "\n".join(dict.fromkeys(strings)).strip()
    return text, title


def normalize(state: FetcherState, bronze: BronzeRecord, raw_html: bytes) -> SilverRecord | None:
    """Turn one (BronzeRecord + raw bytes) into a SilverRecord."""
    scientific_result: ScientificProcessingResult | None = None
    model_text = ""
    source_metadata_text = ""
    structured_text = ""
    segments: list[SilverSegment] = []
    projection_version = "document-v1"
    source_word_count = 0
    training_word_count = 0
    included_section_count = 0
    excluded_section_count = 0
    excluded_sections: list[str] = []
    if bronze.source_format in {"metadata", "review"}:
        text, title = _structured_payload_text(raw_html)
        model_text = text
        source_metadata_text = title or ""
        extracted_with = bronze.extraction_pipeline
        extraction_pipeline = bronze.extraction_pipeline
    elif bronze.source_format == "code":
        text = raw_html.decode("utf-8", errors="replace").strip()
        title = str(bronze.url).rsplit("/", 1)[-1] or None
        model_text = text
        source_metadata_text = title or ""
        extracted_with = bronze.extraction_pipeline
        extraction_pipeline = bronze.extraction_pipeline
    elif bronze.source_format == "pdf":
        if state.scientific is None:
            return None
        scientific_result = state.scientific.process_pdf(
            doc_id=bronze.doc_id,
            source_url=str(bronze.url),
            pdf=raw_html,
            extraction_pipeline=bronze.extraction_pipeline,
        )
        text = scientific_result.text
        model_text = scientific_result.model_text
        source_metadata_text = scientific_result.source_metadata_text
        structured_text = scientific_result.structured_text
        title = scientific_result.document.title
        extracted_with = bronze.extraction_pipeline
        extraction_pipeline = bronze.extraction_pipeline
    else:
        extracted = state.extractor.extract(raw_html)
        text = extracted.text.strip()
        model_text = text
        source_metadata_text = extracted.title or ""
        title = extracted.title
        extracted_with = extracted.extracted_with
        extraction_pipeline = bronze.extraction_pipeline
    if not text:
        return None
    artifact_uri: str | None = None
    figure_count = 0
    table_count = 0
    equation_count = 0
    citation_count = 0
    extraction_warnings: list[str] = []
    if bronze.source_format == "pdf" and scientific_result is not None:
        artifact_uri = scientific_result.artifact_s3_uri
        figure_count = len(scientific_result.document.figures)
        table_count = len(scientific_result.document.tables)
        equation_count = len(scientific_result.document.equations)
        citation_count = len(scientific_result.document.citations)
        extraction_warnings = list(scientific_result.document.warnings)
    elif bronze.source_format == "html" and state.scientific is not None:
        scientific_result = state.scientific.process(
            doc_id=bronze.doc_id,
            source_url=str(bronze.url),
            html=raw_html,
            plain_text=text,
            title=title,
            extraction_pipeline=extraction_pipeline,
        )
        text = scientific_result.text
        model_text = scientific_result.model_text
        source_metadata_text = scientific_result.source_metadata_text
        structured_text = scientific_result.structured_text
        title = scientific_result.document.title
        artifact_uri = scientific_result.artifact_s3_uri
        figure_count = len(scientific_result.document.figures)
        table_count = len(scientific_result.document.tables)
        equation_count = len(scientific_result.document.equations)
        citation_count = len(scientific_result.document.citations)
        extraction_warnings = list(scientific_result.document.warnings)
    if scientific_result is not None:
        document = scientific_result.document
        segments = [
            SilverSegment(
                segment_id=section.section_id,
                title=section.title,
                role=section.role,
                text=section.text,
                word_count=section.word_count,
            )
            for section in document.sections
            if section.include_in_training and section.text.strip()
        ]
        projection_version = document.projection_version
        source_word_count = document.source_word_count
        training_word_count = document.training_word_count
        included_section_count = document.included_section_count
        excluded_section_count = document.excluded_section_count
        excluded_sections = list(document.excluded_sections)
    else:
        source_word_count = len(text.split())
        training_word_count = source_word_count
        included_section_count = 1 if text else 0
        if text:
            segments = [
                SilverSegment(
                    segment_id="document",
                    title=title or "Document",
                    text=model_text or text,
                    word_count=len((model_text or text).split()),
                )
            ]
    lang_result = state.lang_id.identify(model_text or text)
    sig = state.minhasher.signature(text)
    html_text = (
        raw_html.decode("utf-8", errors="replace")
        if raw_html and bronze.source_format == "html"
        else ""
    )
    interval = state.validity.enrich(
        url=str(bronze.url),
        fetched_at=bronze.fetched_at,
        http_last_modified=bronze.http_last_modified,
        html=html_text,
    )
    return SilverRecord(
        doc_id=bronze.doc_id,
        url=bronze.url,
        title=title,
        text=text,
        model_text=model_text or text,
        source_metadata_text=source_metadata_text,
        structured_text=structured_text,
        segments=segments,
        projection_version=projection_version,
        source_word_count=source_word_count,
        training_word_count=training_word_count,
        included_section_count=included_section_count,
        excluded_section_count=excluded_section_count,
        excluded_sections=excluded_sections,
        lang=lang_result.lang,
        lang_score=lang_result.score,
        lang_detector_revision=lang_result.detector,
        extracted_with=extracted_with,
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
        source_feed=bronze.source_feed,
        source_format=bronze.source_format,
        extraction_pipeline=extraction_pipeline,
        spdx_license=bronze.spdx_license,
        spdx_license_source=bronze.spdx_license_source,
        scientific_artifact_s3_uri=artifact_uri,
        figure_count=figure_count,
        table_count=table_count,
        equation_count=equation_count,
        citation_count=citation_count,
        extraction_warnings=extraction_warnings,
    )


def process_bronze_payload(
    state: FetcherState,
    payload: bytes,
    *,
    metrics: ProcessorMetrics | None = None,
) -> SilverRecord | None:
    """Deserialize a Kafka payload, run the pipeline, return the silver row."""
    bronze = common.bronze_loads(payload)
    # Defence in depth for legacy producers and replayed topics. This check is
    # intentionally before the MinIO GET, extraction, OCR, and model pipeline.
    if not is_training_permitted(bronze.spdx_license, source_format=bronze.source_format):
        return None
    raw_html = fetch_raw_bytes(state, bronze)
    silver = normalize(state, bronze, raw_html)
    if silver is not None and metrics is not None:
        metrics.record_normalized(source_feed=silver.source_feed)
    return silver


def build_dataflow(cfg: common.ProcessorConfig) -> object:
    """Construct the Bytewax dataflow.

    Imports of ``bytewax.*`` happen inside this function so unit tests can
    import :mod:`processor.fetcher` without paying the runtime dependency.
    Bytewax is only required when actually running the dataflow.
    """
    from bytewax import operators as op
    from bytewax.connectors.kafka import KafkaSink, KafkaSinkMessage, KafkaSource
    from bytewax.dataflow import Dataflow

    tracer = common.init_tracer("s2p-fetcher", cfg)
    log = common.get_logger("s2p.fetcher")
    # A synchronous Wayback HTTP call per record cannot keep up with bursty
    # ingestion. Keep the optional enrichment available for controlled runs,
    # while the live streaming path falls back to fetched_at when source dates
    # are absent.
    with_wayback = os.environ.get("S2P_WAYBACK_LOOKUP_ENABLED", "0") == "1"
    state = build_state(cfg, with_wayback=with_wayback)
    flow = Dataflow("s2p-fetcher")
    payload_max_bytes = common.kafka_payload_max_bytes()
    # ``beginning`` ensures a fresh deploy or offset reset replays from the
    # topic's retention window (at-least-once). Override via env if a debug
    # run needs to skip backlog: ``S2P_KAFKA_START_OFFSET=end``.
    start_offset = common.kafka_starting_offset()
    source = KafkaSource(
        brokers=cfg.redpanda_brokers.split(","),
        topics=[cfg.raw_topic],
        starting_offset=start_offset,
        add_config=common.kafka_consumer_config(cfg.consumer_group),
    )
    inp = op.input("raw_fetched", flow, source)

    def _step(msg: object) -> KafkaSinkMessage | None:
        with tracer.start_as_current_span("fetcher.process") as span:
            payload = getattr(msg, "value", None)
            if payload is None:
                return None
            try:
                silver = process_bronze_payload(state, payload)
            except Exception as exc:
                span.record_exception(exc)
                PROCESSOR_METRICS.record_failure(stage="normalize", reason=type(exc).__name__)
                log.warning(
                    "fetcher record failed",
                    error=str(exc),
                    exception_type=type(exc).__name__,
                    exc_info=True,
                )
                return None
            if silver is None:
                return None
            encoded = common.silver_dumps(silver)
            if len(encoded) > payload_max_bytes:
                PROCESSOR_METRICS.record_failure(stage="normalize", reason="payload_too_large")
                log.warning(
                    "normalized payload exceeds bounded Kafka record size",
                    doc_id=silver.doc_id,
                    payload_bytes=len(encoded),
                    payload_max_bytes=payload_max_bytes,
                    source_feed=silver.source_feed,
                )
                return None
            PROCESSOR_METRICS.record_normalized(source_feed=silver.source_feed)
            span.set_attribute("doc_id", silver.doc_id)
            span.set_attribute("lang", silver.lang)
            return KafkaSinkMessage(
                key=silver.doc_id.encode("utf-8"),
                value=encoded,
                headers=[("trace_id", silver.trace_id.encode("ascii"))],
            )

    mapped = op.map("fetcher_normalize", inp, _step)
    filtered = op.filter("fetcher_drop_none", mapped, lambda m: m is not None)
    sink = KafkaSink(
        brokers=cfg.redpanda_brokers.split(","),
        topic=cfg.normalized_topic,
        add_config=common.kafka_producer_config(),
    )
    op.output("fetcher_sink", filtered, sink)
    return flow


def main() -> None:
    """Entrypoint: ``s2p-fetcher`` console script."""
    cfg = common.load_config()
    common.configure_logging(cfg.log_level, json_output=not cfg.is_dev)
    log = common.get_logger("s2p.fetcher")
    log.info("starting fetcher dataflow", brokers=cfg.redpanda_brokers, topic=cfg.raw_topic)
    flow = build_dataflow(cfg)
    # Do not publish readiness until heavyweight model and recovery-state
    # initialization has succeeded. Otherwise Kubernetes can declare a rollout
    # healthy in the short interval before an initialization OOM kills it.
    start_probe_server(metrics_provider=PROCESSOR_METRICS.render_prometheus)
    common.run_bytewax_flow(flow, cfg, "fetcher")


def serialize_for_kafka(record: SilverRecord) -> tuple[bytes, bytes]:
    """Pure helper used in tests: returns (key, value) for a SilverRecord."""
    return record.doc_id.encode("utf-8"), common.silver_dumps(record)


def deserialize_for_test(payload: bytes) -> dict[str, Any]:
    """Round-trip helper for tests."""
    return cast(dict[str, Any], orjson.loads(payload))
