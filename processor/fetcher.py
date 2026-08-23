"""Bytewax fetcher: ``raw.fetched`` -> ``docs.normalized``.

Per record the dataflow:

1. Reads the BronzeRecord pointer + raw HTML from MinIO (the bronze object
   key is in ``raw_html_s3_uri``).
2. Runs the Resiliparse extractor.
3. Detects the language with fastlangid (proxy fallback in CI).
4. Builds a SilverRecord with a placeholder MinHash signature and an
   ``open-ended`` validity interval seeded by the validity-interval
   enricher.
5. Emits the SilverRecord on ``docs.normalized``.

Bytewax recovery owns source progress. Production and smoke traffic run as
separate executions with separate recovery directories so a deployment canary
cannot advance production progress or mutate production state.
"""

from __future__ import annotations

import gzip
import io
import os
import re
from dataclasses import dataclass
from typing import Any, cast

import boto3
import orjson
from botocore.exceptions import BotoCoreError, ClientError
from defusedxml import ElementTree as DefusedElementTree  # type: ignore[import-untyped]
from defusedxml.common import DefusedXmlException  # type: ignore[import-untyped]

from ingest.common.license_admission import (
    is_posttrain_transform_permitted,
    is_training_permitted,
)
from processor import common
from processor.metrics import PROCESSOR_METRICS, ProcessorMetrics
from processor.operators.extract import ResiliparseExtractor
from processor.operators.langid import LangIdentifier
from processor.operators.minhash import MinHasher
from processor.operators.validity import ValidityEnricher, WaybackLookup
from processor.probes import start_probe_server
from processor.scientific import ScientificProcessingResult, ScientificProcessor
from processor.source_policy import resolve_source_policy
from schemas.bronze import BronzeRecord
from schemas.silver import SilverRecord, SilverSegment, SilverTags

FETCHER_FLOW_NAME = "s2p-fetcher-v2"
FETCHER_RECOVERY_NAME = "fetcher-v2"


@dataclass(slots=True)
class FetcherState:
    """Per-worker state for extraction and normalization.

    The members are eagerly constructed once so each replica pays the
    model-load cost exactly once.
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

    Strips the ``s3://<bucket>/`` prefix and uses the configured MinIO client
    for the actual GET. Invalid pointers, size violations, corrupt compression,
    and storage failures raise so Bytewax cannot checkpoint past an unread body.
    """
    uri = bronze.raw_html_s3_uri
    if not uri.startswith("s3://"):
        raise ValueError(f"invalid raw object URI for {bronze.doc_id}")
    rest = uri[len("s3://") :]
    bucket, _, key = rest.partition("/")
    if not bucket or not key:
        raise ValueError(f"incomplete raw object URI for {bronze.doc_id}")
    max_object_bytes = int(os.environ.get("S2P_MAX_RAW_OBJECT_BYTES", str(64 * 1024 * 1024)))
    max_expanded_bytes = int(os.environ.get("S2P_MAX_EXPANDED_OBJECT_BYTES", str(64 * 1024 * 1024)))
    if max_object_bytes <= 0 or max_expanded_bytes <= 0:
        raise ValueError("raw object byte limits must be positive")
    if bronze.bytes_size is not None and bronze.bytes_size > max_object_bytes:
        raise ValueError(f"raw object exceeds the configured bound for {bronze.doc_id}")
    try:
        resp = state.s3.get_object(Bucket=bucket, Key=key)
        content_length = resp.get("ContentLength")
        if isinstance(content_length, int) and content_length > max_object_bytes:
            raise ValueError(f"raw object exceeds the configured bound for {bronze.doc_id}")
        body = cast(bytes, resp["Body"].read(max_object_bytes + 1))
    except (BotoCoreError, ClientError) as exc:
        raise RuntimeError(f"raw object read failed for {bronze.doc_id}") from exc
    if len(body) > max_object_bytes:
        raise ValueError(f"raw object exceeds the configured bound for {bronze.doc_id}")
    if uri.endswith(".gz") or resp.get("ContentEncoding") == "gzip":
        try:
            with gzip.GzipFile(fileobj=io.BytesIO(body)) as compressed:
                expanded = compressed.read(max_expanded_bytes + 1)
            if len(expanded) > max_expanded_bytes:
                raise ValueError(
                    f"expanded raw object exceeds the configured bound for {bronze.doc_id}"
                )
            return expanded
        except (EOFError, OSError) as exc:
            raise ValueError(f"corrupt gzip body for {bronze.doc_id}") from exc
    if len(body) > max_expanded_bytes:
        raise ValueError(f"raw object exceeds the configured bound for {bronze.doc_id}")
    return body


def _structured_payload_text(payload: bytes) -> tuple[str, str | None]:
    """Project JSON or XML metadata/review payloads into deterministic plain text."""
    try:
        value = orjson.loads(payload)
    except orjson.JSONDecodeError:
        try:
            root = DefusedElementTree.fromstring(payload)
        except (DefusedElementTree.ParseError, DefusedXmlException, ValueError):
            text = payload.decode("utf-8", errors="replace").strip()
            return text, None

        xml_title: str | None = None
        xml_strings: list[str] = []
        for element in root.iter():
            local_name = str(element.tag).rsplit("}", 1)[-1].lower()
            value = " ".join(part.strip() for part in element.itertext() if part.strip())
            if not value:
                continue
            if xml_title is None and local_name == "title":
                xml_title = value
            if len(element) == 0 and not value.startswith(("http://", "https://")):
                xml_strings.append(value)
        return "\n".join(dict.fromkeys(xml_strings)).strip(), xml_title

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


_MARKDOWN_LINK = re.compile(r"!?\[([^\]]*)\]\([^)]*\)")
_MARKDOWN_HTML = re.compile(r"<[^>]+>")
_REVIEW_ADMIN_FIELDS = frozenset(
    {
        "authors",
        "authorids",
        "cdate",
        "confidence",
        "decision",
        "forum",
        "id",
        "invitation",
        "license",
        "license_url",
        "mdate",
        "note_id",
        "rating",
        "recommendation",
        "reviewer",
        "reviewer_id",
        "signatures",
        "venue",
        "year",
    }
)


def _markdown_prose_projection(payload: bytes) -> tuple[str, str | None, str]:
    """Extract card/README prose while excluding YAML and fenced code.

    The immutable Bronze object remains the exact source. This projection is
    the text sent to FineWeb-Edu, privacy, deduplication, and export.
    """
    raw = payload.decode("utf-8", errors="replace").strip()
    lines = raw.splitlines()
    metadata_lines: list[str] = []
    start = 0
    if lines and lines[0].strip() == "---":
        for index in range(1, len(lines)):
            if lines[index].strip() in {"---", "..."}:
                metadata_lines = lines[1:index]
                start = index + 1
                break

    prose: list[str] = []
    title: str | None = None
    in_fence = False
    fence_marker = ""
    for raw_line in lines[start:]:
        stripped = raw_line.strip()
        if stripped.startswith(("```", "~~~")):
            marker = stripped[:3]
            if not in_fence:
                in_fence = True
                fence_marker = marker
            elif marker == fence_marker:
                in_fence = False
            continue
        if in_fence:
            continue
        if stripped.startswith("<!--") or not stripped:
            if prose and prose[-1] != "":
                prose.append("")
            continue
        cleaned = stripped.lstrip("#> ").strip()
        cleaned = re.sub(r"^[-*+]\s+", "", cleaned)
        cleaned = _MARKDOWN_LINK.sub(lambda match: match.group(1), cleaned)
        cleaned = _MARKDOWN_HTML.sub(" ", cleaned)
        cleaned = " ".join(cleaned.split())
        if not cleaned:
            continue
        if title is None and stripped.startswith("#"):
            title = cleaned
        prose.append(cleaned)
    text = "\n".join(prose).strip()
    return text, title, "\n".join(metadata_lines)[:32768]


def _openreview_value(value: object) -> object:
    """Unwrap the ``{"value": ...}`` envelope used by OpenReview API v2."""
    if isinstance(value, dict) and "value" in value:
        return value["value"]
    return value


def _review_payload_text(payload: bytes) -> tuple[str, str | None, str]:
    """Project public OpenReview form fields without administrative labels.

    Rating, confidence, recommendation, and decision are retained as audit
    metadata. They are never interpreted as review-quality labels.
    """
    try:
        raw = orjson.loads(payload)
    except orjson.JSONDecodeError:
        text = payload.decode("utf-8", errors="replace").strip()
        return text, None, "legacy_unstructured_review"
    if not isinstance(raw, dict):
        return "", None, "invalid_review_envelope"

    content = raw.get("content")
    fields = content if isinstance(content, dict) else raw
    title_value = _openreview_value(raw.get("title"))
    if not isinstance(title_value, str):
        title_value = _openreview_value(fields.get("title"))
    title = title_value.strip() if isinstance(title_value, str) and title_value.strip() else None

    metadata: list[str] = []
    for key in ("id", "note_id", "forum", "invitation", "venue", "year"):
        value = _openreview_value(raw.get(key))
        if value not in (None, ""):
            metadata.append(f"{key}: {value}")

    blocks: list[str] = []
    for key, wrapped in fields.items():
        normalized_key = str(key).strip().lower().replace(" ", "_")
        value = _openreview_value(wrapped)
        if normalized_key in _REVIEW_ADMIN_FIELDS or normalized_key == "title":
            if value not in (None, ""):
                metadata.append(f"{normalized_key}: {value}")
            continue
        values: list[str] = []
        if isinstance(value, str):
            values = [value]
        elif isinstance(value, list):
            values = [item for item in value if isinstance(item, str)]
        elif isinstance(value, dict):
            values = [item for item in value.values() if isinstance(item, str)]
        cleaned = "\n".join(item.strip() for item in values if item.strip()).strip()
        if cleaned:
            blocks.append(f"[FIELD {normalized_key}]\n{cleaned}")
    return "\n\n".join(blocks), title, "\n".join(metadata)[:32768]


def uses_scientific_extraction(bronze: BronzeRecord) -> bool:
    """Return whether an HTML record belongs to a scientific-document source.

    General blogs and crawled web pages must stay on Resiliparse/FineWeb. The
    presence of an HTML wire format alone does not make a page a paper.
    """
    return (
        resolve_source_policy(
            source_feed=bronze.source_feed,
            source_format=bronze.source_format,
            extraction_pipeline=bronze.extraction_pipeline,
        ).family
        == "scientific_paper"
    )


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
    if bronze.source_format == "metadata":
        text, title = _structured_payload_text(raw_html)
        # Discovery envelopes are retained in Bronze only and normally bypass
        # normalize. Keeping an empty model projection here makes direct replay
        # and legacy calls fail closed as well.
        model_text = ""
        source_metadata_text = text[:32768]
        extracted_with = bronze.extraction_pipeline
        extraction_pipeline = bronze.extraction_pipeline
    elif bronze.source_format == "review":
        text, title, source_metadata_text = _review_payload_text(raw_html)
        model_text = text
        extracted_with = bronze.extraction_pipeline
        extraction_pipeline = bronze.extraction_pipeline
    elif bronze.source_format == "web" and (
        "markdown" in bronze.content_type.lower()
        or resolve_source_policy(
            source_feed=bronze.source_feed,
            source_format=bronze.source_format,
            extraction_pipeline=bronze.extraction_pipeline,
        ).family
        == "repository_documentation"
    ):
        text, title, source_metadata_text = _markdown_prose_projection(raw_html)
        model_text = text
        extracted_with = bronze.extraction_pipeline
        extraction_pipeline = bronze.extraction_pipeline
    elif bronze.source_format in {"code", "latex", "markdown"}:
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
    elif uses_scientific_extraction(bronze) and state.scientific is not None:
        if bronze.source_format in {"latex", "markdown"}:
            scientific_result = state.scientific.process_text(
                doc_id=bronze.doc_id,
                source_url=str(bronze.url),
                text=text,
                title=title,
                source_format=bronze.source_format,
                extraction_pipeline=extraction_pipeline,
            )
        else:
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
        training_word_count = len(model_text.split())
        included_section_count = 1 if model_text else 0
        if model_text:
            segments = [
                SilverSegment(
                    segment_id="document",
                    title=title or "Document",
                    text=model_text,
                    word_count=len(model_text.split()),
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
        model_text=model_text,
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
        training_usage=bronze.training_usage,
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
    # Metadata envelopes schedule content work but are not corpus documents.
    # Skip before the MinIO read, extraction, OCR, language, and MinHash stages.
    if bronze.source_format == "metadata":
        return None
    # Defence in depth for legacy producers and replayed topics. This check is
    # intentionally before the MinIO GET, extraction, OCR, and model pipeline.
    pretrain_allowed = is_training_permitted(
        bronze.spdx_license, source_format=bronze.source_format
    )
    transform_allowed = (
        bronze.training_usage == "posttrain_transform_only"
        and is_posttrain_transform_permitted(bronze.spdx_license)
    )
    if not pretrain_allowed and not transform_allowed:
        return None
    raw_html = fetch_raw_bytes(state, bronze)
    if not raw_html:
        raise RuntimeError(f"raw body is unavailable for {bronze.doc_id}")
    silver = normalize(state, bronze, raw_html)
    if silver is None:
        raise ValueError(f"extraction produced no trainable body for {bronze.doc_id}")
    if silver is not None and metrics is not None:
        metrics.record_normalized(source_feed=silver.source_feed)
    return silver


def build_dataflow(
    cfg: common.ProcessorConfig,
    *,
    runtime_status: common.BytewaxRuntimeStatus | None = None,
) -> object:
    """Construct the production or isolated-smoke Bytewax execution."""
    from bytewax import operators as op
    from bytewax.connectors.kafka import KafkaSink, KafkaSinkMessage
    from bytewax.dataflow import Dataflow

    tracer = common.init_tracer("s2p-fetcher", cfg)
    input_topics = fetcher_input_topics(cfg)
    if len(input_topics) != 1:
        raise RuntimeError("one Bytewax fetcher execution must own exactly one traffic-class topic")
    output_topic = os.environ.get("S2P_FETCHER_OUTPUT_TOPIC", cfg.normalized_topic).strip()
    if not output_topic:
        raise RuntimeError("S2P_FETCHER_OUTPUT_TOPIC must not be empty")
    smoke_input = os.environ.get("S2P_SMOKE_RAW_TOPIC", "raw.smoke").strip()
    smoke_output = os.environ.get("S2P_SMOKE_NORMALIZED_TOPIC", "docs.normalized.smoke").strip()
    is_smoke_execution = input_topics[0] == smoke_input
    if (input_topics[0] == smoke_input) != (output_topic == smoke_output):
        raise RuntimeError("fetcher smoke input and output must use the isolated smoke lane")
    state = build_state(
        cfg,
        with_wayback=os.environ.get("S2P_WAYBACK_LOOKUP_ENABLED", "0") == "1",
    )
    failure_writer = common.DurableProcessingFailureWriter.from_config(cfg)
    flow_name = os.environ.get("S2P_BYTEWAX_FLOW_NAME", FETCHER_FLOW_NAME).strip()
    if not flow_name:
        raise RuntimeError("S2P_BYTEWAX_FLOW_NAME must not be empty")
    flow = Dataflow(flow_name)
    source = common.tracked_kafka_source(
        runtime_status=runtime_status,
        source_name="raw_fetched",
        brokers=cfg.redpanda_brokers.split(","),
        topics=input_topics,
        starting_offset=common.kafka_starting_offset(),
        add_config=common.kafka_consumer_config(cfg.consumer_group),
    )
    inp = op.input("raw_fetched", flow, source)
    payload_max_bytes = common.kafka_payload_max_bytes()

    def _step(msg: object) -> KafkaSinkMessage | None:
        payload = getattr(msg, "value", None)
        if payload is None:
            failure_writer.record(stage="fetcher", message=msg, reason="kafka_tombstone")
            PROCESSOR_METRICS.record_failure(stage="normalize", reason="kafka_tombstone")
            return None
        with tracer.start_as_current_span("fetcher.process") as span:
            try:
                silver = process_bronze_payload(state, payload)
                if silver is None:
                    return None
                encoded = common.silver_dumps(silver)
                if len(encoded) > payload_max_bytes:
                    raise common.DeterministicProcessingError(
                        f"normalized payload for {silver.doc_id} is {len(encoded)} bytes; "
                        f"limit is {payload_max_bytes}"
                    )
            except ValueError as exc:
                span.record_exception(exc)
                reason = type(exc).__name__
                failure_writer.record(stage="fetcher", message=msg, reason=reason)
                PROCESSOR_METRICS.record_failure(stage="normalize", reason=reason)
                return None
            except ClientError as exc:
                error_code = str(exc.response.get("Error", {}).get("Code", ""))
                if is_smoke_execution and error_code in {
                    "404",
                    "NoSuchKey",
                    "NoSuchObject",
                    "NotFound",
                }:
                    # Successful canaries delete their synthetic Bronze body.
                    # If a canary recovery PVC is rebuilt before raw.smoke
                    # retention expires, skip that now-expired exact fixture
                    # without poisoning the production failure ledger.
                    reason = "expired_smoke_bronze"
                    failure_writer.record(stage="fetcher-smoke", message=msg, reason=reason)
                    PROCESSOR_METRICS.record_failure(stage="normalize", reason=reason)
                    return None
                span.record_exception(exc)
                PROCESSOR_METRICS.record_failure(stage="normalize", reason=type(exc).__name__)
                raise
            except Exception as exc:
                span.record_exception(exc)
                PROCESSOR_METRICS.record_failure(stage="normalize", reason=type(exc).__name__)
                # Unknown, storage, extraction, and model failures must stop the
                # execution before Bytewax snapshots source progress.
                raise
            PROCESSOR_METRICS.record_normalized(source_feed=silver.source_feed)
            span.set_attribute("doc_id", silver.doc_id)
            return KafkaSinkMessage(
                key=silver.doc_id.encode("utf-8"),
                value=encoded,
                headers=[("trace_id", silver.trace_id.encode("ascii"))],
            )

    mapped = op.map("fetcher_normalize", inp, _step)
    filtered = op.filter("fetcher_drop_intentional", mapped, lambda message: message is not None)
    sink = KafkaSink(
        brokers=cfg.redpanda_brokers.split(","),
        topic=output_topic,
        add_config=common.kafka_producer_config(),
    )
    op.output("fetcher_sink", filtered, sink)
    return flow


def fetcher_input_topics(cfg: common.ProcessorConfig) -> list[str]:
    """Return this worker's explicitly assigned traffic class.

    Production and deployment-canary records use separate Bytewax executions.
    The default is production-only; smoke traffic must always be selected
    explicitly so it cannot mutate production recovery.
    """
    configured = os.environ.get("S2P_FETCHER_INPUT_TOPICS", "").strip()
    if configured:
        topics = [topic.strip() for topic in configured.split(",") if topic.strip()]
        if not topics:
            raise RuntimeError("S2P_FETCHER_INPUT_TOPICS did not contain a topic")
        return list(dict.fromkeys(topics))
    return [cfg.raw_topic]


def main() -> None:
    """Entrypoint: ``s2p-fetcher`` console script."""
    cfg = common.load_config()
    common.configure_logging(cfg.log_level, json_output=not cfg.is_dev)
    log = common.get_logger("s2p.fetcher")
    topics = fetcher_input_topics(cfg)
    log.info("starting Bytewax fetcher", brokers=cfg.redpanda_brokers, topics=topics)
    runtime_status = common.BytewaxRuntimeStatus()
    flow = build_dataflow(cfg, runtime_status=runtime_status)
    start_probe_server(
        metrics_provider=PROCESSOR_METRICS.render_prometheus,
        readiness_provider=runtime_status.is_ready,
    )
    recovery_name = os.environ.get("S2P_BYTEWAX_RECOVERY_NAME", FETCHER_RECOVERY_NAME).strip()
    if not recovery_name:
        raise RuntimeError("S2P_BYTEWAX_RECOVERY_NAME must not be empty")
    common.run_bytewax_flow(
        flow,
        cfg,
        recovery_name,
        runtime_status=runtime_status,
    )


def serialize_for_kafka(record: SilverRecord) -> tuple[bytes, bytes]:
    """Pure helper used in tests: returns (key, value) for a SilverRecord."""
    return record.doc_id.encode("utf-8"), common.silver_dumps(record)


def deserialize_for_test(payload: bytes) -> dict[str, Any]:
    """Round-trip helper for tests."""
    return cast(dict[str, Any], orjson.loads(payload))
