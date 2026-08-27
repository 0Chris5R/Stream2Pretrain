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

FETCHER_FLOW_NAME = "s2p-fetcher-live-v5"
FETCHER_RECOVERY_NAME = "fetcher-live-v5"


class PdfProcessingTemporarilyDisabled(common.DeterministicProcessingError):
    """Audit-only deferral used while the deployment lacks PDF worker RAM.

    This is intentionally record-local and deterministic: Bytewax writes the
    input coordinate to the durable processing-failure ledger and advances,
    allowing HTML, web, and card records behind the PDF to run.
    Re-enable ``S2P_PDF_PROCESSING_ENABLED`` after the checkpoint-pinned node
    has enough memory for the full Docling, Tesseract, and TableFormer path.
    """


class RawObjectMissing(common.DeterministicProcessingError):
    """The immutable Bronze pointer names an object that no longer exists.

    S3 and MinIO provide read-after-write consistency for object creation. A
    confirmed ``NoSuchKey`` response is therefore not repaired by restarting
    the Bytewax execution at the same Kafka offset. Record it in the durable
    processing-failure ledger and advance so one expired Bronze body cannot
    block every later source record in that partition. Transport, permission,
    timeout, and server failures remain retryable and still stop the flow.
    """


class RawObjectEmpty(common.DeterministicProcessingError):
    """A retained Bronze object has no content that can be normalized."""


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
    except ClientError as exc:
        error_code = str(exc.response.get("Error", {}).get("Code", ""))
        if error_code in {"404", "NoSuchKey", "NoSuchObject", "NotFound"}:
            raise RawObjectMissing(f"raw object is missing for {bronze.doc_id}") from exc
        raise RuntimeError(f"raw object read failed for {bronze.doc_id}") from exc
    except BotoCoreError as exc:
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


def _markdown_prose_projection(payload: bytes) -> tuple[str, str | None, str]:
    """Compatibility facade for tests and callers that only need prose."""
    text, title, metadata, _ = _markdown_prose_sections(payload)
    return text, title, metadata


def _markdown_prose_sections(
    payload: bytes,
) -> tuple[str, str | None, str, list[SilverSegment]]:
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
    section_lines: list[str] = []
    section_title = "Overview"
    section_index = 0
    parsed_sections: list[SilverSegment] = []
    title: str | None = None
    in_fence = False
    fence_marker = ""
    in_html_comment = False
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
        if in_html_comment:
            if "-->" not in stripped:
                continue
            stripped = stripped.split("-->", 1)[1].strip()
            in_html_comment = False
        if "<!--" in stripped:
            before_comment, after_comment = stripped.split("<!--", 1)
            if "-->" in after_comment:
                after_comment = after_comment.split("-->", 1)[1]
                stripped = f"{before_comment} {after_comment}".strip()
            else:
                stripped = before_comment.strip()
                in_html_comment = True
        if not stripped:
            if prose and prose[-1] != "":
                prose.append("")
            continue
        is_heading = stripped.startswith("#")
        cleaned = stripped.lstrip("#> ").strip()
        cleaned = re.sub(r"^[-*+]\s+", "", cleaned)
        cleaned = _MARKDOWN_LINK.sub(lambda match: match.group(1), cleaned)
        cleaned = _MARKDOWN_HTML.sub(" ", cleaned)
        cleaned = " ".join(cleaned.split())
        if not cleaned:
            continue
        if title is None and is_heading:
            # README headings are untrusted, user-authored input. Keep the
            # complete heading in the prose projection, but bound the compact
            # title carried by SilverRecord to its declared schema limit.
            title = cleaned[:2048]
        if is_heading:
            if section_lines:
                section_text = "\n".join(section_lines).strip()
                parsed_sections.append(
                    SilverSegment(
                        segment_id=f"card-section-{section_index}",
                        title=section_title,
                        text=section_text,
                        word_count=len(section_text.split()),
                    )
                )
                section_index += 1
                section_lines = []
            section_title = cleaned[:2048]
            prose.append(cleaned)
            continue
        prose.append(cleaned)
        section_lines.append(cleaned)
    if section_lines:
        section_text = "\n".join(section_lines).strip()
        parsed_sections.append(
            SilverSegment(
                segment_id=f"card-section-{section_index}",
                title=section_title,
                text=section_text,
                word_count=len(section_text.split()),
            )
        )
    text = "\n".join(prose).strip()
    return text, title, "\n".join(metadata_lines)[:32768], parsed_sections


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
    elif bronze.source_format == "web" and "markdown" in bronze.content_type.lower():
        text, title, source_metadata_text, segments = _markdown_prose_sections(raw_html)
        model_text = text
        projection_version = "hf-card-prose-v2"
        extracted_with = bronze.extraction_pipeline
        extraction_pipeline = bronze.extraction_pipeline
    elif bronze.source_format in {"latex", "markdown"}:
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
        if model_text and not segments:
            segments = [
                SilverSegment(
                    segment_id="document",
                    title=title or "Document",
                    text=model_text,
                    word_count=len(model_text.split()),
                )
            ]
        included_section_count = len(segments)
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
        raw_html_s3_uri=bronze.raw_html_s3_uri,
        source_content_type=bronze.content_type,
        source_http_status=bronze.http_status,
        source_fetched_at=bronze.fetched_at,
        source_http_last_modified=bronze.http_last_modified,
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
    if metrics is not None:
        metrics.record_received(source_feed=bronze.source_feed)
    # Temporary deployment capacity switch. Do not replace this with a silent
    # drop or a reduced PDF parser: disabled PDFs receive an idempotent durable
    # deferral in ``processing-failures/`` before Bytewax checkpoints them.
    # The full extraction code and models remain intact for re-enablement.
    if bronze.source_format == "pdf" and os.environ.get("S2P_PDF_PROCESSING_ENABLED", "1") != "1":
        raise PdfProcessingTemporarilyDisabled(
            "PDF processing is temporarily disabled until the extraction node is resized"
        )
    source_policy = resolve_source_policy(
        source_feed=bronze.source_feed,
        source_format=bronze.source_format,
        extraction_pipeline=bronze.extraction_pipeline,
    )
    is_hf_card = source_policy.family in {"hf_model_card", "hf_dataset_card"}
    raw_html = fetch_raw_bytes(state, bronze)
    if not raw_html:
        if is_hf_card:
            # Empty public READMEs are valid Hub repository states but contain
            # no corpus item. Skip them without recording a processing failure
            # or fabricating a Silver/Gold document.
            return None
        raise RawObjectEmpty(f"raw body is unavailable for {bronze.doc_id}")
    silver = normalize(state, bronze, raw_html)
    if silver is None:
        if is_hf_card:
            # Frontmatter-only, comment-only, and fenced-code-only cards have
            # no admitted prose projection by design. This is an intentional
            # source skip rather than a normalization defect.
            return None
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
        batch_size=common.kafka_source_batch_size(),
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
                silver = process_bronze_payload(state, payload, metrics=PROCESSOR_METRICS)
                if silver is None:
                    return None
                encoded = common.silver_dumps(silver)
                if len(encoded) > payload_max_bytes:
                    raise common.DeterministicProcessingError(
                        f"normalized payload for {silver.doc_id} is {len(encoded)} bytes; "
                        f"limit is {payload_max_bytes}"
                    )
            except (ValueError, common.DeterministicProcessingError) as exc:
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
