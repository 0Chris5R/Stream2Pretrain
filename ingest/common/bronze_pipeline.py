"""Common 'fetch one URL, store to MinIO, publish BronzeRecord' pipeline.

Pollers vary in *how* they discover URLs, but they all do the same work after:
download, gzip into bronze, emit a BronzeRecord on the topic. This module
factors that out so all pollers honour the same trace structure, error
handling, and dedup logic.
"""

from __future__ import annotations

from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from typing import TYPE_CHECKING, cast

import httpx
from opentelemetry import trace

from ingest.common.hashing import canonical_url, doc_id_for_url
from ingest.common.license_admission import decide_license_admission
from ingest.common.s3 import bronze_object_key, bronze_s3_uri
from schemas.bronze import BronzeRecord, SourceFormat, SpdxLicenseSource

if TYPE_CHECKING:
    from ingest.common.kafka_producer import BronzeProducer, LicenseAdmissionProducer
    from ingest.common.minio_writer import MinioWriter


def parse_http_date(value: str | None) -> datetime | None:
    """Parse an RFC 1123 HTTP date into a tz-aware UTC datetime."""
    if not value:
        return None
    try:
        dt = parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


async def fetch_and_publish(
    client: httpx.AsyncClient,
    url: str,
    *,
    source_feed: str,
    producer: BronzeProducer,
    minio: MinioWriter,
    bucket: str,
    extension: str = "html.gz",
    expected_content_type: str | None = None,
    extra_headers: dict[str, str] | None = None,
    seen: set[str] | None = None,
    license_value: str | None = None,
    license_source: str = "unknown",
    license_resolver: str | None = None,
    license_evidence_url: str | None = None,
    license_evidence_revision: str | None = None,
    license_evidence_scope: str | None = None,
    source_format: SourceFormat = "html",
    extraction_pipeline: str = "raw-fetch",
    admission_producer: LicenseAdmissionProducer,
) -> BronzeRecord | None:
    """Fetch ``url``, store to MinIO bronze, emit BronzeRecord. Returns the record.

    Returns ``None`` when the document was skipped (304 Not Modified, dedup hit,
    or unsupported content type). Network errors propagate up so the caller can
    decide whether to retry the whole feed pass.
    """
    tracer = trace.get_tracer("ingest.bronze_pipeline")
    canon = canonical_url(url)
    doc_id = doc_id_for_url(canon)
    if seen is not None:
        if doc_id in seen:
            return None
        seen.add(doc_id)

    admission = decide_license_admission(
        source_url=canon,
        source_feed=source_feed,
        license_value=license_value,
        license_source=license_source,
        source_format=source_format,
        resolver=license_resolver,
        evidence_url=license_evidence_url,
        evidence_revision=license_evidence_revision,
        evidence_scope=license_evidence_scope,
    )
    # The decision must be durably published before a content request starts.
    # A Kafka failure therefore fails the ingest attempt closed.
    await admission_producer.send(admission.decision)
    if not admission.fetch_allowed:
        return None

    with tracer.start_as_current_span(
        "fetch_and_publish",
        attributes={
            "source_feed": source_feed,
            "url": canon,
            "doc_id": doc_id,
        },
    ) as span:
        with tracer.start_as_current_span("http.request") as http_span:
            resp = await client.get(canon, headers=extra_headers)
            http_span.set_attribute("http.status_code", resp.status_code)
        if resp.status_code == 304:
            span.set_attribute("ingest.skipped", "not_modified")
            return None
        if resp.status_code >= 400:
            span.set_attribute("ingest.error", f"status={resp.status_code}")
            resp.raise_for_status()
        content_type = resp.headers.get("content-type", "application/octet-stream").split(";")[0]
        if expected_content_type and not content_type.startswith(expected_content_type):
            span.set_attribute("ingest.skipped", f"content_type={content_type}")
            return None

        payload = resp.content
        fetched_at = datetime.now(tz=UTC)
        key = bronze_object_key(
            source_feed=source_feed,
            doc_id=doc_id,
            fetched_at=fetched_at,
            extension=extension,
        )
        with tracer.start_as_current_span("s3.put") as s3_span:
            stored = await minio.put_bronze(
                key=key,
                payload=payload,
                content_type=content_type,
                gzip_compress=True,
                metadata={
                    "doc_id": doc_id,
                    "source_feed": source_feed,
                    "url": canon,
                },
            )
            s3_span.set_attribute("s3.bytes", stored)

        trace_id_hex = admission.decision.trace_id
        record = BronzeRecord(
            doc_id=doc_id,
            url=canon,  # type: ignore[arg-type]
            fetched_at=fetched_at,
            http_status=resp.status_code,
            http_last_modified=parse_http_date(resp.headers.get("last-modified")),
            content_type=content_type,
            raw_html_s3_uri=bronze_s3_uri(
                bucket=bucket,
                source_feed=source_feed,
                doc_id=doc_id,
                fetched_at=fetched_at,
                extension=extension,
            ),
            source_feed=source_feed,
            trace_id=trace_id_hex,
            etag=resp.headers.get("etag"),
            bytes_size=stored,
            source_format=source_format,
            extraction_pipeline=extraction_pipeline,
            spdx_license=admission.license_id,
            spdx_license_source=cast(SpdxLicenseSource, admission.decision.license_source),
            training_usage=admission.training_usage,  # type: ignore[arg-type]
        )
        with tracer.start_as_current_span("kafka.produce"):
            await producer.send(record)
        return record
