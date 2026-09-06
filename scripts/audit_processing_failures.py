"""Read-only correlation of durable processing failures with Kafka metadata.

Run this inside a processor-fetcher Pod. It lists the deterministic failure
objects in MinIO, seeks to their exact retained Kafka coordinates without
committing offsets, and prints aggregate source/reason counts. Raw payloads,
document text, object-store credentials, and Kafka credentials are never
printed.
"""

from __future__ import annotations

import gzip
import hashlib
import io
import json
import os
import re
from collections import Counter
from typing import Any

import boto3
import orjson

_MAX_CARD_BYTES = 8 * 1024 * 1024
_MARKDOWN_LINK = re.compile(r"!?\[([^\]]*)\]\([^)]*\)")
_MARKDOWN_HTML = re.compile(r"<[^>]+>")


def _required(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"{name} is required")
    return value


def inspect_markdown(payload: bytes) -> dict[str, Any]:
    """Return bounded structural facts using the fetcher's projection rules."""
    raw = payload.decode("utf-8", errors="replace").strip()
    lines = raw.splitlines()
    start = 0
    if lines and lines[0].strip() == "---":
        for index in range(1, len(lines)):
            if lines[index].strip() in {"---", "..."}:
                start = index + 1
                break

    prose: list[str] = []
    first_heading_length = 0
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
        if in_fence or stripped.startswith("<!--") or not stripped:
            continue
        is_heading = stripped.startswith("#")
        cleaned = stripped.lstrip("#> ").strip()
        cleaned = re.sub(r"^[-*+]\s+", "", cleaned)
        cleaned = _MARKDOWN_LINK.sub(lambda match: match.group(1), cleaned)
        cleaned = _MARKDOWN_HTML.sub(" ", cleaned)
        cleaned = " ".join(cleaned.split())
        if cleaned:
            prose.append(cleaned)
            if is_heading and first_heading_length == 0:
                first_heading_length = len(cleaned)
    return {
        "has_prose": bool(prose),
        "first_heading_length": first_heading_length,
    }


def classify_failure(
    failure: dict[str, Any],
    record: dict[str, Any] | None,
    raw_body: bytes | None,
) -> str:
    """Classify a failure without executing extraction or model code."""
    reason = str(failure.get("reason") or "unknown")
    source = str((record or {}).get("source_feed") or "unknown")
    is_hf = source in {"hf-models", "hf-datasets"}
    if reason == "RawObjectEmpty" and is_hf:
        return "intentional_empty_hf_card"
    if not is_hf or raw_body is None:
        return reason
    inspection = inspect_markdown(raw_body)
    if not inspection["has_prose"]:
        return "intentional_no_prose_hf_card"
    if reason == "ValidationError" and inspection["first_heading_length"] > 2048:
        return "oversized_hf_card_title"
    return reason


def _read_raw_card(s3: Any, record: dict[str, Any]) -> bytes | None:
    uri = record.get("raw_html_s3_uri")
    if not isinstance(uri, str) or not uri.startswith("s3://"):
        return None
    bucket, _, key = uri[5:].partition("/")
    if not bucket or not key:
        return None
    response = s3.get_object(Bucket=bucket, Key=key)
    body = bytes(response["Body"].read(_MAX_CARD_BYTES + 1))
    if len(body) > _MAX_CARD_BYTES:
        return None
    if uri.endswith(".gz") or response.get("ContentEncoding") == "gzip":
        with gzip.GzipFile(fileobj=io.BytesIO(body)) as compressed:
            body = compressed.read(_MAX_CARD_BYTES + 1)
        if len(body) > _MAX_CARD_BYTES:
            return None
    return body


def _failure_objects(s3: Any, *, bucket: str, prefix: str) -> list[dict[str, Any]]:
    failures: list[dict[str, Any]] = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for item in page.get("Contents", []):
            key = str(item.get("Key") or "")
            if not key.endswith(".json"):
                continue
            response = s3.get_object(Bucket=bucket, Key=key)
            value = orjson.loads(response["Body"].read())
            if isinstance(value, dict):
                failures.append(value)
    return failures


def _kafka_records(failures: list[dict[str, Any]]) -> dict[tuple[str, int, int], dict[str, Any]]:
    from confluent_kafka import Consumer, TopicPartition

    consumer = Consumer(
        {
            "bootstrap.servers": _required("REDPANDA_BROKERS"),
            "group.id": "s2p-read-only-processing-failure-audit",
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
            "auto.offset.reset": "error",
        }
    )
    records: dict[tuple[str, int, int], dict[str, Any]] = {}
    try:
        for failure in failures:
            topic = str(failure.get("topic") or "")
            partition = int(failure.get("partition", -1))
            offset = int(failure.get("offset", -1))
            if not topic or partition < 0 or offset < 0:
                continue
            coordinate = (topic, partition, offset)
            consumer.assign([TopicPartition(topic, partition, offset)])
            message = consumer.poll(10.0)
            if message is None or message.error() or message.offset() != offset:
                continue
            payload = bytes(message.value() or b"")
            if hashlib.sha256(payload).hexdigest() != failure.get("payload_sha256"):
                continue
            try:
                value = orjson.loads(payload)
            except orjson.JSONDecodeError:
                continue
            if isinstance(value, dict):
                records[coordinate] = value
    finally:
        consumer.close()
    return records


def main() -> None:
    s3 = boto3.client(
        "s3",
        endpoint_url=_required("MINIO_ENDPOINT"),
        aws_access_key_id=os.environ.get("MINIO_ACCESS_KEY") or _required("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.environ.get("MINIO_SECRET_KEY")
        or _required("AWS_SECRET_ACCESS_KEY"),
        region_name=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
    )
    bucket = (
        os.environ.get("S2P_PROCESSING_FAILURE_BUCKET")
        or os.environ.get("S2P_STATE_BUCKET")
        or _required("MINIO_GOLD_BUCKET")
    )
    base_prefix = os.environ.get("S2P_PROCESSING_FAILURE_PREFIX", "processing-failures").strip("/")
    prefix = f"{base_prefix}/stage=fetcher/topic=raw.fetched/"
    failures = _failure_objects(s3, bucket=bucket, prefix=prefix)
    records = _kafka_records(failures)

    by_source_reason: Counter[tuple[str, str, str]] = Counter()
    unresolved = 0
    for failure in failures:
        coordinate = (
            str(failure.get("topic") or ""),
            int(failure.get("partition", -1)),
            int(failure.get("offset", -1)),
        )
        record = records.get(coordinate)
        if record is None:
            unresolved += 1
        source = str((record or {}).get("source_feed") or "unresolved")
        raw_body = None
        if record is not None and source in {"hf-models", "hf-datasets"}:
            try:
                raw_body = _read_raw_card(s3, record)
            except Exception:
                raw_body = None
        category = classify_failure(failure, record, raw_body)
        by_source_reason[(source, str(failure.get("reason") or "unknown"), category)] += 1

    print(
        json.dumps(
            {
                "failure_objects": len(failures),
                "kafka_records_unresolved": unresolved,
                "counts": [
                    {
                        "source": source,
                        "reason": reason,
                        "category": category,
                        "count": count,
                    }
                    for (source, reason, category), count in sorted(by_source_reason.items())
                ],
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
