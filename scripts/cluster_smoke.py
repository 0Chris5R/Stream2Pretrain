"""Inject one controlled document and verify the live cluster data path."""

from __future__ import annotations

import gzip
import hashlib
import json
import os
import secrets
import time
from datetime import UTC, datetime
from typing import Any

import boto3
from confluent_kafka import Consumer, Producer

from schemas.bronze import BronzeRecord


def required_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"missing environment variable: {name}")
    return value


def consume_document(topic: str, doc_id: str, timeout_seconds: float) -> dict[str, Any] | None:
    consumer = Consumer(
        {
            "bootstrap.servers": required_env("REDPANDA_BROKERS"),
            "group.id": f"s2p-cluster-smoke-{secrets.token_hex(6)}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([topic])
    deadline = time.monotonic() + timeout_seconds
    try:
        while time.monotonic() < deadline:
            message = consumer.poll(1.0)
            if message is None or message.error():
                continue
            try:
                record = json.loads(message.value())
            except (TypeError, ValueError):
                continue
            if record.get("doc_id") == doc_id:
                return record
    finally:
        consumer.close()
    return None


def main() -> None:
    started = time.monotonic()
    now = datetime.now(UTC)
    probe_id = secrets.token_hex(8)
    url = f"https://example.org/stream2pretrain/cluster-smoke/{probe_id}"
    doc_id = "sha256:" + hashlib.sha256(url.encode()).hexdigest()
    body = " ".join(
        f"Experiment {probe_id}-{index} measures a distinct research signal with "
        f"reproducible method {hashlib.sha256(f'{probe_id}-{index}'.encode()).hexdigest()[:12]}. "
        "The observation records extraction, stateful routing, benchmark screening, "
        "and durable lakehouse storage."
        for index in range(24)
    )
    html = (
        "<!doctype html><html><head><title>Cluster smoke research paper</title></head>"
        "<body><article><h1>Cluster smoke research paper</h1><h2>Abstract</h2><p>"
        + body
        + "</p><h2>Methods</h2><p>Every stage is checked by document identifier.</p>"
        "</article></body></html>"
    ).encode()
    payload = gzip.compress(html)

    bucket = required_env("MINIO_BRONZE_BUCKET")
    key = (
        f"year={now:%Y}/month={now:%m}/day={now:%d}/"
        f"source=cluster-smoke/{doc_id.removeprefix('sha256:')}.html.gz"
    )
    s3 = boto3.client(
        "s3",
        endpoint_url=required_env("MINIO_ENDPOINT"),
        aws_access_key_id=required_env("MINIO_ACCESS_KEY"),
        aws_secret_access_key=required_env("MINIO_SECRET_KEY"),
        region_name=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
    )
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=payload,
        ContentType="text/html",
        ContentEncoding="gzip",
    )

    bronze = BronzeRecord(
        doc_id=doc_id,
        url=url,
        fetched_at=now,
        http_status=200,
        content_type="text/html",
        raw_html_s3_uri=f"s3://{bucket}/{key}",
        source_feed="cluster-smoke",
        trace_id=secrets.token_hex(16),
        bytes_size=len(payload),
        source_format="html",
        extraction_pipeline="cluster-smoke-1.0",
        spdx_license="CC0-1.0",
        spdx_license_source="manual_override",
    )
    producer = Producer({"bootstrap.servers": required_env("REDPANDA_BROKERS")})
    producer.produce(
        required_env("S2P_RAW_TOPIC"),
        key=doc_id.encode(),
        value=json.dumps(bronze.model_dump(mode="json")).encode(),
    )
    if producer.flush(10.0):
        raise RuntimeError("the controlled Bronze record was not delivered")

    decision_topic = required_env("S2P_DECISIONS_TOPIC")
    decision = consume_document(decision_topic, doc_id, 60.0)
    if decision is None:
        raise RuntimeError(f"no {decision_topic} result for {doc_id} within 60 seconds")

    trainable = (
        decision.get("risk_tier") == 1
        and decision.get("route") in {"broad_pretraining", "reasoning_candidate"}
        and not decision.get("reject_reasons")
        and not decision.get("pii_flags")
        and not decision.get("contaminated_with")
    )
    curated_seen = False
    if trainable:
        curated_seen = consume_document(required_env("S2P_CURATED_TOPIC"), doc_id, 30.0) is not None
        if not curated_seen:
            raise RuntimeError(f"training-eligible document missing from docs.curated: {doc_id}")

    print(
        json.dumps(
            {
                "doc_id": doc_id,
                "bronze_s3_uri": bronze.raw_html_s3_uri,
                "decision_route": decision.get("route"),
                "risk_tier": decision.get("risk_tier"),
                "reject_reasons": decision.get("reject_reasons"),
                "curated_seen": curated_seen,
                "elapsed_seconds": round(time.monotonic() - started, 3),
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
