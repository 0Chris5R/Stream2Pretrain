"""Inject one controlled document and verify the live cluster data path."""

from __future__ import annotations

import gzip
import hashlib
import json
import os
import secrets
import time
import zlib
from contextlib import suppress
from datetime import UTC, datetime
from typing import Any

import boto3
from confluent_kafka import Consumer, Producer, TopicPartition

from ingest.common.license_admission import decide_license_admission
from schemas.bronze import BronzeRecord

_CANARY_SENTENCES = (
    "The study evaluates a streaming system under a controlled and repeatable workload.",
    "Each observation follows the same documented protocol and records its processing time.",
    "The research question concerns reliable data movement across independent pipeline stages.",
    "Measurements are collected at the input, transformation, classification, and storage layers.",
    "A reproducible method allows later runs to be compared with the original experiment.",
    "The analysis distinguishes temporary service delays from persistent processing failures.",
    "Results are linked by a stable document identifier throughout the distributed workflow.",
    "The source material uses ordinary technical prose and an explicitly permissive license.",
    "Quality checks examine language, structure, duplication, privacy, and statistical typicality.",
    "The experiment verifies that accepted records reach durable analytical storage.",
    "A separate decision event explains why the document was accepted or rejected.",
    "The evaluation preserves enough metadata to support an independent audit of every stage.",
    "Researchers can repeat the procedure without relying on hidden manual intervention.",
    "The system processes the sample with the same services used for production documents.",
    "Structured headings identify the abstract, method, observations, and resulting conclusion.",
    "The benchmark checks both message delivery and the semantic outcome of classification.",
    "Consumer offsets are observed only after the corresponding record has been processed.",
    "The method avoids assumptions about the amount of unrelated work already in the queue.",
    "Storage verification confirms that the accepted text is available to downstream training jobs.",
    "Operational metrics provide additional evidence about latency and resource utilization.",
    "The controlled sample contains no personal information or restricted copyrighted material.",
    "Failure is reported when any required stage omits the document within its bounded deadline.",
    "The experiment therefore measures an end to end property rather than a shallow health check.",
    "Independent consumers observe normalization, policy decisions, and curated output events.",
    "The final record retains provenance needed to explain its origin and permitted purpose.",
    "A successful run demonstrates that the deployed components agree on their shared schemas.",
    "The procedure is intentionally small so it does not distort normal pipeline capacity.",
    "Repeated trials vary their natural language evidence while preserving the same research goal.",
    "The reported outcome includes timing information for diagnosing future regressions.",
    "These observations support a clear conclusion about the readiness of the deployed release.",
)


def canary_body(probe_id: str) -> str:
    """Return varied natural prose without injecting an out-of-vocabulary nonce.

    The URL already makes every canary unique. Selecting a probe-specific subset
    prevents MinHash collisions across releases while keeping the language valid
    for the production KenLM typicality policy.
    """
    ranked = sorted(
        _CANARY_SENTENCES,
        key=lambda sentence: hashlib.sha256(f"{probe_id}:{sentence}".encode()).digest(),
    )
    return " ".join(ranked[:15])


def required_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"missing environment variable: {name}")
    return value


def topic_partition_count(producer: Producer, topic: str) -> int:
    metadata = producer.list_topics(topic, timeout=10.0)
    topic_metadata = metadata.topics.get(topic)
    if topic_metadata is None or topic_metadata.error is not None:
        raise RuntimeError(f"cannot inspect smoke topic {topic}: {topic_metadata}")
    count = len(topic_metadata.partitions)
    if count < 1:
        raise RuntimeError(f"smoke topic has no partitions: {topic}")
    return count


def tail_consumer(topic: str) -> Consumer:
    """Create a consumer assigned to the current tail of every topic partition."""
    consumer = Consumer(
        {
            "bootstrap.servers": required_env("REDPANDA_BROKERS"),
            "group.id": f"s2p-cluster-smoke-{secrets.token_hex(6)}",
            "enable.auto.commit": False,
        }
    )
    metadata = consumer.list_topics(topic, timeout=10.0)
    topic_metadata = metadata.topics.get(topic)
    if topic_metadata is None or topic_metadata.error is not None:
        consumer.close()
        raise RuntimeError(f"cannot inspect smoke topic {topic}: {topic_metadata}")
    assignments: list[TopicPartition] = []
    for partition in sorted(topic_metadata.partitions):
        topic_partition = TopicPartition(topic, partition)
        _, high = consumer.get_watermark_offsets(topic_partition, timeout=10.0)
        assignments.append(TopicPartition(topic, partition, high))
    if not assignments:
        consumer.close()
        raise RuntimeError(f"smoke topic has no partitions: {topic}")
    consumer.assign(assignments)
    return consumer


def consume_document(
    consumer: Consumer, topic: str, doc_id: str, timeout_seconds: float
) -> dict[str, Any] | None:
    deadline = time.monotonic() + timeout_seconds
    try:
        while time.monotonic() < deadline:
            message = consumer.poll(1.0)
            if message is None or message.error():
                continue
            value = message.value()
            if value is None:
                continue
            try:
                record = json.loads(value)
            except (TypeError, ValueError):
                continue
            if not isinstance(record, dict):
                continue
            if record.get("doc_id") == doc_id:
                return {str(key): value for key, value in record.items()}
    finally:
        consumer.close()
    return None


def assert_document_absent(
    consumers: dict[str, Consumer], doc_id: str, timeout_seconds: float
) -> None:
    """Prove that an isolated canary did not emit into production topics."""
    if timeout_seconds <= 0:
        raise RuntimeError("S2P_SMOKE_ISOLATION_TIMEOUT_SECONDS must be positive")
    deadline = time.monotonic() + timeout_seconds
    try:
        while time.monotonic() < deadline:
            for topic, consumer in consumers.items():
                message = consumer.poll(0.1)
                if message is None or message.error() or message.value() is None:
                    continue
                try:
                    record = json.loads(message.value())
                except (TypeError, ValueError):
                    continue
                if isinstance(record, dict) and record.get("doc_id") == doc_id:
                    raise RuntimeError(
                        f"isolated canary document leaked into production topic {topic}: {doc_id}"
                    )
    finally:
        for consumer in consumers.values():
            consumer.close()


def close_consumers(consumers: list[Consumer]) -> None:
    """Best-effort resource cleanup for every canary exit path."""
    for consumer in consumers:
        with suppress(Exception):
            consumer.close()


def main() -> None:
    started = time.monotonic()
    now = datetime.now(UTC)
    # A fixed Bytewax canary fetcher owns this short-retention lane with its own
    # flow identity, outputs, and recovery database. It uses the production
    # image and models without advancing production recovery or state.
    raw_topic = required_env("S2P_SMOKE_RAW_TOPIC")
    producer = Producer({"bootstrap.servers": required_env("REDPANDA_BROKERS")})
    raw_partition_count = topic_partition_count(producer, raw_topic)
    for _ in range(100):
        probe_id = secrets.token_hex(8)
        url = f"https://example.org/stream2pretrain/cluster-smoke/{probe_id}"
        doc_id = "sha256:" + hashlib.sha256(url.encode()).hexdigest()
        probe_partition = zlib.crc32(doc_id.encode()) % raw_partition_count
        if raw_partition_count == 1 or probe_partition != 0:
            break
    else:
        raise RuntimeError("could not select a fresh smoke partition")
    admission = decide_license_admission(
        source_url=url,
        source_feed="cluster-smoke",
        license_value="CC0-1.0",
        license_source="manual_override",
    )
    if admission.decision.doc_id != doc_id:
        raise RuntimeError("smoke document identity differs from licence admission identity")
    body = canary_body(probe_id)
    html = (
        "<!doctype html><html><head><title>Cluster smoke research paper.</title></head>"
        "<body><article><h1>Cluster smoke research paper.</h1><h2>Abstract.</h2><p>"
        + body
        + "</p><h2>Methods.</h2><p>Every stage is checked by document identifier.</p>"
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
    normalized_topic = required_env("S2P_SMOKE_NORMALIZED_TOPIC")
    decision_topic = required_env("S2P_SMOKE_DECISIONS_TOPIC")
    curated_topic = required_env("S2P_SMOKE_CURATED_TOPIC")
    normalized_consumer = tail_consumer(normalized_topic)
    decision_consumer = tail_consumer(decision_topic)
    curated_consumer = tail_consumer(curated_topic)
    production_consumers = {
        required_env("S2P_NORMALIZED_TOPIC"): tail_consumer(required_env("S2P_NORMALIZED_TOPIC")),
        required_env("S2P_DECISIONS_TOPIC"): tail_consumer(required_env("S2P_DECISIONS_TOPIC")),
        required_env("S2P_CURATED_TOPIC"): tail_consumer(required_env("S2P_CURATED_TOPIC")),
        required_env("S2P_LICENSE_ADMISSIONS_TOPIC"): tail_consumer(
            required_env("S2P_LICENSE_ADMISSIONS_TOPIC")
        ),
    }
    producer.produce(
        required_env("S2P_SMOKE_LICENSE_ADMISSIONS_TOPIC"),
        key=admission.decision.decision_id.encode(),
        value=admission.decision.model_dump_json().encode(),
    )
    if producer.flush(10.0):
        raise RuntimeError("the controlled licence admission was not delivered")

    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=payload,
        ContentType="text/html",
        ContentEncoding="gzip",
    )

    try:
        bronze = BronzeRecord(
            doc_id=doc_id,
            url=url,
            fetched_at=now,
            http_status=200,
            content_type="text/html",
            raw_html_s3_uri=f"s3://{bucket}/{key}",
            source_feed="cluster-smoke",
            trace_id=admission.decision.trace_id,
            bytes_size=len(payload),
            source_format="html",
            extraction_pipeline="cluster-smoke-1.0",
            spdx_license="CC0-1.0",
            spdx_license_source="manual_override",
        )
        producer.produce(
            raw_topic,
            key=doc_id.encode(),
            value=json.dumps(bronze.model_dump(mode="json")).encode(),
            partition=probe_partition,
        )
        if producer.flush(10.0):
            raise RuntimeError("the controlled Bronze record was not delivered")

        normalized = consume_document(normalized_consumer, normalized_topic, doc_id, 90.0)
        if normalized is None:
            decision_consumer.close()
            curated_consumer.close()
            for consumer in production_consumers.values():
                consumer.close()
            raise RuntimeError(f"no {normalized_topic} result for {doc_id} within 90 seconds")

        decision = consume_document(decision_consumer, decision_topic, doc_id, 120.0)
        if decision is None:
            curated_consumer.close()
            for consumer in production_consumers.values():
                consumer.close()
            raise RuntimeError(f"no {decision_topic} result for {doc_id} within 120 seconds")

        trainable = (
            decision.get("risk_tier") == 1
            and decision.get("route")
            in {"pretrain", "broad_pretraining", "posttrain_candidate", "reasoning_candidate"}
            and not decision.get("reject_reasons")
            and not decision.get("pii_flags")
        )
        if not trainable:
            curated_consumer.close()
            for consumer in production_consumers.values():
                consumer.close()
            raise RuntimeError(
                "controlled permissive canary was rejected instead of exercising docs.curated: "
                f"route={decision.get('route')} risk={decision.get('risk_tier')} "
                f"reasons={decision.get('reject_reasons')}"
            )
        curated_seen = consume_document(curated_consumer, curated_topic, doc_id, 60.0) is not None
        if not curated_seen:
            for consumer in production_consumers.values():
                consumer.close()
            raise RuntimeError(f"training-eligible document missing from docs.curated: {doc_id}")
        assert_document_absent(
            production_consumers,
            doc_id,
            float(os.environ.get("S2P_SMOKE_ISOLATION_TIMEOUT_SECONDS", "5")),
        )
        result = {
            "doc_id": doc_id,
            "bronze_s3_uri": bronze.raw_html_s3_uri,
            "license_admission_decision_id": admission.decision.decision_id,
            "license_admission_topic": required_env("S2P_SMOKE_LICENSE_ADMISSIONS_TOPIC"),
            "normalized_topic": normalized_topic,
            "decision_topic": decision_topic,
            "curated_topic": curated_topic,
            "decision_route": decision.get("route"),
            "risk_tier": decision.get("risk_tier"),
            "reject_reasons": decision.get("reject_reasons"),
            "curated_seen": curated_seen,
            "probe_partition": probe_partition,
            "elapsed_seconds": round(time.monotonic() - started, 3),
        }
    finally:
        # The canary event remains in short-retention smoke topics as release
        # evidence, but its synthetic source body must not accumulate in the
        # production Bronze bucket.
        close_consumers(
            [
                normalized_consumer,
                decision_consumer,
                curated_consumer,
                *production_consumers.values(),
            ]
        )
        s3.delete_object(Bucket=bucket, Key=key)
    result["bronze_object_deleted"] = True
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
