"""End-to-end smoke against the dev stack.

What this exercises:
1. Boot ``docker-compose.dev.yml`` (via the ``dev_stack`` fixture).
2. Inject a synthetic ``BronzeRecord`` directly onto ``raw.fetched``. The v0.2
   pipeline replaces the v0.1 manual submit endpoint with native fulltext
   pollers (``arxiv_html_fetcher`` and ``hf_poller``).
3. Consume the matching scored outcome from ``curation.decisions`` within 30
   seconds. Every document must reach this durable audit stream, including
   quarantine, retry, benchmark-reserve, and training-eligible outcomes.
4. Require a second copy on ``docs.curated`` only when the recorded decision
   is actually training-eligible. A quality gate is allowed to quarantine a
   synthetic probe; that is successful processing, not a missing event.

Skips cleanly when:
- Docker is unavailable or the dev stack cannot be reached.
- ``confluent-kafka`` is not installed (the producer is built on it).
"""

from __future__ import annotations

import gzip
import json
import time
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

import pytest

from schemas.bronze import BronzeRecord
from schemas.topics import CURATION_DECISIONS, DOCS_CURATED, RAW_FETCHED
from tests.conftest import StackEndpoints

pytestmark = pytest.mark.integration

# A pinned arXiv id whose URL is small and stable. The test injects a bronze
# pointer for it directly; no live HTTP fetch is involved.
KNOWN_URL = "https://export.arxiv.org/abs/2402.00159"


def _make_doc_id(url: str) -> str:
    """Mirror ``schemas.bronze.canonical_doc_id`` without the import dance."""
    import hashlib

    return "sha256:" + hashlib.sha256(url.encode("utf-8")).hexdigest()


def _consume_until(
    brokers: str,
    topic: str,
    predicate: Callable[[dict[str, Any]], bool],
    timeout_s: float,
) -> dict[str, Any] | None:
    """Tail ``topic`` from the earliest offset until ``predicate(record)`` is true."""
    confluent_kafka = pytest.importorskip("confluent_kafka", reason="confluent-kafka not installed")
    consumer = confluent_kafka.Consumer(
        {
            "bootstrap.servers": brokers,
            "group.id": f"s2p-it-{int(time.time() * 1000)}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    try:
        consumer.subscribe([topic])
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                continue
            try:
                record = json.loads(msg.value())
            except (TypeError, ValueError):
                continue
            if predicate(record):
                return record
        return None
    finally:
        consumer.close()


def _topic_exists(brokers: str, topic: str) -> bool:
    pytest.importorskip("confluent_kafka", reason="confluent-kafka not installed")
    from confluent_kafka.admin import AdminClient

    admin = AdminClient({"bootstrap.servers": brokers})
    md = admin.list_topics(timeout=5.0)
    return topic in md.topics


def _produce_bronze(
    brokers: str, topic: str, record: dict[str, Any], timeout_s: float = 5.0
) -> None:
    """Produce a single JSON record to ``topic`` and flush before returning."""
    confluent_kafka = pytest.importorskip("confluent_kafka", reason="confluent-kafka not installed")
    producer = confluent_kafka.Producer({"bootstrap.servers": brokers})
    producer.produce(
        topic,
        value=json.dumps(record).encode("utf-8"),
        key=record["doc_id"].encode("utf-8"),
    )
    remaining = producer.flush(timeout_s)
    if remaining:
        raise AssertionError(f"{remaining} bronze records still pending after flush")


def _persist_smoke_document(endpoints: StackEndpoints, *, url: str, body: str) -> dict[str, Any]:
    """Persist valid Bronze bytes and return the current wire schema."""
    import boto3

    doc_id = _make_doc_id(url)
    fetched_at = datetime.now(UTC)
    key = (
        f"year={fetched_at:%Y}/month={fetched_at:%m}/day={fetched_at:%d}/"
        f"source=local-integration-smoke/{doc_id.removeprefix('sha256:')}.html.gz"
    )
    html = (
        "<!doctype html><html><head><title>Integration smoke paper</title></head>"
        "<body><article><h1>Integration smoke paper</h1>"
        "<h6 class='ltx_title_abstract'>Abstract</h6>"
        "<p>This controlled integration paper verifies a complete event path.</p>"
        "<h2>Methods</h2><p>" + body + "</p>"
        "<h2>Results</h2><p>The resulting record is checked on the clean output topic.</p>"
        "</article></body></html>"
    ).encode()
    payload = gzip.compress(html)
    client = boto3.client(
        "s3",
        endpoint_url=endpoints.minio_endpoint,
        aws_access_key_id=endpoints.minio_access_key,
        aws_secret_access_key=endpoints.minio_secret_key,
        region_name="us-east-1",
    )
    client.put_object(
        Bucket="bronze",
        Key=key,
        Body=payload,
        ContentType="text/html",
        ContentEncoding="gzip",
    )
    record = BronzeRecord(
        doc_id=doc_id,
        url=url,
        fetched_at=fetched_at,
        http_status=200,
        content_type="text/html",
        raw_html_s3_uri=f"s3://bronze/{key}",
        source_feed="local-integration-smoke",
        trace_id="0" * 32,
        bytes_size=len(payload),
        source_format="html",
        extraction_pipeline="integration-smoke-1.0",
        spdx_license="CC0-1.0",
        spdx_license_source="manual_override",
    )
    return record.model_dump(mode="json")


def test_bronze_to_durable_decision_within_30s(dev_stack: StackEndpoints) -> None:
    """A synthetic bronze record receives a durable decision in <= 30s."""
    pytest.importorskip("confluent_kafka", reason="confluent-kafka not installed")

    url = f"{KNOWN_URL}?probe=e2e-{time.time_ns()}"
    body = " ".join(
        [
            "A reproducible streaming pipeline records raw bytes, structured extraction, "
            "independent quality signals, and a durable routing decision for later audit."
            for _ in range(14)
        ]
    )
    bronze = _persist_smoke_document(dev_stack, url=url, body=body)
    doc_id = str(bronze["doc_id"])

    start = time.monotonic()
    _produce_bronze(dev_stack.redpanda_brokers, RAW_FETCHED, bronze)

    if not _topic_exists(dev_stack.redpanda_brokers, CURATION_DECISIONS):
        pytest.skip("curation.decisions topic does not exist; processor not running")

    decision = _consume_until(
        brokers=dev_stack.redpanda_brokers,
        topic=CURATION_DECISIONS,
        predicate=lambda r: r.get("doc_id") == doc_id,
        timeout_s=30.0 - (time.monotonic() - start),
    )
    assert decision is not None, (
        f"no record with doc_id={doc_id} arrived on {CURATION_DECISIONS} within 30s"
    )
    assert decision["doc_id"] == doc_id
    assert decision["route"] in {
        "broad_pretraining",
        "posttrain_candidate",
        "quarantine",
        "retry",
    }
    assert isinstance(decision["reject_reasons"], list)

    trainable = (
        decision["risk_tier"] == 1
        and decision["route"] in {"broad_pretraining", "posttrain_candidate"}
        and not decision["reject_reasons"]
        and not decision["pii_flags"]
    )
    if trainable:
        assert _topic_exists(dev_stack.redpanda_brokers, DOCS_CURATED)
        curated = _consume_until(
            brokers=dev_stack.redpanda_brokers,
            topic=DOCS_CURATED,
            predicate=lambda r: r.get("doc_id") == doc_id,
            timeout_s=max(1.0, 30.0 - (time.monotonic() - start)),
        )
        assert curated is not None, (
            f"training-eligible doc_id={doc_id} did not arrive on {DOCS_CURATED}"
        )
    else:
        assert decision["reject_reasons"] or decision["route"] in {
            "retry",
            "quarantine",
        }


def test_bronze_lands_on_raw_fetched(dev_stack: StackEndpoints) -> None:
    """A synthetic bronze record is observable on ``raw.fetched`` immediately.

    This exercises the bus contract on its own, with no curator dependency.
    """
    pytest.importorskip("confluent_kafka", reason="confluent-kafka not installed")

    url = f"{KNOWN_URL}?probe=raw-{time.time_ns()}"
    bronze = _persist_smoke_document(dev_stack, url=url, body="Raw bus probe text. " * 60)
    doc_id = str(bronze["doc_id"])

    _produce_bronze(dev_stack.redpanda_brokers, RAW_FETCHED, bronze)

    record = _consume_until(
        brokers=dev_stack.redpanda_brokers,
        topic=RAW_FETCHED,
        predicate=lambda r: r.get("doc_id") == doc_id,
        timeout_s=10.0,
    )
    assert record is not None, f"injected doc_id={doc_id} not visible on raw.fetched"
    assert record["url"] == url
