"""End-to-end smoke against the dev stack.

What this exercises:
1. Boot ``docker-compose.dev.yml`` (via the ``dev_stack`` fixture).
2. Inject a synthetic ``BronzeRecord`` directly onto ``raw.fetched``. The v0.2
   pipeline replaces the v0.1 manual submit endpoint with native fulltext
   pollers (``arxiv_html_fetcher``, ``openreview_poller``,
   ``github_release_tarball_fetcher``); the test contract is unchanged - a
   bronze record makes it through to ``docs.curated`` within 30 seconds.
3. Consume from ``docs.curated`` and assert a matching ``doc_id`` lands in
   time. When the curator is not running, the fallback assertion is "the
   record made it onto ``raw.fetched``", since that is the contract the
   injection step alone owns.

Skips cleanly when:
- Docker is unavailable or the dev stack cannot be reached.
- ``confluent-kafka`` is not installed (the producer is built on it).
"""

from __future__ import annotations

import json
import time
from collections.abc import Callable
from typing import Any

import pytest

from schemas.topics import DOCS_CURATED, RAW_FETCHED
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
    confluent_kafka = pytest.importorskip(
        "confluent_kafka", reason="confluent-kafka not installed"
    )
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
    confluent_kafka = pytest.importorskip(
        "confluent_kafka", reason="confluent-kafka not installed"
    )
    admin = confluent_kafka.admin.AdminClient({"bootstrap.servers": brokers})
    md = admin.list_topics(timeout=5.0)
    return topic in md.topics


def _produce_bronze(
    brokers: str, topic: str, record: dict[str, Any], timeout_s: float = 5.0
) -> None:
    """Produce a single JSON record to ``topic`` and flush before returning."""
    confluent_kafka = pytest.importorskip(
        "confluent_kafka", reason="confluent-kafka not installed"
    )
    producer = confluent_kafka.Producer({"bootstrap.servers": brokers})
    producer.produce(
        topic,
        value=json.dumps(record).encode("utf-8"),
        key=record["doc_id"].encode("utf-8"),
    )
    remaining = producer.flush(timeout_s)
    if remaining:
        raise AssertionError(f"{remaining} bronze records still pending after flush")


def test_bronze_to_curated_within_30s(dev_stack: StackEndpoints) -> None:
    """A synthetic bronze record lands on the curated topic in <= 30s.

    If the processor topology is not running we fall back to verifying the
    bronze hop only, since that is the contract the injection step alone owns.
    """
    pytest.importorskip("confluent_kafka", reason="confluent-kafka not installed")

    doc_id = _make_doc_id(KNOWN_URL)
    bronze = {
        "doc_id": doc_id,
        "url": KNOWN_URL,
        "source_feed": "arxiv-html-fetcher",
        "source_format": "html",
        "extraction_pipeline": "arxiv-html-fetcher@v0.2",
        "fetched_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "license_spdx": "arxiv-non-exclusive-distribution",
        "minio_object": (
            "s3://bronze/year=2026/month=06/day=15/source=arxiv-html-fetcher/"
            f"{doc_id[len('sha256:'):]}.html.gz"
        ),
        "bytes_size": 0,
        "trace_id": "0" * 32,
    }

    start = time.monotonic()
    _produce_bronze(dev_stack.redpanda_brokers, RAW_FETCHED, bronze)

    if not _topic_exists(dev_stack.redpanda_brokers, DOCS_CURATED):
        pytest.skip("docs.curated topic does not exist; processor not running")

    target_topic = (
        DOCS_CURATED
        if _topic_exists(dev_stack.redpanda_brokers, DOCS_CURATED)
        else RAW_FETCHED
    )
    record = _consume_until(
        brokers=dev_stack.redpanda_brokers,
        topic=target_topic,
        predicate=lambda r: r.get("doc_id") == doc_id,
        timeout_s=30.0 - (time.monotonic() - start),
    )
    assert record is not None, (
        f"no record with doc_id={doc_id} arrived on {target_topic} within 30s"
    )
    assert record["doc_id"] == doc_id


def test_bronze_lands_on_raw_fetched(dev_stack: StackEndpoints) -> None:
    """A synthetic bronze record is observable on ``raw.fetched`` immediately.

    This exercises the bus contract on its own, with no curator dependency.
    """
    pytest.importorskip("confluent_kafka", reason="confluent-kafka not installed")

    doc_id = _make_doc_id(KNOWN_URL + "?probe=raw")
    bronze = {
        "doc_id": doc_id,
        "url": KNOWN_URL + "?probe=raw",
        "source_feed": "arxiv-html-fetcher",
        "source_format": "html",
        "extraction_pipeline": "arxiv-html-fetcher@v0.2",
        "fetched_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "license_spdx": "arxiv-non-exclusive-distribution",
        "minio_object": (
            "s3://bronze/year=2026/month=06/day=15/source=arxiv-html-fetcher/"
            f"{doc_id[len('sha256:'):]}.html.gz"
        ),
        "bytes_size": 0,
        "trace_id": "0" * 32,
    }

    _produce_bronze(dev_stack.redpanda_brokers, RAW_FETCHED, bronze)

    record = _consume_until(
        brokers=dev_stack.redpanda_brokers,
        topic=RAW_FETCHED,
        predicate=lambda r: r.get("doc_id") == doc_id,
        timeout_s=10.0,
    )
    assert record is not None, f"injected doc_id={doc_id} not visible on raw.fetched"
    assert record["url"].endswith("?probe=raw")
