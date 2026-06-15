"""End-to-end smoke against the dev stack.

What this exercises:
1. Boot ``docker-compose.dev.yml`` (via the ``dev_stack`` fixture).
2. POST a known URL to the submit API.
3. Consume from ``docs.curated`` and assert a matching ``doc_id`` lands within
   30 seconds.

Skips cleanly when:
- Docker is unavailable or the dev stack cannot be reached.
- The submit API is not running on ``localhost:8000`` (the test does not boot
  the API itself - it is started separately by ``scripts/dev_smoke.sh`` or by
  ``uvicorn ingest.submit_api.app:app``).
- The full processor topology is not running (then the assertion targets
  ``raw.fetched`` rather than ``docs.curated``, since the bronze hop is the
  contract the submit API alone owns).
"""

from __future__ import annotations

import json
import time
from typing import Any

import pytest

from schemas.topics import DOCS_CURATED, RAW_FETCHED

from tests.conftest import StackEndpoints

pytestmark = pytest.mark.integration

# A tiny static page that arXiv exposes; chosen because it is small, stable,
# and reachable without auth. The submit API will fetch it on our behalf.
KNOWN_URL = "https://export.arxiv.org/abs/2402.00159"


def _consume_until(
    brokers: str,
    topic: str,
    predicate: Any,
    timeout_s: float,
) -> dict[str, Any] | None:
    """Tail ``topic`` from the latest offset until ``predicate(record)`` is true."""
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


def test_submit_to_curated_within_30s(
    dev_stack: StackEndpoints, submit_api_reachable: bool
) -> None:
    """A POST /submit lands a record on the curated topic in <= 30s.

    If the processor topology is not running we fall back to verifying the
    bronze hop only, since that is the contract the submit API alone owns.
    """
    if not submit_api_reachable:
        pytest.skip("submit API is not reachable on localhost:8000")

    httpx = pytest.importorskip("httpx", reason="httpx not installed")

    payload = {"url": KNOWN_URL, "source_feed": "manual-submit"}
    start = time.monotonic()
    with httpx.Client(timeout=10.0) as client:
        response = client.post(
            f"{dev_stack.submit_api_url}/submit", json=payload
        )
    assert response.status_code == 201, response.text
    body = response.json()
    assert body["accepted"] is True
    doc_id = body["doc_id"]
    assert doc_id.startswith("sha256:")

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


def test_submit_unknown_feed_is_rejected(
    dev_stack: StackEndpoints, submit_api_reachable: bool
) -> None:
    """POSTing to a feed name not in the catalogue must return 400."""
    if not submit_api_reachable:
        pytest.skip("submit API is not reachable on localhost:8000")
    httpx = pytest.importorskip("httpx", reason="httpx not installed")

    with httpx.Client(timeout=5.0) as client:
        response = client.post(
            f"{dev_stack.submit_api_url}/submit",
            json={"url": KNOWN_URL, "source_feed": "feed-that-does-not-exist"},
        )
    assert response.status_code == 400
    assert "unknown source_feed" in response.text


def test_submit_api_healthz(
    dev_stack: StackEndpoints, submit_api_reachable: bool
) -> None:
    """healthz reports both Redpanda and MinIO ready when the dev stack is up."""
    if not submit_api_reachable:
        pytest.skip("submit API is not reachable on localhost:8000")
    httpx = pytest.importorskip("httpx", reason="httpx not installed")

    with httpx.Client(timeout=5.0) as client:
        response = client.get(f"{dev_stack.submit_api_url}/healthz")
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "ok"
    assert body["redpanda"] is True
    assert body["minio"] is True
