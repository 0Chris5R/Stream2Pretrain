"""Wire-format tests for the BronzeProducer serializer."""

from __future__ import annotations

import json
from datetime import UTC, datetime

import pytest

from ingest.common.kafka_producer import (
    BronzeProducer,
    deserialize_bronze,
    serialize_bronze,
)
from schemas.bronze import BronzeRecord


def _record() -> BronzeRecord:
    return BronzeRecord(
        doc_id="sha256:" + "a" * 64,
        url="https://example.com/abs/123",  # type: ignore[arg-type]
        fetched_at=datetime(2026, 6, 15, 12, 0, tzinfo=UTC),
        http_status=200,
        content_type="text/html",
        raw_html_s3_uri="s3://bronze/year=2026/month=06/day=15/source=t/abc.html.gz",
        source_feed="t",
        trace_id="0" * 32,
        bytes_size=123,
    )


def test_serialize_round_trip() -> None:
    rec = _record()
    payload = serialize_bronze(rec)
    decoded = deserialize_bronze(payload)
    assert decoded["doc_id"] == rec.doc_id
    assert decoded["http_status"] == 200
    assert decoded["bytes_size"] == 123


@pytest.mark.asyncio
async def test_send_calls_underlying_producer() -> None:
    sent: list[dict] = []

    class _StubProducer:
        async def start(self) -> None:
            pass

        async def stop(self) -> None:
            pass

        async def send_and_wait(
            self,
            topic: str,
            value: bytes,
            *,
            key: bytes,
            headers: list[tuple[str, bytes]],
        ) -> None:
            sent.append(
                {
                    "topic": topic,
                    "key": key,
                    "value": json.loads(value),
                    "headers": dict(headers),
                }
            )

    bp = BronzeProducer("localhost:9092", topic="raw.fetched", producer=_StubProducer())
    await bp.start()
    await bp.send(_record())
    await bp.stop()
    assert len(sent) == 1
    assert sent[0]["topic"] == "raw.fetched"
    assert sent[0]["key"].startswith(b"sha256:")
    assert (
        sent[0]["headers"][b"schema"] == b"BronzeRecord/v1"
        if isinstance(next(iter(sent[0]["headers"])), bytes)
        else sent[0]["headers"]["schema"] == b"BronzeRecord/v1"
    )
