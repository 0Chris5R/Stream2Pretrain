"""Async Kafka producer wrapper for bronze records.

Uses aiokafka. The wrapper:

- serializes the BronzeRecord with orjson (deterministic, fast)
- uses doc_id as the message key so all attempts to fetch the same URL hash to
  the same partition (downstream dedup is therefore partition-local)
- adds W3C-style trace headers so the processor can pick up the active span
- is a context manager so a CronJob entrypoint cleans up cleanly on exit
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Self

import orjson

from schemas.bronze import BronzeRecord

if TYPE_CHECKING:
    from aiokafka import AIOKafkaProducer


class BronzeProducer:
    """Async aiokafka producer scoped to the ``raw.fetched`` topic."""

    def __init__(
        self,
        bootstrap_servers: str,
        topic: str = "raw.fetched",
        *,
        client_id: str = "s2p-ingest",
        producer: "AIOKafkaProducer | None" = None,
    ) -> None:
        self._bootstrap = bootstrap_servers
        self._topic = topic
        self._client_id = client_id
        self._producer = producer
        self._owns_producer = producer is None

    @property
    def topic(self) -> str:
        return self._topic

    async def __aenter__(self) -> Self:
        await self.start()
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.stop()

    async def start(self) -> None:
        """Connect the underlying aiokafka producer."""
        if self._producer is not None:
            await self._producer.start()
            return
        from aiokafka import AIOKafkaProducer

        self._producer = AIOKafkaProducer(
            bootstrap_servers=self._bootstrap,
            client_id=self._client_id,
            enable_idempotence=True,
            acks="all",
            compression_type="zstd",
            linger_ms=20,
            max_batch_size=131072,
        )
        await self._producer.start()

    async def stop(self) -> None:
        if self._producer is None:
            return
        await self._producer.stop()
        if self._owns_producer:
            self._producer = None

    async def send(self, record: BronzeRecord, *, headers: dict[str, str] | None = None) -> None:
        """Publish a single BronzeRecord to ``raw.fetched``.

        The record is serialized via Pydantic's ``model_dump_json`` to honour
        ``HttpUrl`` and ``datetime`` formatting rules.
        """
        if self._producer is None:
            raise RuntimeError("BronzeProducer.send called before start()")
        payload = record.model_dump_json(by_alias=True).encode("utf-8")
        key = record.doc_id.encode("utf-8")
        kafka_headers: list[tuple[str, bytes]] = [
            ("trace_id", record.trace_id.encode("ascii")),
            ("source_feed", record.source_feed.encode("utf-8")),
            ("schema", b"BronzeRecord/v1"),
        ]
        if headers:
            for k, v in headers.items():
                kafka_headers.append((k, v.encode("utf-8")))
        await self._producer.send_and_wait(
            self._topic,
            payload,
            key=key,
            headers=kafka_headers,
        )

    async def send_many(self, records: list[BronzeRecord]) -> int:
        """Send a batch; returns count emitted. Caller handles partial failures."""
        sent = 0
        for r in records:
            await self.send(r)
            sent += 1
        return sent


def serialize_bronze(record: BronzeRecord) -> bytes:
    """Public helper for tests that want the wire shape without aiokafka."""
    return record.model_dump_json(by_alias=True).encode("utf-8")


def deserialize_bronze(payload: bytes) -> dict[str, Any]:
    """Decode a wire payload back to a dict for assertions in tests."""
    return orjson.loads(payload)
