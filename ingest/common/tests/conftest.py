"""Shared pytest fixtures for the ingest test suite."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import pytest

from ingest.common.config import IngestConfig


@pytest.fixture
def cfg(tmp_path: Any) -> IngestConfig:
    """An in-memory config that talks to nothing real."""
    return IngestConfig(
        env="dev",
        log_level="DEBUG",
        redpanda_brokers="localhost:9092",
        minio_endpoint="http://localhost:9000",
        minio_access_key="minioadmin",
        minio_secret_key="minioadmin",
        minio_bronze_bucket="bronze",
        otel_endpoint=None,
        otel_protocol="grpc",
        hf_token="hf_test",
        user_agent="Stream2Pretrain-Test/0.1",
        http_timeout_seconds=5.0,
        http_max_retries=0,
        feed_config_path=None,
        request_jitter_max_seconds=0.0,
    )


@pytest.fixture
def fixed_now() -> datetime:
    """A deterministic UTC instant."""
    return datetime(2026, 6, 15, 12, 0, 0, tzinfo=UTC)


class FakeProducer:
    """In-memory drop-in for ``BronzeProducer`` used in tests."""

    def __init__(self) -> None:
        self.sent: list[dict[str, Any]] = []
        self.started = False
        self.stopped = False

    async def __aenter__(self) -> FakeProducer:
        await self.start()
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.stop()

    async def start(self) -> None:
        self.started = True

    async def stop(self) -> None:
        self.stopped = True

    async def send(self, record: Any, *, headers: dict[str, str] | None = None) -> None:
        self.sent.append({"record": record, "headers": headers})


class FakeMinio:
    """In-memory drop-in for ``MinioWriter``."""

    def __init__(self, bucket: str = "bronze") -> None:
        self.bucket = bucket
        self.objects: dict[str, dict[str, Any]] = {}
        self.started = False

    async def __aenter__(self) -> FakeMinio:
        await self.start()
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.stop()

    async def start(self) -> None:
        self.started = True

    async def stop(self) -> None:
        self.started = False

    async def put_bronze(
        self,
        *,
        key: str,
        payload: bytes,
        content_type: str = "text/html",
        gzip_compress: bool = True,
        metadata: dict[str, str] | None = None,
    ) -> int:
        self.objects[key] = {
            "payload": payload,
            "content_type": content_type,
            "gzip_compress": gzip_compress,
            "metadata": metadata or {},
        }
        return len(payload)

    async def ensure_bucket(self) -> None:
        return None


@pytest.fixture
def fake_producer() -> FakeProducer:
    return FakeProducer()


@pytest.fixture
def fake_minio() -> FakeMinio:
    return FakeMinio()
