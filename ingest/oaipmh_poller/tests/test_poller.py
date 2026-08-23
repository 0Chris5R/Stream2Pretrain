from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import pytest

from ingest.common.config import IngestConfig
from ingest.common.state import FeedStateStore
from ingest.oaipmh_poller import poller
from ingest.oaipmh_poller.client import OAIPage, OAIRecord
from schemas.sourcefeed import SourceFeedSpec


class _AsyncResource:
    async def __aenter__(self) -> _AsyncResource:
        return self

    async def __aexit__(self, *_: object) -> None:
        return None


class _Producer(_AsyncResource):
    def __init__(self, *_: object, **__: object) -> None:
        self.sent: list[Any] = []

    async def send(self, record: Any, **_: object) -> None:
        self.sent.append(record)


class _Minio(_AsyncResource):
    def __init__(self, *_: object, **__: object) -> None:
        pass

    async def put_bronze(self, *, payload: bytes, **_: object) -> int:
        return len(payload)


def _cfg() -> IngestConfig:
    return IngestConfig(
        env="dev",
        log_level="DEBUG",
        redpanda_brokers="redpanda:9092",
        minio_endpoint="http://minio:9000",
        minio_access_key="access",
        minio_secret_key="secret",
        minio_bronze_bucket="bronze",
        otel_endpoint=None,
        otel_protocol="grpc",
        github_token=None,
        hf_token=None,
        user_agent="test",
    )


def _feed() -> SourceFeedSpec:
    return SourceFeedSpec.model_validate(
        {
            "name": "oai-test",
            "protocol": "oai-pmh",
            "endpoint": "https://oai.example.test/oai",
            "pollIntervalSeconds": 7200,
            "rateLimit": {"requestsPerSecond": 1000, "burst": 1000},
            "licenseDefault": "per-record",
        }
    )


def _record(identifier: str) -> OAIRecord:
    return OAIRecord(
        identifier=identifier,
        datestamp="2026-08-22",
        set_specs=["cs"],
        metadata_xml="<metadata><license>CC-BY-4.0</license></metadata>",
        raw=b"<record />",
    )


@pytest.mark.asyncio
async def test_poll_feed_resumes_the_same_window_from_checkpoint(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    pages = [OAIPage(records=[_record("oai:arXiv.org:2608.00001")], resumption_token="next")]
    calls: list[dict[str, Any]] = []

    class _OAIClient:
        def __init__(self, *_: object, **__: object) -> None:
            pass

        async def list_pages(self, **kwargs: Any):  # type: ignore[no-untyped-def]
            calls.append(kwargs)
            for page in pages:
                yield page

    monkeypatch.setattr(poller, "build_async_client", lambda *_args, **_kwargs: _AsyncResource())
    monkeypatch.setattr(poller, "BronzeProducer", _Producer)
    monkeypatch.setattr(poller, "LicenseAdmissionProducer", _Producer)
    monkeypatch.setattr(poller, "MinioWriter", _Minio)
    monkeypatch.setattr(poller, "OAIClient", _OAIClient)
    monkeypatch.setattr(poller, "_today_iso", lambda: "2026-08-22")

    store = FeedStateStore(tmp_path)
    emitted = await poller.poll_feed(
        _feed(),
        _cfg(),
        state_store=store,
        max_pages=1,
    )

    assert emitted == 1
    assert store.get("oai-test") == {
        "window_from": "2024-01-01",
        "window_until": "2026-08-22",
        "resumption_token": "next",
    }
    assert calls[0]["resumption_token"] is None

    pages[:] = [OAIPage(records=[_record("oai:arXiv.org:2608.00002")], resumption_token=None)]
    emitted = await poller.poll_feed(
        _feed(),
        _cfg(),
        state_store=store,
        max_pages=1,
    )

    assert emitted == 1
    assert calls[1]["resumption_token"] == "next"
    assert calls[1]["from_"] == "2024-01-01"
    assert calls[1]["until"] == "2026-08-22"
    assert store.get("oai-test") == {"until": "2026-08-22"}


@pytest.mark.asyncio
async def test_run_reports_feed_failure_after_attempting_all(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    attempted: list[str] = []
    feeds = [_feed(), _feed().model_copy(update={"name": "oai-test-2"})]

    async def failing_poll(feed: SourceFeedSpec, *_: object, **__: object) -> int:
        attempted.append(feed.name)
        raise RuntimeError("upstream unavailable")

    monkeypatch.setattr(poller, "poll_feed", failing_poll)

    with pytest.raises(RuntimeError, match="oai-test, oai-test-2"):
        await poller._run(_cfg(), feeds)

    assert attempted == ["oai-test", "oai-test-2"]


@pytest.mark.asyncio
async def test_poll_feed_rate_limits_page_requests_not_records(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    records = [_record(f"oai:arXiv.org:2608.{index:05d}") for index in range(3)]
    pages = [OAIPage(records=records, resumption_token=None)]
    client_options: list[dict[str, Any]] = []

    class _OAIClient:
        def __init__(self, *_: object, **kwargs: Any) -> None:
            client_options.append(kwargs)

        async def list_pages(self, **_: Any):  # type: ignore[no-untyped-def]
            for page in pages:
                yield page

    slow_feed = SourceFeedSpec.model_validate(
        {
            "name": "oai-slow-request-limit",
            "protocol": "oai-pmh",
            "endpoint": "https://oai.example.test/oai",
            "pollIntervalSeconds": 7200,
            "rateLimit": {"requestsPerSecond": 0.01, "burst": 1},
            "licenseDefault": "per-record",
        }
    )
    monkeypatch.setattr(poller, "build_async_client", lambda *_args, **_kwargs: _AsyncResource())
    monkeypatch.setattr(poller, "BronzeProducer", _Producer)
    monkeypatch.setattr(poller, "LicenseAdmissionProducer", _Producer)
    monkeypatch.setattr(poller, "MinioWriter", _Minio)
    monkeypatch.setattr(poller, "OAIClient", _OAIClient)

    emitted = await asyncio.wait_for(
        poller.poll_feed(slow_feed, _cfg(), state_store=FeedStateStore(tmp_path)),
        timeout=0.25,
    )

    assert emitted == 3
    assert client_options == [{"sleep_between_requests": 100.0}]
