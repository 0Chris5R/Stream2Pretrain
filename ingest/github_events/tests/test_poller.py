"""Tests for the GitHub Events poller loop."""

from __future__ import annotations

import json
from pathlib import Path

import httpx
import pytest

from ingest.common.config import IngestConfig
from ingest.common.tests.conftest import FakeMinio, FakeProducer  # type: ignore[attr-defined]
from ingest.github_events import poller as evt_module


def _cfg(state_dir: Path) -> IngestConfig:
    return IngestConfig(
        env="dev",
        log_level="DEBUG",
        redpanda_brokers="",
        minio_endpoint="",
        minio_access_key="",
        minio_secret_key="",
        minio_bronze_bucket="bronze",
        otel_endpoint=None,
        otel_protocol="grpc",
        github_token="ghp_test",
        hf_token=None,
        user_agent="ua",
        http_timeout_seconds=2.0,
        http_max_retries=0,
    )


def _events_payload() -> list[dict]:
    return [
        {
            "id": "1",
            "type": "ReleaseEvent",
            "repo": {
                "name": "huggingface/transformers",
                "url": "https://api.github.com/repos/huggingface/transformers",
            },
            "payload": {
                "release": {
                    "html_url": "https://github.com/huggingface/transformers/releases/v5.0.0"
                }
            },
        },
        {
            "id": "2",
            "type": "PushEvent",
            "repo": {"name": "acme/widgets"},  # filtered out
            "payload": {},
        },
        {
            "id": "3",
            "type": "PullRequestEvent",
            "repo": {"name": "vllm-project/vllm"},
            "payload": {
                "pull_request": {"html_url": "https://github.com/vllm-project/vllm/pull/9001"}
            },
        },
    ]


@pytest.mark.asyncio
async def test_run_loop_emits_for_curated_repos(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)

    fake_producer = FakeProducer()
    fake_minio = FakeMinio()

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            content=json.dumps(_events_payload()).encode("utf-8"),
            headers={
                "content-type": "application/json",
                "etag": '"a1"',
                "x-poll-interval": "0",
                "x-ratelimit-remaining": "4999",
            },
        )

    transport = httpx.MockTransport(handler)

    # Patch the constructor calls to use our fakes/transport.
    monkeypatch.setattr(
        "ingest.github_events.poller.BronzeProducer",
        lambda *a, **kw: fake_producer,
    )
    monkeypatch.setattr(
        "ingest.github_events.poller.MinioWriter",
        lambda *a, **kw: fake_minio,
    )

    from ingest.common import http_client as hc

    monkeypatch.setattr(
        hc,
        "build_async_client",
        lambda cfg, **kw: hc.httpx.AsyncClient(transport=transport, headers=kw.get("headers", {})),
    )
    monkeypatch.setattr(
        "ingest.github_events.poller.build_async_client",
        lambda cfg, **kw: hc.httpx.AsyncClient(transport=transport, headers=kw.get("headers", {})),
    )

    total = await evt_module.run_loop(_cfg(tmp_path), max_iterations=1)
    assert total == 2
    assert len(fake_producer.sent) == 2
    sent_urls = {item["record"].url.unicode_string().rstrip("/") for item in fake_producer.sent}
    assert "https://github.com/huggingface/transformers/releases/v5.0.0" in sent_urls
    assert "https://github.com/vllm-project/vllm/pull/9001" in sent_urls


def test_event_url_extraction() -> None:
    evt = {
        "type": "PullRequestEvent",
        "payload": {"pull_request": {"html_url": "https://github.com/o/r/pull/1"}},
        "repo": {"name": "o/r", "url": "https://api.github.com/repos/o/r"},
    }
    assert evt_module._event_url(evt) == "https://github.com/o/r/pull/1"

    evt2 = {
        "type": "PushEvent",
        "payload": {},
        "repo": {"name": "o/r", "url": "https://api.github.com/repos/o/r"},
    }
    assert evt_module._event_url(evt2) == "https://github.com/o/r"


@pytest.mark.asyncio
async def test_run_loop_uses_anonymous_api_without_token(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import dataclasses

    monkeypatch.chdir(tmp_path)
    cfg = _cfg(tmp_path)
    cfg2 = dataclasses.replace(cfg, github_token=None)
    fake_producer = FakeProducer()
    fake_minio = FakeMinio()

    def handler(request: httpx.Request) -> httpx.Response:
        assert "authorization" not in request.headers
        return httpx.Response(200, json=[], headers={"x-poll-interval": "0"})

    transport = httpx.MockTransport(handler)
    monkeypatch.setattr(evt_module, "BronzeProducer", lambda *a, **kw: fake_producer)
    monkeypatch.setattr(evt_module, "MinioWriter", lambda *a, **kw: fake_minio)
    monkeypatch.setattr(
        evt_module,
        "build_async_client",
        lambda cfg, **kw: httpx.AsyncClient(
            transport=transport,
            headers=kw.get("headers", {}),
        ),
    )

    assert await evt_module.run_loop(cfg2, max_iterations=1) == 0


@pytest.mark.asyncio
async def test_run_loop_falls_back_when_token_is_rejected(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    fake_producer = FakeProducer()
    fake_minio = FakeMinio()
    authenticated_requests = 0
    anonymous_requests = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal authenticated_requests, anonymous_requests
        if "authorization" in request.headers:
            authenticated_requests += 1
            return httpx.Response(401)
        anonymous_requests += 1
        return httpx.Response(200, json=[], headers={"x-poll-interval": "0"})

    transport = httpx.MockTransport(handler)
    monkeypatch.setattr(evt_module, "BronzeProducer", lambda *a, **kw: fake_producer)
    monkeypatch.setattr(evt_module, "MinioWriter", lambda *a, **kw: fake_minio)
    monkeypatch.setattr(
        evt_module,
        "build_async_client",
        lambda cfg, **kw: httpx.AsyncClient(
            transport=transport,
            headers=kw.get("headers", {}),
        ),
    )

    assert await evt_module.run_loop(_cfg(tmp_path), max_iterations=1) == 0
    assert authenticated_requests == 1
    assert anonymous_requests == 1
