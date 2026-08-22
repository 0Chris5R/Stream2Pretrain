"""Tests for the GitHub Releases Atom poller."""

from __future__ import annotations

import json
from pathlib import Path

import httpx
import pytest

from ingest.common.config import IngestConfig
from ingest.common.http_client import build_async_client
from ingest.common.rate_limit import TokenBucket
from ingest.common.state import FeedStateStore
from ingest.common.tests.conftest import FakeMinio, FakeProducer  # type: ignore[attr-defined]
from ingest.github_releases.poller import _load_configured_repos, poll_repo

ATOM_BODY = """<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <title>Releases - huggingface/transformers</title>
  <updated>2026-06-15T08:00:00Z</updated>
  <entry>
    <id>tag:github.com,2008:Repository/12345/v5.0.0</id>
    <title>v5.0.0</title>
    <link rel="alternate" href="https://github.com/huggingface/transformers/releases/tag/v5.0.0"/>
  </entry>
</feed>
"""


def _cfg() -> IngestConfig:
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
        github_token=None,
        hf_token=None,
        user_agent="ua",
        http_timeout_seconds=2.0,
        http_max_retries=0,
    )


def test_loads_complete_chart_owned_repository_set(tmp_path: Path) -> None:
    path = tmp_path / "github.json"
    path.write_text(
        json.dumps(
            {
                "releases": {
                    "repos": [
                        "microsoft/onnxruntime",
                        "pytorch/torchtitan",
                        "microsoft/onnxruntime",
                    ]
                }
            }
        ),
        encoding="utf-8",
    )
    assert _load_configured_repos(str(path)) == [
        "microsoft/onnxruntime",
        "pytorch/torchtitan",
    ]


@pytest.mark.asyncio
async def test_poll_repo_emits_release(tmp_path: Path) -> None:
    state = FeedStateStore(tmp_path)
    fake_producer = FakeProducer()
    fake_minio = FakeMinio()
    await fake_producer.start()
    await fake_minio.start()

    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            content=ATOM_BODY.encode("utf-8"),
            headers={"content-type": "application/atom+xml", "etag": '"r1"'},
        )

    client = build_async_client(_cfg(), transport=httpx.MockTransport(handler))
    bucket = TokenBucket(rate=10.0, burst=4)
    try:
        emitted = await poll_repo(
            "huggingface/transformers",
            cfg=_cfg(),
            client=client,
            producer=fake_producer,  # type: ignore[arg-type]
            minio=fake_minio,  # type: ignore[arg-type]
            state_store=state,
            bucket=bucket,
        )
    finally:
        await client.aclose()
    assert emitted == 1
    assert len(fake_producer.sent) == 1


@pytest.mark.asyncio
async def test_poll_repo_dedups_known_release(tmp_path: Path) -> None:
    state = FeedStateStore(tmp_path)
    fake_producer = FakeProducer()
    fake_minio = FakeMinio()
    await fake_producer.start()
    await fake_minio.start()

    from ingest.common.hashing import doc_id_for_url

    seen_id = doc_id_for_url("https://github.com/huggingface/transformers/releases/tag/v5.0.0")
    state.put(
        "github-releases:huggingface_transformers",
        {"etag": None, "seen_doc_ids": [seen_id]},
    )

    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=ATOM_BODY.encode("utf-8"))

    client = build_async_client(_cfg(), transport=httpx.MockTransport(handler))
    bucket = TokenBucket(rate=10.0, burst=4)
    try:
        emitted = await poll_repo(
            "huggingface/transformers",
            cfg=_cfg(),
            client=client,
            producer=fake_producer,  # type: ignore[arg-type]
            minio=fake_minio,  # type: ignore[arg-type]
            state_store=state,
            bucket=bucket,
        )
    finally:
        await client.aclose()
    assert emitted == 0


@pytest.mark.asyncio
async def test_poll_repo_handles_304(tmp_path: Path) -> None:
    state = FeedStateStore(tmp_path)
    state.put(
        "github-releases:huggingface_transformers",
        {"etag": '"r1"', "seen_doc_ids": []},
    )

    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(304)

    client = build_async_client(_cfg(), transport=httpx.MockTransport(handler))
    bucket = TokenBucket(rate=10.0, burst=4)
    fake_producer = FakeProducer()
    fake_minio = FakeMinio()
    await fake_producer.start()
    await fake_minio.start()
    try:
        emitted = await poll_repo(
            "huggingface/transformers",
            cfg=_cfg(),
            client=client,
            producer=fake_producer,  # type: ignore[arg-type]
            minio=fake_minio,  # type: ignore[arg-type]
            state_store=state,
            bucket=bucket,
        )
    finally:
        await client.aclose()
    assert emitted == 0
