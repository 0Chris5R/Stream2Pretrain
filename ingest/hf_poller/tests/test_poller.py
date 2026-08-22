"""Tests for the Hugging Face Hub poller."""

from __future__ import annotations

import json
from pathlib import Path

import httpx
import pytest

from ingest.common.config import IngestConfig
from ingest.common.tests.conftest import FakeMinio, FakeProducer  # type: ignore[attr-defined]
from ingest.hf_poller import poller as hf_module


def _cfg(token: str | None = "hf_test") -> IngestConfig:
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
        hf_token=token,
        user_agent="ua",
        http_timeout_seconds=2.0,
        http_max_retries=0,
    )


def _models_payload() -> list[dict]:
    return [
        {
            "id": "meta-llama/Llama-4-8B",
            "lastModified": "2026-06-14T10:00:00Z",
            "license": "Apache-2.0",
        },
        {
            "id": "mistralai/Mistral-Small-3",
            "lastModified": "2026-06-14T11:00:00Z",
            "license": "MIT",
        },
        {"id": "no-last-modified-model"},  # missing lastModified -> skipped
    ]


def _papers_payload() -> list[dict]:
    return [
        {"paper": {"id": "2406.12345", "title": "Paper A", "license": "CC-BY-4.0"}},
        {"paper": {"id": "2406.67890", "title": "Paper B", "license": "CC-BY-SA-4.0"}},
    ]


@pytest.mark.asyncio
async def test_poll_models_emits_two(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.chdir(tmp_path)
    fake_producer = FakeProducer()
    fake_minio = FakeMinio()
    await fake_producer.start()
    await fake_minio.start()

    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            content=json.dumps(_models_payload()).encode("utf-8"),
            headers={"content-type": "application/json"},
        )

    transport = httpx.MockTransport(handler)
    monkeypatch.setattr(
        "ingest.hf_poller.poller.build_async_client",
        lambda cfg, **kw: httpx.AsyncClient(transport=transport, headers=kw.get("headers", {})),
    )
    emitted = await hf_module.poll_models(_cfg(), producer=fake_producer, minio=fake_minio)  # type: ignore[arg-type]
    assert emitted == 2
    assert len(fake_producer.sent) == 2


@pytest.mark.asyncio
async def test_poll_daily_papers_uses_public_api_without_token(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.chdir(tmp_path)
    fake_producer = FakeProducer()
    fake_minio = FakeMinio()

    def handler(request: httpx.Request) -> httpx.Response:
        assert "authorization" not in request.headers
        return httpx.Response(200, json=_papers_payload())

    transport = httpx.MockTransport(handler)
    monkeypatch.setattr(
        hf_module,
        "build_async_client",
        lambda cfg, **kw: httpx.AsyncClient(transport=transport, headers=kw.get("headers", {})),
    )
    emitted = await hf_module.poll_daily_papers(
        _cfg(token=None),
        producer=fake_producer,
        minio=fake_minio,  # type: ignore[arg-type]
    )
    assert emitted == 2


def test_model_license_reads_hub_license_tag() -> None:
    assert hf_module._model_license({"tags": ["pytorch", "license:apache-2.0"]}) == ("Apache-2.0")


@pytest.mark.asyncio
async def test_poll_daily_papers_emits(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.chdir(tmp_path)
    fake_producer = FakeProducer()
    fake_minio = FakeMinio()
    await fake_producer.start()
    await fake_minio.start()

    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            content=json.dumps(_papers_payload()).encode("utf-8"),
            headers={"content-type": "application/json"},
        )

    transport = httpx.MockTransport(handler)
    monkeypatch.setattr(
        "ingest.hf_poller.poller.build_async_client",
        lambda cfg, **kw: httpx.AsyncClient(transport=transport, headers=kw.get("headers", {})),
    )
    emitted = await hf_module.poll_daily_papers(
        _cfg(),
        producer=fake_producer,
        minio=fake_minio,  # type: ignore[arg-type]
    )
    assert emitted == 2


@pytest.mark.asyncio
async def test_models_deployment_repeats_only_models(monkeypatch: pytest.MonkeyPatch) -> None:
    passes: list[str] = []
    sleeps: list[float] = []

    async def fake_run_pass(_: IngestConfig, *, mode: str = "all") -> tuple[int, int]:
        passes.append(mode)
        return 1, 0

    class StopLoopError(Exception):
        pass

    async def fake_sleep(delay: float) -> None:
        sleeps.append(delay)
        if len(sleeps) == 2:
            raise StopLoopError

    monkeypatch.setattr(hf_module, "run_pass", fake_run_pass)
    monkeypatch.setattr(hf_module.asyncio, "sleep", fake_sleep)

    with pytest.raises(StopLoopError):
        await hf_module.run_models_forever(_cfg(), poll_interval_seconds=5)

    # Intervals shorter than one minute are clamped to a polite upstream rate.
    assert passes == ["models", "models"]
    assert sleeps == [60, 60]


@pytest.mark.asyncio
async def test_run_pass_rejects_unknown_mode_before_opening_dependencies() -> None:
    with pytest.raises(ValueError, match="unsupported Hugging Face poll mode"):
        await hf_module.run_pass(_cfg(), mode="paperz")
