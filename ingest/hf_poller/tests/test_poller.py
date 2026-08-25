"""Tests for the Hugging Face Hub poller."""

from __future__ import annotations

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
            "sha": "a" * 40,
            "license": "Apache-2.0",
            "siblings": [{"rfilename": "README.md"}],
        },
        {
            "id": "mistralai/Mistral-Small-3",
            "lastModified": "2026-06-14T11:00:00Z",
            "sha": "b" * 40,
            "license": "MIT",
            "siblings": [{"rfilename": "README.md"}],
        },
        {"id": "no-last-modified-model"},  # missing lastModified -> skipped
    ]


def _dataset_payload() -> list[dict]:
    return [
        {
            "id": "org/research-dataset",
            "lastModified": "2026-08-20T10:00:00Z",
            "sha": "c" * 40,
            "cardData": {"license": "cc-by-4.0"},
        }
    ]


@pytest.mark.asyncio
async def test_poll_models_emits_two(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.chdir(tmp_path)
    fake_producer = FakeProducer()
    fake_minio = FakeMinio()
    await fake_producer.start()
    await fake_minio.start()

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/api/models":
            return httpx.Response(200, json=_models_payload())
        if request.url.path.endswith("/README.md"):
            return httpx.Response(200, text="# Model card\n\nLicensed model documentation.")
        return httpx.Response(404)

    transport = httpx.MockTransport(handler)
    monkeypatch.setattr(
        "ingest.hf_poller.poller.build_async_client",
        lambda cfg, **kw: httpx.AsyncClient(transport=transport, headers=kw.get("headers", {})),
    )
    admissions = FakeProducer()
    emitted = await hf_module.poll_models(
        _cfg(),
        producer=fake_producer,  # type: ignore[arg-type]
        minio=fake_minio,
        admission_producer=admissions,  # type: ignore[arg-type]
    )
    assert emitted == 2
    assert len(fake_producer.sent) == 2
    assert all(item["record"].source_format == "web" for item in fake_producer.sent)
    assert all(
        item["record"].spdx_license == hf_module.HF_PUBLIC_REPOSITORY_TERMS
        for item in fake_producer.sent
    )
    assert all(item["record"].resolver == "hf-public-repository-terms" for item in admissions.sent)


@pytest.mark.asyncio
async def test_poll_dataset_cards_emits_versioned_markdown(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.chdir(tmp_path)
    fake_producer = FakeProducer()
    fake_minio = FakeMinio()

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/api/datasets":
            return httpx.Response(200, json=_dataset_payload())
        if request.url.path.startswith("/api/datasets/org/research-dataset/tree/"):
            return httpx.Response(200, json=[{"type": "file", "path": "README.md"}])
        if request.url.path.endswith("/README.md"):
            return httpx.Response(200, text="# Dataset card\n\nDocumented research data.")
        return httpx.Response(404)

    transport = httpx.MockTransport(handler)
    monkeypatch.setattr(
        hf_module,
        "build_async_client",
        lambda cfg, **kw: httpx.AsyncClient(transport=transport, headers=kw.get("headers", {})),
    )

    admissions = FakeProducer()
    emitted = await hf_module.poll_hub_cards(
        _cfg(),
        kind="dataset",
        producer=fake_producer,  # type: ignore[arg-type]
        minio=fake_minio,
        admission_producer=admissions,  # type: ignore[arg-type]
    )

    assert emitted == 1
    record = fake_producer.sent[0]["record"]
    assert record.source_feed == "hf-datasets"
    assert record.source_format == "web"
    assert record.extraction_pipeline == "hf-dataset-card-markdown-v1"
    assert "/blob/" in str(record.url)
    assert record.spdx_license == hf_module.HF_PUBLIC_REPOSITORY_TERMS
    assert admissions.sent[0]["record"].resolver == "hf-public-repository-terms"


@pytest.mark.asyncio
async def test_model_without_readme_is_discovery_only(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.chdir(tmp_path)
    requests: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request.url.path)
        if request.url.path == "/api/models":
            return httpx.Response(
                200,
                json=[
                    {
                        "id": "example/weights-only",
                        "lastModified": "2026-08-25T12:00:00Z",
                        "sha": "d" * 40,
                        "siblings": [
                            {"rfilename": ".gitattributes"},
                            {"rfilename": "model.safetensors"},
                        ],
                    }
                ],
            )
        return httpx.Response(500, request=request)

    transport = httpx.MockTransport(handler)
    monkeypatch.setattr(
        hf_module,
        "build_async_client",
        lambda cfg, **kw: httpx.AsyncClient(transport=transport, headers=kw.get("headers", {})),
    )
    records = FakeProducer()
    admissions = FakeProducer()

    emitted = await hf_module.poll_models(
        _cfg(),
        producer=records,  # type: ignore[arg-type]
        minio=FakeMinio(),  # type: ignore[arg-type]
        admission_producer=admissions,  # type: ignore[arg-type]
    )

    assert emitted == 0
    assert requests == ["/api/models"]
    assert records.sent == []
    assert admissions.sent == []


@pytest.mark.asyncio
async def test_dataset_without_readme_is_discovery_only(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.chdir(tmp_path)
    requests: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request.url.path)
        if request.url.path == "/api/datasets":
            return httpx.Response(200, json=_dataset_payload())
        if request.url.path.startswith("/api/datasets/org/research-dataset/tree/"):
            return httpx.Response(200, json=[{"type": "file", "path": "data.jsonl"}])
        return httpx.Response(500, request=request)

    transport = httpx.MockTransport(handler)
    monkeypatch.setattr(
        hf_module,
        "build_async_client",
        lambda cfg, **kw: httpx.AsyncClient(transport=transport, headers=kw.get("headers", {})),
    )
    records = FakeProducer()
    admissions = FakeProducer()

    emitted = await hf_module.poll_hub_cards(
        _cfg(),
        kind="dataset",
        producer=records,  # type: ignore[arg-type]
        minio=FakeMinio(),  # type: ignore[arg-type]
        admission_producer=admissions,  # type: ignore[arg-type]
    )

    assert emitted == 0
    assert len(requests) == 2
    assert requests[0] == "/api/datasets"
    assert requests[1].startswith("/api/datasets/org/research-dataset/tree/")
    assert records.sent == []
    assert admissions.sent == []


@pytest.mark.asyncio
async def test_hub_card_without_exact_revision_is_quarantined_before_card_fetch(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.chdir(tmp_path)
    requests: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request.url.path)
        if request.url.path == "/api/datasets":
            return httpx.Response(
                200,
                json=[
                    {
                        "id": "example/no-revision",
                        "lastModified": "2026-08-23T00:00:00Z",
                        "cardData": {"license": "cc-by-4.0"},
                    }
                ],
            )
        return httpx.Response(500, request=request)

    transport = httpx.MockTransport(handler)
    monkeypatch.setattr(
        hf_module,
        "build_async_client",
        lambda cfg, **kw: httpx.AsyncClient(transport=transport, headers=kw.get("headers", {})),
    )
    records = FakeProducer()
    admissions = FakeProducer()

    emitted = await hf_module.poll_hub_cards(
        _cfg(),
        kind="dataset",
        producer=records,  # type: ignore[arg-type]
        minio=FakeMinio(),  # type: ignore[arg-type]
        admission_producer=admissions,  # type: ignore[arg-type]
    )

    assert emitted == 0
    assert requests == ["/api/datasets"]
    assert records.sent == []
    assert admissions.sent == []


@pytest.mark.asyncio
async def test_model_without_exact_revision_is_quarantined_before_card_fetch(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.chdir(tmp_path)
    requests: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request.url.path)
        if request.url.path == "/api/models":
            return httpx.Response(
                200,
                json=[
                    {
                        "id": "example/no-revision",
                        "lastModified": "2026-08-23T00:00:00Z",
                        "license": "Apache-2.0",
                    }
                ],
            )
        return httpx.Response(500, request=request)

    transport = httpx.MockTransport(handler)
    monkeypatch.setattr(
        hf_module,
        "build_async_client",
        lambda cfg, **kw: httpx.AsyncClient(transport=transport, headers=kw.get("headers", {})),
    )
    records = FakeProducer()
    admissions = FakeProducer()
    emitted = await hf_module.poll_models(
        _cfg(),
        producer=records,  # type: ignore[arg-type]
        minio=FakeMinio(),  # type: ignore[arg-type]
        admission_producer=admissions,  # type: ignore[arg-type]
    )

    assert emitted == 0
    assert requests == ["/api/models"]
    assert records.sent == []
    assert admissions.sent == []


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
