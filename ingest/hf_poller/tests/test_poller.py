"""Tests for the Hugging Face Hub poller."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import httpx
import pytest

from ingest.common.config import IngestConfig
from ingest.common.state import FeedStateStore
from ingest.common.tests.conftest import FakeMinio, FakeProducer  # type: ignore[attr-defined]
from ingest.hf_poller import poller as hf_module


def _git_blob(payload: bytes) -> str:
    framed = f"blob {len(payload)}\0".encode("ascii") + payload
    return hashlib.sha1(framed).hexdigest()  # Git object identity, not security.


def _readme_response(request: httpx.Request, payload: bytes) -> httpx.Response:
    if request.method == "HEAD":
        return httpx.Response(200, headers={"etag": f'"{_git_blob(payload)}"'})
    return httpx.Response(200, content=payload)


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

    body = b"# Model card\n\nLicensed model documentation."

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/api/models":
            return httpx.Response(200, json=_models_payload())
        if request.url.path.endswith("/README.md"):
            return _readme_response(request, body)
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
    assert all(
        item["record"].doc_id == admission["record"].doc_id
        for item, admission in zip(fake_producer.sent, admissions.sent, strict=True)
    )


@pytest.mark.asyncio
async def test_poll_dataset_cards_emits_versioned_markdown(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.chdir(tmp_path)
    fake_producer = FakeProducer()
    fake_minio = FakeMinio()

    body = b"# Dataset card\n\nDocumented research data."

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/api/datasets":
            return httpx.Response(200, json=_dataset_payload())
        if request.url.path.endswith("/README.md"):
            return _readme_response(request, body)
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
    assert record.extraction_pipeline == "hf-dataset-card-markdown-v2"
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
        if request.url.path.endswith("/README.md") and request.method == "HEAD":
            return httpx.Response(404)
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
    assert requests[1].endswith("/README.md")
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
async def test_weights_only_repo_commit_does_not_emit_unchanged_readme(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.chdir(tmp_path)
    body = b"# Stable model card\n\nArchitecture and evaluation details."
    phase = 1
    body_gets = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal body_gets
        if request.url.path == "/api/models":
            return httpx.Response(
                200,
                json=[
                    {
                        "id": "org/stable-card",
                        "lastModified": f"2026-08-30T0{phase}:00:00Z",
                        "sha": ("a" if phase == 1 else "b") * 40,
                        "siblings": [{"rfilename": "README.md"}],
                    }
                ],
            )
        if request.url.path.endswith("/README.md"):
            if request.method == "GET":
                body_gets += 1
            return _readme_response(request, body)
        return httpx.Response(404)

    transport = httpx.MockTransport(handler)
    monkeypatch.setattr(
        hf_module,
        "build_async_client",
        lambda cfg, **kw: httpx.AsyncClient(transport=transport, headers=kw.get("headers", {})),
    )
    records = FakeProducer()
    admissions = FakeProducer()
    first = await hf_module.poll_models(
        _cfg(),
        producer=records,  # type: ignore[arg-type]
        minio=FakeMinio(),  # type: ignore[arg-type]
        admission_producer=admissions,  # type: ignore[arg-type]
    )
    phase = 2
    second = await hf_module.poll_models(
        _cfg(),
        producer=records,  # type: ignore[arg-type]
        minio=FakeMinio(),  # type: ignore[arg-type]
        admission_producer=admissions,  # type: ignore[arg-type]
    )

    assert (first, second) == (1, 0)
    assert body_gets == 1
    assert len(records.sent) == 1
    assert len(admissions.sent) == 1


@pytest.mark.asyncio
async def test_paginated_scan_handles_same_timestamp_ties_and_completes_watermark(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.chdir(tmp_path)
    bodies = {
        "org/old": b"# Old boundary card",
        "org/a": b"# Card A",
        "org/b": b"# Card B",
        "org/c": b"# Card C",
        "org/older": b"# Older card",
    }
    phase = 1

    def row(repo_id: str, timestamp: str, revision: str) -> dict[str, object]:
        return {
            "id": repo_id,
            "lastModified": timestamp,
            "sha": revision * 40,
            "siblings": [{"rfilename": "README.md"}],
        }

    def list_response(request: httpx.Request) -> httpx.Response:
        cursor = request.url.params.get("cursor")
        if phase == 1:
            return httpx.Response(200, json=[row("org/old", "2026-08-30T12:00:00Z", "d")])
        if cursor is None:
            return httpx.Response(
                200,
                json=[
                    row("org/b", "2026-08-30T13:00:00Z", "b"),
                    row("org/a", "2026-08-30T13:00:00Z", "a"),
                ],
                headers={"link": ('<https://huggingface.co/api/models?cursor=page-2>; rel="next"')},
            )
        if cursor == "page-2":
            return httpx.Response(
                200,
                json=[
                    row("org/old", "2026-08-30T12:00:00Z", "d"),
                    row("org/c", "2026-08-30T13:00:00Z", "c"),
                ],
                headers={"link": ('<https://huggingface.co/api/models?cursor=page-3>; rel="next"')},
            )
        assert cursor == "page-3"
        return httpx.Response(
            200,
            json=[row("org/older", "2026-08-30T11:00:00Z", "e")],
        )

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/api/models":
            return list_response(request)
        if request.url.path.endswith("/README.md"):
            repo_id = "/".join(request.url.path.split("/")[1:3])
            return _readme_response(request, bodies[repo_id])
        return httpx.Response(404)

    transport = httpx.MockTransport(handler)
    monkeypatch.setattr(
        hf_module,
        "build_async_client",
        lambda cfg, **kw: httpx.AsyncClient(transport=transport, headers=kw.get("headers", {})),
    )
    records = FakeProducer()
    admissions = FakeProducer()
    assert (
        await hf_module.poll_models(
            _cfg(),
            producer=records,  # type: ignore[arg-type]
            minio=FakeMinio(),  # type: ignore[arg-type]
            admission_producer=admissions,  # type: ignore[arg-type]
            limit=2,
        )
        == 1
    )
    records.sent.clear()
    admissions.sent.clear()
    phase = 2

    emitted = await hf_module.poll_models(
        _cfg(),
        producer=records,  # type: ignore[arg-type]
        minio=FakeMinio(),  # type: ignore[arg-type]
        admission_producer=admissions,  # type: ignore[arg-type]
        limit=2,
    )

    assert emitted == 3
    assert [str(item["record"].url).split("/")[3:5] for item in records.sent] == [
        ["org", "a"],
        ["org", "b"],
        ["org", "c"],
    ]
    state = FeedStateStore(tmp_path / ".s2p-state" / "hf").get(hf_module.SOURCE_FEED_MODELS)
    assert "scan" not in state
    assert state["completed"]["last_modified"] == "2026-08-30T13:00:00.000000Z"
    assert state["completed"]["catalogue_revisions"] == sorted(
        json.dumps([f"org/{name}", name * 40], separators=(",", ":")) for name in ("a", "b", "c")
    )


@pytest.mark.asyncio
async def test_page_crash_resumes_without_reemitting_completed_items(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.chdir(tmp_path)
    bodies = {
        "org/old": b"# Old boundary card",
        "org/a": b"# Card A",
        "org/b": b"# Card B",
        "org/older": b"# Older card",
    }
    phase = 1
    fail_b_once = True
    get_counts: dict[str, int] = {}

    def row(repo_id: str, timestamp: str, revision: str) -> dict[str, object]:
        return {
            "id": repo_id,
            "lastModified": timestamp,
            "sha": revision * 40,
            "siblings": [{"rfilename": "README.md"}],
        }

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal fail_b_once
        if request.url.path == "/api/models":
            if phase == 1:
                return httpx.Response(200, json=[row("org/old", "2026-08-30T10:00:00Z", "d")])
            if request.url.params.get("cursor") == "page-2":
                return httpx.Response(
                    200,
                    json=[row("org/older", "2026-08-30T09:00:00Z", "e")],
                )
            return httpx.Response(
                200,
                json=[
                    row("org/b", "2026-08-30T11:00:00Z", "b"),
                    row("org/a", "2026-08-30T12:00:00Z", "a"),
                ],
                headers={"link": ('<https://huggingface.co/api/models?cursor=page-2>; rel="next"')},
            )
        if request.url.path.endswith("/README.md"):
            repo_id = "/".join(request.url.path.split("/")[1:3])
            if request.method == "GET":
                get_counts[repo_id] = get_counts.get(repo_id, 0) + 1
                if repo_id == "org/b" and fail_b_once:
                    fail_b_once = False
                    return httpx.Response(500)
            return _readme_response(request, bodies[repo_id])
        return httpx.Response(404)

    transport = httpx.MockTransport(handler)
    monkeypatch.setattr(
        hf_module,
        "build_async_client",
        lambda cfg, **kw: httpx.AsyncClient(transport=transport, headers=kw.get("headers", {})),
    )
    records = FakeProducer()
    admissions = FakeProducer()
    await hf_module.poll_models(
        _cfg(),
        producer=records,  # type: ignore[arg-type]
        minio=FakeMinio(),  # type: ignore[arg-type]
        admission_producer=admissions,  # type: ignore[arg-type]
        limit=2,
    )
    records.sent.clear()
    admissions.sent.clear()
    phase = 2

    with pytest.raises(httpx.HTTPStatusError):
        await hf_module.poll_models(
            _cfg(),
            producer=records,  # type: ignore[arg-type]
            minio=FakeMinio(),  # type: ignore[arg-type]
            admission_producer=admissions,  # type: ignore[arg-type]
            limit=2,
        )

    failed_state = FeedStateStore(tmp_path / ".s2p-state" / "hf").get(hf_module.SOURCE_FEED_MODELS)
    assert failed_state["completed"]["last_modified"] == "2026-08-30T10:00:00.000000Z"
    assert failed_state["scan"]["processed_catalogue_revisions"] == [
        json.dumps(["org/a", "a" * 40], separators=(",", ":"))
    ]

    emitted = await hf_module.poll_models(
        _cfg(),
        producer=records,  # type: ignore[arg-type]
        minio=FakeMinio(),  # type: ignore[arg-type]
        admission_producer=admissions,  # type: ignore[arg-type]
        limit=2,
    )

    assert emitted == 1
    assert get_counts == {"org/old": 1, "org/a": 1, "org/b": 2}
    assert [str(item["record"].url).split("/")[3:5] for item in records.sent] == [
        ["org", "a"],
        ["org", "b"],
    ]
    completed = FeedStateStore(tmp_path / ".s2p-state" / "hf").get(hf_module.SOURCE_FEED_MODELS)
    assert "scan" not in completed
    assert completed["completed"]["last_modified"] == "2026-08-30T12:00:00.000000Z"


@pytest.mark.asyncio
async def test_full_page_without_next_link_cannot_advance_watermark(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.chdir(tmp_path)
    old_watermark = {
        "last_modified": "2026-08-30T10:00:00.000000Z",
        "catalogue_revisions": [],
        "legacy_repositories": [],
    }
    FeedStateStore(tmp_path / ".s2p-state" / "hf").put(
        hf_module.SOURCE_FEED_MODELS,
        {"version": hf_module.HF_SCAN_STATE_VERSION, "completed": old_watermark},
    )
    bodies = {"org/a": b"# Card A", "org/b": b"# Card B"}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/api/models":
            return httpx.Response(
                200,
                json=[
                    {
                        "id": "org/a",
                        "lastModified": "2026-08-30T12:00:00Z",
                        "sha": "a" * 40,
                        "siblings": [{"rfilename": "README.md"}],
                    },
                    {
                        "id": "org/b",
                        "lastModified": "2026-08-30T11:00:00Z",
                        "sha": "b" * 40,
                        "siblings": [{"rfilename": "README.md"}],
                    },
                ],
                # Deliberately no Link: rel=next.
            )
        if request.url.path.endswith("/README.md"):
            repo_id = "/".join(request.url.path.split("/")[1:3])
            return _readme_response(request, bodies[repo_id])
        return httpx.Response(404)

    transport = httpx.MockTransport(handler)
    monkeypatch.setattr(
        hf_module,
        "build_async_client",
        lambda cfg, **kw: httpx.AsyncClient(transport=transport, headers=kw.get("headers", {})),
    )

    with pytest.raises(RuntimeError, match="pagination ended before the durable watermark"):
        await hf_module.poll_models(
            _cfg(),
            producer=FakeProducer(),  # type: ignore[arg-type]
            minio=FakeMinio(),  # type: ignore[arg-type]
            admission_producer=FakeProducer(),  # type: ignore[arg-type]
            limit=2,
        )

    state = FeedStateStore(tmp_path / ".s2p-state" / "hf").get(hf_module.SOURCE_FEED_MODELS)
    assert state["completed"] == old_watermark
    assert state["scan"]["head_last_modified"] == "2026-08-30T12:00:00.000000Z"


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
