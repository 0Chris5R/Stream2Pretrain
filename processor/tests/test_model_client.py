"""Tests for the strict curator model-service client facades."""

from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urlsplit

import httpx
import pytest

from processor.model_client import (
    CuratorModelClient,
    ModelServiceError,
    RemoteKenLMScorer,
    RemoteQualityClassifier,
)


def _metadata() -> dict[str, object]:
    return {
        "ready": True,
        "quality": {
            "source-pretrain-quality": {
                "backend": "transformers-cpu",
                "revision": "finepdfs@pinned",
            },
        },
        "kenlm": {
            "backend": "kenlm-sentencepiece",
            "scorer": "kenlm-sentencepiece:en.arpa.bin",
        },
    }


def test_remote_model_facades_preserve_revisions_and_results() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/v1/metadata":
            return httpx.Response(200, json=_metadata())
        if request.url.path == "/v1/quality:batch":
            body = request.read().decode()
            item_count = body.count('"paper"') + body.count('"page"')
            return httpx.Response(
                200,
                json={
                    "results": [
                        {"edu_score": 4.25, "revision": "finepdfs@pinned"}
                        for _ in range(item_count)
                    ]
                },
            )
        if request.url.path == "/v1/perplexity":
            return httpx.Response(
                200,
                json={
                    "perplexity": 42.0,
                    "bucket": "head",
                    "scorer": "kenlm-sentencepiece:en.arpa.bin",
                },
            )
        return httpx.Response(404)

    transport = httpx.MockTransport(handler)
    http_client = httpx.Client(base_url="http://models", transport=transport)
    client = CuratorModelClient("http://models", client=http_client)
    finepdfs = RemoteQualityClassifier(client, "source-pretrain-quality")
    kenlm = RemoteKenLMScorer(client)

    assert finepdfs.score("paper").edu_score == 4.25
    assert [result.revision for result in finepdfs.score_many(["paper", "paper"])] == [
        "finepdfs@pinned",
        "finepdfs@pinned",
    ]
    assert finepdfs.revision == "finepdfs@pinned"
    assert kenlm.score("text").perplexity == 42.0
    client.close()


def test_model_service_5xx_is_transient() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/v1/metadata":
            return httpx.Response(200, json=_metadata())
        return httpx.Response(503)

    client = CuratorModelClient(
        "http://models",
        client=httpx.Client(base_url="http://models", transport=httpx.MockTransport(handler)),
    )
    with pytest.raises(ModelServiceError, match="returned 503"):
        client.perplexity("retry me")
    client.close()


def test_quality_batch_requires_one_result_per_input() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/v1/metadata":
            return httpx.Response(200, json=_metadata())
        return httpx.Response(
            200,
            json={"results": [{"edu_score": 4.0, "revision": "finepdfs@pinned"}]},
        )

    client = CuratorModelClient(
        "http://models",
        client=httpx.Client(base_url="http://models", transport=httpx.MockTransport(handler)),
    )
    with pytest.raises(ModelServiceError, match="invalid quality batch"):
        client.quality_many("source-pretrain-quality", ["one", "two"])
    client.close()


def test_oversize_combined_batch_preserves_the_singleton_path(monkeypatch) -> None:
    requests: list[list[str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/v1/metadata":
            return httpx.Response(200, json=_metadata())
        payload = request.read().decode()
        texts = ["left"] if "left" in payload else ["right"]
        requests.append(texts)
        return httpx.Response(
            200,
            json={"results": [{"edu_score": 4.0, "revision": "finepdfs@pinned"} for _ in texts]},
        )

    monkeypatch.setattr("processor.model_client.MODEL_SERVICE_MAX_REQUEST_BYTES", 1)
    client = CuratorModelClient(
        "http://models",
        client=httpx.Client(base_url="http://models", transport=httpx.MockTransport(handler)),
    )

    results = client.quality_many("source-pretrain-quality", ["left", "right"])

    assert len(results) == 2
    assert requests == [["left"], ["right"]]
    client.close()


def test_six_concurrent_calls_lease_six_distinct_ready_endpoints() -> None:
    endpoints = [f"http://model-{index}:8094" for index in range(6)]
    inference_backends: list[str] = []
    inference_lock = threading.Lock()
    inference_barrier = threading.Barrier(6)

    def endpoint_factory(endpoint: str) -> httpx.Client:
        backend = str(urlsplit(endpoint).hostname)

        def handler(request: httpx.Request) -> httpx.Response:
            if request.url.path == "/v1/metadata":
                return httpx.Response(200, json=_metadata())
            if request.url.path == "/v1/quality:batch":
                with inference_lock:
                    inference_backends.append(backend)
                inference_barrier.wait(timeout=2)
                return httpx.Response(
                    200,
                    headers={"X-S2P-Model-Backend": backend},
                    json={"results": [{"edu_score": 4.25, "revision": "finepdfs@pinned"}]},
                )
            return httpx.Response(404)

        return httpx.Client(base_url=endpoint, transport=httpx.MockTransport(handler))

    base_client = httpx.Client(
        base_url="http://quality:8094",
        transport=httpx.MockTransport(
            lambda request: (
                httpx.Response(200, json=_metadata())
                if request.url.path == "/v1/metadata"
                else httpx.Response(404)
            )
        ),
    )
    client = CuratorModelClient(
        "http://quality:8094",
        client=base_client,
        profile="finepdfs",
        endpoint_resolver=lambda: endpoints,
        endpoint_client_factory=endpoint_factory,
    )

    with ThreadPoolExecutor(max_workers=6) as pool:
        results = list(
            pool.map(
                lambda index: client.quality("source-pretrain-quality", f"paper-{index}"),
                range(6),
            )
        )

    assert [result.revision for result in results] == ["finepdfs@pinned"] * 6
    assert set(inference_backends) == {f"model-{index}" for index in range(6)}
    assert len(inference_backends) == 6
    client.close()


def test_endpoint_churn_retries_same_payload_on_replacement_backend() -> None:
    resolver_calls = 0
    posted: list[tuple[str, bytes]] = []

    def resolver() -> list[str]:
        nonlocal resolver_calls
        resolver_calls += 1
        return ["http://old:8094"] if resolver_calls == 1 else ["http://new:8094"]

    def endpoint_factory(endpoint: str) -> httpx.Client:
        backend = str(urlsplit(endpoint).hostname)

        def handler(request: httpx.Request) -> httpx.Response:
            if request.url.path == "/v1/metadata":
                return httpx.Response(200, json=_metadata())
            body = request.read()
            posted.append((backend, body))
            if backend == "old":
                raise httpx.ConnectError("Pod disappeared", request=request)
            return httpx.Response(
                200,
                headers={"X-S2P-Model-Backend": backend},
                json={"results": [{"edu_score": 4.5, "revision": "finepdfs@pinned"}]},
            )

        return httpx.Client(base_url=endpoint, transport=httpx.MockTransport(handler))

    base_client = httpx.Client(
        base_url="http://quality:8094",
        transport=httpx.MockTransport(
            lambda request: (
                httpx.Response(200, json=_metadata())
                if request.url.path == "/v1/metadata"
                else httpx.Response(404)
            )
        ),
    )
    client = CuratorModelClient(
        "http://quality:8094",
        client=base_client,
        profile="finepdfs",
        endpoint_resolver=resolver,
        endpoint_client_factory=endpoint_factory,
    )

    result = client.quality("source-pretrain-quality", "same paper")

    assert result.edu_score == 4.5
    assert [backend for backend, _body in posted] == ["old", "new"]
    assert posted[0][1] == posted[1][1]
    assert resolver_calls == 2
    client.close()


def test_initial_partial_availability_uses_only_revision_matching_endpoint() -> None:
    inference_backends: list[str] = []

    def endpoint_factory(endpoint: str) -> httpx.Client:
        backend = str(urlsplit(endpoint).hostname)

        def handler(request: httpx.Request) -> httpx.Response:
            if request.url.path == "/v1/metadata":
                if backend == "starting":
                    return httpx.Response(503)
                metadata = _metadata()
                if backend == "drifted":
                    quality = metadata["quality"]
                    assert isinstance(quality, dict)
                    finepdfs = quality["source-pretrain-quality"]
                    assert isinstance(finepdfs, dict)
                    finepdfs["revision"] = "finepdfs@wrong"
                return httpx.Response(200, json=metadata)
            inference_backends.append(backend)
            return httpx.Response(
                200,
                headers={"X-S2P-Model-Backend": backend},
                json={"results": [{"edu_score": 4.75, "revision": "finepdfs@pinned"}]},
            )

        return httpx.Client(base_url=endpoint, transport=httpx.MockTransport(handler))

    base_client = httpx.Client(
        base_url="http://quality:8094",
        transport=httpx.MockTransport(
            lambda request: (
                httpx.Response(200, json=_metadata())
                if request.url.path == "/v1/metadata"
                else httpx.Response(404)
            )
        ),
    )
    client = CuratorModelClient(
        "http://quality:8094",
        client=base_client,
        profile="finepdfs",
        endpoint_resolver=lambda: [
            "http://starting:8094",
            "http://drifted:8094",
            "http://ready:8094",
        ],
        endpoint_client_factory=endpoint_factory,
    )

    result = client.quality("source-pretrain-quality", "paper")

    assert result.revision == "finepdfs@pinned"
    assert inference_backends == ["ready"]
    client.close()
