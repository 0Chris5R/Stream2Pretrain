"""Tests for the strict curator model-service client facades."""

from __future__ import annotations

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
            "finepdfs-edu-v2": {
                "backend": "transformers-cpu",
                "revision": "finepdfs@pinned",
            },
            "fineweb-edu": {
                "backend": "transformers-cpu",
                "revision": "fineweb@pinned",
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
            revision = "finepdfs@pinned" if "finepdfs" in body else "fineweb@pinned"
            item_count = body.count('"paper"') + body.count('"page"')
            return httpx.Response(
                200,
                json={
                    "results": [
                        {"edu_score": 4.25, "revision": revision} for _ in range(item_count)
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
    finepdfs = RemoteQualityClassifier(client, "finepdfs-edu-v2")
    fineweb = RemoteQualityClassifier(client, "fineweb-edu")
    kenlm = RemoteKenLMScorer(client)

    assert finepdfs.score("paper").edu_score == 4.25
    assert [result.revision for result in finepdfs.score_many(["paper", "paper"])] == [
        "finepdfs@pinned",
        "finepdfs@pinned",
    ]
    assert finepdfs.revision == "finepdfs@pinned"
    assert fineweb.score("page").revision == "fineweb@pinned"
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
        client.quality_many("finepdfs-edu-v2", ["one", "two"])
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

    results = client.quality_many("finepdfs-edu-v2", ["left", "right"])

    assert len(results) == 2
    assert requests == [["left"], ["right"]]
    client.close()
