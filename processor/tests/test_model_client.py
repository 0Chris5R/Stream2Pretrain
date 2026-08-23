"""Tests for the strict curator model-service client facades."""

from __future__ import annotations

import httpx
import pytest

from processor.model_client import (
    CuratorModelClient,
    ModelServiceError,
    RemoteEmbeddingSketch,
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
        "embedding": {
            "backend": "onnxruntime-cpu",
            "revision": "e5@pinned",
        },
    }


def test_remote_model_facades_preserve_revisions_and_results() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/v1/metadata":
            return httpx.Response(200, json=_metadata())
        if request.url.path == "/v1/quality":
            family = request.read().decode()
            revision = "finepdfs@pinned" if "finepdfs" in family else "fineweb@pinned"
            return httpx.Response(200, json={"edu_score": 4.25, "revision": revision})
        if request.url.path == "/v1/perplexity":
            return httpx.Response(
                200,
                json={
                    "perplexity": 42.0,
                    "bucket": "head",
                    "scorer": "kenlm-sentencepiece:en.arpa.bin",
                },
            )
        if request.url.path == "/v1/embed":
            return httpx.Response(200, json={"embedding": [1.0, 0.0]})
        return httpx.Response(404)

    transport = httpx.MockTransport(handler)
    http_client = httpx.Client(base_url="http://models", transport=transport)
    client = CuratorModelClient("http://models", client=http_client)
    finepdfs = RemoteQualityClassifier(client, "finepdfs-edu-v2")
    fineweb = RemoteQualityClassifier(client, "fineweb-edu")
    kenlm = RemoteKenLMScorer(client)
    embeddings = RemoteEmbeddingSketch(client)

    assert finepdfs.score("paper").edu_score == 4.25
    assert finepdfs.revision == "finepdfs@pinned"
    assert fineweb.score("page").revision == "fineweb@pinned"
    assert kenlm.score("text").perplexity == 42.0
    embeddings.add("MMLU", "prompt")
    assert list(embeddings.query("candidate")) == [("MMLU", 1.0)]
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
