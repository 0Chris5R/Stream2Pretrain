"""Strict client facades for the stateless curator model service."""

from __future__ import annotations

import math
from collections.abc import Iterable
from typing import Any

import httpx

from processor.operators.kenlm_score import PerplexityResult
from processor.operators.quality import QualityScore
from schemas.decon import BenchmarkName


class ModelServiceError(RuntimeError):
    """Transient model-service failure that must not advance a Kafka offset."""


class CuratorModelClient:
    """Synchronous client shared by the curation model facades."""

    def __init__(
        self,
        base_url: str,
        *,
        timeout_seconds: float = 180.0,
        client: httpx.Client | None = None,
    ) -> None:
        self._client = client or httpx.Client(
            base_url=base_url.rstrip("/"),
            timeout=httpx.Timeout(timeout_seconds, connect=10.0),
            trust_env=False,
        )
        try:
            response = self._client.get("/v1/metadata")
            response.raise_for_status()
        except httpx.HTTPError as exc:
            raise ModelServiceError("curator model service metadata is unavailable") from exc
        try:
            payload = response.json()
        except ValueError as exc:
            raise ModelServiceError("curator model service metadata is not valid JSON") from exc
        if not isinstance(payload, dict) or payload.get("ready") is not True:
            raise RuntimeError("curator model service did not report ready")
        self.metadata: dict[str, Any] = payload

    def quality(self, model_family: str, text: str) -> QualityScore:
        payload = self._post("/v1/quality", {"model_family": model_family, "text": text})
        try:
            score = QualityScore(
                edu_score=float(payload["edu_score"]),
                revision=str(payload["revision"]),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise ModelServiceError("curator model service returned invalid quality data") from exc
        expected = str(self.metadata["quality"][model_family]["revision"])
        if score.revision != expected:
            raise ModelServiceError(
                f"curator model service revision drift: {score.revision} != {expected}"
            )
        return score

    def perplexity(self, text: str) -> PerplexityResult:
        payload = self._post("/v1/perplexity", {"text": text})
        try:
            result = PerplexityResult(
                perplexity=float(payload["perplexity"]),
                bucket=str(payload["bucket"]),
                scorer=str(payload["scorer"]),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise ModelServiceError("curator model service returned invalid KenLM data") from exc
        expected = str(self.metadata["kenlm"]["scorer"])
        if result.scorer != expected:
            raise ModelServiceError(
                f"curator model service scorer drift: {result.scorer} != {expected}"
            )
        return result

    def embed(self, text: str) -> list[float]:
        payload = self._post("/v1/embed", {"text": text})
        vector = payload.get("embedding")
        if not isinstance(vector, list) or not vector:
            raise ModelServiceError("curator model service returned an empty embedding")
        try:
            return [float(value) for value in vector]
        except (TypeError, ValueError) as exc:
            raise ModelServiceError("curator model service returned an invalid embedding") from exc

    def _post(self, path: str, payload: dict[str, str]) -> dict[str, Any]:
        try:
            response = self._client.post(path, json=payload)
        except httpx.HTTPError as exc:
            raise ModelServiceError(f"curator model service request failed for {path}") from exc
        if response.status_code >= 500:
            raise ModelServiceError(
                f"curator model service returned {response.status_code} for {path}"
            )
        if response.status_code >= 400:
            raise ValueError(f"curator model service rejected {path} with {response.status_code}")
        try:
            value = response.json()
        except ValueError as exc:
            raise ModelServiceError(
                f"curator model service returned invalid JSON for {path}"
            ) from exc
        if not isinstance(value, dict):
            raise ModelServiceError(f"curator model service returned invalid JSON for {path}")
        return value

    def close(self) -> None:
        self._client.close()


class RemoteQualityClassifier:
    """QualityClassifier-compatible facade backed by the model service."""

    def __init__(self, client: CuratorModelClient, model_family: str) -> None:
        self._client = client
        self._model_family = model_family
        metadata = client.metadata.get("quality", {}).get(model_family, {})
        if metadata.get("backend") not in {"onnxruntime", "transformers-cpu"}:
            raise RuntimeError(f"the remote {model_family} classifier is not real")
        self._revision = str(metadata.get("revision", ""))
        if not self._revision:
            raise RuntimeError(f"the remote {model_family} classifier has no revision")

    @property
    def revision(self) -> str:
        return self._revision

    @property
    def backend(self) -> str:
        metadata = self._client.metadata["quality"][self._model_family]
        return str(metadata["backend"])

    @property
    def is_model_loaded(self) -> bool:
        return True

    def score(self, text: str) -> QualityScore:
        return self._client.quality(self._model_family, text)


class RemoteKenLMScorer:
    """KenLMScorer-compatible facade backed by the model service."""

    def __init__(self, client: CuratorModelClient) -> None:
        self._client = client
        metadata = client.metadata.get("kenlm", {})
        if metadata.get("backend") != "kenlm-sentencepiece":
            raise RuntimeError("the remote KenLM scorer is not real")
        self._scorer = str(metadata.get("scorer", ""))
        if not self._scorer:
            raise RuntimeError("the remote KenLM scorer has no revision")

    @property
    def scorer(self) -> str:
        return self._scorer

    @property
    def is_model_loaded(self) -> bool:
        return True

    def score(self, text: str) -> PerplexityResult:
        return self._client.perplexity(text)


class RemoteEmbeddingSketch:
    """Decon-Gate embedding index using remote E5 inference."""

    def __init__(self, client: CuratorModelClient) -> None:
        self._client = client
        metadata = client.metadata.get("embedding", {})
        if metadata.get("backend") != "onnxruntime-cpu":
            raise RuntimeError("the remote E5 embedding backend is not real")
        self._revision = str(metadata.get("revision", ""))
        if not self._revision:
            raise RuntimeError("the remote E5 embedding backend has no revision")
        self._index: dict[BenchmarkName, list[list[float]]] = {}

    @property
    def revision(self) -> str:
        return self._revision

    @property
    def backend(self) -> str:
        return "onnxruntime-cpu-remote"

    def add(self, benchmark: BenchmarkName, text: str) -> None:
        self._index.setdefault(benchmark, []).append(self._client.embed(text))

    def query(self, text: str) -> Iterable[tuple[BenchmarkName, float]]:
        if not self._index:
            return []
        query = self._client.embed(text)
        return [
            (benchmark, max((_cosine(query, vector) for vector in vectors), default=0.0))
            for benchmark, vectors in self._index.items()
        ]


def _cosine(left: list[float], right: list[float]) -> float:
    if not left or len(left) != len(right):
        return 0.0
    numerator = sum(a * b for a, b in zip(left, right, strict=True))
    left_norm = math.sqrt(sum(value * value for value in left))
    right_norm = math.sqrt(sum(value * value for value in right))
    if left_norm == 0 or right_norm == 0:
        return 0.0
    return numerator / (left_norm * right_norm)
