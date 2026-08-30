"""Strict client facades for the stateless curator model service."""

from __future__ import annotations

import json
from collections.abc import Sequence
from typing import Any

import httpx

from processor.operators.kenlm_score import PerplexityResult
from processor.operators.quality import QualityScore

MODEL_SERVICE_MAX_REQUEST_BYTES = 2 * 1024 * 1024


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
            # Kubernetes balances TCP connections, not individual requests on
            # an already-open connection.  The curator deliberately opens a
            # fresh in-cluster connection for each bounded classifier batch so
            # all ready replicas can receive work instead of one long-lived
            # connection pinning the stream to one Pod.
            limits=httpx.Limits(max_keepalive_connections=0, max_connections=16),
            headers={"Connection": "close"},
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
        return self.quality_many(model_family, [text])[0]

    def quality_many(self, model_family: str, texts: Sequence[str]) -> list[QualityScore]:
        """Score one bounded batch while preserving input order and revisions."""
        if not texts:
            return []
        request_payload = {"model_family": model_family, "texts": list(texts)}
        # Combining otherwise valid singleton calls must never make a document
        # fail the service's unchanged 2 MiB request limit. Fall back to the
        # exact singleton path when only the batch envelope crosses that bound.
        encoded_size = len(json.dumps(request_payload).encode("utf-8"))
        if len(texts) > 1 and encoded_size > MODEL_SERVICE_MAX_REQUEST_BYTES:
            return [self.quality_many(model_family, [text])[0] for text in texts]
        payload = self._post(
            "/v1/quality:batch",
            request_payload,
        )
        raw_results = payload.get("results")
        if not isinstance(raw_results, list) or len(raw_results) != len(texts):
            raise ModelServiceError("curator model service returned invalid quality batch data")
        scores: list[QualityScore] = []
        try:
            for item in raw_results:
                if not isinstance(item, dict):
                    raise TypeError("quality batch item is not an object")
                scores.append(
                    QualityScore(
                        edu_score=float(item["edu_score"]),
                        revision=str(item["revision"]),
                    )
                )
        except (KeyError, TypeError, ValueError) as exc:
            raise ModelServiceError("curator model service returned invalid quality data") from exc
        expected = str(self.metadata["quality"][model_family]["revision"])
        for score in scores:
            if score.revision != expected:
                raise ModelServiceError(
                    f"curator model service revision drift: {score.revision} != {expected}"
                )
        return scores

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

    def _post(self, path: str, payload: dict[str, Any]) -> dict[str, Any]:
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

    def score_many(self, texts: Sequence[str]) -> list[QualityScore]:
        return self._client.quality_many(self._model_family, texts)


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
