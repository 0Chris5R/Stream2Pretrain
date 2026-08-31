"""Strict client facades for the stateless curator model service."""

from __future__ import annotations

import json
import socket
import threading
import time
from collections import deque
from collections.abc import Callable, Sequence
from typing import Any
from urllib.parse import urlsplit

import httpx
from prometheus_client import Counter, Gauge, Histogram

from processor.operators.kenlm_score import PerplexityResult
from processor.operators.quality import QualityScore

MODEL_SERVICE_MAX_REQUEST_BYTES = 2 * 1024 * 1024

MODEL_CLIENT_ENDPOINTS = Gauge(
    "s2p_curator_model_endpoints",
    "Ready model Pod endpoints known to the curator.",
    ["profile"],
)
MODEL_CLIENT_WAITING = Gauge(
    "s2p_curator_model_waiting_requests",
    "Curator requests waiting for a free model Pod endpoint.",
    ["profile"],
)
MODEL_CLIENT_WAIT_SECONDS = Histogram(
    "s2p_curator_model_wait_seconds",
    "Time a curator request waits for a free model Pod endpoint.",
    ["profile"],
    buckets=(0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5, 15, 60),
)
MODEL_CLIENT_REQUESTS = Counter(
    "s2p_curator_model_requests_total",
    "Direct curator requests by resolved model backend.",
    ["profile", "backend", "status"],
)


class ModelServiceError(RuntimeError):
    """Transient model-service failure that must not advance a Kafka offset."""


EndpointResolver = Callable[[], Sequence[str]]
HttpClientFactory = Callable[[str], httpx.Client]


def _new_http_client(base_url: str, timeout_seconds: float) -> httpx.Client:
    return httpx.Client(
        base_url=base_url.rstrip("/"),
        timeout=httpx.Timeout(timeout_seconds, connect=10.0),
        # Each backend owns one inference lock. Keep direct connections short
        # lived so a replaced Pod cannot retain a stale connection.
        limits=httpx.Limits(max_keepalive_connections=0, max_connections=16),
        headers={"Connection": "close"},
        trust_env=False,
    )


def _metadata(client: httpx.Client) -> dict[str, Any]:
    try:
        response = client.get("/v1/metadata")
        response.raise_for_status()
    except httpx.HTTPError as exc:
        raise ModelServiceError("curator model service metadata is unavailable") from exc
    try:
        payload = response.json()
    except ValueError as exc:
        raise ModelServiceError("curator model service metadata is not valid JSON") from exc
    if not isinstance(payload, dict) or payload.get("ready") is not True:
        raise RuntimeError("curator model service did not report ready")
    return payload


def resolved_endpoint_urls(service_host: str, *, scheme: str, port: int) -> list[str]:
    """Resolve one headless Service into stable, numeric Pod URLs."""
    addresses: set[str] = set()
    try:
        records = socket.getaddrinfo(
            service_host,
            port,
            family=socket.AF_UNSPEC,
            type=socket.SOCK_STREAM,
        )
    except OSError as exc:
        raise ModelServiceError(f"model endpoint discovery failed for {service_host}") from exc
    for _family, _type, _protocol, _canonical, socket_address in records:
        host = str(socket_address[0])
        authority = f"[{host}]" if ":" in host else host
        addresses.add(f"{scheme}://{authority}:{port}")
    if not addresses:
        raise ModelServiceError(f"model endpoint discovery returned no Pods for {service_host}")
    return sorted(addresses)


def headless_endpoint_resolver(base_url: str, service_host: str) -> EndpointResolver:
    """Build a resolver that keeps the configured service scheme and port."""
    parsed = urlsplit(base_url)
    port = parsed.port
    if parsed.scheme not in {"http", "https"} or port is None:
        raise ValueError("model service URL must contain an HTTP(S) scheme and explicit port")
    return lambda: resolved_endpoint_urls(
        service_host,
        scheme=parsed.scheme,
        port=port,
    )


class _EndpointPool:
    """Lease at most one request to each single-lock model Pod."""

    def __init__(
        self,
        *,
        profile: str,
        resolver: EndpointResolver,
        client_factory: HttpClientFactory,
        expected_metadata: dict[str, Any],
        refresh_seconds: float,
    ) -> None:
        if refresh_seconds <= 0:
            raise ValueError("endpoint refresh interval must be positive")
        self._profile = profile
        self._resolver = resolver
        self._client_factory = client_factory
        self._expected_metadata = expected_metadata
        self._refresh_seconds = refresh_seconds
        self._condition = threading.Condition()
        self._refresh_lock = threading.Lock()
        self._clients: dict[str, httpx.Client] = {}
        self._available: deque[str] = deque()
        self._leased: set[str] = set()
        self._stale: set[str] = set()
        self._next_refresh = 0.0
        self._closed = False
        self._refresh(force=True)

    def post(self, path: str, payload: dict[str, Any]) -> dict[str, Any]:
        failed_endpoints: set[str] = set()
        last_error: ModelServiceError | None = None
        # A direct classifier call is side-effect free. Retry once on a
        # different ready Pod when an endpoint disappears during HPA churn;
        # otherwise surface the transient error so Bytewax does not advance.
        for attempt in range(2):
            endpoint, client = self._lease(excluding=failed_endpoints)
            discard = False
            try:
                value, backend = _post_json(client, path, payload)
            except ModelServiceError as exc:
                last_error = exc
                discard = True
                failed_endpoints.add(endpoint)
            else:
                MODEL_CLIENT_REQUESTS.labels(self._profile, backend or endpoint, "success").inc()
                return value
            finally:
                self._release(endpoint, discard=discard)
            MODEL_CLIENT_REQUESTS.labels(self._profile, endpoint, "error").inc()
            if attempt == 0:
                self._refresh(force=True)
        assert last_error is not None
        raise last_error

    def _lease(self, *, excluding: set[str]) -> tuple[str, httpx.Client]:
        waiting = False
        wait_started = time.monotonic()
        try:
            while True:
                self._refresh(force=False)
                with self._condition:
                    if self._closed:
                        raise ModelServiceError("curator model endpoint pool is closed")
                    for _ in range(len(self._available)):
                        endpoint = self._available.popleft()
                        if endpoint in excluding or endpoint in self._stale:
                            self._available.append(endpoint)
                            continue
                        client = self._clients.get(endpoint)
                        if client is None:
                            continue
                        self._leased.add(endpoint)
                        return endpoint, client
                    if not any(
                        endpoint not in excluding and endpoint not in self._stale
                        for endpoint in self._clients
                    ):
                        raise ModelServiceError("no untried model Pod endpoint is available")
                    if not waiting:
                        waiting = True
                        MODEL_CLIENT_WAITING.labels(self._profile).inc()
                    self._condition.wait(timeout=min(1.0, self._refresh_seconds))
        finally:
            if waiting:
                MODEL_CLIENT_WAITING.labels(self._profile).dec()
                MODEL_CLIENT_WAIT_SECONDS.labels(self._profile).observe(
                    time.monotonic() - wait_started
                )

    def _release(self, endpoint: str, *, discard: bool) -> None:
        client_to_close: httpx.Client | None = None
        with self._condition:
            self._leased.discard(endpoint)
            if discard or endpoint in self._stale:
                client_to_close = self._clients.pop(endpoint, None)
                self._stale.discard(endpoint)
                self._available = deque(item for item in self._available if item != endpoint)
            elif endpoint in self._clients:
                self._available.append(endpoint)
            MODEL_CLIENT_ENDPOINTS.labels(self._profile).set(len(self._clients))
            self._condition.notify_all()
        if client_to_close is not None:
            client_to_close.close()

    def _refresh(self, *, force: bool) -> None:
        now = time.monotonic()
        with self._condition:
            if not force and now < self._next_refresh:
                return
        with self._refresh_lock:
            now = time.monotonic()
            with self._condition:
                if not force and now < self._next_refresh:
                    return
            try:
                resolved = set(self._resolver())
            except ModelServiceError:
                with self._condition:
                    if self._clients:
                        self._next_refresh = now + self._refresh_seconds
                        return
                raise
            if not resolved:
                with self._condition:
                    if self._clients:
                        self._next_refresh = now + self._refresh_seconds
                        return
                raise ModelServiceError("model endpoint discovery returned no ready Pods")

            with self._condition:
                current = set(self._clients)
            additions: dict[str, httpx.Client] = {}
            for endpoint in sorted(resolved - current):
                client = self._client_factory(endpoint)
                try:
                    endpoint_metadata = _metadata(client)
                    if endpoint_metadata != self._expected_metadata:
                        raise ModelServiceError(f"model endpoint metadata drift at {endpoint}")
                except Exception:
                    client.close()
                    continue
                additions[endpoint] = client

            clients_to_close: list[httpx.Client] = []
            with self._condition:
                for endpoint, client in additions.items():
                    if endpoint not in self._clients:
                        self._clients[endpoint] = client
                        self._available.append(endpoint)
                    else:
                        clients_to_close.append(client)
                for endpoint in set(self._clients) - resolved:
                    if endpoint in self._leased:
                        self._stale.add(endpoint)
                    else:
                        client = self._clients.pop(endpoint)
                        clients_to_close.append(client)
                        self._available = deque(
                            item for item in self._available if item != endpoint
                        )
                self._next_refresh = now + self._refresh_seconds
                MODEL_CLIENT_ENDPOINTS.labels(self._profile).set(len(self._clients))
                self._condition.notify_all()
                if not self._clients:
                    raise ModelServiceError("no revision-matching model Pod endpoint is ready")
            for client in clients_to_close:
                client.close()

    def close(self) -> None:
        with self._condition:
            self._closed = True
            clients = list(self._clients.values())
            self._clients.clear()
            self._available.clear()
            self._stale.clear()
            MODEL_CLIENT_ENDPOINTS.labels(self._profile).set(0)
            self._condition.notify_all()
        for client in clients:
            client.close()


def _post_json(
    client: httpx.Client,
    path: str,
    payload: dict[str, Any],
) -> tuple[dict[str, Any], str]:
    try:
        response = client.post(path, json=payload)
    except httpx.HTTPError as exc:
        raise ModelServiceError(f"curator model service request failed for {path}") from exc
    if response.status_code >= 500:
        raise ModelServiceError(f"curator model service returned {response.status_code} for {path}")
    if response.status_code >= 400:
        raise ValueError(f"curator model service rejected {path} with {response.status_code}")
    try:
        value = response.json()
    except ValueError as exc:
        raise ModelServiceError(f"curator model service returned invalid JSON for {path}") from exc
    if not isinstance(value, dict):
        raise ModelServiceError(f"curator model service returned invalid JSON for {path}")
    return value, response.headers.get("X-S2P-Model-Backend", "").strip()


class CuratorModelClient:
    """Synchronous client shared by the curation model facades."""

    def __init__(
        self,
        base_url: str,
        *,
        timeout_seconds: float = 180.0,
        client: httpx.Client | None = None,
        profile: str = "combined",
        endpoint_resolver: EndpointResolver | None = None,
        endpoint_refresh_seconds: float = 5.0,
        endpoint_client_factory: HttpClientFactory | None = None,
    ) -> None:
        self._client = client or _new_http_client(base_url, timeout_seconds)
        self.metadata = _metadata(self._client)
        factory = endpoint_client_factory or (
            lambda endpoint: _new_http_client(endpoint, timeout_seconds)
        )
        self._endpoint_pool = (
            _EndpointPool(
                profile=profile,
                resolver=endpoint_resolver,
                client_factory=factory,
                expected_metadata=self.metadata,
                refresh_seconds=endpoint_refresh_seconds,
            )
            if endpoint_resolver is not None
            else None
        )

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
        if self._endpoint_pool is not None:
            return self._endpoint_pool.post(path, payload)
        return _post_json(self._client, path, payload)[0]

    def close(self) -> None:
        if self._endpoint_pool is not None:
            self._endpoint_pool.close()
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
