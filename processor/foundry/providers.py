"""OpenAI-compatible control plane with exact route provenance."""

from __future__ import annotations

import json
import time
import uuid
from collections.abc import Callable, Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

import httpx
from pydantic import BaseModel

from processor.foundry.config import ProviderConfig
from processor.foundry.util import model_family, sha256
from schemas.foundry import ProviderModelSnapshot, ProviderTrace


class ProviderError(RuntimeError):
    pass


class ProviderRateLimitedError(ProviderError):
    pass


class ProviderBudgetExhaustedError(ProviderError):
    """The provider rejected work because the external account has no budget."""

    def __init__(self, provider: str, reason: str) -> None:
        self.provider = provider
        super().__init__(f"{provider} budget exhausted: {reason}")


class ProviderDriftError(ProviderError):
    pass


class StructuredGeneration(BaseModel):
    data: dict[str, Any] | list[Any]
    trace: ProviderTrace


class StructuredProvider(Protocol):
    name: str

    def discover_models(self) -> ProviderModelSnapshot: ...

    def generate_json(
        self,
        *,
        role: str,
        system: str,
        user: str,
        prompt_version: str,
        max_output_tokens: int,
        temperature: float = 0.0,
        seed: int = 7342,
        checkpoint: Callable[[str, str], None] | None = None,
    ) -> StructuredGeneration: ...


class OpenAICompatibleProvider:
    """Authenticated discovery plus structured chat completion transport."""

    def __init__(
        self,
        config: ProviderConfig,
        *,
        timeout_seconds: float = 180.0,
        max_retries: int = 2,
        client: httpx.Client | None = None,
    ) -> None:
        if not config.api_key:
            raise ProviderError(f"{config.api_key_env} is required for live foundry mode")
        self.name = config.name
        self.config = config
        self._client = client or httpx.Client(
            timeout=httpx.Timeout(timeout_seconds, connect=20.0),
            headers={
                "Authorization": f"Bearer {config.api_key}",
                "Content-Type": "application/json",
                "User-Agent": "Stream2Train-Foundry/1",
            },
        )
        self._max_retries = max_retries
        self._model_snapshot: ProviderModelSnapshot | None = None

    def discover_models(self) -> ProviderModelSnapshot:
        response = self._request("GET", "/models")
        payload = response.json()
        raw_models = payload.get("data", payload.get("models", []))
        if not isinstance(raw_models, list):
            raise ProviderError(f"{self.name} /models returned no model list")
        models = [item if isinstance(item, dict) else {"id": str(item)} for item in raw_models]
        configured = [
            model_id
            for item in models
            if (model_id := str(item.get("id", item.get("name", ""))))
            and self._model_allowed(model_id)
        ]
        stable_models = [
            {key: value for key, value in item.items() if key != "created"} for item in models
        ]
        snapshot = ProviderModelSnapshot(
            provider=self.name,  # type: ignore[arg-type]
            discovered_at=datetime.now(UTC),
            response_hash=sha256(sorted(stable_models, key=lambda item: str(item.get("id", "")))),
            models=models,
            configured_model_ids=configured,
        )
        self._model_snapshot = snapshot
        if not configured:
            raise ProviderDriftError(
                f"{self.name} exposes none of the configured models {self.config.preferred_models}"
            )
        return snapshot

    def generate_json(
        self,
        *,
        role: str,
        system: str,
        user: str,
        prompt_version: str,
        max_output_tokens: int,
        temperature: float = 0.0,
        seed: int = 7342,
        checkpoint: Callable[[str, str], None] | None = None,
    ) -> StructuredGeneration:
        model = self._select_model()
        request_body: dict[str, Any] = {
            "model": model,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            "temperature": temperature,
            "max_tokens": max_output_tokens,
            "seed": seed,
            "response_format": {"type": "json_object"},
            "stream": True,
            "stream_options": {"include_usage": True},
        }
        if "Qwen3.8" in model:
            # Qwen3.8 thinks by default. With a bounded JSON output budget it
            # can spend the entire completion on hidden reasoning and emit no
            # answer content, so structured compiler calls use its documented
            # non-thinking chat-template mode.
            request_body["chat_template_kwargs"] = {"enable_thinking": False}
        started = datetime.now(UTC)
        before = time.perf_counter()
        payload = self._stream_completion(request_body, checkpoint=checkpoint)
        elapsed_ms = int((time.perf_counter() - before) * 1000)
        content = _message_content(payload)
        data = _parse_json_content(content)
        usage = payload.get("usage", {}) if isinstance(payload, dict) else {}
        returned_model = str(payload.get("model") or model)
        upstream_provider = _upstream_provider(payload)
        trace = ProviderTrace(
            trace_id=f"provider:{uuid.uuid4()}",
            provider=self.name,  # type: ignore[arg-type]
            credential_label=self.config.credential_label,
            role=role,
            base_url=self.config.base_url,
            requested_model=model,
            returned_model=returned_model,
            upstream_provider=upstream_provider,
            provider_request_id=(str(payload["id"]) if payload.get("id") else None),
            dynamic_route=self.config.dynamic_route or returned_model != model,
            model_family=model_family(returned_model),
            model_license=(self.config.model_license(returned_model) or (None, None))[0],
            model_license_source=(self.config.model_license(returned_model) or (None, None))[1],
            prompt_version=prompt_version,
            request_hash=sha256(request_body),
            response_hash=sha256(payload),
            input_tokens=int(usage.get("prompt_tokens", usage.get("input_tokens", 0)) or 0),
            output_tokens=int(usage.get("completion_tokens", usage.get("output_tokens", 0)) or 0),
            request_attempts=int(payload.get("_request_attempts", 1)),
            latency_ms=elapsed_ms,
            time_to_first_token_ms=int(payload.get("_time_to_first_token_ms", 0)),
            output_tokens_per_second=(
                float(
                    int(usage.get("completion_tokens", usage.get("output_tokens", 0)) or 0)
                    / max(elapsed_ms / 1000, 1e-9)
                )
            ),
            sampling={
                "temperature": temperature,
                "max_output_tokens": max_output_tokens,
                "seed": seed,
            },
            terms_snapshot_hash=self.config.terms_snapshot_hash,
            started_at=started,
            completed_at=datetime.now(UTC),
        )
        if not self._model_allowed(returned_model):
            raise ProviderDriftError(
                f"{self.name} returned model without a configured exact license record: {returned_model}"
            )
        return StructuredGeneration(data=data, trace=trace)

    def _select_model(self) -> str:
        snapshot = self._model_snapshot or self.discover_models()
        for preferred in self.config.preferred_models:
            if preferred in snapshot.configured_model_ids:
                return preferred
        return snapshot.configured_model_ids[0]

    def _model_allowed(self, model_id: str) -> bool:
        return (
            model_id in self.config.preferred_models
            and self.config.model_license(model_id) is not None
        )

    def _request(
        self,
        method: str,
        path: str,
        *,
        json_body: Mapping[str, Any] | None = None,
    ) -> httpx.Response:
        last_error: Exception | None = None
        for attempt in range(self._max_retries + 1):
            try:
                response = self._client.request(
                    method,
                    f"{self.config.base_url}{path}",
                    json=dict(json_body) if json_body is not None else None,
                )
                if response.status_code == 402:
                    raise ProviderBudgetExhaustedError(
                        self.name,
                        _provider_error_reason(response),
                    )
                if response.status_code == 429:
                    if attempt >= self._max_retries:
                        raise ProviderRateLimitedError(f"{self.name} rate limited the request")
                    _bounded_backoff(attempt, response)
                    continue
                response.raise_for_status()
                return response
            except (ProviderRateLimitedError, ProviderBudgetExhaustedError):
                raise
            except (httpx.HTTPError, ValueError) as exc:
                last_error = exc
                if attempt >= self._max_retries:
                    break
                time.sleep(min(2**attempt, 4))
        raise ProviderError(
            f"{self.name} request failed: {_transport_error_reason(last_error)}"
        ) from last_error

    def _stream_completion(
        self,
        request_body: Mapping[str, Any],
        *,
        checkpoint: Callable[[str, str], None] | None,
    ) -> dict[str, Any]:
        """Consume OpenAI-compatible SSE and expose reconstructable partial output."""
        last_error: Exception | None = None
        for attempt in range(self._max_retries + 1):
            content_parts: list[str] = []
            final: dict[str, Any] = {"choices": [{"message": {"content": ""}}]}
            stream_started = time.perf_counter()
            first_token_ms: int | None = None
            try:
                with self._client.stream(
                    "POST",
                    f"{self.config.base_url}/chat/completions",
                    json=dict(request_body),
                    headers={"Idempotency-Key": sha256(request_body).removeprefix("sha256:")},
                ) as response:
                    if response.status_code == 402:
                        response.read()
                        raise ProviderBudgetExhaustedError(
                            self.name,
                            _provider_error_reason(response),
                        )
                    if response.status_code == 429:
                        if attempt >= self._max_retries:
                            raise ProviderRateLimitedError(f"{self.name} rate limited the request")
                        response.read()
                        _bounded_backoff(attempt, response)
                        continue
                    if response.is_error:
                        response.read()
                    response.raise_for_status()
                    for line in response.iter_lines():
                        stripped = line.strip()
                        if not stripped or stripped.startswith(":"):
                            continue
                        if stripped.startswith("data:"):
                            stripped = stripped[5:].strip()
                        if stripped == "[DONE]":
                            break
                        chunk = json.loads(stripped)
                        if not isinstance(chunk, dict):
                            continue
                        if chunk.get("model"):
                            final["model"] = chunk["model"]
                        if chunk.get("id"):
                            final["id"] = chunk["id"]
                        if chunk.get("usage"):
                            final["usage"] = chunk["usage"]
                        upstream = _upstream_provider(chunk)
                        if upstream:
                            final["upstream_provider"] = upstream
                        choices = chunk.get("choices") or []
                        if not choices or not isinstance(choices[0], dict):
                            continue
                        delta = choices[0].get("delta") or choices[0].get("message") or {}
                        if not isinstance(delta, dict) or not delta.get("content"):
                            continue
                        piece = delta["content"]
                        if isinstance(piece, list):
                            piece = "".join(
                                str(part.get("text", ""))
                                for part in piece
                                if isinstance(part, dict)
                            )
                        content_parts.append(str(piece))
                        if first_token_ms is None:
                            first_token_ms = int((time.perf_counter() - stream_started) * 1000)
                        partial = "".join(content_parts)
                        if checkpoint is not None:
                            checkpoint(partial, sha256(partial))
                    content = "".join(content_parts)
                    if not content:
                        raise ProviderError(f"{self.name} stream returned no content")
                    final["choices"] = [{"message": {"content": content}}]
                    final["_time_to_first_token_ms"] = first_token_ms or 0
                    final["_request_attempts"] = attempt + 1
                    return final
            except ProviderRateLimitedError:
                raise
            except (httpx.HTTPError, ValueError, json.JSONDecodeError) as exc:
                last_error = exc
                if checkpoint is not None and content_parts:
                    partial = "".join(content_parts)
                    checkpoint(partial, sha256(partial))
                if attempt >= self._max_retries:
                    break
                time.sleep(min(2**attempt, 4))
        raise ProviderError(
            f"{self.name} streaming request failed: {_transport_error_reason(last_error)}"
        ) from last_error


class ReplayProvider:
    """Deterministic recorded-response provider for CI and local control-flow tests."""

    def __init__(self, name: str, fixture_path: str) -> None:
        self.name = name
        payload = json.loads(Path(fixture_path).read_text(encoding="utf-8"))
        entries = payload.get(name)
        if not isinstance(entries, dict):
            raise ProviderError(f"replay fixture has no {name} responses")
        self._entries: dict[str, list[dict[str, Any]]] = {
            str(role): list(values) for role, values in entries.items()
        }
        self._offsets: dict[str, int] = {}

    def discover_models(self) -> ProviderModelSnapshot:
        model_id = f"replay/{self.name}"
        return ProviderModelSnapshot(
            provider=self.name,  # type: ignore[arg-type]
            discovered_at=datetime.fromtimestamp(0, UTC),
            response_hash=sha256({"model": model_id}),
            models=[{"id": model_id, "recorded": True}],
            configured_model_ids=[model_id],
        )

    def generate_json(
        self,
        *,
        role: str,
        system: str,
        user: str,
        prompt_version: str,
        max_output_tokens: int,
        temperature: float = 0.0,
        seed: int = 7342,
        checkpoint: Callable[[str, str], None] | None = None,
    ) -> StructuredGeneration:
        values = self._entries.get(role, [])
        offset = self._offsets.get(role, 0)
        if offset >= len(values):
            raise ProviderError(f"replay fixture exhausted for role {role}")
        data = values[offset]
        self._offsets[role] = offset + 1
        response_hash = sha256(data)
        if checkpoint is not None:
            checkpoint(json.dumps(data, sort_keys=True), response_hash)
        now = datetime.fromtimestamp(offset, UTC)
        model_id = f"replay/{self.name}"
        trace = ProviderTrace(
            trace_id=f"replay:{self.name}:{role}:{offset}",
            provider="replay",
            credential_label="recorded-fixture",
            role=role,
            base_url="replay://local",
            requested_model=model_id,
            returned_model=model_id,
            model_family=self.name,
            prompt_version=prompt_version,
            request_hash=sha256({"system": system, "user": user}),
            response_hash=response_hash,
            input_tokens=0,
            output_tokens=0,
            latency_ms=0,
            sampling={
                "temperature": temperature,
                "max_output_tokens": max_output_tokens,
                "seed": seed,
            },
            terms_snapshot_hash=sha256("recorded-fixture"),
            started_at=now,
            completed_at=now,
        )
        return StructuredGeneration(data=data, trace=trace)


def build_providers(
    configs: Mapping[str, ProviderConfig],
    *,
    mode: str,
    replay_fixture: str | None,
    timeout_seconds: float,
    max_retries: int,
) -> dict[str, StructuredProvider]:
    if mode == "replay":
        if not replay_fixture:
            raise ProviderError("S2P_FOUNDRY_REPLAY_FIXTURE is required in replay mode")
        return {name: ReplayProvider(name, replay_fixture) for name in configs}
    if mode != "live":
        raise ProviderError("S2P_FOUNDRY_PROVIDER_MODE must be live or replay")
    return {
        name: OpenAICompatibleProvider(
            config,
            timeout_seconds=timeout_seconds,
            max_retries=max_retries,
        )
        for name, config in configs.items()
    }


def _message_content(payload: Any) -> str:
    try:
        content = payload["choices"][0]["message"]["content"]
    except (KeyError, IndexError, TypeError) as exc:
        raise ProviderError("completion response has no message content") from exc
    if isinstance(content, list):
        return "".join(str(part.get("text", "")) for part in content if isinstance(part, dict))
    return str(content)


def _parse_json_content(content: str) -> dict[str, Any] | list[Any]:
    cleaned = content.strip()
    if cleaned.startswith("```"):
        cleaned = cleaned.removeprefix("```json").removeprefix("```")
        cleaned = cleaned.removesuffix("```").strip()
    try:
        value = json.loads(cleaned)
    except json.JSONDecodeError as exc:
        raise ProviderError("model did not return valid JSON") from exc
    if not isinstance(value, (dict, list)):
        raise ProviderError("model JSON root must be an object or array")
    return value


def _upstream_provider(payload: Any) -> str | None:
    if not isinstance(payload, dict):
        return None
    for key in ("provider", "upstream_provider", "provider_name"):
        value = payload.get(key)
        if value:
            return str(value)
    metadata = payload.get("metadata")
    if isinstance(metadata, dict):
        for key in ("provider", "upstream_provider"):
            if metadata.get(key):
                return str(metadata[key])
    return None


def _provider_error_reason(response: httpx.Response) -> str:
    """Return a bounded provider error without headers, credentials, or response dumps."""
    try:
        payload = response.json()
    except ValueError:
        return f"HTTP {response.status_code}"
    if isinstance(payload, dict):
        error = payload.get("error")
        if isinstance(error, dict) and isinstance(error.get("message"), str):
            return error["message"][:300]
        if isinstance(error, str):
            return error[:300]
        if isinstance(payload.get("message"), str):
            return payload["message"][:300]
    return f"HTTP {response.status_code}"


def _transport_error_reason(error: Exception | None) -> str:
    """Return bounded diagnostics without request headers or prompt content."""
    if isinstance(error, httpx.HTTPStatusError):
        return _provider_error_reason(error.response)
    if isinstance(error, httpx.TimeoutException):
        return error.__class__.__name__
    if error is None:
        return "unknown transport error"
    return error.__class__.__name__


def _bounded_backoff(attempt: int, response: httpx.Response) -> None:
    retry_after = response.headers.get("retry-after")
    try:
        delay = min(float(retry_after), 10.0) if retry_after else min(2**attempt, 4)
    except ValueError:
        delay = min(2**attempt, 4)
    time.sleep(max(0.0, delay))


__all__ = [
    "OpenAICompatibleProvider",
    "ProviderBudgetExhaustedError",
    "ProviderDriftError",
    "ProviderError",
    "ProviderRateLimitedError",
    "ReplayProvider",
    "StructuredGeneration",
    "StructuredProvider",
    "build_providers",
]
