"""Provider calls wrapped in quota, event, drift, and provenance controls."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from processor.foundry.config import FoundryConfig
from processor.foundry.providers import (
    ProviderBudgetExhaustedError,
    ProviderRateLimitedError,
    StructuredProvider,
)
from processor.foundry.quota import QuotaLedger
from processor.foundry.routing import provider_for_role
from processor.foundry.store import FoundryStore
from processor.foundry.util import sha256
from schemas.foundry import FoundryEvent, ProviderTrace


class ProviderDiscoveryError(RuntimeError):
    pass


class ProviderControlPlane:
    def __init__(
        self,
        *,
        config: FoundryConfig,
        providers: dict[str, StructuredProvider],
        quota: QuotaLedger,
        store: FoundryStore,
        event_sink: Callable[[FoundryEvent], None] | None = None,
    ) -> None:
        self.config = config
        self.providers = providers
        self.quota = quota
        self.store = store
        self.event_sink = event_sink

    def discover_models(self) -> dict[str, Any]:
        snapshots: dict[str, Any] = {}
        failures: list[str] = []
        for name, provider in self.providers.items():
            try:
                discovered = provider.discover_models()
            except Exception as exc:
                failures.append(f"{name} model discovery failed: {exc.__class__.__name__}")
                continue
            snapshot = self.store.record_model_snapshot(discovered)
            snapshots[name] = snapshot
        if failures:
            raise ProviderDiscoveryError(
                "foundry provider discovery failed: " + "; ".join(failures)
            )
        return snapshots

    def call(
        self,
        *,
        job_id: str,
        paper_id: str,
        role: str,
        system: str,
        user: str,
        max_output_tokens: int,
        temperature: float = 0.0,
        seed: int = 7342,
        attempt: int | None = None,
        call_key: str | None = None,
    ) -> tuple[dict[str, Any] | list[Any], ProviderTrace]:
        provider_name = provider_for_role(role)
        provider = self.providers[provider_name]
        suffix = call_key or role
        request_hash = sha256(
            {
                "provider": provider_name,
                "configured_models": self.config.providers[provider_name].preferred_models,
                "role": role,
                "system": system,
                "user": user,
                "prompt_version": self.config.prompt_version,
                "max_output_tokens": max_output_tokens,
                "temperature": temperature,
                "seed": seed,
            }
        )
        cached = self.store.cached_provider_result(
            job_id=job_id,
            call_key=suffix,
            prompt_version=self.config.prompt_version,
            request_hash=request_hash,
        )
        if (
            cached is not None
            and cached[1].returned_model
            in self.config.providers[provider_name].preferred_models
        ):
            return cached
        resolved_attempt = attempt or self.store.next_provider_call_attempt(job_id)
        estimated_input = max(1, (len(system) + len(user) + 3) // 4)
        self._event(
            job_id=job_id,
            paper_id=paper_id,
            state="CALL_PLANNED",
            metadata={
                "provider": provider_name,
                "role": role,
                "estimated_input_tokens": estimated_input,
                "max_output_tokens": max_output_tokens,
            },
            attempt=resolved_attempt,
            suffix=suffix,
        )
        reservation = self.quota.reserve(
            provider_name,
            input_tokens=estimated_input,
            output_tokens=max_output_tokens,
            requests=self.config.max_retries + 1,
        )
        self._event(
            job_id=job_id,
            paper_id=paper_id,
            state="QUOTA_RESERVED",
            metadata={
                "provider": provider_name,
                "role": role,
                "reservation_id": reservation.reservation_id,
            },
            attempt=resolved_attempt,
            suffix=suffix,
        )
        self._event(
            job_id=job_id,
            paper_id=paper_id,
            state="CALL_STARTED",
            metadata={"provider": provider_name, "role": role},
            attempt=resolved_attempt,
            suffix=suffix,
        )
        trace: ProviderTrace | None = None
        checkpoint_announced = False

        def checkpoint(partial_text: str, response_hash: str) -> None:
            nonlocal checkpoint_announced
            self.store.save_stream_checkpoint(
                job_id=job_id,
                call_key=suffix,
                attempt=resolved_attempt,
                partial_text=partial_text,
                partial_hash=response_hash,
            )
            if checkpoint_announced:
                return
            checkpoint_announced = True
            self._event(
                job_id=job_id,
                paper_id=paper_id,
                state="STREAM_CHECKPOINTED",
                metadata={
                    "provider": provider_name,
                    "role": role,
                    "response_checkpoint_hash": response_hash,
                    "partial_characters": len(partial_text),
                },
                attempt=resolved_attempt,
                suffix=suffix,
                update_job_state=False,
            )

        try:
            result = provider.generate_json(
                role=role,
                system=system,
                user=user,
                prompt_version=self.config.prompt_version,
                max_output_tokens=max_output_tokens,
                temperature=temperature,
                seed=seed,
                checkpoint=checkpoint,
            )
            trace = result.trace
            self.store.record_provider_result(
                job_id=job_id,
                call_key=suffix,
                prompt_version=self.config.prompt_version,
                request_hash=request_hash,
                response=result.data,
                trace=trace,
            )
            self.store.record_trace(job_id, trace)
            self._event(
                job_id=job_id,
                paper_id=paper_id,
                state="CALL_SUCCEEDED",
                provider_trace_id=trace.trace_id,
                metadata={
                    "provider": provider_name,
                    "role": role,
                    "returned_model": trace.returned_model,
                    "model_family": trace.model_family,
                    "input_tokens": trace.input_tokens,
                    "output_tokens": trace.output_tokens,
                    "latency_ms": trace.latency_ms,
                    "time_to_first_token_ms": trace.time_to_first_token_ms,
                    "output_tokens_per_second": trace.output_tokens_per_second,
                },
                attempt=resolved_attempt,
                suffix=suffix,
            )
            return result.data, result.trace
        except Exception as exc:
            self._event(
                job_id=job_id,
                paper_id=paper_id,
                state=(
                    "CALL_RATE_LIMITED"
                    if isinstance(exc, (ProviderRateLimitedError, ProviderBudgetExhaustedError))
                    else "CALL_FAILED"
                ),
                reason=str(exc),
                metadata={"provider": provider_name, "role": role},
                attempt=resolved_attempt,
                suffix=suffix,
            )
            raise
        finally:
            self.quota.reconcile(reservation, trace)
            self._event(
                job_id=job_id,
                paper_id=paper_id,
                state="QUOTA_RECONCILED",
                provider_trace_id=trace.trace_id if trace else None,
                metadata={"provider": provider_name, "role": role},
                attempt=resolved_attempt,
                suffix=suffix,
                update_job_state=False,
            )

    def _event(
        self,
        *,
        job_id: str,
        paper_id: str,
        state: str,
        reason: str | None = None,
        provider_trace_id: str | None = None,
        metadata: dict[str, Any] | None = None,
        attempt: int,
        suffix: str,
        update_job_state: bool = True,
    ) -> FoundryEvent:
        event = self.store.append_event(
            job_id=job_id,
            paper_id=paper_id,
            state=state,
            reason=reason,
            provider_trace_id=provider_trace_id,
            metadata=metadata,
            attempt=attempt,
            idempotency_suffix=f"{suffix}:{state}",
            update_job_state=update_job_state,
        )
        if self.event_sink is not None:
            self.event_sink(event)
        return event


__all__ = ["ProviderControlPlane", "ProviderDiscoveryError"]
