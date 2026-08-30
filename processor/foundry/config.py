"""Environment-backed foundry configuration with safe production defaults."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

from processor.foundry.util import sha256

DENIED_CREDENTIAL_NAMES = frozenset({"ZAI_API_KEY", "GLM_API_KEY"})
APPROVED_MODEL_LICENSES: dict[str, tuple[str, str]] = {
    "Qwen3.8-27B": (
        "Apache-2.0",
        "https://docs.hetzner.com/general/company-and-policy/experiments/inference/",
    ),
}

_REPOSITORY_ROOT = Path(__file__).resolve().parents[2]


@dataclass(frozen=True, slots=True)
class ProviderConfig:
    name: str
    base_url: str
    api_key_env: str
    credential_label: str
    preferred_models: tuple[str, ...]
    allowed_model_families: tuple[str, ...]
    terms_url: str
    terms_audit_date: str
    terms_snapshot_path: str | None = None
    dynamic_route: bool = False
    minute_requests: int | None = None
    daily_requests: int | None = None
    minute_input_tokens: int | None = None
    minute_output_tokens: int | None = None
    daily_input_tokens: int | None = None
    daily_output_tokens: int | None = None

    @property
    def api_key(self) -> str:
        return os.environ.get(self.api_key_env, "").strip()

    @property
    def terms_snapshot_hash(self) -> str:
        if self.terms_snapshot_path:
            path = Path(self.terms_snapshot_path)
            if not path.is_file():
                raise RuntimeError(f"provider terms snapshot is missing: {path}")
            return sha256(path.read_bytes())
        return sha256(
            {
                "provider": self.name,
                "terms_url": self.terms_url,
                "audit_date": self.terms_audit_date,
            }
        )

    def model_license(self, model_id: str) -> tuple[str, str] | None:
        return APPROVED_MODEL_LICENSES.get(model_id)


@dataclass(frozen=True, slots=True)
class FoundryConfig:
    providers: dict[str, ProviderConfig] = field(default_factory=dict)
    daily_run_hour_utc: int = 0
    daily_run_minute_utc: int = 0
    daily_not_before_utc: datetime | None = None
    tasks_per_paper: int = 6
    accepted_tasks_per_paper: int = 3
    queue_poll_seconds: int = 60
    state_dir: str = "/var/lib/s2p/foundry"
    minio_bucket: str = "posttrain"
    provider_mode: str = "live"
    replay_fixture: str | None = None
    max_retries: int = 2
    timeout_seconds: float = 180.0
    provider_context_window_tokens: int = 262_144
    policy_version: str = "posttrain-policy-v4"
    prompt_version: str = "paper-foundry-prompts-v4"

    @classmethod
    def from_env(cls) -> FoundryConfig:
        reject_denied_credentials()
        return cls(
            providers=provider_configs(),
            daily_run_hour_utc=_bounded_int("S2P_FOUNDRY_DAILY_RUN_HOUR_UTC", 0, 0, 23),
            daily_run_minute_utc=_bounded_int("S2P_FOUNDRY_DAILY_RUN_MINUTE_UTC", 0, 0, 59),
            daily_not_before_utc=_optional_utc_datetime("S2P_FOUNDRY_DAILY_NOT_BEFORE_UTC"),
            tasks_per_paper=_bounded_int("S2P_FOUNDRY_TASKS_PER_PAPER", 6, 1, 12),
            accepted_tasks_per_paper=_bounded_int("S2P_FOUNDRY_ACCEPTED_TASKS_PER_PAPER", 3, 1, 6),
            queue_poll_seconds=_bounded_int("S2P_FOUNDRY_QUEUE_POLL_SECONDS", 60, 5, 3600),
            state_dir=os.environ.get("S2P_FOUNDRY_STATE_DIR", "/var/lib/s2p/foundry"),
            minio_bucket=os.environ.get("MINIO_POSTTRAIN_BUCKET", "posttrain"),
            provider_mode=os.environ.get("S2P_FOUNDRY_PROVIDER_MODE", "live"),
            replay_fixture=os.environ.get("S2P_FOUNDRY_REPLAY_FIXTURE") or None,
            max_retries=_bounded_int("S2P_FOUNDRY_MAX_RETRIES", 2, 0, 5),
            timeout_seconds=_bounded_float("S2P_FOUNDRY_TIMEOUT_SECONDS", 180.0, 10.0, 600.0),
            provider_context_window_tokens=_bounded_int(
                "S2P_FOUNDRY_CONTEXT_WINDOW_TOKENS", 262_144, 1, 10_000_000
            ),
        )


def provider_configs() -> dict[str, ProviderConfig]:
    """The sole configured provider and only its verified public limits."""
    return {
        "hetzner": ProviderConfig(
            name="hetzner",
            base_url=os.environ.get(
                "S2P_HETZNER_BASE_URL", "https://inference.hetzner.com/api/v1"
            ).rstrip("/"),
            api_key_env="HETZNER_INFERENCE_API_KEY",
            credential_label="stream2train-hetzner",
            preferred_models=tuple(_csv(os.environ.get("S2P_HETZNER_MODELS", "Qwen3.8-27B"))),
            allowed_model_families=("qwen",),
            terms_url="https://docs.hetzner.com/general/company-and-policy/experiments/inference/",
            terms_audit_date="2026-08-19",
            terms_snapshot_path=os.environ.get(
                "S2P_HETZNER_TERMS_SNAPSHOT",
                str(_REPOSITORY_ROOT / "docs/provider-terms/hetzner-inference-2026-08-19.md"),
            ),
            minute_requests=10,
            minute_input_tokens=4_000_000,
            minute_output_tokens=100_000,
        ),
    }


def reject_denied_credentials(environ: dict[str, str] | None = None) -> None:
    values = os.environ if environ is None else environ
    loaded = sorted(name for name in DENIED_CREDENTIAL_NAMES if values.get(name, "").strip())
    if loaded:
        raise RuntimeError(f"denied personal credential present: {', '.join(loaded)}")


def _csv(value: str) -> list[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


def _positive_int(name: str, default: int) -> int:
    return _bounded_int(name, default, 1, 1_000_000)


def _bounded_int(name: str, default: int, minimum: int, maximum: int) -> int:
    value = int(os.environ.get(name, str(default)))
    if not minimum <= value <= maximum:
        raise ValueError(f"{name} must be between {minimum} and {maximum}")
    return value


def _bounded_float(name: str, default: float, minimum: float, maximum: float) -> float:
    value = float(os.environ.get(name, str(default)))
    if not minimum <= value <= maximum:
        raise ValueError(f"{name} must be between {minimum} and {maximum}")
    return value


def _optional_utc_datetime(name: str) -> datetime | None:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return None
    parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError(f"{name} must include a timezone")
    return parsed.astimezone(UTC)


__all__ = [
    "APPROVED_MODEL_LICENSES",
    "DENIED_CREDENTIAL_NAMES",
    "FoundryConfig",
    "ProviderConfig",
    "provider_configs",
    "reject_denied_credentials",
]
