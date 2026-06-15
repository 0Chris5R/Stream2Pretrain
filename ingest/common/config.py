"""Runtime configuration loaded from environment variables.

Components never read os.environ directly. They take an ``IngestConfig`` either
constructed in their entrypoint via ``load_config()`` or injected by tests.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field


def _env(name: str, default: str | None = None) -> str:
    val = os.environ.get(name, default)
    if val is None:
        raise RuntimeError(f"required environment variable {name} is not set")
    return val


def _env_optional(name: str, default: str | None = None) -> str | None:
    return os.environ.get(name, default)


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise RuntimeError(f"env {name} must be an int, got {raw!r}") from exc


@dataclass(frozen=True, slots=True)
class IngestConfig:
    """Strongly-typed view of the env-vars an ingest pod cares about."""

    env: str
    log_level: str
    redpanda_brokers: str
    minio_endpoint: str
    minio_access_key: str
    minio_secret_key: str
    minio_bronze_bucket: str
    otel_endpoint: str | None
    otel_protocol: str
    github_token: str | None
    hf_token: str | None
    user_agent: str
    http_timeout_seconds: float = 30.0
    http_max_retries: int = 4
    feed_config_path: str | None = None
    request_jitter_max_seconds: float = 0.5
    raw_topic: str = "raw.fetched"

    @property
    def is_dev(self) -> bool:
        return self.env == "dev"


def load_config() -> IngestConfig:
    """Load the ingest configuration from the process environment.

    Defaults follow ``.env.example``. ``GITHUB_TOKEN`` and ``HF_TOKEN`` are
    optional; the GitHub events poller will refuse to run without one.
    """
    return IngestConfig(
        env=_env("S2P_ENV", "dev"),
        log_level=_env("LOG_LEVEL", "INFO").upper(),
        redpanda_brokers=_env("REDPANDA_BROKERS", "localhost:9092"),
        minio_endpoint=_env("MINIO_ENDPOINT", "http://localhost:9000"),
        minio_access_key=_env("MINIO_ACCESS_KEY", "minioadmin"),
        minio_secret_key=_env("MINIO_SECRET_KEY", "minioadmin"),
        minio_bronze_bucket=_env("MINIO_BRONZE_BUCKET", "bronze"),
        otel_endpoint=_env_optional("OTEL_EXPORTER_OTLP_ENDPOINT"),
        otel_protocol=_env("OTEL_EXPORTER_OTLP_PROTOCOL", "grpc"),
        github_token=_env_optional("GITHUB_TOKEN"),
        hf_token=_env_optional("HF_TOKEN"),
        user_agent=_env(
            "S2P_USER_AGENT",
            "Stream2Pretrain/0.1 (+https://github.com/stream2pretrain/stream2pretrain)",
        ),
        http_timeout_seconds=float(_env("S2P_HTTP_TIMEOUT", "30")),
        http_max_retries=_env_int("S2P_HTTP_MAX_RETRIES", 4),
        feed_config_path=_env_optional("S2P_FEED_CONFIG"),
        raw_topic=_env("S2P_RAW_TOPIC", "raw.fetched"),
    )
