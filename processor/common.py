"""Shared runtime helpers for every processor dataflow.

Centralises configuration, structured logging, OTel tracer init, and the
Kafka serde used by ``processor.fetcher``, ``processor.curate``, and
``processor.iceberg_writer``. Keeping this in one module means topic
constants, span attribute keys, and serialization rules cannot drift between
dataflows.

Public API
----------
- :class:`ProcessorConfig`       - typed view of the processor env-vars
- :func:`load_config`            - construct a config from os.environ
- :func:`configure_logging`      - structlog JSON renderer with OTel context
- :func:`get_logger`             - bound logger
- :func:`init_tracer`            - install the OTel tracer provider
- :func:`current_trace_id_hex`   - read the active 32-char trace id
- :func:`bronze_loads`           - decode wire bytes into a dict
- :func:`silver_dumps` / :func:`silver_loads` - SilverRecord serde
- :func:`gold_dumps`   / :func:`gold_loads`   - GoldRecord serde
- :func:`decon_dumps`  / :func:`decon_loads`  - DeconAttestation serde
- :func:`new_trace_id` - W3C 32-char hex when there is no upstream trace
"""

from __future__ import annotations

import logging
import os
import secrets
import sys
from dataclasses import dataclass
from typing import Any

import orjson
import structlog
from opentelemetry import trace
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from structlog.types import EventDict, WrappedLogger

from schemas.bronze import BronzeRecord
from schemas.decon import DeconAttestation
from schemas.gold import GoldRecord
from schemas.silver import SilverRecord


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------


def _env(name: str, default: str | None = None) -> str:
    """Return env var ``name`` or raise if missing and no default given."""
    val = os.environ.get(name, default)
    if val is None:
        raise RuntimeError(f"required environment variable {name} is not set")
    return val


def _env_optional(name: str, default: str | None = None) -> str | None:
    """Return env var ``name`` or ``default`` (which may be None)."""
    return os.environ.get(name, default)


def _env_int(name: str, default: int) -> int:
    """Parse integer env var; fall back to ``default`` if unset/empty."""
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise RuntimeError(f"env {name} must be an int, got {raw!r}") from exc


def _env_float(name: str, default: float) -> float:
    """Parse float env var; fall back to ``default`` if unset/empty."""
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    try:
        return float(raw)
    except ValueError as exc:
        raise RuntimeError(f"env {name} must be a float, got {raw!r}") from exc


@dataclass(frozen=True, slots=True)
class ProcessorConfig:
    """Strongly-typed view of the env-vars a processor pod cares about."""

    env: str
    log_level: str
    redpanda_brokers: str
    consumer_group: str
    raw_topic: str
    normalized_topic: str
    curated_topic: str
    decon_attest_topic: str

    minio_endpoint: str
    minio_access_key: str
    minio_secret_key: str
    bronze_bucket: str
    silver_bucket: str
    gold_bucket: str

    polaris_uri: str
    polaris_warehouse: str
    polaris_token: str | None

    otel_endpoint: str | None
    otel_protocol: str

    user_agent: str
    http_timeout_seconds: float
    http_max_retries: int

    state_dir: str
    models_dir: str

    # Decon-Gate
    benchmark_set_version: str
    benchmark_corpus_path: str | None

    # Mixture Controller
    proxy_lm_window_minutes: int
    promotion_threshold: float
    promotion_required_windows: int

    @property
    def is_dev(self) -> bool:
        return self.env == "dev"


def load_config() -> ProcessorConfig:
    """Build a :class:`ProcessorConfig` from the process environment."""
    return ProcessorConfig(
        env=_env("S2P_ENV", "dev"),
        log_level=_env("LOG_LEVEL", "INFO").upper(),
        redpanda_brokers=_env("REDPANDA_BROKERS", "localhost:9092"),
        consumer_group=_env("S2P_CONSUMER_GROUP", "s2p-processor"),
        raw_topic=_env("S2P_RAW_TOPIC", "raw.fetched"),
        normalized_topic=_env("S2P_NORMALIZED_TOPIC", "docs.normalized"),
        curated_topic=_env("S2P_CURATED_TOPIC", "docs.curated"),
        decon_attest_topic=_env("S2P_DECON_TOPIC", "decon.attest"),
        minio_endpoint=_env("MINIO_ENDPOINT", "http://localhost:9000"),
        minio_access_key=_env("MINIO_ACCESS_KEY", "minioadmin"),
        minio_secret_key=_env("MINIO_SECRET_KEY", "minioadmin"),
        bronze_bucket=_env("MINIO_BRONZE_BUCKET", "bronze"),
        silver_bucket=_env("MINIO_SILVER_BUCKET", "silver"),
        gold_bucket=_env("MINIO_GOLD_BUCKET", "gold"),
        polaris_uri=_env("POLARIS_URI", "http://polaris:8181/api/catalog"),
        polaris_warehouse=_env("POLARIS_WAREHOUSE", "stream2pretrain"),
        polaris_token=_env_optional("POLARIS_TOKEN"),
        otel_endpoint=_env_optional("OTEL_EXPORTER_OTLP_ENDPOINT"),
        otel_protocol=_env("OTEL_EXPORTER_OTLP_PROTOCOL", "grpc"),
        user_agent=_env(
            "S2P_USER_AGENT",
            "Stream2Pretrain/0.1 (+https://github.com/stream2pretrain/stream2pretrain)",
        ),
        http_timeout_seconds=_env_float("S2P_HTTP_TIMEOUT", 30.0),
        http_max_retries=_env_int("S2P_HTTP_MAX_RETRIES", 4),
        state_dir=_env("S2P_STATE_DIR", "/var/lib/s2p"),
        models_dir=_env("S2P_MODELS_DIR", "/opt/models"),
        benchmark_set_version=_env("S2P_BENCH_SET_VERSION", "v2026-06-01"),
        benchmark_corpus_path=_env_optional("S2P_BENCH_CORPUS_PATH"),
        proxy_lm_window_minutes=_env_int("S2P_PROXY_LM_WINDOW_MIN", 10),
        promotion_threshold=_env_float("S2P_PROMOTION_THRESHOLD", 0.05),
        promotion_required_windows=_env_int("S2P_PROMOTION_REQUIRED_WINDOWS", 3),
    )


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------


def _otel_context_processor(_: WrappedLogger, __: str, event_dict: EventDict) -> EventDict:
    """Inject the active OTel span ids into every log record."""
    try:
        span = trace.get_current_span()
        ctx = span.get_span_context()
        if ctx and ctx.is_valid:
            event_dict.setdefault("trace_id", format(ctx.trace_id, "032x"))
            event_dict.setdefault("span_id", format(ctx.span_id, "016x"))
    except Exception:
        # Logging must never crash the pipeline.
        return event_dict
    return event_dict


def configure_logging(level: str = "INFO", *, json_output: bool = True) -> None:
    """Configure structlog and stdlib logging in one go."""
    log_level = getattr(logging, level.upper(), logging.INFO)
    timestamper = structlog.processors.TimeStamper(fmt="iso", utc=True)
    shared: list[structlog.types.Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        timestamper,
        _otel_context_processor,
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
    ]
    renderer: structlog.types.Processor = (
        structlog.processors.JSONRenderer()
        if json_output
        else structlog.dev.ConsoleRenderer(colors=False)
    )
    structlog.configure(
        processors=[*shared, renderer],
        wrapper_class=structlog.make_filtering_bound_logger(log_level),
        logger_factory=structlog.PrintLoggerFactory(file=sys.stdout),
        cache_logger_on_first_use=True,
    )
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter("%(message)s"))
    root = logging.getLogger()
    root.handlers = [handler]
    root.setLevel(log_level)
    for noisy in ("httpx", "httpcore", "urllib3", "asyncio", "kafka"):
        logging.getLogger(noisy).setLevel(max(log_level, logging.WARNING))


def get_logger(name: str) -> structlog.stdlib.BoundLogger:
    """Return a bound structlog logger for ``name``."""
    return structlog.get_logger(name)


# ---------------------------------------------------------------------------
# Tracing
# ---------------------------------------------------------------------------


_TRACER_INITIALIZED = False


def init_tracer(service_name: str, cfg: ProcessorConfig) -> trace.Tracer:
    """Idempotent tracer provider installer; returns a tracer for ``service_name``."""
    global _TRACER_INITIALIZED  # noqa: PLW0603
    if _TRACER_INITIALIZED:
        return trace.get_tracer(service_name)
    resource = Resource.create(
        {SERVICE_NAME: service_name, "service.namespace": "stream2pretrain"}
    )
    provider = TracerProvider(resource=resource)
    if cfg.otel_endpoint:
        if cfg.otel_protocol.lower() == "http/protobuf":
            from opentelemetry.exporter.otlp.proto.http.trace_exporter import (
                OTLPSpanExporter as HttpExporter,
            )

            exporter = HttpExporter(endpoint=cfg.otel_endpoint)
        else:
            from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import (
                OTLPSpanExporter as GrpcExporter,
            )

            exporter = GrpcExporter(endpoint=cfg.otel_endpoint, insecure=True)
        provider.add_span_processor(BatchSpanProcessor(exporter))
    trace.set_tracer_provider(provider)
    _TRACER_INITIALIZED = True
    return trace.get_tracer(service_name)


def current_trace_id_hex() -> str | None:
    """Active W3C trace_id as 32-char lowercase hex, or None outside a span."""
    span = trace.get_current_span()
    ctx = span.get_span_context()
    if ctx and ctx.is_valid:
        return format(ctx.trace_id, "032x")
    return None


def new_trace_id() -> str:
    """Generate a fresh 32-char hex trace id (used when no upstream context)."""
    return secrets.token_hex(16)


# ---------------------------------------------------------------------------
# Serde - keeps Pydantic JSON rules (HttpUrl, datetime ISO) consistent
# everywhere. Reads use orjson for speed; writes go through Pydantic so HttpUrl
# and datetime are formatted identically across producers.
# ---------------------------------------------------------------------------


def bronze_loads(payload: bytes) -> BronzeRecord:
    """Parse a wire payload into a :class:`BronzeRecord`."""
    return BronzeRecord.model_validate_json(payload)


def bronze_loads_dict(payload: bytes) -> dict[str, Any]:
    """Decode without validating - useful in fast paths and tests."""
    return orjson.loads(payload)


def silver_dumps(record: SilverRecord) -> bytes:
    """Serialize a SilverRecord to JSON bytes (binary minhash_sig is base64)."""
    return record.model_dump_json(by_alias=True).encode("utf-8")


def silver_loads(payload: bytes) -> SilverRecord:
    """Parse a SilverRecord from JSON bytes."""
    return SilverRecord.model_validate_json(payload)


def gold_dumps(record: GoldRecord) -> bytes:
    """Serialize a GoldRecord to JSON bytes."""
    return record.model_dump_json(by_alias=True).encode("utf-8")


def gold_loads(payload: bytes) -> GoldRecord:
    """Parse a GoldRecord from JSON bytes."""
    return GoldRecord.model_validate_json(payload)


def decon_dumps(record: DeconAttestation) -> bytes:
    """Serialize a DeconAttestation to canonical JSON bytes (sorted keys)."""
    obj = record.model_dump(mode="json")
    return orjson.dumps(obj, option=orjson.OPT_SORT_KEYS)


def decon_loads(payload: bytes) -> DeconAttestation:
    """Parse a DeconAttestation from JSON bytes."""
    return DeconAttestation.model_validate_json(payload)


__all__ = [
    "ProcessorConfig",
    "load_config",
    "configure_logging",
    "get_logger",
    "init_tracer",
    "current_trace_id_hex",
    "new_trace_id",
    "bronze_loads",
    "bronze_loads_dict",
    "silver_dumps",
    "silver_loads",
    "gold_dumps",
    "gold_loads",
    "decon_dumps",
    "decon_loads",
]
