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
import threading
from dataclasses import dataclass, field
from datetime import timedelta
from pathlib import Path
from typing import Any

import boto3
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
    decisions_topic: str
    decon_attest_topic: str

    minio_endpoint: str
    minio_access_key: str
    minio_secret_key: str
    bronze_bucket: str
    silver_bucket: str
    gold_bucket: str
    decon_bucket: str

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
    license_admissions_topic: str = "license.admissions"

    @property
    def is_dev(self) -> bool:
        return self.env == "dev"


class DeterministicProcessingError(ValueError):
    """A record-local error that will recur unchanged when replayed."""


@dataclass(slots=True)
class BytewaxRuntimeStatus:
    """Readiness shared between a Bytewax source and the HTTP probe thread.

    A process is ready only after the runtime has started and every registered
    Kafka source has successfully built at least one assigned partition.  This
    avoids advertising readiness merely because the model objects and probe
    socket were created.
    """

    required_sources: set[str] = field(default_factory=set)
    assigned_sources: set[str] = field(default_factory=set)
    runtime_started: bool = False
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def register_source(self, source_name: str) -> None:
        with self._lock:
            self.required_sources.add(source_name)

    def mark_source_assigned(self, source_name: str) -> None:
        with self._lock:
            self.assigned_sources.add(source_name)

    def mark_runtime_started(self) -> None:
        with self._lock:
            self.runtime_started = True

    def mark_runtime_stopped(self) -> None:
        with self._lock:
            self.runtime_started = False

    def is_ready(self) -> bool:
        with self._lock:
            return (
                self.runtime_started
                and bool(self.required_sources)
                and self.required_sources.issubset(self.assigned_sources)
            )


@dataclass(frozen=True, slots=True)
class DurableProcessingFailureWriter:
    """Write idempotent processing quarantines without adding a Kafka topic.

    The object key is derived from the Kafka coordinate and payload digest.
    Replaying the same poison record therefore overwrites identical JSON rather
    than creating unbounded audit objects.  A failed object-store write raises,
    so Bytewax cannot snapshot beyond a failure that was not durably recorded.
    """

    s3: Any
    bucket: str
    prefix: str = "processing-failures"
    error_revision: str = "processing-failure-v1"

    @classmethod
    def from_config(cls, cfg: ProcessorConfig) -> DurableProcessingFailureWriter:
        return cls(
            s3=boto3.client(
                "s3",
                endpoint_url=cfg.minio_endpoint,
                aws_access_key_id=cfg.minio_access_key,
                aws_secret_access_key=cfg.minio_secret_key,
                region_name="us-east-1",
            ),
            bucket=os.environ.get(
                "S2P_PROCESSING_FAILURE_BUCKET",
                os.environ.get("S2P_STATE_BUCKET", cfg.gold_bucket),
            ),
            prefix=os.environ.get("S2P_PROCESSING_FAILURE_PREFIX", "processing-failures"),
        )

    def record(self, *, stage: str, message: object, reason: str) -> str:
        import hashlib
        import re

        payload = getattr(message, "value", None)
        payload_bytes = bytes(payload) if payload is not None else b""
        payload_sha256 = hashlib.sha256(payload_bytes).hexdigest()
        topic = str(getattr(message, "topic", None) or "unknown")
        partition = int(getattr(message, "partition", -1) or 0)
        offset = int(getattr(message, "offset", -1) or 0)
        safe_stage = re.sub(r"[^a-z0-9_.-]+", "-", stage.lower()).strip("-") or "unknown"
        safe_topic = re.sub(r"[^A-Za-z0-9_.-]+", "-", topic).strip("-") or "unknown"
        safe_prefix = "/".join(
            re.sub(r"[^A-Za-z0-9_.-]+", "-", part).strip("-")
            for part in self.prefix.strip("/").split("/")
            if part.strip("/")
        )
        if not safe_prefix:
            raise RuntimeError("S2P_PROCESSING_FAILURE_PREFIX must contain a path segment")
        doc_id: str | None = None
        trace_id: str | None = None
        if payload_bytes:
            try:
                decoded = orjson.loads(payload_bytes)
            except orjson.JSONDecodeError:
                decoded = None
            if isinstance(decoded, dict):
                candidate_doc_id = decoded.get("doc_id")
                candidate_trace_id = decoded.get("trace_id")
                doc_id = candidate_doc_id if isinstance(candidate_doc_id, str) else None
                trace_id = candidate_trace_id if isinstance(candidate_trace_id, str) else None
        message_key = getattr(message, "key", None)
        if doc_id is None and message_key:
            try:
                doc_id = bytes(message_key).decode("utf-8")
            except (TypeError, UnicodeDecodeError):
                doc_id = None
        headers = getattr(message, "headers", None) or []
        if trace_id is None:
            for header_name, header_value in headers:
                if header_name != "trace_id" or not header_value:
                    continue
                try:
                    trace_id = bytes(header_value).decode("ascii")
                except (TypeError, UnicodeDecodeError):
                    trace_id = None
                break
        coordinate = f"{topic}:{partition}:{offset}:{payload_sha256}"
        if doc_id is None:
            doc_id = f"unresolved:{payload_sha256}"
        if trace_id is None:
            trace_id = hashlib.sha256(coordinate.encode("utf-8")).hexdigest()[:32]
        body = orjson.dumps(
            {
                "schema_version": 1,
                "stage": stage,
                "reason": reason,
                "retry_classification": "deterministic",
                "error_revision": self.error_revision,
                "topic": topic,
                "partition": partition,
                "offset": offset,
                "payload_sha256": payload_sha256,
                "doc_id": doc_id,
                "trace_id": trace_id,
            },
            option=orjson.OPT_SORT_KEYS,
        )
        key = (
            f"{safe_prefix}/stage={safe_stage}/topic={safe_topic}/"
            f"partition={partition}/offset={offset}-{payload_sha256[:16]}.json"
        )
        self.s3.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=body,
            ContentType="application/json",
        )
        return f"s3://{self.bucket}/{key}"


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
        decisions_topic=_env("S2P_DECISIONS_TOPIC", "curation.decisions"),
        decon_attest_topic=_env("S2P_DECON_TOPIC", "decon.attest"),
        minio_endpoint=_env("MINIO_ENDPOINT", "http://localhost:9000"),
        minio_access_key=_env("MINIO_ACCESS_KEY", "minioadmin"),
        minio_secret_key=_env("MINIO_SECRET_KEY", "minioadmin"),
        bronze_bucket=_env("MINIO_BRONZE_BUCKET", "bronze"),
        silver_bucket=_env("MINIO_SILVER_BUCKET", "silver"),
        gold_bucket=_env("MINIO_GOLD_BUCKET", "gold"),
        decon_bucket=_env("MINIO_DECON_BUCKET", "decon"),
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
        license_admissions_topic=_env("S2P_LICENSE_ADMISSIONS_TOPIC", "license.admissions"),
    )


def kafka_starting_offset() -> int:
    """Return Bytewax Kafka offset constant from ``S2P_KAFKA_START_OFFSET``."""
    raw = os.environ.get("S2P_KAFKA_START_OFFSET", "beginning").strip().lower()
    offsets = {
        "beginning": -2,
        "earliest": -2,
        "start": -2,
        "end": -1,
        "latest": -1,
        # confluent_kafka.OFFSET_STORED. This is the one-time bridge from
        # existing broker commits into a new Bytewax recovery database. Once
        # the first snapshot exists, recovery progress is authoritative.
        "stored": -1000,
    }
    if raw in offsets:
        return offsets[raw]
    try:
        return int(raw)
    except ValueError as exc:
        raise RuntimeError(
            "S2P_KAFKA_START_OFFSET must be beginning/earliest/end/latest/stored or an int"
        ) from exc


def kafka_consumer_config(group_id: str) -> dict[str, str]:
    """Config passed to Bytewax KafkaSource for Redpanda consumer groups."""
    return {
        "group.id": group_id,
        "auto.offset.reset": "earliest",
        "fetch.message.max.bytes": str(_env_int("S2P_KAFKA_MESSAGE_MAX_BYTES", 1_048_576)),
    }


def kafka_producer_config() -> dict[str, str]:
    """Config passed to Bytewax KafkaSink for full-document payloads."""
    return {"message.max.bytes": str(_env_int("S2P_KAFKA_MESSAGE_MAX_BYTES", 1_048_576))}


def kafka_payload_max_bytes() -> int:
    """Largest payload that is safe below the configured Kafka record limit."""
    message_max = _env_int("S2P_KAFKA_MESSAGE_MAX_BYTES", 1_048_576)
    configured = _env_int("S2P_KAFKA_PAYLOAD_MAX_BYTES", message_max - 65_536)
    if message_max <= 65_536:
        raise RuntimeError("S2P_KAFKA_MESSAGE_MAX_BYTES must be greater than 65536")
    if configured <= 0 or configured >= message_max:
        raise RuntimeError(
            "S2P_KAFKA_PAYLOAD_MAX_BYTES must be positive and smaller than "
            "S2P_KAFKA_MESSAGE_MAX_BYTES"
        )
    return configured


def tracked_kafka_source(
    *,
    runtime_status: BytewaxRuntimeStatus | None,
    source_name: str,
    brokers: list[str],
    topics: list[str],
    starting_offset: int,
    add_config: dict[str, str],
) -> object:
    """Build a KafkaSource that reports real partition assignment readiness."""
    from bytewax.connectors.kafka import KafkaSource

    if runtime_status is None:
        return KafkaSource(
            brokers=brokers,
            topics=topics,
            starting_offset=starting_offset,
            add_config=add_config,
        )
    runtime_status.register_source(source_name)

    class _TrackedKafkaSource(KafkaSource):
        def build_part(self, step_id: str, for_part: str, resume_state: int | None) -> object:
            partition = super().build_part(step_id, for_part, resume_state)
            runtime_status.mark_source_assigned(source_name)
            return partition

    return _TrackedKafkaSource(
        brokers=brokers,
        topics=topics,
        starting_offset=starting_offset,
        add_config=add_config,
    )


def run_bytewax_flow(
    flow: object,
    cfg: ProcessorConfig,
    recovery_name: str,
    *,
    runtime_status: BytewaxRuntimeStatus | None = None,
) -> None:
    """Run one flow with durable source-offset recovery enabled.

    Bytewax's Kafka connector deliberately disables broker-side offset commits
    and stores source offsets in its recovery database. Without this explicit
    recovery configuration, every process restart begins at
    ``S2P_KAFKA_START_OFFSET`` and re-emits the retained topic.
    """
    from bytewax.recovery import RecoveryConfig, init_db_dir
    from bytewax.run import cli_main

    recovery_dir = Path(cfg.state_dir) / "bytewax" / recovery_name
    recovery_dir.mkdir(parents=True, exist_ok=True)
    partitions = _env_int("S2P_BYTEWAX_RECOVERY_PARTITIONS", 1)
    if partitions < 1:
        raise RuntimeError("S2P_BYTEWAX_RECOVERY_PARTITIONS must be positive")
    existing_databases = sorted(recovery_dir.glob("part-*.sqlite3"))
    if not existing_databases:
        init_db_dir(recovery_dir, partitions)
        existing_databases = sorted(recovery_dir.glob("part-*.sqlite3"))
    expected_databases = {
        recovery_dir / f"part-{partition}.sqlite3" for partition in range(partitions)
    }
    if set(existing_databases) != expected_databases:
        existing_names = [path.name for path in existing_databases]
        raise RuntimeError(
            f"Bytewax recovery partition mismatch for {recovery_name}: "
            f"configured={partitions} existing={existing_names}"
        )
    interval = timedelta(seconds=_env_float("S2P_BYTEWAX_SNAPSHOT_SECONDS", 1.0))
    if interval.total_seconds() <= 0:
        raise RuntimeError("S2P_BYTEWAX_SNAPSHOT_SECONDS must be positive")
    if runtime_status is not None:
        runtime_status.mark_runtime_started()
    try:
        cli_main(
            flow,
            epoch_interval=interval,
            recovery_config=RecoveryConfig(recovery_dir),
        )
    finally:
        if runtime_status is not None:
            runtime_status.mark_runtime_stopped()


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
    global _TRACER_INITIALIZED
    if _TRACER_INITIALIZED:
        return trace.get_tracer(service_name)
    resource = Resource.create({SERVICE_NAME: service_name, "service.namespace": "stream2pretrain"})
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
    "BytewaxRuntimeStatus",
    "DeterministicProcessingError",
    "DurableProcessingFailureWriter",
    "ProcessorConfig",
    "bronze_loads",
    "bronze_loads_dict",
    "configure_logging",
    "current_trace_id_hex",
    "decon_dumps",
    "decon_loads",
    "get_logger",
    "gold_dumps",
    "gold_loads",
    "init_tracer",
    "kafka_payload_max_bytes",
    "kafka_starting_offset",
    "load_config",
    "new_trace_id",
    "run_bytewax_flow",
    "silver_dumps",
    "silver_loads",
    "tracked_kafka_source",
]
