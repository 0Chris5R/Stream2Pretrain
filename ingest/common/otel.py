"""OpenTelemetry tracer initialization for ingest pods.

Every poller spans look like:

    poller.<feed_name>
      |- http.request          (one per fetched URL)
      |- s3.put                (one per bronze object stored)
      `- kafka.produce         (one per BronzeRecord emitted)

The tracer reads the OTLP endpoint from ``IngestConfig.otel_endpoint``. If unset
(common in unit tests), we install the no-op tracer so callers can use the same
``with tracer.start_as_current_span(...)`` pattern unconditionally.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from opentelemetry import trace
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

if TYPE_CHECKING:
    from ingest.common.config import IngestConfig


_INITIALIZED = False


def init_tracer(service_name: str, cfg: IngestConfig) -> trace.Tracer:
    """Initialize a global tracer provider once and return a tracer.

    ``service_name`` becomes the ``service.name`` resource attribute and is what
    Tempo / Grafana group spans by.
    """
    global _INITIALIZED
    if _INITIALIZED:
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
    _INITIALIZED = True
    return trace.get_tracer(service_name)


def current_trace_id_hex() -> str | None:
    """Return the active 32-char trace_id hex, or None if outside a span."""
    span = trace.get_current_span()
    ctx = span.get_span_context()
    if ctx and ctx.is_valid:
        return format(ctx.trace_id, "032x")
    return None
