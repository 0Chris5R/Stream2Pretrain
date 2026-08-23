"""Stream2Pretrain ingest shared utilities.

Every poller imports its Kafka producer, HTTP client, structlog setup, OTel tracer,
content-hash helper, and robots.txt cache from this package. Keeping the shared
bits in one place ensures all components emit identical span attributes, identical
``BronzeRecord`` shapes, and the same retry / timeout policy.
"""

from __future__ import annotations

from importlib import import_module
from typing import Any

# Keep this package facade lazy. A narrow consumer such as the processor
# fetcher imports ``license_admission`` and should not load the optional
# aiobotocore/Kubernetes poller stack merely because Python initializes this
# parent package first.
_EXPORTS = {
    "BronzeProducer": ("ingest.common.kafka_producer", "BronzeProducer"),
    "IngestConfig": ("ingest.common.config", "IngestConfig"),
    "MinioWriter": ("ingest.common.minio_writer", "MinioWriter"),
    "RobotsCache": ("ingest.common.robots", "RobotsCache"),
    "bronze_s3_uri": ("ingest.common.s3", "bronze_s3_uri"),
    "build_async_client": ("ingest.common.http_client", "build_async_client"),
    "build_headers": ("ingest.common.http_client", "build_headers"),
    "canonical_url": ("ingest.common.hashing", "canonical_url"),
    "configure_logging": ("ingest.common.logging", "configure_logging"),
    "content_sha256": ("ingest.common.hashing", "content_sha256"),
    "doc_id_for_url": ("ingest.common.hashing", "doc_id_for_url"),
    "get_logger": ("ingest.common.logging", "get_logger"),
    "init_tracer": ("ingest.common.otel", "init_tracer"),
    "load_config": ("ingest.common.config", "load_config"),
}


def __getattr__(name: str) -> Any:
    """Resolve the established facade exports only when a caller needs one."""
    try:
        module_name, attribute = _EXPORTS[name]
    except KeyError as exc:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}") from exc
    value = getattr(import_module(module_name), attribute)
    globals()[name] = value
    return value

__all__ = [
    *_EXPORTS,
]
