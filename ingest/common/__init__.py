"""Stream2Pretrain ingest shared utilities.

Every poller imports its Kafka producer, HTTP client, structlog setup, OTel tracer,
content-hash helper, and robots.txt cache from this package. Keeping the shared
bits in one place ensures all components emit identical span attributes, identical
``BronzeRecord`` shapes, and the same retry / timeout policy.
"""

from __future__ import annotations

from ingest.common.config import IngestConfig, load_config
from ingest.common.hashing import canonical_url, content_sha256, doc_id_for_url
from ingest.common.http_client import build_async_client, build_headers
from ingest.common.kafka_producer import BronzeProducer
from ingest.common.logging import configure_logging, get_logger
from ingest.common.minio_writer import MinioWriter
from ingest.common.otel import init_tracer
from ingest.common.robots import RobotsCache
from ingest.common.s3 import bronze_s3_uri

__all__ = [
    "BronzeProducer",
    "IngestConfig",
    "MinioWriter",
    "RobotsCache",
    "bronze_s3_uri",
    "build_async_client",
    "build_headers",
    "canonical_url",
    "configure_logging",
    "content_sha256",
    "doc_id_for_url",
    "get_logger",
    "init_tracer",
    "load_config",
]
