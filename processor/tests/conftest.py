"""Shared pytest fixtures for the processor test suite."""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from pathlib import Path

import pytest

from processor.common import ProcessorConfig
from schemas.bronze import BronzeRecord
from schemas.silver import SilverRecord, SilverTags


@pytest.fixture
def cfg(tmp_path: Path) -> ProcessorConfig:
    """A processor config that points everything at temp directories."""
    return ProcessorConfig(
        env="dev",
        log_level="DEBUG",
        redpanda_brokers="localhost:9092",
        consumer_group="s2p-tests",
        raw_topic="raw.fetched",
        normalized_topic="docs.normalized",
        curated_topic="docs.curated",
        decon_attest_topic="decon.attest",
        minio_endpoint="http://localhost:9000",
        minio_access_key="minioadmin",
        minio_secret_key="minioadmin",
        bronze_bucket="bronze",
        silver_bucket="silver",
        gold_bucket="gold",
        decon_bucket="decon",
        polaris_uri="http://polaris:8181/api/catalog",
        polaris_warehouse="stream2pretrain",
        polaris_token=None,
        otel_endpoint=None,
        otel_protocol="grpc",
        user_agent="s2p-tests/0.1",
        http_timeout_seconds=5.0,
        http_max_retries=0,
        state_dir=str(tmp_path / "state"),
        models_dir=str(tmp_path / "models"),
        benchmark_set_version="v2026-test-01",
        benchmark_corpus_path=None,
        proxy_lm_window_minutes=10,
        promotion_threshold=0.05,
        promotion_required_windows=2,
    )


@pytest.fixture
def fixed_now() -> datetime:
    """A deterministic UTC instant for fixtures."""
    return datetime(2026, 6, 15, 12, 0, 0, tzinfo=UTC)


def _doc_id(url: str) -> str:
    return "sha256:" + hashlib.sha256(url.encode("utf-8")).hexdigest()


@pytest.fixture
def bronze_record(fixed_now: datetime) -> BronzeRecord:
    return BronzeRecord(
        doc_id=_doc_id("https://example.com/a"),
        url="https://example.com/a",
        fetched_at=fixed_now,
        http_status=200,
        http_last_modified=fixed_now,
        content_type="text/html",
        raw_html_s3_uri="s3://bronze/2026/06/15/source=test/a.html.gz",
        source_feed="rss-test",
        trace_id="0123456789abcdef0123456789abcdef",
    )


@pytest.fixture
def silver_record(fixed_now: datetime) -> SilverRecord:
    text = (
        "The streaming language modelling pipeline curates documents. "
        "It writes deterministic shards to an Iceberg lakehouse and signs "
        "a contamination attestation for every snapshot. The pipeline "
        "supports per-document validity intervals so train-time replays are "
        "fully reproducible. "
    ) * 8
    return SilverRecord(
        doc_id=_doc_id("https://example.com/silver"),
        url="https://example.com/silver",
        title="A Test Article",
        text=text,
        lang="en",
        lang_score=0.92,
        extracted_with="resiliparse-0.14",
        tags=SilverTags(
            gopher_pass=True,
            c4_nopunc_pass=True,
            perplexity=120.0,
            perplexity_bucket="head",
        ),
        minhash_sig=bytes(112 * 4),
        near_dup_cluster_id=None,
        valid_from=fixed_now,
        valid_to=None,
        valid_from_source="http_last_modified",
        trace_id="0123456789abcdef0123456789abcdef",
    )


@pytest.fixture
def long_english_text() -> str:
    """A long-enough block of natural English to satisfy Gopher and C4."""
    paragraph = (
        "The Stream2Pretrain pipeline curates documents into training shards. "
        "It uses Bytewax for streaming, Resiliparse for extraction, and KenLM "
        "for perplexity scoring. The architecture is deliberately modular so "
        "operators can be replaced without touching the dataflow. The decon "
        "gate emits a signed attestation per snapshot so contamination can be "
        "audited later by replay. "
    )
    return paragraph * 6


def _utf8(s: str) -> bytes:
    return s.encode("utf-8")
