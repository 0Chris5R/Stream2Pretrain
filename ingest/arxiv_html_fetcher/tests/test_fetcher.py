"""Tests for the arXiv-HTML fetcher's HTTP path.

We exercise three branches end-to-end:

1. ``arxiv.org/html/<id>`` returns 200 -> primary success.
2. ``arxiv.org/html/<id>`` returns 404 -> fall back to
   ``ar5iv.labs.arxiv.org/html/<id>`` and succeed there.
3. Both endpoints 404 -> emit a metadata stub with ``source_format=metadata``.

The test mocks the network with ``httpx.MockTransport`` and stubs the MinIO
writer + Kafka producer with in-memory shims so the full
:func:`run_for_ids` path stays under test without a Redpanda or MinIO
dependency.
"""

from __future__ import annotations

from typing import Any

import httpx
import pytest

from ingest.arxiv_html_fetcher.fetcher import (
    build_metadata_stub,
    canonical_arxiv_url,
    fetch_one,
    is_valid_arxiv_id,
    load_backfill_ids,
    make_bronze_record,
    run_for_ids,
)
from ingest.common.config import IngestConfig
from ingest.common.http_client import build_async_client
from ingest.common.rate_limit import TokenBucket
from schemas.bronze import BronzeRecord

ARXIV_HTML_SAMPLE = """<!DOCTYPE html>
<html><head>
<title>Sample Paper Title</title>
<meta name="citation_publication_date" content="2026/05/12">
<meta name="citation_license" content="https://creativecommons.org/licenses/by/4.0/">
</head><body>
<article>
<h1>Sample Paper Title</h1>
<h2>1. Introduction</h2>
<p>We propose a method.</p>
<math display="block"><annotation encoding="application/x-tex">E = mc^2</annotation></math>
<h2>2. Method</h2>
<p>Inline <math display="inline"><annotation encoding="application/x-tex">x</annotation></math> token.</p>
</article>
</body></html>
"""

AR5IV_HTML_SAMPLE = """<!DOCTYPE html>
<html><head>
<title>Older Paper</title>
<meta name="citation_publication_date" content="2022-04-01">
</head><body>
<div class="ltx_page_main">
<h1>Older Paper</h1>
<p>Body text.</p>
</div>
</body></html>
"""


def _cfg() -> IngestConfig:
    return IngestConfig(
        env="dev",
        log_level="DEBUG",
        redpanda_brokers="localhost:9092",
        minio_endpoint="http://localhost:9000",
        minio_access_key="x",
        minio_secret_key="x",
        minio_bronze_bucket="bronze",
        otel_endpoint=None,
        otel_protocol="grpc",
        github_token=None,
        hf_token=None,
        user_agent="s2p-test/0.2",
        http_timeout_seconds=2.0,
        http_max_retries=0,
    )


class _FakeMinio:
    def __init__(self) -> None:
        self.objects: list[dict[str, Any]] = []

    async def start(self) -> None:
        return None

    async def stop(self) -> None:
        return None

    async def put_bronze(
        self,
        *,
        key: str,
        payload: bytes,
        content_type: str = "text/html",
        gzip_compress: bool = True,
        metadata: dict[str, str] | None = None,
    ) -> int:
        # We do not actually gzip; the size we return must still be > 0
        # for the type contract.
        self.objects.append(
            {
                "key": key,
                "payload": payload,
                "content_type": content_type,
                "gzip": gzip_compress,
                "metadata": metadata or {},
            }
        )
        return len(payload)


class _FakeProducer:
    def __init__(self) -> None:
        self.records: list[BronzeRecord] = []

    async def start(self) -> None:
        return None

    async def stop(self) -> None:
        return None

    async def send(
        self, record: BronzeRecord, *, headers: dict[str, str] | None = None
    ) -> None:
        self.records.append(record)


def test_is_valid_arxiv_id_accepts_modern_and_legacy_forms() -> None:
    assert is_valid_arxiv_id("2401.12345")
    assert is_valid_arxiv_id("2401.12345v3")
    assert is_valid_arxiv_id("cs/0703123")
    assert is_valid_arxiv_id("hep-ph/0703123v2")
    assert not is_valid_arxiv_id("not-an-id")
    assert not is_valid_arxiv_id("")


def test_canonical_arxiv_url_picks_mirror() -> None:
    assert canonical_arxiv_url("2401.12345") == "https://arxiv.org/html/2401.12345"
    assert (
        canonical_arxiv_url("2401.12345", mirror="ar5iv")
        == "https://ar5iv.labs.arxiv.org/html/2401.12345"
    )


@pytest.mark.asyncio
async def test_fetch_one_returns_extracted_when_arxiv_html_is_200() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert "arxiv.org/html/2401.12345" in str(request.url)
        return httpx.Response(
            200,
            text=ARXIV_HTML_SAMPLE,
            headers={"content-type": "text/html"},
        )

    client = build_async_client(_cfg(), transport=httpx.MockTransport(handler))
    try:
        bucket = TokenBucket(rate=64.0, burst=64)
        outcome = await fetch_one(
            "2401.12345", client, bucket=bucket, min_sleep_s=0.0
        )
    finally:
        await client.aclose()
    assert outcome.status == 200
    assert not outcome.fallback_used
    assert outcome.extracted is not None
    assert outcome.extraction_pipeline == "arxiv-html-2026-06"
    assert outcome.extracted.spdx_license == "CC-BY-4.0"


@pytest.mark.asyncio
async def test_fetch_one_falls_back_to_ar5iv_on_404() -> None:
    state = {"primary_calls": 0, "fallback_calls": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if "arxiv.org/html/" in url and "ar5iv.labs" not in url:
            state["primary_calls"] += 1
            return httpx.Response(404, text="not found")
        if "ar5iv.labs.arxiv.org/html/" in url:
            state["fallback_calls"] += 1
            return httpx.Response(
                200,
                text=AR5IV_HTML_SAMPLE,
                headers={"content-type": "text/html"},
            )
        raise AssertionError(f"unexpected url: {url}")

    client = build_async_client(_cfg(), transport=httpx.MockTransport(handler))
    try:
        bucket = TokenBucket(rate=64.0, burst=64)
        outcome = await fetch_one(
            "2104.00001", client, bucket=bucket, min_sleep_s=0.0
        )
    finally:
        await client.aclose()
    assert state["primary_calls"] == 1
    assert state["fallback_calls"] == 1
    assert outcome.status == 200
    assert outcome.fallback_used is True
    assert outcome.extraction_pipeline == "ar5iv-2026-06"
    assert outcome.extracted is not None


@pytest.mark.asyncio
async def test_fetch_one_returns_404_outcome_when_both_fail() -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(404, text="missing")

    client = build_async_client(_cfg(), transport=httpx.MockTransport(handler))
    try:
        bucket = TokenBucket(rate=64.0, burst=64)
        outcome = await fetch_one(
            "9912.99999", client, bucket=bucket, min_sleep_s=0.0
        )
    finally:
        await client.aclose()
    assert outcome.status == 404
    assert outcome.fallback_used is True
    assert outcome.extracted is None
    assert outcome.html is None


def test_make_bronze_record_html_branch() -> None:
    from datetime import datetime, timezone

    from ingest.arxiv_html_fetcher.fetcher import FetchOutcome
    from ingest.arxiv_html_fetcher.extractor import extract_arxiv_html

    extracted = extract_arxiv_html(ARXIV_HTML_SAMPLE)
    outcome = FetchOutcome(
        status=200,
        url="https://arxiv.org/html/2401.12345",
        html=ARXIV_HTML_SAMPLE.encode(),
        extracted=extracted,
        extraction_pipeline="arxiv-html-2026-06",
        fallback_used=False,
        fetched_at=datetime(2026, 6, 15, 8, 30, tzinfo=timezone.utc),
        etag=None,
        last_modified=None,
    )
    record, key, content_type = make_bronze_record(
        arxiv_id="2401.12345",
        outcome=outcome,
        feed_name="arxiv-html-fetcher",
        bucket="bronze",
        license_default="arxiv-non-exclusive-distribution",
        bytes_size=len(ARXIV_HTML_SAMPLE),
    )
    assert content_type == "text/html"
    assert key.startswith("year=2026/month=06/day=15/source=arxiv-html-fetcher/")
    assert key.endswith(".html.gz")
    assert record.source_format == "html"
    assert record.extraction_pipeline == "arxiv-html-2026-06"
    assert record.spdx_license == "CC-BY-4.0"
    assert record.spdx_license_source == "html_meta"
    assert record.http_status == 200


def test_make_bronze_record_metadata_stub_branch() -> None:
    from datetime import datetime, timezone

    from ingest.arxiv_html_fetcher.fetcher import FetchOutcome

    outcome = FetchOutcome(
        status=404,
        url="https://ar5iv.labs.arxiv.org/html/9912.99999",
        html=None,
        extracted=None,
        extraction_pipeline="ar5iv-2026-06",
        fallback_used=True,
        fetched_at=datetime(2026, 6, 15, 9, 0, tzinfo=timezone.utc),
        etag=None,
        last_modified=None,
    )
    stub = build_metadata_stub("9912.99999", outcome)
    record, key, content_type = make_bronze_record(
        arxiv_id="9912.99999",
        outcome=outcome,
        feed_name="arxiv-html-fetcher",
        bucket="bronze",
        license_default="arxiv-non-exclusive-distribution",
        bytes_size=len(stub),
    )
    assert content_type == "application/json"
    assert key.endswith(".stub.json.gz")
    assert record.source_format == "metadata"
    assert record.http_status == 404
    assert record.spdx_license_source == "manual_override"
    assert b"fulltext_unavailable" in stub


def test_load_backfill_ids_filters_blank_and_invalid(tmp_path: Any) -> None:
    p = tmp_path / "ids.txt"
    p.write_text(
        "\n".join(
            [
                "# header",
                "",
                "2401.12345",
                "not-an-id",
                "cs/0703123v1",
                "   ",
            ]
        ),
        encoding="utf-8",
    )
    ids = load_backfill_ids(p)
    assert ids == ["2401.12345", "cs/0703123v1"]


@pytest.mark.asyncio
async def test_run_for_ids_emits_one_record_per_id() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if "arxiv.org/html/" in url and "ar5iv.labs" not in url:
            return httpx.Response(
                200,
                text=ARXIV_HTML_SAMPLE,
                headers={"content-type": "text/html"},
            )
        return httpx.Response(404)

    minio = _FakeMinio()
    producer = _FakeProducer()
    emitted = await run_for_ids(
        ["2401.12345", "2401.12346"],
        _cfg(),
        feed_name="arxiv-html-fetcher",
        license_default="arxiv-non-exclusive-distribution",
        rate_per_second=64.0,
        burst=64,
        min_sleep_s=0.0,
        transport=httpx.MockTransport(handler),
        producer_override=producer,  # type: ignore[arg-type]
        minio_override=minio,  # type: ignore[arg-type]
    )
    assert emitted == 2
    assert len(producer.records) == 2
    assert {r.source_format for r in producer.records} == {"html"}
    assert {r.extraction_pipeline for r in producer.records} == {"arxiv-html-2026-06"}
    assert len(minio.objects) == 2
