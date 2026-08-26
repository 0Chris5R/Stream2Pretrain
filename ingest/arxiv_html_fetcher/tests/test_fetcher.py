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

from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any

import httpx
import pytest

from ingest.arxiv_html_fetcher import fetcher as fetcher_module
from ingest.arxiv_html_fetcher.fetcher import (
    ArxivCandidate,
    FetchOutcome,
    _is_arxiv_source_feed,
    build_metadata_stub,
    canonical_arxiv_url,
    fetch_arxiv_license,
    fetch_arxiv_license_with_source,
    fetch_one,
    is_valid_arxiv_id,
    make_bronze_record,
    run_for_ids,
)
from ingest.common.config import IngestConfig
from ingest.common.http_client import build_async_client
from ingest.common.rate_limit import TokenBucket
from schemas.bronze import BronzeRecord
from schemas.license_admission import LicenseAdmissionDecision

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


def test_stream_source_filter_matches_deployed_feed_names() -> None:
    assert _is_arxiv_source_feed("oai-arxiv-cs", None)
    assert _is_arxiv_source_feed("rss-arxiv-cs-lg", None)
    assert _is_arxiv_source_feed("arxiv-oai-cs", None)
    assert _is_arxiv_source_feed("arxiv-extraction-retry", None)
    assert _is_arxiv_source_feed("custom-arxiv", ("custom-arxiv",))


@pytest.mark.asyncio
async def test_stream_reuses_one_consumer_and_commits_each_handled_batch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Consumer:
        def __init__(self) -> None:
            self.commits = 0

        async def commit(self) -> None:
            self.commits += 1

    consumer = Consumer()
    batches: list[int] = []

    async def candidates(*_args: Any, **kwargs: Any) -> Any:
        kwargs["commit_callback"](consumer)
        for index in range(33):
            yield ArxivCandidate(arxiv_id=f"2401.{index:05d}")

    async def run_batch(ids: list[ArxivCandidate], *_args: Any, **kwargs: Any) -> int:
        assert kwargs["raise_on_fetch_error"] is True
        batches.append(len(ids))
        return len(ids)

    monkeypatch.setattr(fetcher_module, "stream_ids_from_topic", candidates)
    monkeypatch.setattr(fetcher_module, "run_for_ids", run_batch)
    args = SimpleNamespace(
        stream_topic="docs.normalized",
        consumer_group="s2p-arxiv-html-fetcher-v2",
        auto_offset_reset="latest",
        max_records=None,
        feed_name="arxiv-html-fetcher",
    )

    assert await fetcher_module._run_stream(args, _cfg()) == 33
    assert batches == [1] * 33
    assert consumer.commits == 33


@pytest.mark.asyncio
async def test_stream_commits_a_fully_quarantined_batch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Consumer:
        commits = 0

        async def commit(self) -> None:
            self.commits += 1

    consumer = Consumer()

    async def candidates(*_args: Any, **kwargs: Any) -> Any:
        kwargs["commit_callback"](consumer)
        yield ArxivCandidate(arxiv_id="2401.00001")

    async def quarantine(*_args: Any, **_kwargs: Any) -> int:
        return 0

    monkeypatch.setattr(fetcher_module, "stream_ids_from_topic", candidates)
    monkeypatch.setattr(fetcher_module, "run_for_ids", quarantine)
    args = SimpleNamespace(
        stream_topic="docs.normalized",
        consumer_group="s2p-arxiv-html-fetcher-v2",
        auto_offset_reset="latest",
        max_records=None,
        feed_name="arxiv-html-fetcher",
    )

    assert await fetcher_module._run_stream(args, _cfg()) == 0
    assert consumer.commits == 1


def _atom_license(value: str) -> str:
    return (
        "<feed xmlns='http://www.w3.org/2005/Atom' "
        "xmlns:arxiv='http://arxiv.org/schemas/atom'><entry>"
        f"<arxiv:license>{value}</arxiv:license></entry></feed>"
    )


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

    async def send(self, record: BronzeRecord, *, headers: dict[str, str] | None = None) -> None:
        self.records.append(record)


class _FakeAdmissionProducer:
    def __init__(self) -> None:
        self.records: list[LicenseAdmissionDecision] = []

    async def start(self) -> None:
        return None

    async def stop(self) -> None:
        return None

    async def send(self, record: LicenseAdmissionDecision) -> None:
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
async def test_fetch_arxiv_license_reads_abstract_rights() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.host == "arxiv.org"
        return httpx.Response(
            200,
            text=(
                '<div class="abs-license"><a '
                'href="http://creativecommons.org/licenses/by/4.0/">view license</a></div>'
            ),
        )

    client = build_async_client(_cfg(), transport=httpx.MockTransport(handler))
    try:
        value = await fetch_arxiv_license(
            "2401.12345",
            client,
            bucket=TokenBucket(rate=64.0, burst=64),
            min_sleep_s=0.0,
        )
    finally:
        await client.aclose()

    assert value == "http://creativecommons.org/licenses/by/4.0/"


@pytest.mark.asyncio
async def test_fetch_arxiv_license_falls_back_to_atom_metadata() -> None:
    calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(str(request.url))
        if request.url.host == "arxiv.org" and request.url.path == "/abs/2112.10074":
            return httpx.Response(200, text="<html><body>no rights link</body></html>")
        if request.url.host == "export.arxiv.org":
            return httpx.Response(
                200, text=_atom_license("http://creativecommons.org/licenses/by/4.0/")
            )
        raise AssertionError(f"unexpected URL {request.url}")

    client = build_async_client(_cfg(), transport=httpx.MockTransport(handler))
    try:
        value, source = await fetch_arxiv_license_with_source(
            "2112.10074",
            client,
            bucket=TokenBucket(rate=64.0, burst=64),
            min_sleep_s=0.0,
        )
    finally:
        await client.aclose()

    assert value == "http://creativecommons.org/licenses/by/4.0/"
    assert source == "arxiv_api"
    assert calls == [
        "https://arxiv.org/abs/2112.10074",
        "https://export.arxiv.org/api/query?id_list=2112.10074&start=0&max_results=1",
    ]


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
        outcome = await fetch_one("2401.12345", client, bucket=bucket, min_sleep_s=0.0)
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
        if "arxiv.org/abs/" in url:
            return httpx.Response(
                200,
                text=(
                    '<div class="abs-license"><a '
                    'href="https://creativecommons.org/licenses/by/4.0/">'
                    "view license</a></div>"
                ),
            )
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
        outcome = await fetch_one("2104.00001", client, bucket=bucket, min_sleep_s=0.0)
    finally:
        await client.aclose()
    assert state["primary_calls"] == 1
    assert state["fallback_calls"] == 1
    assert outcome.status == 200
    assert outcome.fallback_used is True
    assert outcome.extraction_pipeline == "ar5iv-2026-06"
    assert outcome.extracted is not None


@pytest.mark.asyncio
async def test_fetch_one_rejects_redirected_landing_page_and_uses_pdf() -> None:
    calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(str(request.url))
        if request.url.host == "arxiv.org" and "/html/" in request.url.path:
            return httpx.Response(404, text="missing")
        if request.url.host == "ar5iv.labs.arxiv.org":
            return httpx.Response(
                200, text="<html><body><h1>Abstract landing page</h1></body></html>"
            )
        if request.url.host == "arxiv.org" and "/pdf/" in request.url.path:
            return httpx.Response(
                200,
                content=b"%PDF-1.7\ncontrolled",
                headers={"content-type": "application/pdf"},
            )
        raise AssertionError(f"unexpected url: {request.url}")

    client = build_async_client(_cfg(), transport=httpx.MockTransport(handler))
    try:
        outcome = await fetch_one(
            "2104.00001",
            client,
            bucket=TokenBucket(rate=64.0, burst=64),
            min_sleep_s=0.0,
        )
    finally:
        await client.aclose()

    assert outcome.status == 200
    assert outcome.source_format == "pdf"
    assert any("/pdf/2104.00001" in url for url in calls)


@pytest.mark.asyncio
async def test_fetch_one_returns_404_outcome_when_both_fail() -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(404, text="missing")

    client = build_async_client(_cfg(), transport=httpx.MockTransport(handler))
    try:
        bucket = TokenBucket(rate=64.0, burst=64)
        outcome = await fetch_one("9912.99999", client, bucket=bucket, min_sleep_s=0.0)
    finally:
        await client.aclose()
    assert outcome.status == 404
    assert outcome.fallback_used is True
    assert outcome.extracted is None
    assert outcome.html is None
    assert outcome.source_format == "metadata"


@pytest.mark.asyncio
async def test_fetch_one_uses_pdf_when_both_html_sources_are_missing() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if "/pdf/" in str(request.url):
            return httpx.Response(
                200,
                content=b"%PDF-1.7\ncontrolled",
                headers={"content-type": "application/pdf"},
            )
        return httpx.Response(404, text="missing")

    client = build_async_client(_cfg(), transport=httpx.MockTransport(handler))
    try:
        outcome = await fetch_one(
            "9912.99999",
            client,
            bucket=TokenBucket(rate=64.0, burst=64),
            min_sleep_s=0.0,
        )
    finally:
        await client.aclose()

    assert outcome.status == 200
    assert outcome.source_format == "pdf"
    assert outcome.html == b"%PDF-1.7\ncontrolled"
    assert outcome.extraction_pipeline == "docling-pdf-cpu-2.114.0"


@pytest.mark.asyncio
async def test_extraction_retry_fetches_pdf_without_repeating_html() -> None:
    calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(str(request.url))
        assert "/pdf/" in request.url.path
        return httpx.Response(
            200,
            content=b"%PDF-1.7\nretry",
            headers={"content-type": "application/pdf"},
        )

    client = build_async_client(_cfg(), transport=httpx.MockTransport(handler))
    try:
        outcome = await fetch_one(
            "2401.12345",
            client,
            bucket=TokenBucket(rate=64.0, burst=64),
            min_sleep_s=0.0,
            force_pdf=True,
        )
    finally:
        await client.aclose()

    assert calls == ["https://arxiv.org/pdf/2401.12345"]
    assert outcome.source_format == "pdf"
    assert outcome.extraction_pipeline == "docling-pdf-cpu-2.114.0"


def test_make_bronze_record_html_branch() -> None:
    from datetime import datetime

    from ingest.arxiv_html_fetcher.extractor import extract_arxiv_html
    from ingest.arxiv_html_fetcher.fetcher import FetchOutcome

    extracted = extract_arxiv_html(ARXIV_HTML_SAMPLE)
    outcome = FetchOutcome(
        status=200,
        url="https://arxiv.org/html/2401.12345",
        html=ARXIV_HTML_SAMPLE.encode(),
        extracted=extracted,
        extraction_pipeline="arxiv-html-2026-06",
        fallback_used=False,
        fetched_at=datetime(2026, 6, 15, 8, 30, tzinfo=UTC),
        etag=None,
        last_modified=None,
    )
    record, key, content_type = make_bronze_record(
        arxiv_id="2401.12345",
        outcome=outcome,
        feed_name="arxiv-html-fetcher",
        bucket="bronze",
        admitted_license="CC-BY-4.0",
        admitted_license_source="oai_metadata",
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
    from datetime import datetime

    from ingest.arxiv_html_fetcher.fetcher import FetchOutcome

    outcome = FetchOutcome(
        status=404,
        url="https://ar5iv.labs.arxiv.org/html/9912.99999",
        html=None,
        extracted=None,
        extraction_pipeline="ar5iv-2026-06",
        fallback_used=True,
        fetched_at=datetime(2026, 6, 15, 9, 0, tzinfo=UTC),
        etag=None,
        last_modified=None,
    )
    stub = build_metadata_stub("9912.99999", outcome)
    record, key, content_type = make_bronze_record(
        arxiv_id="9912.99999",
        outcome=outcome,
        feed_name="arxiv-html-fetcher",
        bucket="bronze",
        admitted_license="arxiv-non-exclusive-distribution",
        admitted_license_source="arxiv_api",
        bytes_size=len(stub),
    )
    assert content_type == "application/json"
    assert key.endswith(".stub.json.gz")
    assert record.source_format == "metadata"
    assert record.http_status == 404
    assert record.spdx_license_source == "arxiv_api"
    assert b"fulltext_unavailable" in stub


def test_make_bronze_record_pdf_branch() -> None:
    outcome = FetchOutcome(
        status=200,
        url="https://arxiv.org/pdf/9912.99999",
        html=b"%PDF-1.7\ncontrolled",
        extracted=None,
        extraction_pipeline="docling-pdf-cpu-2.114.0",
        fallback_used=True,
        fetched_at=datetime(2026, 6, 15, 9, 0, tzinfo=UTC),
        etag=None,
        last_modified=None,
        source_format="pdf",
    )
    record, key, content_type = make_bronze_record(
        arxiv_id="9912.99999",
        outcome=outcome,
        feed_name="arxiv-html-fetcher",
        bucket="bronze",
        admitted_license="arxiv-non-exclusive-distribution",
        admitted_license_source="arxiv_api",
        bytes_size=len(outcome.html or b""),
    )

    assert content_type == "application/pdf"
    assert key.endswith(".pdf.gz")
    assert record.source_format == "pdf"


@pytest.mark.asyncio
async def test_run_for_ids_emits_one_record_per_id() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if request.url.host == "arxiv.org" and "/abs/" in request.url.path:
            return httpx.Response(
                200,
                text=(
                    '<div class="abs-license"><a '
                    'href="https://creativecommons.org/licenses/by/4.0/">'
                    "view license</a></div>"
                ),
            )
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
        [
            ArxivCandidate("2401.12345", "CC-BY-4.0", "oai_metadata"),
            ArxivCandidate("2401.12346", "CC-BY-4.0", "rss_entry"),
        ],
        _cfg(),
        feed_name="arxiv-html-fetcher",
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


@pytest.mark.asyncio
async def test_run_for_ids_preserves_bounded_pdf_retry_attempt() -> None:
    calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(str(request.url))
        if "/abs/" in request.url.path:
            return httpx.Response(
                200,
                text=(
                    '<div class="abs-license"><a '
                    'href="https://creativecommons.org/licenses/by/4.0/">license</a></div>'
                ),
            )
        if "/pdf/" in request.url.path:
            return httpx.Response(
                200,
                content=b"%PDF-1.7\nretry",
                headers={"content-type": "application/pdf"},
            )
        raise AssertionError(f"unexpected retry URL: {request.url}")

    minio = _FakeMinio()
    producer = _FakeProducer()
    emitted = await run_for_ids(
        [ArxivCandidate("2401.12345", force_pdf=True, retry_attempt=2)],
        _cfg(),
        feed_name="arxiv-html-fetcher",
        rate_per_second=64.0,
        burst=64,
        min_sleep_s=0.0,
        transport=httpx.MockTransport(handler),
        producer_override=producer,  # type: ignore[arg-type]
        minio_override=minio,  # type: ignore[arg-type]
    )

    assert emitted == 1
    assert all("/html/" not in url for url in calls)
    assert producer.records[0].source_format == "pdf"
    assert producer.records[0].extraction_pipeline.endswith("|curation-retry=2")


@pytest.mark.asyncio
async def test_non_item_provenance_never_replaces_item_level_arxiv_lookup() -> None:
    calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(str(request.url))
        if request.url.host == "arxiv.org" and "/abs/" in request.url.path:
            return httpx.Response(
                200,
                text=(
                    '<div class="abs-license"><a '
                    'href="https://creativecommons.org/licenses/by/4.0/">'
                    "view license</a></div>"
                ),
            )
        if request.url.host == "arxiv.org" and "/html/" in request.url.path:
            return httpx.Response(200, text=ARXIV_HTML_SAMPLE)
        raise AssertionError(f"unexpected URL {request.url}")

    emitted = await run_for_ids(
        [ArxivCandidate("2401.12345", "arxiv-non-exclusive-distribution", "manual_override")],
        _cfg(),
        feed_name="arxiv-html-fetcher",
        rate_per_second=64.0,
        burst=64,
        min_sleep_s=0.0,
        transport=httpx.MockTransport(handler),
        producer_override=_FakeProducer(),  # type: ignore[arg-type]
        admission_producer_override=_FakeAdmissionProducer(),  # type: ignore[arg-type]
        minio_override=_FakeMinio(),  # type: ignore[arg-type]
    )

    assert emitted == 1
    assert calls[0] == "https://arxiv.org/abs/2401.12345"
    assert calls[1] == "https://arxiv.org/html/2401.12345"


@pytest.mark.asyncio
async def test_backfill_preflights_license_before_fulltext() -> None:
    calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(str(request.url))
        if request.url.host == "arxiv.org" and "/abs/" in request.url.path:
            return httpx.Response(
                200,
                text=('<div class="abs-license"><a href="CC-BY-4.0">view license</a></div>'),
            )
        if request.url.host == "arxiv.org" and "/html/" in request.url.path:
            return httpx.Response(200, text=ARXIV_HTML_SAMPLE)
        raise AssertionError(f"unexpected URL {request.url}")

    minio = _FakeMinio()
    producer = _FakeProducer()
    admissions = _FakeAdmissionProducer()
    emitted = await run_for_ids(
        ["2401.12345"],
        _cfg(),
        feed_name="arxiv-html-backfill",
        rate_per_second=64.0,
        burst=64,
        min_sleep_s=0.0,
        transport=httpx.MockTransport(handler),
        producer_override=producer,  # type: ignore[arg-type]
        admission_producer_override=admissions,  # type: ignore[arg-type]
        minio_override=minio,  # type: ignore[arg-type]
    )

    assert emitted == 1
    assert calls[0] == "https://arxiv.org/abs/2401.12345"
    assert "/html/2401.12345" in calls[1]
    assert admissions.records[0].license_source == "html_meta"
    assert admissions.records[0].status == "admitted"
    assert admissions.records[0].evidence_revision == "2401.12345"


@pytest.mark.asyncio
async def test_disallowed_backfill_license_never_fetches_body() -> None:
    calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(str(request.url))
        return httpx.Response(
            200,
            text=(
                '<div class="abs-license"><a '
                'href="https://creativecommons.org/licenses/by-nc-nd/4.0/">'
                "view license</a></div>"
            ),
        )

    minio = _FakeMinio()
    producer = _FakeProducer()
    admissions = _FakeAdmissionProducer()
    emitted = await run_for_ids(
        ["2401.12345"],
        _cfg(),
        feed_name="arxiv-html-backfill",
        rate_per_second=64.0,
        burst=64,
        min_sleep_s=0.0,
        transport=httpx.MockTransport(handler),
        producer_override=producer,  # type: ignore[arg-type]
        admission_producer_override=admissions,  # type: ignore[arg-type]
        minio_override=minio,  # type: ignore[arg-type]
    )

    assert emitted == 0
    assert len(calls) == 1
    assert calls[0] == "https://arxiv.org/abs/2401.12345"
    assert admissions.records[0].status == "quarantined"
    assert not producer.records
    assert not minio.objects
