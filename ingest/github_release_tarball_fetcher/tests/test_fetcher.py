"""Unit tests for the GitHub release tarball fetcher worker."""

from __future__ import annotations

import io
import tarfile
from datetime import UTC, datetime

import httpx
import pytest

from ingest.common.config import IngestConfig
from ingest.common.http_client import build_async_client
from ingest.common.rate_limit import TokenBucket
from ingest.common.tests.conftest import FakeMinio  # type: ignore[attr-defined]
from ingest.github_release_tarball_fetcher.fetcher import (
    FetcherConfig,
    ReleaseRef,
    TarballMetrics,
    _is_tarball_job,
    code_object_key,
    code_s3_uri,
    fetch_repo_license,
    fetch_repo_license_evidence,
    fetch_tarball,
    is_release_candidate,
    parse_release_url,
    process_release,
)
from processor.common import bronze_loads
from schemas.bronze import BronzeRecord
from schemas.license_admission import LicenseAdmissionDecision


def _cfg() -> IngestConfig:
    return IngestConfig(
        env="dev",
        log_level="DEBUG",
        redpanda_brokers="",
        minio_endpoint="",
        minio_access_key="",
        minio_secret_key="",
        minio_bronze_bucket="bronze",
        otel_endpoint=None,
        otel_protocol="grpc",
        github_token=None,
        hf_token=None,
        user_agent="ua",
        http_timeout_seconds=2.0,
        http_max_retries=0,
    )


@pytest.mark.parametrize(
    ("headers", "expected"),
    [
        ({"source_feed": "github-releases"}, True),
        (
            {
                "source_feed": "github-releases",
                "tarball_job_dispatched": "true",
            },
            False,
        ),
        ({"source_feed": "github-release-tarballs"}, False),
        ({}, False),
    ],
)
def test_tarball_job_filter_avoids_dual_publish_duplicates(
    headers: dict[str, str], expected: bool
) -> None:
    assert _is_tarball_job(headers) is expected


class _FakeCodeProducer:
    def __init__(self) -> None:
        self.sent: list[BronzeRecord] = []
        self.headers: list[dict[str, str] | None] = []

    async def send(self, record: BronzeRecord, *, headers: dict[str, str] | None = None) -> None:
        self.sent.append(record)
        self.headers.append(headers)


class _FakeAdmissionProducer:
    def __init__(self) -> None:
        self.sent: list[LicenseAdmissionDecision] = []

    async def send(self, decision: LicenseAdmissionDecision) -> None:
        self.sent.append(decision)


def _build_tarball(files: dict[str, bytes]) -> bytes:
    buf = io.BytesIO()
    top = "huggingface-transformers-deadbee"
    with tarfile.open(fileobj=buf, mode="w:gz") as tf:
        for path, data in files.items():
            info = tarfile.TarInfo(name=f"{top}/{path}")
            info.size = len(data)
            tf.addfile(info, io.BytesIO(data))
    return buf.getvalue()


@pytest.mark.parametrize(
    "url,expected",
    [
        (
            "https://github.com/huggingface/transformers/releases/tag/v5.0.0",
            ReleaseRef("huggingface", "transformers", "v5.0.0"),
        ),
        (
            "https://github.com/owner.with.dot/repo-name/releases/tag/2026.06.0",
            ReleaseRef("owner.with.dot", "repo-name", "2026.06.0"),
        ),
        ("https://github.com/owner/repo/releases", None),
        ("https://example.com/owner/repo/releases/tag/v1", None),
        ("not a url", None),
        ("", None),
    ],
)
def test_parse_release_url(url: str, expected: ReleaseRef | None) -> None:
    assert parse_release_url(url) == expected


def test_code_object_key_layout() -> None:
    key = code_object_key(owner="huggingface", repo="transformers", ref="v5.0.0", path="src/foo.py")
    assert key == "code/repo=huggingface__transformers/ref=v5.0.0/src/foo.py"
    uri = code_s3_uri(
        bucket="bronze",
        owner="huggingface",
        repo="transformers",
        ref="v5.0.0",
        path="src/foo.py",
    )
    assert uri == "s3://bronze/" + key


@pytest.mark.asyncio
async def test_github_requests_fall_back_anonymously_after_stale_token() -> None:
    ref = ReleaseRef("huggingface", "transformers", "v5.0.0")

    def authenticated_handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(401, json={"message": "Bad credentials"})

    def anonymous_handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/repos/huggingface/transformers/license"):
            assert request.url.params["ref"] == "v5.0.0"
            return httpx.Response(
                200,
                json={"license": {"spdx_id": "Apache-2.0"}, "sha": "a" * 40},
            )
        if "/tarball/" in str(request.url):
            return httpx.Response(200, content=b"public-tarball")
        return httpx.Response(404)

    authenticated = httpx.AsyncClient(transport=httpx.MockTransport(authenticated_handler))
    anonymous = httpx.AsyncClient(transport=httpx.MockTransport(anonymous_handler))
    try:
        assert (
            await fetch_repo_license(authenticated, ref, anonymous_client=anonymous) == "Apache-2.0"
        )
        assert (
            await fetch_tarball(authenticated, ref, anonymous_client=anonymous) == b"public-tarball"
        )
    finally:
        await authenticated.aclose()
        await anonymous.aclose()


@pytest.mark.asyncio
async def test_repo_license_requires_and_retains_immutable_blob_sha() -> None:
    ref = ReleaseRef("huggingface", "transformers", "v5.0.0")

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.params["ref"] == ref.tag
        return httpx.Response(
            200,
            json={
                "license": {"spdx_id": "Apache-2.0"},
                "sha": "f" * 40,
                "path": "LICENSE",
                "html_url": (
                    "https://github.com/huggingface/transformers/blob/"
                    f"{'f' * 40}/LICENSE"
                ),
            },
        )

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    try:
        evidence = await fetch_repo_license_evidence(client, ref)
    finally:
        await client.aclose()

    assert evidence is not None
    assert evidence.spdx_id == "Apache-2.0"
    assert evidence.blob_sha == "f" * 40
    assert evidence.path == "LICENSE"
    assert evidence.api_url.endswith("/license?ref=v5.0.0")


@pytest.mark.asyncio
async def test_process_release_emits_records_and_writes_minio() -> None:
    ref = ReleaseRef("huggingface", "transformers", "v5.0.0")
    tarball = _build_tarball(
        {
            "src/foo.py": b"x = 1\nprint(x)\n",
            "README.md": b"# Hello\n",
            "skip.bin": b"\x00\x01",
        }
    )

    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if request.url.path.endswith("/repos/huggingface/transformers/license"):
            assert request.url.params["ref"] == "v5.0.0"
            return httpx.Response(
                200,
                json={
                    "license": {"spdx_id": "Apache-2.0"},
                    "sha": "b" * 40,
                    "path": "LICENSE",
                },
                headers={"content-type": "application/json"},
            )
        if "/tarball/" in url:
            return httpx.Response(
                200,
                content=tarball,
                headers={"content-type": "application/x-gzip"},
            )
        return httpx.Response(404)

    client = build_async_client(_cfg(), transport=httpx.MockTransport(handler))
    minio = FakeMinio()
    await minio.start()
    producer = _FakeCodeProducer()
    bucket = TokenBucket(rate=100.0, burst=8)
    fetcher_cfg = FetcherConfig()
    valid_from = datetime(2026, 6, 15, 12, 0, 0, tzinfo=UTC)
    try:
        emitted = await process_release(
            ref,
            cfg=_cfg(),
            fetcher_cfg=fetcher_cfg,
            client=client,
            minio=minio,
            producer=producer,
            bucket=bucket,
            admission_producer=_FakeAdmissionProducer(),  # type: ignore[arg-type]
            valid_from=valid_from,
        )
    finally:
        await client.aclose()

    assert emitted == 2
    paths = {h["github_path"] for h in producer.headers if h is not None}
    assert paths == {"src/foo.py", "README.md"}
    for r in producer.sent:
        assert r.source_feed == "github-release-tarballs"
        assert r.spdx_license == "Apache-2.0"
        assert r.spdx_license_source == "github_api"
        assert r.fetched_at == valid_from
        assert r.raw_html_s3_uri.startswith("s3://bronze/code/repo=huggingface__transformers/")
        round_trip = bronze_loads(r.model_dump_json().encode("utf-8"))
        if str(r.url).endswith("README.md"):
            assert round_trip.source_format == "web"
            assert round_trip.extraction_pipeline == "github-readme-markdown-v1"
        else:
            assert round_trip.source_format == "code"
            assert round_trip.extraction_pipeline == "github-release-tarball-2026-06"
    # And one MinIO object per emitted file.
    keys = list(minio.objects.keys())
    assert len(keys) == 2
    assert all(k.startswith("code/repo=huggingface__transformers/ref=v5.0.0/") for k in keys)


@pytest.mark.asyncio
async def test_process_release_skips_disallowed_license() -> None:
    ref = ReleaseRef("evil", "gpl-only", "v1.0.0")

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/repos/evil/gpl-only/license"):
            return httpx.Response(
                200,
                json={"license": {"spdx_id": "GPL-3.0"}, "sha": "c" * 40},
            )
        # The tarball endpoint must not be hit if the license check rejects.
        return httpx.Response(500, text="should not be called")

    client = build_async_client(_cfg(), transport=httpx.MockTransport(handler))
    minio = FakeMinio()
    await minio.start()
    producer = _FakeCodeProducer()
    bucket = TokenBucket(rate=100.0, burst=8)
    try:
        emitted = await process_release(
            ref,
            cfg=_cfg(),
            fetcher_cfg=FetcherConfig(),
            client=client,
            minio=minio,
            producer=producer,
            bucket=bucket,
            admission_producer=_FakeAdmissionProducer(),  # type: ignore[arg-type]
            valid_from=None,
        )
    finally:
        await client.aclose()

    assert emitted == 0
    assert producer.sent == []
    assert minio.objects == {}


@pytest.mark.asyncio
async def test_process_release_skips_when_license_missing() -> None:
    ref = ReleaseRef("noLicense", "mystery", "v0")

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/repos/noLicense/mystery/license"):
            return httpx.Response(200, json={"license": None})
        return httpx.Response(404)

    client = build_async_client(_cfg(), transport=httpx.MockTransport(handler))
    minio = FakeMinio()
    await minio.start()
    producer = _FakeCodeProducer()
    bucket = TokenBucket(rate=100.0, burst=8)
    try:
        emitted = await process_release(
            ref,
            cfg=_cfg(),
            fetcher_cfg=FetcherConfig(),
            client=client,
            minio=minio,
            producer=producer,
            bucket=bucket,
            admission_producer=_FakeAdmissionProducer(),  # type: ignore[arg-type]
        )
    finally:
        await client.aclose()
    assert emitted == 0


@pytest.mark.asyncio
async def test_process_release_handles_tarball_error() -> None:
    ref = ReleaseRef("hf", "transformers", "v9.9.9")

    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if request.url.path.endswith("/repos/hf/transformers/license"):
            return httpx.Response(
                200,
                json={"license": {"spdx_id": "MIT"}, "sha": "d" * 40},
            )
        if "/tarball/" in url:
            return httpx.Response(502, text="bad gateway")
        return httpx.Response(404)

    client = build_async_client(_cfg(), transport=httpx.MockTransport(handler))
    minio = FakeMinio()
    await minio.start()
    producer = _FakeCodeProducer()
    bucket = TokenBucket(rate=100.0, burst=8)
    try:
        emitted = await process_release(
            ref,
            cfg=_cfg(),
            fetcher_cfg=FetcherConfig(),
            client=client,
            minio=minio,
            producer=producer,
            bucket=bucket,
            admission_producer=_FakeAdmissionProducer(),  # type: ignore[arg-type]
        )
    finally:
        await client.aclose()
    assert emitted == 0
    assert producer.sent == []


def test_release_ref_tarball_url() -> None:
    ref = ReleaseRef("hf", "transformers", "v5.0.0")
    assert ref.tarball_url == "https://api.github.com/repos/hf/transformers/tarball/v5.0.0"
    assert ref.full_name == "hf/transformers"


@pytest.mark.parametrize(
    ("tag", "expected"),
    [
        ("v5.0.0", True),
        ("apache-iceberg-1.10.1", True),
        ("ciflow%2Finductor%2F194479", False),
        ("trunk%2F52310c5c0b79395d1b8691a1908d0f5b3ccb9992", False),
    ],
)
def test_release_candidate_excludes_observed_ci_refs(tag: str, expected: bool) -> None:
    assert is_release_candidate(ReleaseRef("pytorch", "pytorch", tag), FetcherConfig()) is expected


@pytest.mark.asyncio
async def test_process_release_does_not_silently_cap_release_files() -> None:
    ref = ReleaseRef("huggingface", "transformers", "v5.0.0")
    tarball = _build_tarball({f"src/file_{index}.py": b"x = 1\n" for index in range(5)})

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/repos/huggingface/transformers/license"):
            return httpx.Response(
                200,
                json={"license": {"spdx_id": "Apache-2.0"}, "sha": "e" * 40},
            )
        return httpx.Response(200, content=tarball)

    client = build_async_client(_cfg(), transport=httpx.MockTransport(handler))
    minio = FakeMinio()
    await minio.start()
    producer = _FakeCodeProducer()
    try:
        emitted = await process_release(
            ref,
            cfg=_cfg(),
            fetcher_cfg=FetcherConfig(),
            client=client,
            minio=minio,
            producer=producer,
            bucket=TokenBucket(rate=100.0, burst=8),
            admission_producer=_FakeAdmissionProducer(),  # type: ignore[arg-type]
        )
    finally:
        await client.aclose()

    assert emitted == 5
    assert len(minio.objects) == 5
    assert len(producer.sent) == 5


def test_tarball_metrics_render_prometheus() -> None:
    metrics = TarballMetrics()

    metrics.release_started()
    body = metrics.render_prometheus().decode("utf-8")
    assert "s2p_github_releases_seen_total 1" in body
    assert "s2p_github_releases_unprocessed 1" in body

    metrics.release_finished(emitted=3, failed=False)
    body = metrics.render_prometheus().decode("utf-8")
    assert "s2p_github_releases_unprocessed 0" in body
    assert "s2p_github_releases_processed_total 1" in body
    assert "s2p_github_code_records_emitted_total 3" in body
