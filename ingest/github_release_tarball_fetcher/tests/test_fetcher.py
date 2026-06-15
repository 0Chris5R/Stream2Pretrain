"""Unit tests for the GitHub release tarball fetcher worker."""

from __future__ import annotations

import io
import tarfile
from datetime import datetime, timezone
from typing import Any

import httpx
import pytest

from ingest.common.config import IngestConfig
from ingest.common.http_client import build_async_client
from ingest.common.rate_limit import TokenBucket
from ingest.common.tests.conftest import FakeMinio  # type: ignore[attr-defined]
from ingest.github_release_tarball_fetcher.fetcher import (
    DEFAULT_ALLOWED_LICENSES,
    FetcherConfig,
    ReleaseRef,
    code_object_key,
    code_s3_uri,
    parse_release_url,
    process_release,
)
from schemas.code import CodeFileRecord


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


class _FakeCodeProducer:
    def __init__(self) -> None:
        self.sent: list[CodeFileRecord] = []
        self.headers: list[dict[str, str] | None] = []

    async def send(
        self, record: CodeFileRecord, *, headers: dict[str, str] | None = None
    ) -> None:
        self.sent.append(record)
        self.headers.append(headers)


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
    key = code_object_key(
        owner="huggingface", repo="transformers", ref="v5.0.0", path="src/foo.py"
    )
    assert key == "code/repo=huggingface__transformers/ref=v5.0.0/src/foo.py"
    uri = code_s3_uri(
        bucket="bronze",
        owner="huggingface",
        repo="transformers",
        ref="v5.0.0",
        path="src/foo.py",
    )
    assert uri == "s3://bronze/" + key


def test_default_allowed_licenses_contains_apache_and_mit() -> None:
    assert "Apache-2.0" in DEFAULT_ALLOWED_LICENSES
    assert "MIT" in DEFAULT_ALLOWED_LICENSES
    # GPL is intentionally excluded.
    assert "GPL-3.0" not in DEFAULT_ALLOWED_LICENSES


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
        if url.endswith("/repos/huggingface/transformers"):
            return httpx.Response(
                200,
                json={"license": {"spdx_id": "Apache-2.0"}},
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
    valid_from = datetime(2026, 6, 15, 12, 0, 0, tzinfo=timezone.utc)
    try:
        emitted = await process_release(
            ref,
            cfg=_cfg(),
            fetcher_cfg=fetcher_cfg,
            client=client,
            minio=minio,
            producer=producer,
            bucket=bucket,
            valid_from=valid_from,
        )
    finally:
        await client.aclose()

    assert emitted == 2
    paths = {r.path for r in producer.sent}
    assert paths == {"src/foo.py", "README.md"}
    for r in producer.sent:
        assert r.repo_full_name == "huggingface/transformers"
        assert r.ref == "v5.0.0"
        assert r.license == "Apache-2.0"
        assert r.license_source == "github_api"
        assert r.valid_from == valid_from
        assert r.raw_s3_uri.startswith("s3://bronze/code/repo=huggingface__transformers/")
    # And one MinIO object per emitted file.
    keys = list(minio.objects.keys())
    assert len(keys) == 2
    assert all(k.startswith("code/repo=huggingface__transformers/ref=v5.0.0/") for k in keys)


@pytest.mark.asyncio
async def test_process_release_skips_disallowed_license() -> None:
    ref = ReleaseRef("evil", "gpl-only", "v1.0.0")

    def handler(request: httpx.Request) -> httpx.Response:
        if str(request.url).endswith("/repos/evil/gpl-only"):
            return httpx.Response(
                200, json={"license": {"spdx_id": "GPL-3.0"}}
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
        if str(request.url).endswith("/repos/noLicense/mystery"):
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
        )
    finally:
        await client.aclose()
    assert emitted == 0


@pytest.mark.asyncio
async def test_process_release_handles_tarball_error() -> None:
    ref = ReleaseRef("hf", "transformers", "v9.9.9")

    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if url.endswith("/repos/hf/transformers"):
            return httpx.Response(200, json={"license": {"spdx_id": "MIT"}})
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
        )
    finally:
        await client.aclose()
    assert emitted == 0
    assert producer.sent == []


def test_release_ref_tarball_url() -> None:
    ref = ReleaseRef("hf", "transformers", "v5.0.0")
    assert ref.tarball_url == "https://api.github.com/repos/hf/transformers/tarball/v5.0.0"
    assert ref.full_name == "hf/transformers"
