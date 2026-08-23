from __future__ import annotations

import httpx
import pytest

from ingest.common.page_license import (
    license_from_html_head,
    license_from_link_header,
    resolve_page_license,
)


def test_link_header_requires_license_relation() -> None:
    value = (
        '<https://example.org/next>; rel="next", '
        '<https://creativecommons.org/licenses/by/4.0/>; rel="license"'
    )

    assert license_from_link_header(value, base_url="https://example.org/paper") == (
        "https://creativecommons.org/licenses/by/4.0/"
    )


def test_html_head_supports_link_meta_and_json_ld() -> None:
    assert (
        license_from_html_head(
            b'<html><head><link rel="license" href="/terms/cc-by"></head>',
            base_url="https://example.org/paper",
        )
        == "https://example.org/terms/cc-by"
    )
    assert (
        license_from_html_head(
            b'<meta name="dcterms.license" content="CC-BY-SA-4.0">',
            base_url="https://example.org/paper",
        )
        == "CC-BY-SA-4.0"
    )
    assert (
        license_from_html_head(
            b'<script type="application/ld+json">{"license":"CC-BY-4.0"}</script>',
            base_url="https://example.org/paper",
        )
        == "CC-BY-4.0"
    )


@pytest.mark.asyncio
async def test_item_rights_skip_every_page_request() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(500, request=request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        evidence = await resolve_page_license(
            client,
            "https://example.org/paper",
            item_license="CC-BY-4.0",
            item_license_source="rss_entry",
            item_evidence_url="https://example.org/feed.xml",
            item_evidence_revision='"feed-v3"',
        )

    assert requests == []
    assert evidence.raw_license == "CC-BY-4.0"
    assert evidence.license_source == "rss_entry"
    assert evidence.evidence_url == "https://example.org/feed.xml"
    assert evidence.evidence_revision == '"feed-v3"'


@pytest.mark.asyncio
async def test_http_link_evidence_avoids_html_probe() -> None:
    methods: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        methods.append(request.method)
        return httpx.Response(
            200,
            request=request,
            headers={
                "link": '<https://creativecommons.org/licenses/by/4.0/>; rel="license"',
                "etag": '"item-v2"',
            },
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        evidence = await resolve_page_license(client, "https://example.org/paper")

    assert methods == ["HEAD"]
    assert evidence.license_source == "http_link"
    assert evidence.evidence_revision == '"item-v2"'


@pytest.mark.asyncio
async def test_bounded_html_probe_is_separate_from_full_body_fetch() -> None:
    requests: list[tuple[str, str | None]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append((request.method, request.headers.get("range")))
        if request.method == "HEAD":
            return httpx.Response(200, request=request)
        return httpx.Response(
            206,
            request=request,
            content=b'<meta name="license" content="CC-BY-NC-4.0">body-never-stored',
            headers={"etag": '"head-v1"'},
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        evidence = await resolve_page_license(
            client,
            "https://example.org/paper",
            max_probe_bytes=128,
        )

    assert requests == [("HEAD", None), ("GET", "bytes=0-127")]
    assert evidence.raw_license == "CC-BY-NC-4.0"
    assert evidence.license_source == "html_meta"
    assert evidence.evidence_revision == '"head-v1"'


@pytest.mark.asyncio
async def test_unresolved_page_remains_fail_closed() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, request=request, content=b"<html><head></head></html>")

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        evidence = await resolve_page_license(client, "https://example.org/unlicensed")

    assert evidence.resolved is False
    assert evidence.license_source == "unknown"
    assert evidence.evidence_scope == "unknown"


@pytest.mark.asyncio
async def test_transient_probe_failure_is_retried_not_quarantined() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        status = 200 if request.method == "HEAD" else 503
        return httpx.Response(status, request=request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(httpx.HTTPStatusError):
            await resolve_page_license(client, "https://example.org/temporarily-unavailable")
