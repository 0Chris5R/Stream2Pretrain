"""Tests for the hand-written async OAI-PMH client."""

from __future__ import annotations

import httpx
import pytest

from ingest.common.config import IngestConfig
from ingest.common.http_client import build_async_client
from ingest.oaipmh_poller.client import OAIClient, OAIError

OAI_PAGE_1 = """<?xml version="1.0" encoding="UTF-8"?>
<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/">
  <responseDate>2026-06-15T08:00:00Z</responseDate>
  <ListRecords>
    <record>
      <header>
        <identifier>oai:arXiv.org:2026.001</identifier>
        <datestamp>2026-06-15</datestamp>
        <setSpec>cs</setSpec>
      </header>
      <metadata>
        <arXiv xmlns="http://arxiv.org/OAI/arXiv/">
          <id>2026.001</id>
          <title>Paper A</title>
        </arXiv>
      </metadata>
    </record>
    <resumptionToken>token1</resumptionToken>
  </ListRecords>
</OAI-PMH>
"""

OAI_PAGE_2 = """<?xml version="1.0" encoding="UTF-8"?>
<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/">
  <responseDate>2026-06-15T08:00:01Z</responseDate>
  <ListRecords>
    <record>
      <header>
        <identifier>oai:arXiv.org:2026.002</identifier>
        <datestamp>2026-06-15</datestamp>
        <setSpec>cs</setSpec>
      </header>
      <metadata><arXiv><id>2026.002</id></arXiv></metadata>
    </record>
    <resumptionToken/>
  </ListRecords>
</OAI-PMH>
"""

OAI_ERROR = """<?xml version="1.0" encoding="UTF-8"?>
<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/">
  <responseDate>2026-06-15T08:00:00Z</responseDate>
  <error code="badArgument">bad from</error>
</OAI-PMH>
"""


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
        hf_token=None,
        user_agent="ua",
        http_timeout_seconds=2.0,
        http_max_retries=0,
    )


@pytest.mark.asyncio
async def test_list_records_follows_resumption_token() -> None:
    state = {"call": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        state["call"] += 1
        params = dict(request.url.params)
        if "resumptionToken" in params:
            return httpx.Response(200, text=OAI_PAGE_2, headers={"content-type": "application/xml"})
        return httpx.Response(200, text=OAI_PAGE_1, headers={"content-type": "application/xml"})

    client = build_async_client(_cfg(), transport=httpx.MockTransport(handler))
    try:
        oai = OAIClient("https://oai.example.org/oai", client, sleep_between_requests=0.0)
        seen = []
        async for rec in oai.list_records(metadata_prefix="arXiv", set_spec="cs"):
            seen.append(rec.identifier)
    finally:
        await client.aclose()
    assert seen == ["oai:arXiv.org:2026.001", "oai:arXiv.org:2026.002"]


@pytest.mark.asyncio
async def test_list_pages_can_resume_from_a_durable_token() -> None:
    seen_params: list[dict[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen_params.append(dict(request.url.params))
        return httpx.Response(200, text=OAI_PAGE_2, headers={"content-type": "application/xml"})

    client = build_async_client(_cfg(), transport=httpx.MockTransport(handler))
    try:
        oai = OAIClient("https://oai.example.org/oai", client, sleep_between_requests=0.0)
        pages = [page async for page in oai.list_pages(resumption_token="token1")]
    finally:
        await client.aclose()

    assert seen_params == [{"verb": "ListRecords", "resumptionToken": "token1"}]
    assert [[record.identifier for record in page.records] for page in pages] == [
        ["oai:arXiv.org:2026.002"]
    ]
    assert pages[0].resumption_token is None


@pytest.mark.asyncio
async def test_list_records_raises_on_oai_error() -> None:
    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(200, text=OAI_ERROR, headers={"content-type": "application/xml"})

    client = build_async_client(_cfg(), transport=httpx.MockTransport(handler))
    try:
        oai = OAIClient("https://oai.example.org/oai", client, sleep_between_requests=0.0)
        with pytest.raises(OAIError):
            async for _ in oai.list_records():
                pass
    finally:
        await client.aclose()


def test_arxiv_abs_url() -> None:
    from ingest.oaipmh_poller.client import OAIRecord

    rec = OAIRecord(
        identifier="oai:arXiv.org:2026.001",
        datestamp="2026-06-15",
        set_specs=["cs"],
        metadata_xml="",
        raw=b"",
    )
    assert rec.arxiv_abs_url() == "https://arxiv.org/abs/2026.001"
