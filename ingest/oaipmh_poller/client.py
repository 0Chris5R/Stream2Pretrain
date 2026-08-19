"""Hand-written async OAI-PMH 2.0 client.

We avoid sickle because it is sync-only and does not honour ``Retry-After``.
Implements the four verbs we need (``Identify``, ``ListIdentifiers``,
``ListRecords``, ``GetRecord``) with resumption-token handling.

Reference: http://www.openarchives.org/OAI/openarchivesprotocol.html
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Any
from xml.etree import ElementTree as ET

import httpx

OAI_NS = "http://www.openarchives.org/OAI/2.0/"
ARXIV_NS = "http://arxiv.org/OAI/arXiv/"
DC_NS = "http://purl.org/dc/elements/1.1/"


def _strip_ns(tag: str) -> str:
    return tag.split("}", 1)[1] if "}" in tag else tag


@dataclass(slots=True)
class OAIRecord:
    """A single OAI-PMH ``<record>`` element."""

    identifier: str
    datestamp: str
    set_specs: list[str]
    metadata_xml: str
    raw: bytes
    deleted: bool = False

    def arxiv_abs_url(self) -> str | None:
        """Return ``https://arxiv.org/abs/<id>`` if this is an arXiv record."""
        # arXiv OAI ids look like ``oai:arXiv.org:1234.56789``.
        if not self.identifier.startswith("oai:arXiv.org:"):
            return None
        arxiv_id = self.identifier.split(":", 2)[2]
        return f"https://arxiv.org/abs/{arxiv_id}"

    def license_value(self) -> str | None:
        """Return a per-record licence from arXiv/DC metadata, if present."""
        if not self.metadata_xml:
            return None
        try:
            root = ET.fromstring(self.metadata_xml)
        except ET.ParseError:
            return None
        for element in root.iter():
            local_name = _strip_ns(element.tag).lower()
            if local_name in {"license", "rights"} and (element.text or "").strip():
                return (element.text or "").strip()
        return None


class OAIError(Exception):
    """Raised on an OAI-PMH ``<error>`` response."""


class OAIClient:
    """Minimal async OAI-PMH 2.0 client.

    Politeness: arXiv asks for a 1 second sleep between requests and a max of
    4 req/s. Default settings here meet both.
    """

    def __init__(
        self,
        base_url: str,
        client: httpx.AsyncClient,
        *,
        sleep_between_requests: float = 1.0,
    ) -> None:
        self._base = base_url
        self._client = client
        self._sleep = sleep_between_requests

    async def list_records(
        self,
        *,
        metadata_prefix: str = "arXiv",
        set_spec: str | None = None,
        from_: str | None = None,
        until: str | None = None,
        max_records: int | None = None,
    ) -> AsyncIterator[OAIRecord]:
        """Iterate every record in a window, transparently following resumption tokens."""
        params: dict[str, Any] = {"verb": "ListRecords", "metadataPrefix": metadata_prefix}
        if set_spec:
            params["set"] = set_spec
        if from_:
            params["from"] = from_
        if until:
            params["until"] = until

        emitted = 0
        while True:
            xml_bytes = await self._request(params)
            root = ET.fromstring(xml_bytes)
            self._raise_if_error(root)

            list_records = root.find(f"{{{OAI_NS}}}ListRecords")
            if list_records is None:
                return
            for rec_el in list_records.findall(f"{{{OAI_NS}}}record"):
                record = self._parse_record(rec_el)
                yield record
                emitted += 1
                if max_records is not None and emitted >= max_records:
                    return

            token_el = list_records.find(f"{{{OAI_NS}}}resumptionToken")
            if token_el is None or not (token_el.text or "").strip():
                return
            params = {"verb": "ListRecords", "resumptionToken": token_el.text.strip()}

    async def _request(self, params: dict[str, Any]) -> bytes:
        await asyncio.sleep(self._sleep)
        resp = await self._client.get(self._base, params=params)
        resp.raise_for_status()
        return resp.content

    def _raise_if_error(self, root: ET.Element) -> None:
        err = root.find(f"{{{OAI_NS}}}error")
        if err is not None:
            code = err.attrib.get("code", "unknown")
            msg = (err.text or "").strip()
            # noRecordsMatch is benign on incremental polls.
            if code == "noRecordsMatch":
                return
            raise OAIError(f"{code}: {msg}")

    @staticmethod
    def _parse_record(rec_el: ET.Element) -> OAIRecord:
        header = rec_el.find(f"{{{OAI_NS}}}header")
        if header is None:
            raise OAIError("record missing header")
        identifier_el = header.find(f"{{{OAI_NS}}}identifier")
        datestamp_el = header.find(f"{{{OAI_NS}}}datestamp")
        identifier = (identifier_el.text or "").strip() if identifier_el is not None else ""
        datestamp = (datestamp_el.text or "").strip() if datestamp_el is not None else ""
        set_specs = [(s.text or "").strip() for s in header.findall(f"{{{OAI_NS}}}setSpec")]
        deleted = header.attrib.get("status") == "deleted"
        meta_el = rec_el.find(f"{{{OAI_NS}}}metadata")
        metadata_xml = ET.tostring(meta_el, encoding="unicode") if meta_el is not None else ""
        raw = ET.tostring(rec_el, encoding="utf-8")
        return OAIRecord(
            identifier=identifier,
            datestamp=datestamp,
            set_specs=set_specs,
            metadata_xml=metadata_xml,
            raw=raw,
            deleted=deleted,
        )
