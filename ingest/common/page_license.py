"""Resolve licence evidence attached to one web page before content ingest.

Generic RSS and sitemap discovery records do not establish rights in the
linked page.  This module performs a bounded metadata probe for the individual
URL and returns an auditable evidence record.  The caller still publishes the
licence decision before it performs the separate full-body fetch, Bronze
storage, extraction, OCR, or classification.

Resolution precedence is deliberately narrow:

1. item-level rights carried by the discovery record;
2. an RFC 8288 ``Link: <...>; rel=license`` response header;
3. ``<link rel=license>``, DC/DCTERMS rights meta, or schema.org ``license``
   in the first bounded HTML bytes of the item page.

Site ownership, a sitemap, an RSS channel copyright notice, and a SourceFeed
default are not item-level evidence and are never accepted here.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from html.parser import HTMLParser
from typing import Literal
from urllib.parse import urljoin

import httpx

from ingest.common.license_admission import normalize_license
from schemas.bronze import SpdxLicenseSource

EvidenceScope = Literal["item", "source_terms", "dataset_wrapper", "unknown"]

_DEFAULT_PROBE_BYTES = 65_536
_RIGHTS_META_NAMES = frozenset(
    {
        "license",
        "dc.license",
        "dc.rights",
        "dcterms.license",
        "dcterms.rights",
        "copyright",
    }
)


@dataclass(frozen=True, slots=True)
class PageLicenseEvidence:
    """One page-scoped licence observation suitable for admission logging."""

    raw_license: str | None
    license_source: SpdxLicenseSource
    resolver: str
    evidence_url: str | None
    evidence_revision: str | None
    evidence_scope: EvidenceScope

    @property
    def resolved(self) -> bool:
        return normalize_license(self.raw_license) != "unknown"


def unresolved_page_license(url: str) -> PageLicenseEvidence:
    """Return the fail-closed result for a page with no rights evidence."""
    return PageLicenseEvidence(
        raw_license=None,
        license_source="unknown",
        resolver="web-page-license-probe",
        evidence_url=url,
        evidence_revision=None,
        evidence_scope="unknown",
    )


def _response_revision(response: httpx.Response) -> str | None:
    return response.headers.get("etag") or response.headers.get("last-modified")


def _split_link_header(value: str) -> list[str]:
    """Split an RFC 8288 Link header without splitting commas inside ``<>``."""
    out: list[str] = []
    start = 0
    in_angle = False
    in_quote = False
    for index, char in enumerate(value):
        if char == '"':
            in_quote = not in_quote
        elif not in_quote and char == "<":
            in_angle = True
        elif not in_quote and char == ">":
            in_angle = False
        elif char == "," and not in_angle and not in_quote:
            out.append(value[start:index].strip())
            start = index + 1
    out.append(value[start:].strip())
    return [part for part in out if part]


def license_from_link_header(value: str | None, *, base_url: str) -> str | None:
    """Return the first ``rel=license`` target from an HTTP Link header."""
    if not value:
        return None
    for part in _split_link_header(value):
        if not part.startswith("<") or ">" not in part:
            continue
        target, _, params = part[1:].partition(">")
        rel_values: list[str] = []
        for raw_param in params.split(";"):
            name, separator, raw_value = raw_param.strip().partition("=")
            if separator and name.lower() == "rel":
                rel_values.extend(raw_value.strip().strip('"').lower().split())
        if "license" in rel_values:
            return urljoin(base_url, target.strip())
    return None


class _HeadLicenseParser(HTMLParser):
    """Collect page-level rights signals from a bounded HTML head probe."""

    def __init__(self, *, base_url: str) -> None:
        super().__init__(convert_charrefs=True)
        self.base_url = base_url
        self.license_value: str | None = None
        self._json_ld = False
        self._json_parts: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        values = {key.lower(): value for key, value in attrs if value is not None}
        lowered = tag.lower()
        if lowered == "link":
            rel = {part.lower() for part in values.get("rel", "").split()}
            href = values.get("href")
            if "license" in rel and href and self.license_value is None:
                self.license_value = urljoin(self.base_url, href)
        elif lowered == "meta":
            name = (values.get("name") or values.get("property") or "").lower()
            content = values.get("content")
            if name in _RIGHTS_META_NAMES and content and self.license_value is None:
                self.license_value = content.strip()
        elif lowered == "script":
            script_type = values.get("type", "").split(";", 1)[0].strip().lower()
            self._json_ld = script_type == "application/ld+json"
            if self._json_ld:
                self._json_parts = []

    def handle_data(self, data: str) -> None:
        if self._json_ld:
            self._json_parts.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() != "script" or not self._json_ld:
            return
        self._json_ld = False
        try:
            payload = json.loads("".join(self._json_parts))
        except (json.JSONDecodeError, TypeError):
            return
        candidate = _find_json_license(payload)
        if candidate and self.license_value is None:
            self.license_value = (
                urljoin(self.base_url, candidate)
                if candidate.startswith(("http://", "https://", "/", "./", "../"))
                else candidate
            )


def _find_json_license(value: object) -> str | None:
    if isinstance(value, dict):
        candidate = value.get("license")
        if isinstance(candidate, str) and candidate.strip():
            return candidate.strip()
        if isinstance(candidate, dict):
            for key in ("url", "@id", "name"):
                nested = candidate.get(key)
                if isinstance(nested, str) and nested.strip():
                    return nested.strip()
        graph = value.get("@graph")
        if isinstance(graph, list):
            return _find_json_license(graph)
    elif isinstance(value, list):
        for item in value:
            candidate = _find_json_license(item)
            if candidate:
                return candidate
    return None


def license_from_html_head(payload: bytes, *, base_url: str) -> str | None:
    """Extract one page-level licence value from bounded HTML bytes."""
    parser = _HeadLicenseParser(base_url=base_url)
    parser.feed(payload.decode("utf-8", errors="replace"))
    parser.close()
    return parser.license_value


async def resolve_page_license(
    client: httpx.AsyncClient,
    url: str,
    *,
    item_license: str | None = None,
    item_license_source: SpdxLicenseSource = "rss_entry",
    item_evidence_url: str | None = None,
    item_evidence_revision: str | None = None,
    max_probe_bytes: int = _DEFAULT_PROBE_BYTES,
) -> PageLicenseEvidence:
    """Resolve rights for one page without retaining or processing its body.

    The bounded range request is a metadata probe only.  If a server ignores
    ``Range``, the response stream is closed after ``max_probe_bytes``.  A
    later full-body GET is made only after the returned evidence is admitted.
    """
    if max_probe_bytes < 1:
        raise ValueError("max_probe_bytes must be positive")
    if normalize_license(item_license) != "unknown":
        return PageLicenseEvidence(
            raw_license=item_license,
            license_source=item_license_source,
            resolver=f"discovery:{item_license_source}",
            evidence_url=item_evidence_url or url,
            evidence_revision=item_evidence_revision,
            evidence_scope="item",
        )

    head_response: httpx.Response | None = None
    try:
        head_response = await client.head(url)
    except httpx.HTTPError:
        head_response = None
    if head_response is not None and head_response.status_code < 400:
        value = license_from_link_header(
            head_response.headers.get("link"), base_url=str(head_response.url)
        )
        if normalize_license(value) != "unknown":
            return PageLicenseEvidence(
                raw_license=value,
                license_source="http_link",
                resolver="http-link-rel-license",
                evidence_url=str(head_response.url),
                evidence_revision=_response_revision(head_response),
                evidence_scope="item",
            )

    headers = {
        "Accept": "text/html, application/xhtml+xml;q=0.9",
        "Range": f"bytes=0-{max_probe_bytes - 1}",
    }
    try:
        async with client.stream("GET", url, headers=headers) as response:
            if response.status_code in {408, 425, 429} or response.status_code >= 500:
                # A transient upstream failure is not negative licence
                # evidence. Propagate it so the feed cursor is left unchanged
                # and the same item is retried on the next run.
                response.raise_for_status()
            if response.status_code >= 400:
                return unresolved_page_license(url)
            chunks: list[bytes] = []
            remaining = max_probe_bytes
            async for chunk in response.aiter_bytes():
                if remaining <= 0:
                    break
                part = chunk[:remaining]
                chunks.append(part)
                remaining -= len(part)
                if remaining <= 0:
                    break
            payload = b"".join(chunks)
            header_value = license_from_link_header(
                response.headers.get("link"), base_url=str(response.url)
            )
            value = header_value or license_from_html_head(payload, base_url=str(response.url))
            if normalize_license(value) == "unknown":
                return PageLicenseEvidence(
                    raw_license=None,
                    license_source="unknown",
                    resolver="web-page-license-probe",
                    evidence_url=str(response.url),
                    evidence_revision=_response_revision(response),
                    evidence_scope="unknown",
                )
            source: SpdxLicenseSource = "http_link" if header_value else "html_meta"
            return PageLicenseEvidence(
                raw_license=value,
                license_source=source,
                resolver=(
                    "http-link-rel-license"
                    if source == "http_link"
                    else "bounded-html-license-metadata"
                ),
                evidence_url=str(response.url),
                evidence_revision=_response_revision(response),
                evidence_scope="item",
            )
    except httpx.HTTPError:
        # Transport failure is not evidence that the page has no licence.
        # Let the source pass fail and retry without committing its cursor.
        raise


__all__ = [
    "PageLicenseEvidence",
    "license_from_html_head",
    "license_from_link_header",
    "resolve_page_license",
    "unresolved_page_license",
]
