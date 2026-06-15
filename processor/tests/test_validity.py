"""Tests for :mod:`processor.operators.validity`."""

from __future__ import annotations

from datetime import datetime, timezone

from processor.operators.validity import (
    ValidityEnricher,
    first_schema_date,
    first_sitemap_lastmod,
    parse_iso8601,
)


def test_parse_iso8601_z_suffix() -> None:
    dt = parse_iso8601("2026-06-12T08:00:00Z")
    assert dt is not None
    assert dt == datetime(2026, 6, 12, 8, 0, tzinfo=timezone.utc)


def test_parse_iso8601_offset() -> None:
    dt = parse_iso8601("2026-06-12T10:00:00+02:00")
    assert dt is not None
    assert dt.tzinfo is not None
    assert dt.utcoffset().total_seconds() == 0


def test_parse_iso8601_rfc822_fallback() -> None:
    dt = parse_iso8601("Wed, 12 Jun 2026 08:00:00 GMT")
    assert dt is not None
    assert dt.year == 2026


def test_first_schema_date_inline() -> None:
    html = '<html>... <meta name="x"> "datePublished": "2026-06-01T00:00:00Z" ...</html>'
    dt = first_schema_date(html)
    assert dt is not None and dt.year == 2026


def test_first_schema_date_jsonld_block() -> None:
    html = (
        '<script type="application/ld+json">'
        '{"@context":"https://schema.org","datePublished":"2026-05-31T12:00:00Z"}'
        "</script>"
    )
    dt = first_schema_date(html)
    assert dt is not None and dt.month == 5


def test_first_sitemap_lastmod() -> None:
    xml = "<urlset><url><loc>x</loc><lastmod>2026-06-10</lastmod></url></urlset>"
    dt = first_sitemap_lastmod(xml)
    assert dt is not None and dt.day == 10


def test_enricher_precedence_http_last_modified(fixed_now: datetime) -> None:
    enricher = ValidityEnricher()
    out = enricher.enrich(
        url="https://example.com/x",
        fetched_at=fixed_now,
        http_last_modified=datetime(2026, 1, 1, tzinfo=timezone.utc),
        html='"datePublished": "2026-04-04T00:00:00Z"',
    )
    assert out.valid_from_source == "http_last_modified"
    assert out.valid_from.month == 1


def test_enricher_falls_through_to_schema(fixed_now: datetime) -> None:
    enricher = ValidityEnricher()
    out = enricher.enrich(
        url="https://example.com/x",
        fetched_at=fixed_now,
        http_last_modified=None,
        html='"datePublished": "2026-04-04T00:00:00Z"',
    )
    assert out.valid_from_source == "schema_org_date_published"
    assert out.valid_from.month == 4


def test_enricher_falls_through_to_fetched_at(fixed_now: datetime) -> None:
    enricher = ValidityEnricher()
    out = enricher.enrich(
        url="https://example.com/x",
        fetched_at=fixed_now,
    )
    assert out.valid_from_source == "fetched_at"
    assert out.valid_from == fixed_now


def test_valid_to_collapses_when_invalid(fixed_now: datetime) -> None:
    enricher = ValidityEnricher()
    earlier = fixed_now.replace(year=2025)
    out = enricher.enrich(
        url="https://example.com/x",
        fetched_at=fixed_now,
        retraction_date=earlier,
    )
    # ``earlier`` is before fixed_now so the interval is invalid - clamps to None.
    assert out.valid_to is None
