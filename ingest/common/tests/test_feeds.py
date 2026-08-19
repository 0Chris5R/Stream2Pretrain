"""Tests for the feed loader."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from ingest.common.feeds import feeds_by_protocol, load_feeds_from_yaml
from schemas.sourcefeed import SourceFeedSpec


def _sample_feeds() -> list[dict]:
    return [
        {
            "name": "rss-arxiv-cs-cl",
            "protocol": "rss",
            "endpoint": "https://rss.arxiv.org/rss/cs.CL",
            "poll_interval_seconds": 7200,
            "rate_limit": {"requests_per_second": 1.0, "burst": 4},
            "license_default": "per-record",
        },
        {
            "name": "oai-arxiv-cs",
            "protocol": "oai-pmh",
            "endpoint": "https://oaipmh.arxiv.org/oai",
            "poll_interval_seconds": 7200,
            "rate_limit": {"requests_per_second": 4.0, "burst": 4},
        },
        {
            "name": "disabled-feed",
            "protocol": "rss",
            "endpoint": "https://example.com/rss",
            "poll_interval_seconds": 600,
            "rate_limit": {"requests_per_second": 1.0, "burst": 1},
            "enabled": False,
        },
    ]


def test_load_yaml_list(tmp_path: Path) -> None:
    p = tmp_path / "feeds.yaml"
    p.write_text(yaml.safe_dump(_sample_feeds()), encoding="utf-8")
    feeds = load_feeds_from_yaml(p)
    assert len(feeds) == 3
    assert all(isinstance(f, SourceFeedSpec) for f in feeds)


def test_load_yaml_with_top_key(tmp_path: Path) -> None:
    p = tmp_path / "feeds.yaml"
    p.write_text(yaml.safe_dump({"feeds": _sample_feeds()}), encoding="utf-8")
    feeds = load_feeds_from_yaml(p)
    assert {f.name for f in feeds} == {
        "rss-arxiv-cs-cl",
        "oai-arxiv-cs",
        "disabled-feed",
    }


def test_sourcefeed_accepts_kubernetes_camel_case() -> None:
    feed = SourceFeedSpec.model_validate(
        {
            "name": "rss-arxiv-cs-cl",
            "protocol": "rss",
            "endpoint": "https://rss.arxiv.org/rss/cs.CL",
            "pollIntervalSeconds": 7200,
            "rateLimit": {"requestsPerSecond": 1.0, "burst": 4},
            "licenseDefault": "per-record",
            "egressAllow": ["rss.arxiv.org"],
            "acceptContentTypes": ["text/html"],
        }
    )

    assert feed.poll_interval_seconds == 7200
    assert feed.rate_limit.requests_per_second == 1.0
    assert feed.license_default == "per-record"
    assert feed.model_dump(by_alias=True)["pollIntervalSeconds"] == 7200


def test_filter_protocol_excludes_disabled(tmp_path: Path) -> None:
    p = tmp_path / "feeds.yaml"
    p.write_text(yaml.safe_dump(_sample_feeds()), encoding="utf-8")
    feeds = load_feeds_from_yaml(p)
    rss = feeds_by_protocol(feeds, "rss")
    assert [f.name for f in rss] == ["rss-arxiv-cs-cl"]


def test_missing_path_raises(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        load_feeds_from_yaml(tmp_path / "nope.yaml")
