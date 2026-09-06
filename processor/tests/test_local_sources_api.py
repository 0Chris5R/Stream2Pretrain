import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from processor.local_sources_api import LocalSourceStore
from schemas.sourcefeed import RateLimitSpec, SourceFeedSpec


def _spec(name: str = "arxiv-test") -> SourceFeedSpec:
    return SourceFeedSpec(
        name=name,
        protocol="rss",
        endpoint="https://rss.arxiv.org/rss/cs.CL",
        poll_interval_seconds=900,
        rate_limit=RateLimitSpec(requests_per_second=1, burst=2),
        license_default="per-record",
    )


def _seed(path: Path, *specs: SourceFeedSpec) -> None:
    path.write_text(
        json.dumps({"sources": [{"spec": spec.model_dump(mode="json")} for spec in specs]}),
        encoding="utf-8",
    )


def test_local_source_store_loads_read_only_configuration(tmp_path: Path) -> None:
    seed = tmp_path / "source_feeds.json"
    _seed(seed, _spec())

    store = LocalSourceStore(path=tmp_path / "missing-state.json", seed_path=seed)

    assert store.list_sources()[0]["name"] == "arxiv-test"
    assert store.list_sources()[0]["spec"]["enabled"] is True


@pytest.mark.asyncio
async def test_local_source_store_tracks_run_state(tmp_path: Path) -> None:
    seed = tmp_path / "source_feeds.json"
    _seed(seed, _spec())
    store = LocalSourceStore(path=tmp_path / "sources.json", seed_path=seed)
    assert await store.mark_polling("arxiv-test") == _spec()
    assert store.list_sources()[0]["poll_state"] == "polling"

    await store.finish("arxiv-test", emitted=3, seen_ids=["2608.00001"])
    status = store.list_sources()[0]
    assert status["poll_state"] == "idle"
    assert status["documents_24h"] == 3
    assert status["last_success_at"] is not None


@pytest.mark.asyncio
async def test_local_source_store_schedules_only_due_enabled_feeds(tmp_path: Path) -> None:
    seed = tmp_path / "source_feeds.json"
    disabled = _spec("disabled").model_copy(update={"enabled": False})
    _seed(seed, _spec("due"), _spec("recent"), disabled)
    store = LocalSourceStore(path=tmp_path / "sources.json", seed_path=seed)

    now = datetime(2026, 8, 15, 12, tzinfo=UTC)
    store.data["due"]["last_attempt_at"] = (now - timedelta(seconds=901)).isoformat()
    store.data["recent"]["last_attempt_at"] = (now - timedelta(seconds=899)).isoformat()

    assert store.due_names(now) == ["due"]
