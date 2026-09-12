"""Tests for the per-feed state store."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

import ingest.common.state as state_module
from ingest.common.state import FeedStateStore, KubernetesCursorLease


def test_round_trip(tmp_path: Path) -> None:
    store = FeedStateStore(tmp_path)
    store.put("feed-a", {"etag": "abc", "last_modified": "Mon, 14 Jun 2026"})
    assert store.get("feed-a") == {"etag": "abc", "last_modified": "Mon, 14 Jun 2026"}


def test_missing_returns_empty(tmp_path: Path) -> None:
    store = FeedStateStore(tmp_path)
    assert store.get("nope") == {}


def test_corrupt_file_returns_empty(tmp_path: Path) -> None:
    store = FeedStateStore(tmp_path)
    p = tmp_path / "feed-a.json"
    p.write_text("{not json", encoding="utf-8")
    assert store.get("feed-a") == {}


def test_overwrite_atomic(tmp_path: Path) -> None:
    store = FeedStateStore(tmp_path)
    store.put("f", {"v": 1})
    store.put("f", {"v": 2})
    assert store.get("f") == {"v": 2}


def test_dev_state_root_override(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("S2P_STATE_ROOT", str(tmp_path / "state"))
    store = FeedStateStore(".s2p-state/hf")
    store.put("feed", {"cursor": "x"})

    assert (tmp_path / "state" / "hf" / "feed.json").exists()


def test_state_keys_are_portable_file_names(tmp_path: Path) -> None:
    store = FeedStateStore(tmp_path)
    store.put("source:cursor", {"cursor": "abc"})

    assert store.get("source:cursor") == {"cursor": "abc"}
    assert [path.name for path in tmp_path.iterdir()] == ["source%3Acursor.json"]


def test_legacy_state_file_is_read_during_filename_migration(tmp_path: Path) -> None:
    legacy = tmp_path / "rss-arxiv_cs_ai.json"
    legacy.write_text('{"etag": "old"}', encoding="utf-8")

    store = FeedStateStore(tmp_path)

    assert store.get("rss-arxiv/cs_ai") == {"etag": "old"}


class _S3Error(Exception):
    def __init__(self, code: str) -> None:
        self.response = {"Error": {"Code": code}}


class _FakeS3:
    def __init__(self, *, bucket_exists: bool = True) -> None:
        self.bucket_exists = bucket_exists
        self.objects: dict[tuple[str, str], bytes] = {}
        self.created: list[str] = []

    def head_bucket(self, **kwargs: str) -> None:
        assert kwargs["Bucket"]
        if not self.bucket_exists:
            raise _S3Error("NoSuchBucket")

    def create_bucket(self, **kwargs: str) -> None:
        self.bucket_exists = True
        self.created.append(kwargs["Bucket"])

    def get_object(self, **kwargs: str) -> dict[str, Any]:
        try:
            value = self.objects[(kwargs["Bucket"], kwargs["Key"])]
        except KeyError as exc:
            raise _S3Error("NoSuchKey") from exc
        from io import BytesIO

        return {"Body": BytesIO(value)}

    def put_object(self, **kwargs: Any) -> None:
        assert kwargs["ContentType"] == "application/json"
        self.objects[(kwargs["Bucket"], kwargs["Key"])] = kwargs["Body"].read()


def test_s3_round_trip_uses_component_scoped_objects(monkeypatch) -> None:
    monkeypatch.setenv("S2P_COMPONENT", "ingest-rss")
    fake = _FakeS3()
    store = FeedStateStore(
        "/var/lib/s2p-state/rss_poller",
        backend="s3",
        bucket="s2p-state",
        s3_client=fake,
    )

    assert store.get("rss-arxiv-cs-ai") == {}
    store.put("rss-arxiv-cs-ai", {"etag": "abc"})

    assert store.get("rss-arxiv-cs-ai") == {"etag": "abc"}
    assert (
        "s2p-state",
        "ingest-cursors/ingest-rss/rss-arxiv-cs-ai.json",
    ) in fake.objects


def test_s3_backend_creates_missing_state_bucket() -> None:
    fake = _FakeS3(bucket_exists=False)

    FeedStateStore("state", backend="s3", bucket="s2p-state", s3_client=fake)

    assert fake.created == ["s2p-state"]


def test_unknown_state_backend_is_rejected(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="unsupported feed-state backend"):
        FeedStateStore(tmp_path, backend="database")


class _KubeError(Exception):
    def __init__(self, status: int) -> None:
        self.status = status


class _FakeCoordinationApi:
    def __init__(self) -> None:
        self.lease: Any | None = None
        self.version = 0

    def read_namespaced_lease(self, *_: str) -> Any:
        if self.lease is None:
            raise _KubeError(404)
        return self.lease

    def create_namespaced_lease(self, _namespace: str, body: Any) -> None:
        if self.lease is not None:
            raise _KubeError(409)
        self.version += 1
        body.metadata.resource_version = str(self.version)
        self.lease = body

    def replace_namespaced_lease(self, _name: str, _namespace: str, body: Any) -> None:
        if self.lease is None:
            raise _KubeError(404)
        if body.metadata.resource_version != self.lease.metadata.resource_version:
            raise _KubeError(409)
        self.version += 1
        body.metadata.resource_version = str(self.version)
        self.lease = body


def test_kubernetes_cursor_lease_excludes_a_second_owner(monkeypatch) -> None:
    monkeypatch.setenv("S2P_NAMESPACE", "stream2pretrain")
    monkeypatch.setenv("S2P_COMPONENT", "ingest-hf-cards")
    api = _FakeCoordinationApi()
    first = KubernetesCursorLease("hf-models", duration_seconds=600, coordination_api=api)
    second = KubernetesCursorLease("hf-models", duration_seconds=600, coordination_api=api)

    assert first.try_acquire() is True
    assert second.try_acquire() is False

    first.release()
    assert second.try_acquire() is True


def test_kubernetes_cursor_lease_can_take_expired_owner(monkeypatch) -> None:
    monkeypatch.setenv("S2P_NAMESPACE", "stream2pretrain")
    api = _FakeCoordinationApi()
    first = KubernetesCursorLease("oai-arxiv", duration_seconds=1, coordination_api=api)
    second = KubernetesCursorLease("oai-arxiv", duration_seconds=1, coordination_api=api)

    assert first.try_acquire() is True
    api.lease.spec.renew_time = datetime.now(tz=UTC) - timedelta(seconds=2)

    assert second.try_acquire() is True
    assert api.lease.spec.holder_identity == second.identity


@pytest.mark.asyncio
async def test_cursor_lease_renews_while_the_owner_is_running(monkeypatch) -> None:
    monkeypatch.setenv("S2P_CURSOR_LEASE_BACKEND", "kubernetes")
    monkeypatch.setenv("S2P_CURSOR_LEASE_DURATION_SECONDS", "600")
    renewed = asyncio.Event()
    real_sleep = asyncio.sleep

    class FakeLease:
        instance: FakeLease | None = None

        def __init__(self, *_: object, **__: object) -> None:
            self.renewals = 0
            self.released = False
            FakeLease.instance = self

        def try_acquire(self) -> bool:
            return True

        def renew(self) -> bool:
            self.renewals += 1
            renewed.set()
            return True

        def release(self) -> None:
            self.released = True

    async def yield_once(_: float) -> None:
        await real_sleep(0)

    monkeypatch.setattr(state_module, "KubernetesCursorLease", FakeLease)
    monkeypatch.setattr(state_module.asyncio, "sleep", yield_once)

    async with state_module.cursor_lease("source-controller") as owns_cursor:
        assert owns_cursor is True
        await asyncio.wait_for(renewed.wait(), timeout=1)

    assert FakeLease.instance is not None
    assert FakeLease.instance.renewals >= 1
    assert FakeLease.instance.released is True


@pytest.mark.asyncio
async def test_cursor_lease_cancels_owner_when_renewal_is_lost(monkeypatch) -> None:
    monkeypatch.setenv("S2P_CURSOR_LEASE_BACKEND", "kubernetes")
    monkeypatch.setenv("S2P_CURSOR_LEASE_DURATION_SECONDS", "600")
    real_sleep = asyncio.sleep

    class FakeLease:
        instance: FakeLease | None = None

        def __init__(self, *_: object, **__: object) -> None:
            self.released = False
            FakeLease.instance = self

        def try_acquire(self) -> bool:
            return True

        def renew(self) -> bool:
            return False

        def release(self) -> None:
            self.released = True

    async def yield_once(_: float) -> None:
        await real_sleep(0)

    monkeypatch.setattr(state_module, "KubernetesCursorLease", FakeLease)
    monkeypatch.setattr(state_module.asyncio, "sleep", yield_once)

    with pytest.raises(asyncio.CancelledError, match="cursor lease lost: source-controller"):
        async with state_module.cursor_lease("source-controller") as owns_cursor:
            assert owns_cursor is True
            await asyncio.Future()

    assert FakeLease.instance is not None
    assert FakeLease.instance.released is True
