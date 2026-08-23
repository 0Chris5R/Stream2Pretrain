"""Tests for the OpenReview live poller."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import httpx
import pytest

from ingest.common.config import IngestConfig
from ingest.common.rate_limit import TokenBucket
from ingest.common.state import FeedStateStore
from ingest.common.tests.conftest import FakeMinio, FakeProducer  # type: ignore[attr-defined]
from ingest.openreview_poller import live


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
        request_jitter_max_seconds=0.0,
    )


def _mk_note(
    *,
    note_id: str,
    forum: str | None = None,
    invitation: str = "ICLR.cc/2026/Conference/-/Submission",
    pdf: str | None = "/pdf/abc.pdf",
    title: str = "Sample paper",
    cdate: int | None = 1718457600000,
    extra_content: dict[str, Any] | None = None,
) -> dict[str, Any]:
    content: dict[str, Any] = {
        "title": {"value": title},
        "abstract": {"value": "An abstract."},
        "license": {"value": "CC-BY-4.0"},
    }
    if pdf is not None:
        content["pdf"] = {"value": pdf}
    if extra_content:
        content.update({k: {"value": v} for k, v in extra_content.items()})
    return {
        "id": note_id,
        "forum": forum or note_id,
        "invitation": invitation,
        "cdate": cdate,
        "mdate": cdate,
        "content": content,
    }


class _FakeOR:
    """In-memory OpenReview client used in tests."""

    def __init__(
        self,
        *,
        submissions: dict[str, list[dict[str, Any]]],
        replies: dict[str, list[dict[str, Any]]] | None = None,
    ) -> None:
        self._submissions = submissions
        self._replies = replies or {}
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def get_all_notes(self, *, invitation: str, **kwargs: Any) -> list[dict[str, Any]]:
        self.calls.append(("get_all_notes", {"invitation": invitation, **kwargs}))
        return list(self._submissions.get(invitation, []))

    def get_notes(self, **kwargs: Any) -> list[dict[str, Any]]:
        self.calls.append(("get_notes", kwargs))
        forum = kwargs.get("forum") or ""
        return list(self._replies.get(forum, []))


def _pdf_handler(_: httpx.Request) -> httpx.Response:
    return httpx.Response(
        200,
        content=b"%PDF-1.4 fake body",
        headers={"content-type": "application/pdf"},
    )


def test_parse_venues_defaults_when_none() -> None:
    venues = live.parse_venues(None)
    assert len(venues) == len(live.DEFAULT_VENUES)
    assert venues[0].venue_id == "ICLR.cc/2026/Conference"


def test_parse_venues_handles_short_form_and_skips_invalid() -> None:
    venues = live.parse_venues(
        ["ICLR.cc/2026", "NeurIPS.cc/2025/Conference", "bad-entry", "noyear/abc"]
    )
    ids = [v.venue_id for v in venues]
    assert ids == ["ICLR.cc/2026/Conference", "NeurIPS.cc/2025/Conference"]


def test_extract_value_unwraps_v2_wrapper() -> None:
    assert live._extract_value({"value": 7}) == 7
    assert live._extract_value({"value": "x", "extra": 1}) == "x"
    assert live._extract_value("plain") == "plain"


def test_note_view_handles_missing_pdf() -> None:
    note = live._NoteView.from_obj(
        _mk_note(note_id="n1", pdf=None), venue_id="ICLR.cc/2026/Conference"
    )
    assert note.pdf_path is None
    assert note.title == "Sample paper"


def test_note_view_handles_relative_pdf_path() -> None:
    note = live._NoteView.from_obj(
        _mk_note(note_id="n2", pdf="pdf/relpath.pdf"),
        venue_id="ICLR.cc/2026/Conference",
    )
    assert note.pdf_path == "/pdf/relpath.pdf"


@pytest.mark.asyncio
async def test_poll_venue_emits_submission_and_reviews(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.chdir(tmp_path)
    venue = live.VenueSpec("ICLR.cc", 2026)
    invitation = venue.submission_invitation
    submission = _mk_note(note_id="paperA", forum="paperA")
    review = _mk_note(
        note_id="reviewA1",
        forum="paperA",
        invitation="ICLR.cc/2026/Conference/Paper1/-/Official_Review",
        pdf=None,
        extra_content={"review": "Solid empirical paper.", "rating": "8"},
    )
    decision = _mk_note(
        note_id="decisionA",
        forum="paperA",
        invitation="ICLR.cc/2026/Conference/Paper1/-/Decision",
        pdf=None,
        extra_content={"decision": "Accept (poster)"},
    )
    or_client = _FakeOR(
        submissions={invitation: [submission]},
        replies={"paperA": [submission, review, decision]},
    )

    transport = httpx.MockTransport(_pdf_handler)
    fake_producer = FakeProducer()
    fake_admissions = FakeProducer()
    fake_minio = FakeMinio()
    await fake_producer.start()
    await fake_admissions.start()
    await fake_minio.start()
    bucket = TokenBucket(rate=100.0, burst=10)
    state_store = FeedStateStore(tmp_path / "state")

    cfg = _cfg()
    async with httpx.AsyncClient(transport=transport) as http:
        submissions, reviews, skipped = await live.poll_venue(
            venue,
            cfg=cfg,
            producer=fake_producer,  # type: ignore[arg-type]
            minio=fake_minio,  # type: ignore[arg-type]
            http=http,
            or_client=or_client,
            state_store=state_store,
            bucket=bucket,
            admission_producer=fake_admissions,  # type: ignore[arg-type]
        )

    assert submissions == 1
    assert reviews == 2  # review + decision
    assert skipped == 0
    # One PDF + two review JSONs.
    assert len(fake_minio.objects) == 3
    pdf_keys = [k for k in fake_minio.objects if k.endswith(".pdf")]
    assert len(pdf_keys) == 1
    assert pdf_keys[0].startswith("openreview/venue=ICLR.cc/year=2026/")
    # Producer payloads carry the source_format.
    formats = [m["record"].source_format for m in fake_producer.sent]
    assert "pdf" in formats
    assert formats.count("review") == 2
    # State persisted so the next pass skips the same submission.
    persisted = state_store.get(venue.state_key)
    assert any(
        value.startswith("paperA:")
        for value in persisted.get("seen_submission_revisions") or []
    )
    assert any(
        value.startswith("reviewA1:") for value in persisted.get("seen_reply_revisions") or []
    )


@pytest.mark.asyncio
async def test_poll_venue_keeps_review_path_when_paper_license_is_unknown(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.chdir(tmp_path)
    venue = live.VenueSpec("ICLR.cc", 2026)
    submission = _mk_note(note_id="paperA", forum="paperA")
    submission["content"].pop("license")
    review = _mk_note(
        note_id="reviewA1",
        forum="paperA",
        invitation="ICLR.cc/2026/Conference/Paper1/-/Official_Review",
        pdf=None,
        extra_content={"review": "Independent public review evidence."},
    )
    review["content"].pop("license")
    or_client = _FakeOR(
        submissions={venue.submission_invitation: [submission]},
        replies={"paperA": [submission, review]},
    )
    fake_producer = FakeProducer()
    fake_admissions = FakeProducer()
    fake_minio = FakeMinio()
    await fake_producer.start()
    await fake_admissions.start()
    await fake_minio.start()

    async with httpx.AsyncClient(transport=httpx.MockTransport(_pdf_handler)) as http:
        submissions, reviews, skipped = await live.poll_venue(
            venue,
            cfg=_cfg(),
            producer=fake_producer,  # type: ignore[arg-type]
            minio=fake_minio,  # type: ignore[arg-type]
            http=http,
            or_client=or_client,
            state_store=FeedStateStore(tmp_path / "state"),
            bucket=TokenBucket(rate=100.0, burst=10),
            admission_producer=fake_admissions,  # type: ignore[arg-type]
        )

    assert (submissions, reviews, skipped) == (0, 1, 0)
    assert [message["record"].source_format for message in fake_producer.sent] == ["review"]


@pytest.mark.asyncio
async def test_poll_venue_skips_seen(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.chdir(tmp_path)
    venue = live.VenueSpec("ICLR.cc", 2026)
    invitation = venue.submission_invitation
    submission = _mk_note(note_id="paperA", forum="paperA")
    or_client = _FakeOR(submissions={invitation: [submission]})
    transport = httpx.MockTransport(_pdf_handler)
    fake_producer = FakeProducer()
    fake_admissions = FakeProducer()
    fake_minio = FakeMinio()
    await fake_producer.start()
    await fake_admissions.start()
    await fake_minio.start()
    bucket = TokenBucket(rate=100.0, burst=10)
    state_store = FeedStateStore(tmp_path / "state")
    state_store.put(venue.state_key, {"seen_note_ids": ["paperA"]})

    async with httpx.AsyncClient(transport=transport) as http:
        submissions, reviews, skipped = await live.poll_venue(
            venue,
            cfg=_cfg(),
            producer=fake_producer,  # type: ignore[arg-type]
            minio=fake_minio,  # type: ignore[arg-type]
            http=http,
            or_client=or_client,
            state_store=state_store,
            bucket=bucket,
            admission_producer=fake_admissions,  # type: ignore[arg-type]
        )

    assert submissions == 0
    assert reviews == 0
    assert skipped == 1
    assert fake_producer.sent == []


@pytest.mark.asyncio
async def test_poll_venue_skips_pdf_failures(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.chdir(tmp_path)
    venue = live.VenueSpec("NeurIPS.cc", 2025)
    invitation = venue.submission_invitation
    submission = _mk_note(note_id="bad1", forum="bad1", invitation=invitation)
    or_client = _FakeOR(submissions={invitation: [submission]})

    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(404, content=b"")

    transport = httpx.MockTransport(handler)
    fake_producer = FakeProducer()
    fake_admissions = FakeProducer()
    fake_minio = FakeMinio()
    await fake_producer.start()
    await fake_admissions.start()
    await fake_minio.start()
    bucket = TokenBucket(rate=100.0, burst=10)
    state_store = FeedStateStore(tmp_path / "state")

    async with httpx.AsyncClient(transport=transport) as http:
        submissions, reviews, skipped = await live.poll_venue(
            venue,
            cfg=_cfg(),
            producer=fake_producer,  # type: ignore[arg-type]
            minio=fake_minio,  # type: ignore[arg-type]
            http=http,
            or_client=or_client,
            state_store=state_store,
            bucket=bucket,
            admission_producer=fake_admissions,  # type: ignore[arg-type]
        )

    assert submissions == 0
    assert reviews == 0
    assert skipped == 0
    assert fake_producer.sent == []


@pytest.mark.asyncio
async def test_run_pass_fails_closed_when_venue_listing_is_blocked(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.chdir(tmp_path)

    class _BlockedOR(_FakeOR):
        def get_all_notes(self, *, invitation: str, **kwargs: Any) -> list[dict[str, Any]]:
            raise RuntimeError("Challenge verification required")

    fake_producer = FakeProducer()
    fake_admissions = FakeProducer()
    fake_minio = FakeMinio()
    monkeypatch.setattr(live, "BronzeProducer", lambda *a, **kw: fake_producer)
    monkeypatch.setattr(live, "LicenseAdmissionProducer", lambda *a, **kw: fake_admissions)
    monkeypatch.setattr(live, "MinioWriter", lambda *a, **kw: fake_minio)
    monkeypatch.setattr(
        live,
        "build_async_client",
        lambda *a, **kw: httpx.AsyncClient(transport=httpx.MockTransport(_pdf_handler)),
    )

    with pytest.raises(RuntimeError, match="OpenReview venue polling failed"):
        await live.run_pass(
            _cfg(),
            venues=[live.VenueSpec("ICLR.cc", 2026)],
            or_client=_BlockedOR(submissions={}),
            state_dir=str(tmp_path / "state"),
        )
