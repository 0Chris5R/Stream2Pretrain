"""GitHub Events long-running poller.

Endpoint: ``GET /events`` (https://docs.github.com/en/rest/activity/events).

Behaviour:

- Sends ``If-None-Match`` with the last seen ETag; on 304 the response does
  not count toward the rate budget.
- Sleeps for ``X-Poll-Interval`` seconds between calls (default 60s).
- Filters events by repo (see ``repo_filter``).
- For each surviving event, writes the JSON event to bronze and emits a
  ``BronzeRecord``. The ``url`` field is the event's ``html_url`` when present,
  else the API ``url``.
- Watches the rate-limit headers and backs off on 403/429.
"""

from __future__ import annotations

import asyncio
import json
import signal
from contextlib import suppress
from datetime import UTC, datetime
from typing import Any

import httpx

from ingest.common.config import IngestConfig, load_config
from ingest.common.hashing import doc_id_for_url
from ingest.common.http_client import build_async_client, build_headers
from ingest.common.kafka_producer import BronzeProducer
from ingest.common.logging import configure_logging, get_logger
from ingest.common.metrics import INGEST_METRICS
from ingest.common.minio_writer import MinioWriter
from ingest.common.otel import init_tracer
from ingest.common.probes import start_probe_server
from ingest.common.s3 import bronze_object_key, bronze_s3_uri
from ingest.common.state import FeedStateStore
from ingest.github_events.repo_filter import is_relevant_repo
from schemas.bronze import BronzeRecord

log = get_logger(__name__)

GITHUB_EVENTS_URL = "https://api.github.com/events"
SOURCE_FEED = "github-events"
DEFAULT_POLL_INTERVAL = 60.0


def _event_url(evt: dict[str, Any]) -> str | None:
    """Pull a stable URL out of a GitHub event payload.

    Preference order: payload.pull_request.html_url, payload.issue.html_url,
    payload.release.html_url, payload.comment.html_url, repo.url, fallback to
    the API URL of the event itself.
    """
    payload = evt.get("payload") or {}
    for path in (
        ("pull_request", "html_url"),
        ("issue", "html_url"),
        ("release", "html_url"),
        ("comment", "html_url"),
    ):
        cur: Any = payload
        ok = True
        for key in path:
            if not isinstance(cur, dict) or key not in cur:
                ok = False
                break
            cur = cur[key]
        if ok and isinstance(cur, str):
            return cur
    repo = evt.get("repo") or {}
    if isinstance(repo, dict) and isinstance(repo.get("url"), str):
        api = repo["url"]
        return api.replace("https://api.github.com/repos/", "https://github.com/")
    return None


async def _process_events(
    events: list[dict[str, Any]],
    *,
    producer: BronzeProducer,
    minio: MinioWriter,
    cfg: IngestConfig,
    seen: set[str],
) -> int:
    emitted = 0
    for evt in events:
        repo = (evt.get("repo") or {}).get("name")
        if not repo or not is_relevant_repo(repo):
            continue
        url = _event_url(evt)
        if not url:
            continue
        if not url.startswith("http"):
            continue
        try:
            doc_id = doc_id_for_url(url)
        except ValueError:
            continue
        if doc_id in seen:
            continue
        seen.add(doc_id)
        fetched_at = datetime.now(tz=UTC)
        body = json.dumps(evt, sort_keys=True).encode("utf-8")
        key = bronze_object_key(
            source_feed=SOURCE_FEED,
            doc_id=doc_id,
            fetched_at=fetched_at,
            extension="event.json.gz",
        )
        stored = await minio.put_bronze(
            key=key,
            payload=body,
            content_type="application/json",
            gzip_compress=True,
            metadata={
                "doc_id": doc_id,
                "github_event_id": str(evt.get("id", "")),
                "github_event_type": str(evt.get("type", "")),
                "github_repo": repo,
            },
        )
        record = BronzeRecord(
            doc_id=doc_id,
            url=url,  # type: ignore[arg-type]
            fetched_at=fetched_at,
            http_status=200,
            content_type="application/json",
            raw_html_s3_uri=bronze_s3_uri(
                bucket=cfg.minio_bronze_bucket,
                source_feed=SOURCE_FEED,
                doc_id=doc_id,
                fetched_at=fetched_at,
                extension="event.json.gz",
            ),
            source_feed=SOURCE_FEED,
            trace_id=_trace_id(),
            bytes_size=stored,
        )
        await producer.send(record, headers={"github_event_type": str(evt.get("type", ""))})
        emitted += 1
    return emitted


def _trace_id() -> str:
    import secrets

    return secrets.token_hex(16)


async def run_loop(cfg: IngestConfig, *, max_iterations: int | None = None) -> int:
    """Main loop. Returns total emitted records when stopped."""
    if not cfg.github_token:
        raise RuntimeError(
            "GITHUB_TOKEN is required: anonymous /events is rate-limited to 60 req/h"
        )
    state_root = (
        "/var/lib/s2p-state/github_events" if not cfg.is_dev else "./.s2p-state/github_events"
    )
    state = FeedStateStore(state_root)
    feed_state = state.get(SOURCE_FEED)
    seen: set[str] = set(feed_state.get("seen_doc_ids", []))
    last_etag = feed_state.get("etag")

    headers = build_headers(
        cfg,
        accept="application/vnd.github+json",
        extra={
            "Authorization": f"Bearer {cfg.github_token}",
            "X-GitHub-Api-Version": "2022-11-28",
        },
    )

    stop = asyncio.Event()

    def _trigger_stop(*_: object) -> None:
        stop.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        with suppress(NotImplementedError):
            loop.add_signal_handler(sig, _trigger_stop)

    iteration = 0
    total_emitted = 0
    async with (
        build_async_client(cfg, headers=headers) as client,
        BronzeProducer(
            cfg.redpanda_brokers, topic=cfg.raw_topic, client_id="s2p-github-events"
        ) as producer,
        MinioWriter(
            cfg.minio_endpoint,
            cfg.minio_access_key,
            cfg.minio_secret_key,
            bucket=cfg.minio_bronze_bucket,
        ) as minio,
    ):
        while not stop.is_set():
            req_headers: dict[str, str] = {}
            if last_etag:
                req_headers["If-None-Match"] = last_etag
            try:
                resp = await client.get(GITHUB_EVENTS_URL, headers=req_headers)
            except httpx.HTTPError as exc:
                log.warning("github_events.transport_error", err=str(exc))
                INGEST_METRICS.record_feed_poll(source_feed=SOURCE_FEED, outcome="error")
                await _sleep_or_stop(stop, DEFAULT_POLL_INTERVAL)
                continue

            poll_interval = float(resp.headers.get("x-poll-interval", DEFAULT_POLL_INTERVAL))
            if resp.status_code == 304:
                log.debug("github_events.not_modified")
                INGEST_METRICS.record_feed_poll(source_feed=SOURCE_FEED, outcome="not_modified")
            elif resp.status_code == 200:
                INGEST_METRICS.record_feed_poll(source_feed=SOURCE_FEED, outcome="success")
                last_etag = resp.headers.get("etag", last_etag)
                try:
                    events = resp.json()
                except ValueError:
                    events = []
                if isinstance(events, list):
                    emitted = await _process_events(
                        events,
                        producer=producer,
                        minio=minio,
                        cfg=cfg,
                        seen=seen,
                    )
                    total_emitted += emitted
                    log.info(
                        "github_events.batch",
                        events=len(events),
                        emitted=emitted,
                        rate_limit_remaining=resp.headers.get("x-ratelimit-remaining"),
                    )
            elif resp.status_code in {403, 429}:
                # Secondary rate limit. Honour Retry-After if present, else back off.
                retry_after = float(resp.headers.get("retry-after", "60"))
                log.warning(
                    "github_events.rate_limited",
                    status=resp.status_code,
                    retry_after=retry_after,
                )
                INGEST_METRICS.record_feed_poll(source_feed=SOURCE_FEED, outcome="rate_limited")
                await _sleep_or_stop(stop, retry_after)
                continue
            else:
                log.warning("github_events.unexpected_status", status=resp.status_code)
                INGEST_METRICS.record_feed_poll(source_feed=SOURCE_FEED, outcome="error")

            # Persist truncated seen-set so memory stays bounded across restarts.
            if len(seen) > 5000:
                seen = set(list(seen)[-2500:])
            state.put(
                SOURCE_FEED,
                {
                    "etag": last_etag,
                    "seen_doc_ids": sorted(seen),
                },
            )
            iteration += 1
            if max_iterations is not None and iteration >= max_iterations:
                break
            await _sleep_or_stop(stop, poll_interval)
    return total_emitted


async def _sleep_or_stop(stop: asyncio.Event, seconds: float) -> None:
    with suppress(TimeoutError):
        await asyncio.wait_for(stop.wait(), timeout=max(0.1, seconds))


def main() -> None:
    cfg = load_config()
    configure_logging(cfg.log_level, json_output=not cfg.is_dev)
    init_tracer("ingest.github_events", cfg)
    log.info("github_events.start")
    start_probe_server()
    total = asyncio.run(run_loop(cfg))
    log.info("github_events.exit", total_emitted=total)


if __name__ == "__main__":
    main()
