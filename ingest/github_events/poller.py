"""GitHub Events long-running poller.

Endpoint: ``GET /events`` or round-robin ``GET /orgs/{org}/events`` for the
configured AI organizations.

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

import argparse
import asyncio
import json
import signal
from contextlib import suppress
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx

from ingest.common.config import IngestConfig, load_config
from ingest.common.hashing import doc_id_for_url
from ingest.common.http_client import build_async_client, build_headers
from ingest.common.kafka_producer import BronzeProducer, LicenseAdmissionProducer
from ingest.common.license_admission import decide_license_admission
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
    admission_producer: LicenseAdmissionProducer | None = None,
    ai_org_filter: frozenset[str] = frozenset(),
) -> int:
    emitted = 0
    for evt in events:
        repo = (evt.get("repo") or {}).get("name")
        owner = repo.split("/", 1)[0].lower() if isinstance(repo, str) and "/" in repo else ""
        if not repo or (not is_relevant_repo(repo) and owner not in ai_org_filter):
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
        admission = decide_license_admission(
            source_url=url,
            source_feed=SOURCE_FEED,
            license_value=None,
            license_source="unknown",
            source_format="metadata",
        )
        if admission_producer is not None:
            await admission_producer.send(admission.decision)
        if not admission.fetch_allowed:
            continue
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
            trace_id=admission.decision.trace_id,
            bytes_size=stored,
            source_format="metadata",
            extraction_pipeline="github-events-api-json-v1",
            spdx_license=admission.license_id,
            spdx_license_source="unknown",
            training_usage=admission.training_usage,
        )
        await producer.send(record, headers={"github_event_type": str(evt.get("type", ""))})
        emitted += 1
    return emitted


def _trace_id() -> str:
    import secrets

    return secrets.token_hex(16)


async def run_loop(
    cfg: IngestConfig,
    *,
    max_iterations: int | None = None,
    ai_org_filter: frozenset[str] = frozenset(),
) -> int:
    """Main loop. Returns total emitted records when stopped."""
    state_root = (
        "/var/lib/s2p-state/github_events" if not cfg.is_dev else "./.s2p-state/github_events"
    )
    state = FeedStateStore(state_root)
    feed_state = state.get(SOURCE_FEED)
    seen: set[str] = set(feed_state.get("seen_doc_ids", []))
    legacy_etag = feed_state.get("etag")
    raw_etags = feed_state.get("etags", {})
    etags: dict[str, str] = (
        {str(key): str(value) for key, value in raw_etags.items()}
        if isinstance(raw_etags, dict)
        else {}
    )
    targets = (
        [f"https://api.github.com/orgs/{org}/events" for org in sorted(ai_org_filter)]
        if ai_org_filter
        else [GITHUB_EVENTS_URL]
    )
    if legacy_etag and GITHUB_EVENTS_URL not in etags:
        etags[GITHUB_EVENTS_URL] = str(legacy_etag)
    target_index = int(feed_state.get("target_index", 0)) % len(targets)

    anonymous_headers = build_headers(
        cfg,
        accept="application/vnd.github+json",
        extra={"X-GitHub-Api-Version": "2022-11-28"},
    )
    authenticated_headers = dict(anonymous_headers)
    if cfg.github_token:
        authenticated_headers["Authorization"] = f"Bearer {cfg.github_token}"

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
        build_async_client(cfg, headers=authenticated_headers) as authenticated_client,
        build_async_client(cfg, headers=anonymous_headers) as anonymous_client,
        BronzeProducer(
            cfg.redpanda_brokers, topic=cfg.raw_topic, client_id="s2p-github-events"
        ) as producer,
        LicenseAdmissionProducer(
            cfg.redpanda_brokers,
            topic=cfg.license_admissions_topic,
            client_id="s2p-github-events-license-admission",
        ) as admission_producer,
        MinioWriter(
            cfg.minio_endpoint,
            cfg.minio_access_key,
            cfg.minio_secret_key,
            bucket=cfg.minio_bronze_bucket,
        ) as minio,
    ):
        client = authenticated_client if cfg.github_token else anonymous_client
        using_authenticated_client = bool(cfg.github_token)
        while not stop.is_set():
            target_url = targets[target_index]
            req_headers: dict[str, str] = {}
            if etags.get(target_url):
                req_headers["If-None-Match"] = etags[target_url]
            try:
                resp = await client.get(target_url, headers=req_headers)
            except httpx.HTTPError as exc:
                log.warning("github_events.transport_error", err=str(exc))
                INGEST_METRICS.record_feed_poll(source_feed=SOURCE_FEED, outcome="error")
                await _sleep_or_stop(stop, DEFAULT_POLL_INTERVAL)
                continue

            poll_interval = float(resp.headers.get("x-poll-interval", DEFAULT_POLL_INTERVAL))
            successful_poll = False
            if resp.status_code == 304:
                log.info("github_events.not_modified", target=target_url)
                INGEST_METRICS.record_feed_poll(source_feed=SOURCE_FEED, outcome="not_modified")
                successful_poll = True
            elif resp.status_code == 200:
                try:
                    events = resp.json()
                except ValueError as exc:
                    log.warning("github_events.invalid_json", target=target_url, err=str(exc))
                    INGEST_METRICS.record_feed_poll(source_feed=SOURCE_FEED, outcome="error")
                    events = None
                if isinstance(events, list):
                    INGEST_METRICS.record_feed_poll(source_feed=SOURCE_FEED, outcome="success")
                    if resp.headers.get("etag"):
                        etags[target_url] = resp.headers["etag"]
                    emitted = await _process_events(
                        events,
                        producer=producer,
                        minio=minio,
                        cfg=cfg,
                        seen=seen,
                        admission_producer=admission_producer,
                        ai_org_filter=ai_org_filter,
                    )
                    total_emitted += emitted
                    log.info(
                        "github_events.batch",
                        events=len(events),
                        emitted=emitted,
                        target=target_url,
                        rate_limit_remaining=resp.headers.get("x-ratelimit-remaining"),
                    )
                    successful_poll = True
                elif events is not None:
                    log.warning(
                        "github_events.invalid_shape",
                        target=target_url,
                        payload_type=type(events).__name__,
                    )
                    INGEST_METRICS.record_feed_poll(source_feed=SOURCE_FEED, outcome="error")
            elif resp.status_code == 401 and using_authenticated_client:
                # Public events remain available without credentials. A stale
                # optional token must reduce the rate budget, not disable the
                # source indefinitely.
                log.warning("github_events.authentication_rejected_falling_back")
                INGEST_METRICS.record_feed_poll(
                    source_feed=SOURCE_FEED,
                    outcome="authentication_rejected",
                )
                client = anonymous_client
                using_authenticated_client = False
                continue
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

            if successful_poll:
                # Persist only verified responses; malformed success pages and
                # upstream errors must not commit a cursor or ETag.
                if len(seen) > 5000:
                    seen = set(list(seen)[-2500:])
                state.put(
                    SOURCE_FEED,
                    {
                        "etags": etags,
                        "seen_doc_ids": sorted(seen),
                        "target_index": (target_index + 1) % len(targets),
                        "last_success_at": datetime.now(UTC).isoformat(),
                    },
                )
            target_index = (target_index + 1) % len(targets)
            iteration += 1
            if max_iterations is not None and iteration >= max_iterations:
                break
            await _sleep_or_stop(stop, poll_interval)
    return total_emitted


async def _sleep_or_stop(stop: asyncio.Event, seconds: float) -> None:
    with suppress(TimeoutError):
        await asyncio.wait_for(stop.wait(), timeout=max(0.1, seconds))


def _load_ai_org_filter(config_path: str | None) -> frozenset[str]:
    """Load the chart-owned organization filter instead of silently ignoring it."""
    if not config_path:
        return frozenset()
    payload = json.loads(Path(config_path).read_text(encoding="utf-8"))
    events = payload.get("events", {}) if isinstance(payload, dict) else {}
    raw = events.get("ai_org_filter", []) if isinstance(events, dict) else []
    if not isinstance(raw, list) or not all(isinstance(value, str) for value in raw):
        raise ValueError("events.ai_org_filter must be a list of strings")
    return frozenset(value.lower() for value in raw)


def main() -> None:
    parser = argparse.ArgumentParser(description="Poll GitHub's public event stream")
    parser.add_argument("--config", default=None)
    args = parser.parse_args()
    cfg = load_config()
    configure_logging(cfg.log_level, json_output=not cfg.is_dev)
    init_tracer("ingest.github_events", cfg)
    log.info("github_events.start")
    start_probe_server()
    total = asyncio.run(run_loop(cfg, ai_org_filter=_load_ai_org_filter(args.config)))
    log.info("github_events.exit", total_emitted=total)


if __name__ == "__main__":
    main()
