"""GitHub Releases Atom poller (CronJob).

Iterates the curated repo list (``repo_filter.CURATED_REPOS`` from the events
module). For each repo:

- ``GET https://github.com/<owner>/<repo>/releases.atom`` with conditional GET
- parse with feedparser
- for each entry, store the entry XML in bronze and emit a BronzeRecord whose
  URL is the release's ``link`` field (the GitHub release HTML page)

Anonymous Atom requests are not subject to the GitHub REST 60 req/h floor
(they are static XML pages), but we still cap politeness via TokenBucket.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any
from xml.etree import ElementTree as ET

import feedparser
import httpx

from ingest.common.config import IngestConfig, load_config
from ingest.common.hashing import doc_id_for_url
from ingest.common.http_client import build_async_client, build_headers
from ingest.common.kafka_producer import BronzeProducer
from ingest.common.logging import configure_logging, get_logger
from ingest.common.minio_writer import MinioWriter
from ingest.common.otel import init_tracer
from ingest.common.rate_limit import TokenBucket
from ingest.common.s3 import bronze_object_key, bronze_s3_uri
from ingest.common.state import FeedStateStore
from ingest.github_events.repo_filter import CURATED_REPOS
from schemas.bronze import BronzeRecord

log = get_logger(__name__)

ATOM_NS = "http://www.w3.org/2005/Atom"
SOURCE_FEED = "github-releases"


async def fetch_releases_atom(
    client: httpx.AsyncClient,
    owner_repo: str,
    *,
    etag: str | None = None,
) -> tuple[int, str, str | None]:
    """Return (status, body_text, new_etag)."""
    url = f"https://github.com/{owner_repo}/releases.atom"
    headers: dict[str, str] = {}
    if etag:
        headers["If-None-Match"] = etag
    resp = await client.get(url, headers=headers)
    return resp.status_code, resp.text, resp.headers.get("etag")


def _entry_xml(entry: Any) -> bytes:
    """Serialize an entry's raw XML for bronze storage."""
    root = ET.Element(f"{{{ATOM_NS}}}entry")
    for k, v in (entry or {}).items():
        if isinstance(v, str):
            el = ET.SubElement(root, f"{{{ATOM_NS}}}{k}")
            el.text = v
    return ET.tostring(root, encoding="utf-8")


async def poll_repo(
    owner_repo: str,
    *,
    cfg: IngestConfig,
    client: httpx.AsyncClient,
    producer: BronzeProducer,
    minio: MinioWriter,
    state_store: FeedStateStore,
    bucket: TokenBucket,
) -> int:
    """One pass over one repo's releases.atom feed."""
    state_key = f"{SOURCE_FEED}:{owner_repo.replace('/', '_')}"
    feed_state = state_store.get(state_key)
    etag = feed_state.get("etag")

    await bucket.acquire()
    try:
        status, body, new_etag = await fetch_releases_atom(client, owner_repo, etag=etag)
    except httpx.HTTPError as exc:
        log.warning("releases.fetch_failed", repo=owner_repo, err=str(exc))
        return 0

    if status == 304:
        return 0
    if status >= 400:
        log.warning("releases.bad_status", repo=owner_repo, status=status)
        return 0

    parsed: Any = feedparser.parse(body)
    seen: set[str] = set(feed_state.get("seen_doc_ids", []))
    emitted = 0
    for entry in parsed.get("entries", []):
        link = entry.get("link")
        if not link or not isinstance(link, str) or not link.startswith("http"):
            continue
        try:
            doc_id = doc_id_for_url(link)
        except ValueError:
            continue
        if doc_id in seen:
            continue
        seen.add(doc_id)
        fetched_at = datetime.now(tz=timezone.utc)
        payload = _entry_xml(entry)
        key = bronze_object_key(
            source_feed=SOURCE_FEED,
            doc_id=doc_id,
            fetched_at=fetched_at,
            extension="release.atom.xml.gz",
        )
        stored = await minio.put_bronze(
            key=key,
            payload=payload,
            content_type="application/atom+xml",
            gzip_compress=True,
            metadata={
                "doc_id": doc_id,
                "github_repo": owner_repo,
                "release_link": link,
            },
        )
        record = BronzeRecord(
            doc_id=doc_id,
            url=link,  # type: ignore[arg-type]
            fetched_at=fetched_at,
            http_status=200,
            content_type="application/atom+xml",
            raw_html_s3_uri=bronze_s3_uri(
                bucket=cfg.minio_bronze_bucket,
                source_feed=SOURCE_FEED,
                doc_id=doc_id,
                fetched_at=fetched_at,
                extension="release.atom.xml.gz",
            ),
            source_feed=SOURCE_FEED,
            trace_id=_trace_id(),
            bytes_size=stored,
        )
        await producer.send(record, headers={"github_repo": owner_repo})
        emitted += 1

    if len(seen) > 2000:
        seen = set(list(seen)[-1000:])
    state_store.put(state_key, {"etag": new_etag, "seen_doc_ids": sorted(seen)})
    return emitted


def _trace_id() -> str:
    import secrets

    return secrets.token_hex(16)


async def run_pass(cfg: IngestConfig, repos: list[str]) -> int:
    state_root = (
        "/var/lib/s2p-state/github_releases" if not cfg.is_dev else "./.s2p-state/gh_releases"
    )
    store = FeedStateStore(state_root)
    headers = build_headers(cfg, accept="application/atom+xml")
    bucket = TokenBucket(rate=2.0, burst=4)
    total = 0
    async with build_async_client(cfg, headers=headers) as client, BronzeProducer(
        cfg.redpanda_brokers, topic=cfg.raw_topic, client_id="s2p-github-releases"
    ) as producer, MinioWriter(
        cfg.minio_endpoint,
        cfg.minio_access_key,
        cfg.minio_secret_key,
        bucket=cfg.minio_bronze_bucket,
    ) as minio:
        for repo in repos:
            try:
                total += await poll_repo(
                    repo,
                    cfg=cfg,
                    client=client,
                    producer=producer,
                    minio=minio,
                    state_store=store,
                    bucket=bucket,
                )
            except Exception as exc:  # noqa: BLE001
                log.exception("releases.repo_error", repo=repo, err=str(exc))
    return total


def main() -> None:
    cfg = load_config()
    configure_logging(cfg.log_level, json_output=not cfg.is_dev)
    init_tracer("ingest.github_releases", cfg)
    repos = sorted(CURATED_REPOS)
    log.info("github_releases.start", repos=len(repos))
    total = asyncio.run(run_pass(cfg, repos))
    log.info("github_releases.done", emitted=total)


if __name__ == "__main__":
    main()
