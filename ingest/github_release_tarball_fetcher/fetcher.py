"""GitHub release tarball fetcher worker.

A long-running consumer that:

1. Subscribes to ``raw.fetched`` (Redpanda) and filters for messages whose
   ``source_feed`` header is ``github-releases``. Other sources are ignored
   without commit lag.
2. For each release event, derives ``(owner/repo, tag)`` from the release
   atom URL embedded in the BronzeRecord and asks the GitHub License API
   for the SPDX id. Non-permissive licenses cause the release to be skipped.
3. Issues a single ``GET /repos/{o}/{r}/tarball/{tag}`` request via the
   shared retry-aware async HTTP client. The response (a ``tar.gz``) is
   handed to :mod:`ingest.github_release_tarball_fetcher.extractor` for
   stream-extraction.
4. For each extracted file, writes the raw bytes to MinIO under
   ``s3://<bronze-bucket>/code/repo=<owner>__<repo>/ref=<tag>/<path>`` and
   emits one :class:`schemas.bronze.BronzeRecord` with ``source_format="code"``.

Rate budget
-----------
Worst case is one ``/tarball`` plus one ``/repos/{o}/{r}`` per release. The
existing ``ingest/common/rate_limit.TokenBucket`` enforces a per-pod ceiling
configured well below the GitHub REST 5000 req/h threshold (default
``1.0 req/s`` -> 3600 req/h budget per pod, leaving headroom for the
``github_releases`` poller running in the same token namespace).
"""

from __future__ import annotations

import asyncio
import re
import secrets
import threading
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any
from urllib.parse import urlsplit

import httpx

from ingest.common.config import IngestConfig, load_config
from ingest.common.hashing import doc_id_for_url
from ingest.common.http_client import build_async_client, build_headers
from ingest.common.kafka_producer import LicenseAdmissionProducer
from ingest.common.license_admission import decide_license_admission
from ingest.common.logging import configure_logging, get_logger
from ingest.common.minio_writer import MinioWriter
from ingest.common.otel import init_tracer
from ingest.common.probes import start_probe_server
from ingest.common.rate_limit import TokenBucket
from ingest.github_release_tarball_fetcher.extractor import (
    DEFAULT_ALLOWED_EXTENSIONS,
    DEFAULT_MAX_FILE_SIZE_BYTES,
    ExtractedFile,
    iter_tarball_files,
)
from schemas.bronze import BronzeRecord

if TYPE_CHECKING:
    from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

log = get_logger(__name__)

SOURCE_FEED = "github-release-tarballs"
UPSTREAM_FEED = "github-releases"

DEFAULT_ALLOWED_LICENSES: frozenset[str] = frozenset(
    {
        "Apache-2.0",
        "MIT",
        "BSD-2-Clause",
        "BSD-3-Clause",
        "ISC",
        "MPL-2.0",
        "Unlicense",
        "CC0-1.0",
    }
)

# https://github.com/<owner>/<repo>/releases/tag/<tag>
_RELEASE_URL_RE = re.compile(
    r"^/(?P<owner>[A-Za-z0-9._-]+)/(?P<repo>[A-Za-z0-9._-]+)/releases/tag/(?P<tag>.+)$"
)


@dataclass(frozen=True)
class ReleaseRef:
    """Parsed identifiers for one GitHub release."""

    owner: str
    repo: str
    tag: str

    @property
    def full_name(self) -> str:
        return f"{self.owner}/{self.repo}"

    @property
    def tarball_url(self) -> str:
        return f"https://api.github.com/repos/{self.owner}/{self.repo}/tarball/{self.tag}"


@dataclass(frozen=True)
class FetcherConfig:
    """Static configuration for the worker (env-derived)."""

    allowed_extensions: tuple[str, ...] = DEFAULT_ALLOWED_EXTENSIONS
    max_file_size_bytes: int = DEFAULT_MAX_FILE_SIZE_BYTES
    allowed_licenses: frozenset[str] = DEFAULT_ALLOWED_LICENSES
    request_rate_per_second: float = 1.0
    request_burst: int = 4
    consumer_group: str = "s2p-github-tarball-fetcher"


class TarballMetrics:
    """Prometheus text metrics for the GitHub release tarball worker."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._seen = 0
        self._inflight = 0
        self._processed = 0
        self._failed = 0
        self._emitted = 0

    def release_started(self) -> None:
        with self._lock:
            self._seen += 1
            self._inflight += 1

    def release_finished(self, *, emitted: int, failed: bool) -> None:
        with self._lock:
            self._inflight = max(0, self._inflight - 1)
            if failed:
                self._failed += 1
            else:
                self._processed += 1
                self._emitted += emitted

    def render_prometheus(self) -> bytes:
        with self._lock:
            lines = [
                "# HELP s2p_process_up Process-level liveness.",
                "# TYPE s2p_process_up gauge",
                "s2p_process_up 1",
                "# HELP s2p_github_releases_seen_total GitHub release records accepted by the tarball fetcher.",
                "# TYPE s2p_github_releases_seen_total counter",
                f"s2p_github_releases_seen_total {self._seen}",
                "# HELP s2p_github_releases_unprocessed GitHub release records currently being processed by this worker.",
                "# TYPE s2p_github_releases_unprocessed gauge",
                f"s2p_github_releases_unprocessed {self._inflight}",
                "# HELP s2p_github_releases_processed_total GitHub release records completed successfully by the tarball fetcher.",
                "# TYPE s2p_github_releases_processed_total counter",
                f"s2p_github_releases_processed_total {self._processed}",
                "# HELP s2p_github_releases_failed_total GitHub release records that failed tarball processing.",
                "# TYPE s2p_github_releases_failed_total counter",
                f"s2p_github_releases_failed_total {self._failed}",
                "# HELP s2p_github_code_records_emitted_total Code BronzeRecords emitted by GitHub tarball fetcher.",
                "# TYPE s2p_github_code_records_emitted_total counter",
                f"s2p_github_code_records_emitted_total {self._emitted}",
            ]
        return ("\n".join(lines) + "\n").encode("utf-8")


def parse_release_url(url: str) -> ReleaseRef | None:
    """Extract ``(owner, repo, tag)`` from a GitHub release HTML URL.

    Returns ``None`` for any URL that is not a release tag link.
    """
    if not url:
        return None
    parts = urlsplit(url)
    if parts.scheme not in {"http", "https"}:
        return None
    if (parts.hostname or "").lower() not in {"github.com", "www.github.com"}:
        return None
    m = _RELEASE_URL_RE.match(parts.path)
    if not m:
        return None
    return ReleaseRef(
        owner=m.group("owner"),
        repo=m.group("repo"),
        tag=m.group("tag"),
    )


def code_object_key(*, owner: str, repo: str, ref: str, path: str) -> str:
    """Build the bronze object key for one extracted source file.

    Layout: ``code/repo=<owner>__<repo>/ref=<tag>/<path>``. Slashes in ``path``
    are preserved; the repo and ref segments use safe separators.
    """
    safe_repo = repo.replace("/", "_")
    safe_owner = owner.replace("/", "_")
    safe_ref = ref.replace("/", "_")
    return f"code/repo={safe_owner}__{safe_repo}/ref={safe_ref}/{path}"


def code_s3_uri(*, bucket: str, owner: str, repo: str, ref: str, path: str) -> str:
    return f"s3://{bucket}/{code_object_key(owner=owner, repo=repo, ref=ref, path=path)}"


async def fetch_repo_license(client: httpx.AsyncClient, ref: ReleaseRef) -> str | None:
    """Return the SPDX id reported by the GitHub License API.

    Uses ``GET /repos/{owner}/{repo}`` (cheap, single request). ``None`` if
    the response is missing the field or the request fails.
    """
    url = f"https://api.github.com/repos/{ref.owner}/{ref.repo}"
    try:
        resp = await client.get(url, headers={"Accept": "application/vnd.github+json"})
    except httpx.HTTPError as exc:
        log.warning("tarball.license_fetch_failed", repo=ref.full_name, err=str(exc))
        return None
    if resp.status_code >= 400:
        log.warning("tarball.license_bad_status", repo=ref.full_name, status=resp.status_code)
        return None
    payload = resp.json()
    license_obj = payload.get("license") if isinstance(payload, dict) else None
    if not isinstance(license_obj, dict):
        return None
    spdx = license_obj.get("spdx_id")
    if not isinstance(spdx, str) or spdx in {"", "NOASSERTION"}:
        return None
    return spdx


async def fetch_tarball(client: httpx.AsyncClient, ref: ReleaseRef) -> bytes | None:
    """Download the release tarball; ``None`` on non-200."""
    try:
        resp = await client.get(
            ref.tarball_url,
            headers={"Accept": "application/vnd.github+json"},
        )
    except httpx.HTTPError as exc:
        log.warning("tarball.fetch_failed", repo=ref.full_name, tag=ref.tag, err=str(exc))
        return None
    if resp.status_code >= 400:
        log.warning(
            "tarball.bad_status",
            repo=ref.full_name,
            tag=ref.tag,
            status=resp.status_code,
        )
        return None
    return resp.content


def _trace_id() -> str:
    return secrets.token_hex(16)


async def process_release(
    ref: ReleaseRef,
    *,
    cfg: IngestConfig,
    fetcher_cfg: FetcherConfig,
    client: httpx.AsyncClient,
    minio: MinioWriter,
    producer: BronzeProducerProtocol,
    bucket: TokenBucket,
    valid_from: datetime | None = None,
    admission_producer: LicenseAdmissionProducer | None = None,
) -> int:
    """Fetch the tarball for ``ref`` and emit one code BronzeRecord per file.

    Returns the number of records emitted. The ``valid_from`` argument
    populates the per-document validity interval; it should be the release
    ``published_at`` timestamp from the upstream BronzeRecord. When ``None``
    the current UTC time is used as a safe fallback.
    """
    await bucket.acquire()
    spdx = await fetch_repo_license(client, ref)
    admission = decide_license_admission(
        source_url=f"https://github.com/{ref.owner}/{ref.repo}/releases/tag/{ref.tag}",
        source_feed=SOURCE_FEED,
        license_value=spdx,
        license_source="github_api" if spdx else "unknown",
    )
    if admission_producer is not None:
        await admission_producer.send(admission.decision)
    if not admission.admitted or spdx not in fetcher_cfg.allowed_licenses:
        log.info(
            "tarball.skip_license",
            repo=ref.full_name,
            tag=ref.tag,
            spdx=spdx,
        )
        return 0

    await bucket.acquire()
    tar_bytes = await fetch_tarball(client, ref)
    if tar_bytes is None:
        return 0

    valid_from = valid_from or datetime.now(tz=UTC)

    emitted = 0
    for extracted in iter_tarball_files(
        tar_bytes,
        allowed_extensions=fetcher_cfg.allowed_extensions,
        max_file_size_bytes=fetcher_cfg.max_file_size_bytes,
    ):
        try:
            await _emit_one_file(
                extracted,
                ref=ref,
                spdx=spdx,
                cfg=cfg,
                minio=minio,
                producer=producer,
                valid_from=valid_from,
            )
        except Exception as exc:
            log.exception(
                "tarball.emit_failed",
                repo=ref.full_name,
                tag=ref.tag,
                path=extracted.path,
                err=str(exc),
            )
            continue
        emitted += 1

    log.info(
        "tarball.release_done",
        repo=ref.full_name,
        tag=ref.tag,
        emitted=emitted,
        spdx=spdx,
    )
    return emitted


async def _emit_one_file(
    extracted: ExtractedFile,
    *,
    ref: ReleaseRef,
    spdx: str,
    cfg: IngestConfig,
    minio: MinioWriter,
    producer: BronzeProducerProtocol,
    valid_from: datetime,
) -> None:
    file_url = f"https://github.com/{ref.owner}/{ref.repo}/blob/{ref.tag}/{extracted.path}"
    doc_id = doc_id_for_url(file_url)
    key = code_object_key(owner=ref.owner, repo=ref.repo, ref=ref.tag, path=extracted.path)
    await minio.put_bronze(
        key=key,
        payload=extracted.data,
        content_type="application/octet-stream",
        gzip_compress=True,
        metadata={
            "doc_id": doc_id,
            "github_repo": ref.full_name,
            "release_tag": ref.tag,
            "language": extracted.language,
        },
    )
    record = BronzeRecord(
        doc_id=doc_id,
        url=file_url,
        fetched_at=valid_from,
        http_status=200,
        http_last_modified=None,
        content_type="text/plain",
        raw_html_s3_uri=code_s3_uri(
            bucket=cfg.minio_bronze_bucket,
            owner=ref.owner,
            repo=ref.repo,
            ref=ref.tag,
            path=extracted.path,
        ),
        source_feed=SOURCE_FEED,
        trace_id=_trace_id(),
        bytes_size=len(extracted.data),
        source_format="code",
        extraction_pipeline="github-release-tarball-2026-06",
        spdx_license=spdx,
        spdx_license_source="github_api",
    )
    await producer.send(
        record,
        headers={
            "github_repo": ref.full_name,
            "github_ref": ref.tag,
            "github_path": extracted.path,
            "language": extracted.language,
            "sloc": str(extracted.sloc),
        },
    )


# -----------------------------------------------------------------------------
# Producer/consumer wiring
# -----------------------------------------------------------------------------


class BronzeProducerProtocol:
    """Structural type accepted by :func:`process_release`.

    Tests substitute a fake; production wires :class:`BronzeRecordProducer`.
    """

    async def send(
        self, record: BronzeRecord, *, headers: dict[str, str] | None = None
    ) -> None:  # pragma: no cover - protocol
        raise NotImplementedError


class BronzeRecordProducer:
    """aiokafka producer that emits code :class:`BronzeRecord` rows."""

    def __init__(
        self,
        bootstrap_servers: str,
        topic: str = "raw.fetched",
        *,
        client_id: str = "s2p-gh-tarball",
        producer: AIOKafkaProducer | None = None,
    ) -> None:
        self._bootstrap = bootstrap_servers
        self._topic = topic
        self._client_id = client_id
        self._producer = producer
        self._owns_producer = producer is None

    async def __aenter__(self) -> BronzeRecordProducer:
        await self.start()
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.stop()

    async def start(self) -> None:
        if self._producer is not None:
            await self._producer.start()
            return
        from aiokafka import AIOKafkaProducer

        self._producer = AIOKafkaProducer(
            bootstrap_servers=self._bootstrap,
            client_id=self._client_id,
            enable_idempotence=True,
            acks="all",
            compression_type="zstd",
            linger_ms=20,
            max_batch_size=131072,
        )
        await self._producer.start()

    async def stop(self) -> None:
        if self._producer is None:
            return
        await self._producer.stop()
        if self._owns_producer:
            self._producer = None

    async def send(self, record: BronzeRecord, *, headers: dict[str, str] | None = None) -> None:
        if self._producer is None:
            raise RuntimeError("BronzeRecordProducer.send called before start()")
        payload = record.model_dump_json(by_alias=True).encode("utf-8")
        key = record.doc_id.encode("utf-8")
        kafka_headers: list[tuple[str, bytes]] = [
            ("trace_id", record.trace_id.encode("ascii")),
            ("source_feed", SOURCE_FEED.encode("utf-8")),
            ("schema", b"BronzeRecord/v1"),
        ]
        if headers:
            for k, v in headers.items():
                kafka_headers.append((k, v.encode("utf-8")))
        await self._producer.send_and_wait(
            self._topic,
            payload,
            key=key,
            headers=kafka_headers,
        )


def _decode_headers(raw_headers: Iterable[tuple[str, bytes]] | None) -> dict[str, str]:
    out: dict[str, str] = {}
    for k, v in raw_headers or ():
        try:
            out[k] = v.decode("utf-8")
        except UnicodeDecodeError:
            continue
    return out


def _published_at_from_payload(payload: dict[str, Any]) -> datetime | None:
    """Lift ``fetched_at`` from a BronzeRecord JSON payload as the validity-from
    proxy. The upstream ``github_releases`` poller does not currently emit
    ``published_at`` separately; using ``fetched_at`` is the documented
    fallback in the validity-interval enricher (see RESEARCH.md section 9).
    """
    raw = payload.get("fetched_at")
    if not isinstance(raw, str):
        return None
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None


async def _consume_loop(
    cfg: IngestConfig,
    fetcher_cfg: FetcherConfig,
    *,
    consumer: AIOKafkaConsumer,
    client: httpx.AsyncClient,
    producer: BronzeProducerProtocol,
    minio: MinioWriter,
    bucket: TokenBucket,
    stop_event: asyncio.Event,
    metrics: TarballMetrics | None = None,
    admission_producer: LicenseAdmissionProducer | None = None,
) -> int:
    """Inner consume-loop, broken out for tests to drive deterministically.

    Auto-commit is OFF on the consumer (see :func:`run`); we commit only
    after :func:`process_release` has finished landing the tarball + every
    extracted code BronzeRecord. A pod kill or KEDA scale-down mid-tarball
    therefore replays the entire release on the next worker rather than
    silently dropping it. Filtered messages (non-upstream source_feed,
    bad JSON, non-release URLs) are committed too, since their advancement
    does not lose ingest work.
    """
    import orjson

    has_commit = hasattr(consumer, "commit")

    async def _commit_safely() -> None:
        if not has_commit:
            return
        try:
            await consumer.commit()
        except Exception as exc:
            log.warning("tarball.commit_failed", err=str(exc))

    total = 0
    async for msg in consumer:
        if stop_event.is_set():
            break
        headers = _decode_headers(msg.headers)
        if headers.get("source_feed") != UPSTREAM_FEED:
            await _commit_safely()
            continue
        try:
            payload = orjson.loads(msg.value)
        except orjson.JSONDecodeError:
            log.warning("tarball.bad_payload", offset=msg.offset)
            await _commit_safely()
            continue
        url = payload.get("url")
        if not isinstance(url, str):
            await _commit_safely()
            continue
        ref = parse_release_url(url)
        if ref is None:
            await _commit_safely()
            continue
        valid_from = _published_at_from_payload(payload)
        succeeded = False
        emitted = 0
        if metrics is not None:
            metrics.release_started()
        try:
            emitted = await process_release(
                ref,
                cfg=cfg,
                fetcher_cfg=fetcher_cfg,
                client=client,
                minio=minio,
                producer=producer,
                bucket=bucket,
                valid_from=valid_from,
                admission_producer=admission_producer,
            )
            total += emitted
            succeeded = True
        except Exception as exc:
            log.exception(
                "tarball.release_error",
                repo=ref.full_name,
                tag=ref.tag,
                err=str(exc),
            )
        finally:
            if metrics is not None:
                metrics.release_finished(emitted=emitted, failed=not succeeded)
        # Only commit once the release was fully processed (tarball
        # downloaded, all code BronzeRecords emitted). On exception we leave
        # the offset uncommitted so the next worker will replay the
        # release. process_release is idempotent at the doc_id level so a
        # rare double-fetch is harmless.
        if succeeded:
            await _commit_safely()
    return total


async def run(
    cfg: IngestConfig,
    fetcher_cfg: FetcherConfig,
    *,
    metrics: TarballMetrics | None = None,
) -> int:
    """Wire production dependencies and drive :func:`_consume_loop` to exit."""
    from aiokafka import AIOKafkaConsumer

    consumer = AIOKafkaConsumer(
        cfg.raw_topic,
        bootstrap_servers=cfg.redpanda_brokers,
        group_id=fetcher_cfg.consumer_group,
        # Auto-commit OFF: a release tarball can take longer than aiokafka's
        # 5s commit timer (license API + multi-MB download + N MinIO PUTs +
        # N Kafka produces). Auto-commit would advance the offset before
        # process_release returns, so a pod restart between commit and
        # success silently drops the release. _consume_loop calls
        # consumer.commit() only after a successful process_release.
        enable_auto_commit=False,
        auto_offset_reset="latest",
        client_id="s2p-gh-tarball-consumer",
    )
    extra_headers: dict[str, str] = {}
    if cfg.github_token:
        extra_headers["Authorization"] = f"Bearer {cfg.github_token}"
    headers = build_headers(cfg, accept="application/vnd.github+json", extra=extra_headers)
    bucket = TokenBucket(rate=fetcher_cfg.request_rate_per_second, burst=fetcher_cfg.request_burst)
    metrics = metrics or TarballMetrics()
    stop_event = asyncio.Event()
    await consumer.start()
    try:
        async with (
            build_async_client(cfg, headers=headers) as client,
            BronzeRecordProducer(cfg.redpanda_brokers, topic=cfg.raw_topic) as producer,
            LicenseAdmissionProducer(
                cfg.redpanda_brokers,
                topic=cfg.license_admissions_topic,
                client_id="s2p-gh-tarball-license-admission",
            ) as admission_producer,
            MinioWriter(
                cfg.minio_endpoint,
                cfg.minio_access_key,
                cfg.minio_secret_key,
                bucket=cfg.minio_bronze_bucket,
            ) as minio,
        ):
            return await _consume_loop(
                cfg,
                fetcher_cfg,
                consumer=consumer,
                client=client,
                producer=producer,
                minio=minio,
                bucket=bucket,
                stop_event=stop_event,
                metrics=metrics,
                admission_producer=admission_producer,
            )
    finally:
        await consumer.stop()


def _fetcher_config_from_env() -> FetcherConfig:
    import os

    def _csv(name: str, default: tuple[str, ...]) -> tuple[str, ...]:
        raw = os.environ.get(name, "")
        if not raw.strip():
            return default
        return tuple(sorted({s.strip().lower() for s in raw.split(",") if s.strip()}))

    def _int(name: str, default: int) -> int:
        raw = os.environ.get(name, "")
        if not raw.strip():
            return default
        try:
            return int(raw)
        except ValueError:
            return default

    def _float(name: str, default: float) -> float:
        raw = os.environ.get(name, "")
        if not raw.strip():
            return default
        try:
            return float(raw)
        except ValueError:
            return default

    licenses_raw = os.environ.get("S2P_TARBALL_ALLOWED_LICENSES", "")
    if licenses_raw.strip():
        licenses = frozenset(s.strip() for s in licenses_raw.split(",") if s.strip())
    else:
        licenses = DEFAULT_ALLOWED_LICENSES

    return FetcherConfig(
        allowed_extensions=_csv("S2P_TARBALL_ALLOWED_EXTENSIONS", DEFAULT_ALLOWED_EXTENSIONS),
        max_file_size_bytes=_int("S2P_TARBALL_MAX_FILE_SIZE_BYTES", DEFAULT_MAX_FILE_SIZE_BYTES),
        allowed_licenses=licenses,
        request_rate_per_second=_float("S2P_TARBALL_RATE_PER_SECOND", 1.0),
        request_burst=_int("S2P_TARBALL_BURST", 4),
        consumer_group=os.environ.get("S2P_TARBALL_CONSUMER_GROUP", "s2p-github-tarball-fetcher"),
    )


def main() -> None:
    cfg = load_config()
    fetcher_cfg = _fetcher_config_from_env()
    configure_logging(cfg.log_level, json_output=not cfg.is_dev)
    init_tracer("ingest.github_release_tarball_fetcher", cfg)
    log.info(
        "tarball.start",
        allowed_licenses=sorted(fetcher_cfg.allowed_licenses),
        max_file_size_bytes=fetcher_cfg.max_file_size_bytes,
        rate=fetcher_cfg.request_rate_per_second,
    )
    metrics = TarballMetrics()
    start_probe_server(metrics_provider=metrics.render_prometheus)
    total = asyncio.run(run(cfg, fetcher_cfg, metrics=metrics))
    log.info("tarball.done", emitted=total)


if __name__ == "__main__":
    main()
