"""GitHub release tarball fetcher worker.

A long-running consumer that:

1. Subscribes only to the dedicated ``github.release.jobs`` topic. Release
   metadata remains on ``raw.fetched`` for the normal fetch pipeline, but the
   tarball worker must not consume its own per-file output topic.
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
configured so the maximum four replicas remain below the GitHub REST
5000 req/h threshold in aggregate (Helm uses ``0.25 req/s`` per pod).
"""

from __future__ import annotations

import asyncio
import hashlib
import re
import secrets
import threading
from collections.abc import Iterable
from contextlib import AsyncExitStack
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any
from urllib.parse import quote, unquote, urlencode, urlsplit

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
from schemas.bronze import BronzeRecord, TrainingUsage

if TYPE_CHECKING:
    from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

log = get_logger(__name__)

SOURCE_FEED = "github-release-tarballs"
UPSTREAM_FEED = "github-releases"

_SPDX_HEADER = re.compile(
    rb"SPDX-License-Identifier\s*:\s*([A-Za-z0-9.+-]{1,128})",
    re.IGNORECASE,
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
        encoded_ref = quote(self.tag, safe="")
        return f"https://api.github.com/repos/{self.owner}/{self.repo}/tarball/{encoded_ref}"


@dataclass(frozen=True)
class RepoLicenseEvidence:
    """GitHub's licence result for one repository ref and licence blob."""

    spdx_id: str
    api_url: str
    ref: str
    blob_sha: str
    path: str | None = None
    html_url: str | None = None


@dataclass(frozen=True)
class FetcherConfig:
    """Static configuration for the worker (env-derived)."""

    allowed_extensions: tuple[str, ...] = DEFAULT_ALLOWED_EXTENSIONS
    max_file_size_bytes: int = DEFAULT_MAX_FILE_SIZE_BYTES
    excluded_tag_prefixes: tuple[str, ...] = ("ciflow/", "trunk/", "viable/")
    request_rate_per_second: float = 1.0
    request_burst: int = 4
    consumer_group: str = "s2p-github-tarball-fetcher"
    consumer_max_poll_interval_ms: int = 900_000

    def __post_init__(self) -> None:
        if self.max_file_size_bytes < 1:
            raise ValueError("tarball file limit must be positive")
        if self.consumer_max_poll_interval_ms < 300_000:
            raise ValueError("tarball max poll interval must be at least five minutes")


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
        # Atom links percent-encode slashes. Keep the canonical ref decoded so
        # query parameters are encoded exactly once by httpx.
        tag=unquote(m.group("tag")),
    )


def is_release_candidate(ref: ReleaseRef, fetcher_cfg: FetcherConfig) -> bool:
    """Reject observed CI snapshot tags that are not software releases."""
    tag = unquote(ref.tag).lower()
    return not any(tag.startswith(prefix.lower()) for prefix in fetcher_cfg.excluded_tag_prefixes)


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


async def _get_with_anonymous_fallback(
    client: httpx.AsyncClient,
    url: str,
    *,
    headers: dict[str, str],
    anonymous_client: httpx.AsyncClient | None = None,
    params: dict[str, str] | None = None,
) -> httpx.Response:
    """GET once with configured auth, then retry a rejected token anonymously.

    GitHub returns 401 when an installed token has expired or been revoked. Public
    repository metadata and tarballs remain available without credentials, so a
    stale optional secret must reduce the rate budget rather than stop ingestion.
    """
    response = await client.get(url, headers=headers, params=params)
    if response.status_code == 401 and anonymous_client is not None:
        log.warning("tarball.auth_rejected_falling_back_anonymous", url=url)
        await response.aclose()
        return await anonymous_client.get(url, headers=headers, params=params)
    return response


async def fetch_repo_license(
    client: httpx.AsyncClient,
    ref: ReleaseRef,
    *,
    anonymous_client: httpx.AsyncClient | None = None,
) -> str | None:
    """Compatibility projection of :func:`fetch_repo_license_evidence`."""
    evidence = await fetch_repo_license_evidence(
        client,
        ref,
        anonymous_client=anonymous_client,
    )
    return evidence.spdx_id if evidence is not None else None


async def fetch_repo_license_evidence(
    client: httpx.AsyncClient,
    ref: ReleaseRef,
    *,
    anonymous_client: httpx.AsyncClient | None = None,
) -> RepoLicenseEvidence | None:
    """Return immutable licence evidence reported for an exact release ref.

    Resolves the exact release ref through ``GET /repos/{owner}/{repo}/license``.
    Git tags can be moved, so the API's licence-file blob SHA is mandatory and
    becomes the immutable evidence revision.
    """
    url = f"https://api.github.com/repos/{ref.owner}/{ref.repo}/license"
    try:
        resp = await _get_with_anonymous_fallback(
            client,
            url,
            headers={"Accept": "application/vnd.github+json"},
            anonymous_client=anonymous_client,
            params={"ref": ref.tag},
        )
    except httpx.HTTPError as exc:
        log.warning("tarball.license_fetch_failed", repo=ref.full_name, err=str(exc))
        raise
    if resp.status_code >= 400:
        log.warning("tarball.license_bad_status", repo=ref.full_name, status=resp.status_code)
        if resp.status_code in {408, 425, 429} or resp.status_code >= 500:
            resp.raise_for_status()
        return None
    payload = resp.json()
    if not isinstance(payload, dict):
        return None
    license_obj = payload.get("license")
    spdx = license_obj.get("spdx_id") if isinstance(license_obj, dict) else None
    blob_sha = payload.get("sha")
    if (
        not isinstance(spdx, str)
        or spdx in {"", "NOASSERTION"}
        or not isinstance(blob_sha, str)
        or not blob_sha
    ):
        return None
    path = payload.get("path")
    html_url = payload.get("html_url")
    return RepoLicenseEvidence(
        spdx_id=spdx,
        api_url=f"{url}?{urlencode({'ref': ref.tag})}",
        ref=ref.tag,
        blob_sha=blob_sha,
        path=path if isinstance(path, str) else None,
        html_url=html_url if isinstance(html_url, str) else None,
    )


async def fetch_tarball(
    client: httpx.AsyncClient,
    ref: ReleaseRef,
    *,
    anonymous_client: httpx.AsyncClient | None = None,
) -> bytes | None:
    """Download the release tarball; ``None`` on non-200."""
    try:
        resp = await _get_with_anonymous_fallback(
            client,
            ref.tarball_url,
            headers={"Accept": "application/vnd.github+json"},
            anonymous_client=anonymous_client,
        )
    except httpx.HTTPError as exc:
        log.warning("tarball.fetch_failed", repo=ref.full_name, tag=ref.tag, err=str(exc))
        raise
    if resp.status_code >= 400:
        log.warning(
            "tarball.bad_status",
            repo=ref.full_name,
            tag=ref.tag,
            status=resp.status_code,
        )
        if resp.status_code not in {404, 410, 422}:
            resp.raise_for_status()
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
    anonymous_client: httpx.AsyncClient | None = None,
    minio: MinioWriter,
    producer: BronzeProducerProtocol,
    bucket: TokenBucket,
    admission_producer: LicenseAdmissionProducer,
    valid_from: datetime | None = None,
) -> int:
    """Fetch the tarball for ``ref`` and emit one code BronzeRecord per file.

    Returns the number of records emitted. The ``valid_from`` argument
    populates the per-document validity interval; it should be the release
    ``published_at`` timestamp from the upstream BronzeRecord. When ``None``
    the current UTC time is used as a safe fallback.
    """
    if not is_release_candidate(ref, fetcher_cfg):
        log.info("tarball.skip_non_release_tag", repo=ref.full_name, tag=ref.tag)
        return 0

    await bucket.acquire()
    repo_evidence = await fetch_repo_license_evidence(
        client,
        ref,
        anonymous_client=anonymous_client,
    )
    spdx = repo_evidence.spdx_id if repo_evidence is not None else None
    admission = decide_license_admission(
        source_url=f"https://github.com/{ref.owner}/{ref.repo}/releases/tag/{ref.tag}",
        source_feed=SOURCE_FEED,
        license_value=spdx,
        license_source="github_api" if spdx else "unknown",
        source_format="code",
        resolver="github-license-api-ref",
        evidence_url=repo_evidence.api_url
        if repo_evidence is not None
        else (f"https://api.github.com/repos/{ref.owner}/{ref.repo}/license?ref={ref.tag}"),
        evidence_revision=repo_evidence.blob_sha if repo_evidence is not None else None,
        evidence_scope="repository_ref" if spdx else "unknown",
    )
    await admission_producer.send(admission.decision)
    if not admission.fetch_allowed:
        log.info(
            "tarball.skip_license",
            repo=ref.full_name,
            tag=ref.tag,
            spdx=spdx,
        )
        return 0

    await bucket.acquire()
    tar_bytes = await fetch_tarball(client, ref, anonymous_client=anonymous_client)
    if tar_bytes is None:
        return 0

    valid_from = valid_from or datetime.now(tz=UTC)

    emitted = 0
    failed_paths: list[str] = []
    for extracted in iter_tarball_files(
        tar_bytes,
        allowed_extensions=fetcher_cfg.allowed_extensions,
        max_file_size_bytes=fetcher_cfg.max_file_size_bytes,
    ):
        header_match = _SPDX_HEADER.search(extracted.data[:65536])
        file_license = (
            header_match.group(1).decode("ascii", errors="ignore")
            if header_match is not None
            else spdx
        )
        file_source = "file_header" if header_match is not None else "github_api"
        encoded_ref = quote(ref.tag, safe="")
        file_url = f"https://github.com/{ref.owner}/{ref.repo}/blob/{encoded_ref}/{extracted.path}"
        file_digest = f"sha256:{hashlib.sha256(extracted.data).hexdigest()}"
        source_format = (
            "web" if extracted.language in {"markdown", "restructuredtext", "text"} else "code"
        )
        file_admission = decide_license_admission(
            source_url=file_url,
            source_feed=SOURCE_FEED,
            license_value=file_license,
            license_source=file_source,
            source_format=source_format,
            resolver=("spdx-file-header" if header_match is not None else "github-license-api-ref"),
            evidence_url=file_url
            if header_match is not None
            else (repo_evidence.api_url if repo_evidence is not None else file_url),
            evidence_revision=(
                file_digest
                if header_match is not None
                else repo_evidence.blob_sha
                if repo_evidence is not None
                else None
            ),
            evidence_scope="file" if header_match is not None else "repository_ref",
        )
        await admission_producer.send(file_admission.decision)
        if not file_admission.fetch_allowed:
            continue
        try:
            await _emit_one_file(
                extracted,
                ref=ref,
                spdx=file_admission.license_id,
                spdx_source=file_source,
                training_usage=file_admission.training_usage,
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
            failed_paths.append(extracted.path)
            continue
        emitted += 1

    if failed_paths:
        raise RuntimeError(
            f"{len(failed_paths)} tarball objects failed to land; first path: {failed_paths[0]}"
        )

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
    spdx: str | None,
    spdx_source: str = "github_api",
    training_usage: TrainingUsage,
    cfg: IngestConfig,
    minio: MinioWriter,
    producer: BronzeProducerProtocol,
    valid_from: datetime,
) -> None:
    encoded_ref = quote(ref.tag, safe="")
    file_url = f"https://github.com/{ref.owner}/{ref.repo}/blob/{encoded_ref}/{extracted.path}"
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
        content_type=(
            "text/markdown; charset=utf-8"
            if extracted.language == "markdown"
            else "text/plain; charset=utf-8"
        ),
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
        source_format=(
            "web" if extracted.language in {"markdown", "restructuredtext", "text"} else "code"
        ),
        extraction_pipeline=(
            "github-readme-markdown-v1"
            if extracted.language == "markdown"
            else "repository-documentation-text-v1"
            if extracted.language in {"restructuredtext", "text"}
            else "github-release-tarball-2026-06"
        ),
        spdx_license=spdx,
        spdx_license_source=spdx_source if spdx else "unknown",  # type: ignore[arg-type]
        training_usage=training_usage,
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


def _is_tarball_job(headers: dict[str, str]) -> bool:
    """Accept dedicated jobs and unmarked legacy raw release records."""
    return (
        headers.get("source_feed") == UPSTREAM_FEED
        and headers.get("tarball_job_dispatched") != "true"
    )


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
    anonymous_client: httpx.AsyncClient | None = None,
    producer: BronzeProducerProtocol,
    minio: MinioWriter,
    bucket: TokenBucket,
    stop_event: asyncio.Event,
    admission_producer: LicenseAdmissionProducer,
    metrics: TarballMetrics | None = None,
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
        # A swallowed commit error caused completed 11k-file PyTorch releases
        # to replay indefinitely. Failing the worker is safe because object
        # keys and Kafka record keys are idempotent; pretending the commit
        # succeeded is not.
        await consumer.commit()

    total = 0
    async for msg in consumer:
        if stop_event.is_set():
            break
        headers = _decode_headers(msg.headers)
        # The release poller keeps its metadata record in raw.fetched and
        # sends a second copy to github.release.jobs. Only the unmarked job
        # copy is a tarball work item; marked copies are still rejected
        # defensively if one is forwarded into this topic.
        if not _is_tarball_job(headers):
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
                anonymous_client=anonymous_client,
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
            # Do not continue to a later job. A later commit would advance the
            # consumer-group position past this failed release and silently
            # lose it despite ``enable_auto_commit=False``.
            raise
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
        cfg.github_release_jobs_topic,
        bootstrap_servers=cfg.redpanda_brokers,
        group_id=fetcher_cfg.consumer_group,
        # Auto-commit OFF: a release tarball can take longer than aiokafka's
        # 5s commit timer (license API + multi-MB download + N MinIO PUTs +
        # N Kafka produces). Auto-commit would advance the offset before
        # process_release returns, so a pod restart between commit and
        # success silently drops the release. _consume_loop calls
        # consumer.commit() only after a successful process_release.
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        client_id="s2p-gh-tarball-consumer",
        max_poll_interval_ms=fetcher_cfg.consumer_max_poll_interval_ms,
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
        async with AsyncExitStack() as stack:
            client = await stack.enter_async_context(build_async_client(cfg, headers=headers))
            anonymous_client = None
            if cfg.github_token:
                anonymous_headers = build_headers(cfg, accept="application/vnd.github+json")
                anonymous_client = await stack.enter_async_context(
                    build_async_client(cfg, headers=anonymous_headers)
                )
            producer = await stack.enter_async_context(
                BronzeRecordProducer(cfg.redpanda_brokers, topic=cfg.raw_topic)
            )
            admission_producer = await stack.enter_async_context(
                LicenseAdmissionProducer(
                    cfg.redpanda_brokers,
                    topic=cfg.license_admissions_topic,
                    client_id="s2p-gh-tarball-license-admission",
                )
            )
            minio = await stack.enter_async_context(
                MinioWriter(
                    cfg.minio_endpoint,
                    cfg.minio_access_key,
                    cfg.minio_secret_key,
                    bucket=cfg.minio_bronze_bucket,
                )
            )
            return await _consume_loop(
                cfg,
                fetcher_cfg,
                consumer=consumer,
                client=client,
                anonymous_client=anonymous_client,
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

    return FetcherConfig(
        allowed_extensions=_csv("S2P_TARBALL_ALLOWED_EXTENSIONS", DEFAULT_ALLOWED_EXTENSIONS),
        max_file_size_bytes=_int("S2P_TARBALL_MAX_FILE_SIZE_BYTES", DEFAULT_MAX_FILE_SIZE_BYTES),
        excluded_tag_prefixes=_csv(
            "S2P_TARBALL_EXCLUDED_TAG_PREFIXES", ("ciflow/", "trunk/", "viable/")
        ),
        request_rate_per_second=_float("S2P_TARBALL_RATE_PER_SECOND", 1.0),
        request_burst=_int("S2P_TARBALL_BURST", 4),
        consumer_group=os.environ.get("S2P_TARBALL_CONSUMER_GROUP", "s2p-github-tarball-fetcher"),
        consumer_max_poll_interval_ms=_int("S2P_TARBALL_MAX_POLL_INTERVAL_MS", 900_000),
    )


def main() -> None:
    cfg = load_config()
    fetcher_cfg = _fetcher_config_from_env()
    configure_logging(cfg.log_level, json_output=not cfg.is_dev)
    init_tracer("ingest.github_release_tarball_fetcher", cfg)
    log.info(
        "tarball.start",
        max_file_size_bytes=fetcher_cfg.max_file_size_bytes,
        excluded_tag_prefixes=fetcher_cfg.excluded_tag_prefixes,
        rate=fetcher_cfg.request_rate_per_second,
    )
    metrics = TarballMetrics()
    start_probe_server(metrics_provider=metrics.render_prometheus)
    total = asyncio.run(run(cfg, fetcher_cfg, metrics=metrics))
    log.info("tarball.done", emitted=total)


if __name__ == "__main__":
    main()
