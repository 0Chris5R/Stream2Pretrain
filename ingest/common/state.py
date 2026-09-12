"""Durable polling state backed by local files or S3-compatible storage.

The CronJob entrypoints need to remember per-feed cursors across runs:

- RSS / Atom: the ``ETag`` and ``Last-Modified`` headers seen in the last 200
- OAI-PMH: the ``from`` timestamp + outstanding resumption token (if any)
- HF Hub: the maximum ``lastModified`` seen so far

Production uses the existing MinIO service. This avoids coupling unrelated
pollers to one ReadWriteOnce volume, which prevents jobs from scheduling across
nodes. Development and unit tests keep the atomic JSON-on-disk backend.
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
import os
import socket
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from io import BytesIO
from pathlib import Path
from typing import Any
from urllib.parse import quote


class FeedStateStore:
    """Tiny JSON key/value store, one object or file per feed."""

    def __init__(
        self,
        root: str | Path,
        *,
        backend: str | None = None,
        bucket: str | None = None,
        s3_client: Any | None = None,
    ) -> None:
        self._root = _resolve_state_root(Path(root))
        self._backend = (backend or os.environ.get("S2P_STATE_BACKEND") or "file").lower()
        if self._backend not in {"file", "s3"}:
            raise ValueError(f"unsupported feed-state backend: {self._backend}")

        self._bucket = bucket or os.environ.get("S2P_STATE_BUCKET")
        self._s3 = s3_client
        scope = os.environ.get("S2P_COMPONENT") or self._root.name or "ingest"
        configured_prefix = os.environ.get("S2P_STATE_PREFIX", "ingest-cursors")
        self._object_prefix = f"{configured_prefix.strip('/')}/{quote(scope, safe='._-')}".strip(
            "/"
        )
        if self._backend == "file":
            self._root.mkdir(parents=True, exist_ok=True)
            return

        if not self._bucket:
            raise RuntimeError("S2P_STATE_BUCKET is required for the s3 feed-state backend")
        if self._s3 is None:
            self._s3 = _build_s3_client()
        self._ensure_bucket()

    def _path_for(self, feed_name: str) -> Path:
        # Percent-encode separators and Windows-reserved characters while
        # preserving readable feed names. Source state is also exercised by
        # the local Windows profile, where keys such as ``source:cursor``
        # cannot be used as file names.
        safe = quote(feed_name, safe="._-")
        return self._root / f"{safe}.json"

    def _legacy_path_for(self, feed_name: str) -> Path:
        safe = feed_name.replace("/", "_").replace(" ", "_")
        return self._root / f"{safe}.json"

    def _object_key_for(self, feed_name: str) -> str:
        safe = quote(feed_name, safe="._-")
        return f"{self._object_prefix}/{safe}.json"

    def _ensure_bucket(self) -> None:
        assert self._s3 is not None
        assert self._bucket is not None
        try:
            self._s3.head_bucket(Bucket=self._bucket)
        except Exception as exc:
            if not _is_missing_bucket(exc):
                raise
            try:
                self._s3.create_bucket(Bucket=self._bucket)
            except Exception as create_exc:
                if not _is_bucket_already_owned(create_exc):
                    raise

    def get(self, feed_name: str) -> dict[str, Any]:
        if self._backend == "s3":
            assert self._s3 is not None
            assert self._bucket is not None
            try:
                response = self._s3.get_object(
                    Bucket=self._bucket,
                    Key=self._object_key_for(feed_name),
                )
            except Exception as exc:
                if _is_missing_key(exc):
                    return {}
                raise
            body = response.get("Body", b"")
            raw = body.read() if hasattr(body, "read") else body
            try:
                return json.loads(bytes(raw).decode("utf-8"))
            except (json.JSONDecodeError, UnicodeDecodeError, TypeError):
                return {}

        p = self._path_for(feed_name)
        legacy = self._legacy_path_for(feed_name)
        if not p.exists() and legacy != p and legacy.exists():
            p = legacy
        if not p.exists():
            return {}
        try:
            return json.loads(p.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return {}

    def put(self, feed_name: str, state: dict[str, Any]) -> None:
        if self._backend == "s3":
            assert self._s3 is not None
            assert self._bucket is not None
            payload = json.dumps(state, sort_keys=True, default=str).encode("utf-8")
            self._s3.put_object(
                Bucket=self._bucket,
                Key=self._object_key_for(feed_name),
                Body=BytesIO(payload),
                ContentType="application/json",
            )
            return

        p = self._path_for(feed_name)
        tmp = p.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(state, sort_keys=True, default=str), encoding="utf-8")
        tmp.replace(p)


class KubernetesCursorLease:
    """Renewable per-cursor ownership using a Kubernetes Lease object."""

    def __init__(
        self,
        cursor_key: str,
        *,
        duration_seconds: int,
        coordination_api: Any | None = None,
    ) -> None:
        if duration_seconds <= 0:
            raise ValueError("cursor lease duration must be positive")
        namespace = os.environ.get("S2P_NAMESPACE", "").strip()
        if not namespace:
            raise RuntimeError("S2P_NAMESPACE is required for Kubernetes cursor leases")
        component = os.environ.get("S2P_COMPONENT", "ingest").strip() or "ingest"
        pod_name = os.environ.get("POD_NAME", "").strip() or socket.gethostname()
        digest = hashlib.sha256(cursor_key.encode("utf-8")).hexdigest()[:16]
        safe_component = "".join(
            char if char.isalnum() else "-" for char in component.lower()
        ).strip("-")
        self.name = f"s2p-cursor-{safe_component[:24]}-{digest}"[:63].rstrip("-")
        self.namespace = namespace
        self.identity = f"{pod_name}:{uuid.uuid4()}"
        self.duration_seconds = duration_seconds
        self._api = coordination_api or _build_coordination_api()

    def try_acquire(self) -> bool:
        """Create or atomically take an expired lease."""
        now = datetime.now(tz=UTC)
        try:
            current = self._api.read_namespaced_lease(self.name, self.namespace)
        except Exception as exc:
            if _kubernetes_status(exc) != 404:
                raise
            try:
                self._api.create_namespaced_lease(
                    self.namespace,
                    _lease_body(
                        name=self.name,
                        namespace=self.namespace,
                        holder_identity=self.identity,
                        duration_seconds=self.duration_seconds,
                        now=now,
                    ),
                )
            except Exception as create_exc:
                if _kubernetes_status(create_exc) == 409:
                    return False
                raise
            return True

        if not _lease_available(current, now=now, identity=self.identity):
            return False
        replacement = _lease_body(
            name=self.name,
            namespace=self.namespace,
            holder_identity=self.identity,
            duration_seconds=self.duration_seconds,
            now=now,
            resource_version=current.metadata.resource_version,
        )
        try:
            self._api.replace_namespaced_lease(self.name, self.namespace, replacement)
        except Exception as exc:
            if _kubernetes_status(exc) in {404, 409}:
                return False
            raise
        return True

    def renew(self) -> bool:
        """Renew only while this process still owns the current lease version."""
        try:
            current = self._api.read_namespaced_lease(self.name, self.namespace)
        except Exception as exc:
            if _kubernetes_status(exc) == 404:
                return False
            raise
        if getattr(current.spec, "holder_identity", None) != self.identity:
            return False
        now = datetime.now(tz=UTC)
        replacement = _lease_body(
            name=self.name,
            namespace=self.namespace,
            holder_identity=self.identity,
            duration_seconds=self.duration_seconds,
            now=now,
            resource_version=current.metadata.resource_version,
            acquire_time=getattr(current.spec, "acquire_time", None),
        )
        try:
            self._api.replace_namespaced_lease(self.name, self.namespace, replacement)
        except Exception as exc:
            if _kubernetes_status(exc) in {404, 409}:
                return False
            raise
        return True

    def release(self) -> None:
        """Clear this process's ownership with a resource-version guarded replace."""
        try:
            current = self._api.read_namespaced_lease(self.name, self.namespace)
        except Exception as exc:
            if _kubernetes_status(exc) == 404:
                return
            raise
        if getattr(current.spec, "holder_identity", None) != self.identity:
            return
        replacement = _lease_body(
            name=self.name,
            namespace=self.namespace,
            holder_identity=None,
            duration_seconds=self.duration_seconds,
            now=datetime.now(tz=UTC),
            resource_version=current.metadata.resource_version,
            acquire_time=getattr(current.spec, "acquire_time", None),
        )
        try:
            self._api.replace_namespaced_lease(self.name, self.namespace, replacement)
        except Exception as exc:
            if _kubernetes_status(exc) not in {404, 409}:
                raise


@asynccontextmanager
async def cursor_lease(cursor_key: str) -> AsyncIterator[bool]:
    """Yield cursor ownership and cancel the caller if lease renewal is lost."""
    backend = os.environ.get("S2P_CURSOR_LEASE_BACKEND", "none").strip().lower()
    if backend == "none":
        yield True
        return
    if backend != "kubernetes":
        raise ValueError(f"unsupported cursor lease backend: {backend}")
    raw_duration = os.environ.get("S2P_CURSOR_LEASE_DURATION_SECONDS", "").strip()
    if not raw_duration:
        raise RuntimeError(
            "S2P_CURSOR_LEASE_DURATION_SECONDS is required for Kubernetes cursor leases"
        )
    try:
        duration_seconds = int(raw_duration)
    except ValueError as exc:
        raise RuntimeError("S2P_CURSOR_LEASE_DURATION_SECONDS must be an integer") from exc

    lease = KubernetesCursorLease(cursor_key, duration_seconds=duration_seconds)
    acquired = await asyncio.to_thread(lease.try_acquire)
    if not acquired:
        yield False
        return

    owner_task = asyncio.current_task()

    async def renew_until_stopped() -> None:
        while True:
            await asyncio.sleep(duration_seconds / 3)
            try:
                renewed = await asyncio.to_thread(lease.renew)
            except Exception:
                renewed = False
            if not renewed:
                if owner_task is not None:
                    owner_task.cancel(f"cursor lease lost: {cursor_key}")
                return

    renewal = asyncio.create_task(renew_until_stopped())
    try:
        yield True
    finally:
        renewal.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await renewal
        await asyncio.to_thread(lease.release)


def _resolve_state_root(root: Path) -> Path:
    override = os.environ.get("S2P_STATE_ROOT")
    if override and not root.is_absolute() and root.parts[:1] == (".s2p-state",):
        return Path(override).joinpath(*root.parts[1:])
    return root


def _build_coordination_api() -> Any:
    from kubernetes import client, config

    config.load_incluster_config()
    return client.CoordinationV1Api()


def _lease_body(
    *,
    name: str,
    namespace: str,
    holder_identity: str | None,
    duration_seconds: int,
    now: datetime,
    resource_version: str | None = None,
    acquire_time: datetime | None = None,
) -> Any:
    from kubernetes import client

    return client.V1Lease(
        metadata=client.V1ObjectMeta(
            name=name,
            namespace=namespace,
            resource_version=resource_version,
        ),
        spec=client.V1LeaseSpec(
            holder_identity=holder_identity,
            lease_duration_seconds=duration_seconds,
            acquire_time=acquire_time or now,
            renew_time=now,
        ),
    )


def _lease_available(lease: Any, *, now: datetime, identity: str) -> bool:
    holder = getattr(lease.spec, "holder_identity", None)
    if not holder or holder == identity:
        return True
    renewed = getattr(lease.spec, "renew_time", None) or getattr(lease.spec, "acquire_time", None)
    duration = getattr(lease.spec, "lease_duration_seconds", None)
    if not isinstance(renewed, datetime) or not isinstance(duration, int):
        return False
    if renewed.tzinfo is None:
        renewed = renewed.replace(tzinfo=UTC)
    return renewed + timedelta(seconds=duration) <= now


def _kubernetes_status(exc: Exception) -> int | None:
    status = getattr(exc, "status", None)
    return status if isinstance(status, int) else None


def _build_s3_client() -> Any:
    import boto3
    from botocore.config import Config

    return boto3.client(
        "s3",
        endpoint_url=os.environ.get("MINIO_ENDPOINT"),
        aws_access_key_id=os.environ.get("MINIO_ACCESS_KEY") or os.environ.get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.environ.get("MINIO_SECRET_KEY")
        or os.environ.get("AWS_SECRET_ACCESS_KEY"),
        region_name=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
        config=Config(signature_version="s3v4"),
    )


def _error_code(exc: Exception) -> str:
    response = getattr(exc, "response", None)
    if not isinstance(response, dict):
        return ""
    error = response.get("Error")
    if not isinstance(error, dict):
        return ""
    return str(error.get("Code") or "")


def _is_missing_key(exc: Exception) -> bool:
    return _error_code(exc) in {"404", "NoSuchKey", "NotFound"}


def _is_missing_bucket(exc: Exception) -> bool:
    return _error_code(exc) in {"404", "NoSuchBucket", "NotFound"}


def _is_bucket_already_owned(exc: Exception) -> bool:
    return _error_code(exc) in {"BucketAlreadyExists", "BucketAlreadyOwnedByYou"}
