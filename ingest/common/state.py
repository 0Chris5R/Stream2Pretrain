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

import json
import os
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
        # the local Windows profile, where keys such as ``openreview:venue``
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


def _resolve_state_root(root: Path) -> Path:
    override = os.environ.get("S2P_STATE_ROOT")
    if override and not root.is_absolute() and root.parts[:1] == (".s2p-state",):
        return Path(override).joinpath(*root.parts[1:])
    return root


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
