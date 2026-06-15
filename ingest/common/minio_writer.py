"""Async MinIO bronze writer.

We deliberately do not import the official ``minio`` SDK because it is sync-only
and would force every poller to schedule blocking work on the loop's executor.
Instead we use ``aiobotocore`` (S3-compatible). MinIO speaks the S3 API verbatim
so this works against the in-cluster MinIO and against AWS S3 if needed.
"""

from __future__ import annotations

import gzip
from typing import TYPE_CHECKING, Self

if TYPE_CHECKING:
    from types_aiobotocore_s3.client import S3Client


class MinioWriter:
    """Async S3-compatible PUT for the bronze tier.

    Construct one writer per pod and reuse it; the underlying aiobotocore
    session is re-entrant.
    """

    def __init__(
        self,
        endpoint_url: str,
        access_key: str,
        secret_key: str,
        *,
        region: str = "us-east-1",
        bucket: str = "bronze",
    ) -> None:
        self._endpoint = endpoint_url
        self._access_key = access_key
        self._secret_key = secret_key
        self._region = region
        self._bucket = bucket
        self._client: "S3Client | None" = None
        self._exit_stack = None  # type: ignore[var-annotated]

    @property
    def bucket(self) -> str:
        return self._bucket

    async def __aenter__(self) -> Self:
        await self.start()
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.stop()

    async def start(self) -> None:
        from contextlib import AsyncExitStack

        from aiobotocore.session import get_session

        if self._client is not None:
            return
        self._exit_stack = AsyncExitStack()
        await self._exit_stack.__aenter__()
        session = get_session()
        cm = session.create_client(
            "s3",
            endpoint_url=self._endpoint,
            aws_access_key_id=self._access_key,
            aws_secret_access_key=self._secret_key,
            region_name=self._region,
        )
        self._client = await self._exit_stack.enter_async_context(cm)

    async def stop(self) -> None:
        if self._exit_stack is not None:
            await self._exit_stack.__aexit__(None, None, None)
            self._exit_stack = None
            self._client = None

    async def put_bronze(
        self,
        *,
        key: str,
        payload: bytes,
        content_type: str = "text/html",
        gzip_compress: bool = True,
        metadata: dict[str, str] | None = None,
    ) -> int:
        """Write an object to the bronze bucket. Returns stored byte count."""
        if self._client is None:
            raise RuntimeError("MinioWriter.put_bronze called before start()")
        body = gzip.compress(payload) if gzip_compress else payload
        ce = "gzip" if gzip_compress else None
        kwargs: dict[str, object] = {
            "Bucket": self._bucket,
            "Key": key,
            "Body": body,
            "ContentType": content_type,
        }
        if ce:
            kwargs["ContentEncoding"] = ce
        if metadata:
            # S3 metadata keys must be ASCII; we strip non-ascii values pragmatically.
            kwargs["Metadata"] = {
                k: v.encode("ascii", "ignore").decode("ascii") for k, v in metadata.items()
            }
        await self._client.put_object(**kwargs)  # type: ignore[arg-type]
        return len(body)

    async def ensure_bucket(self) -> None:
        """Create the bucket if it does not exist. Idempotent. Used in dev."""
        if self._client is None:
            raise RuntimeError("MinioWriter.ensure_bucket called before start()")
        try:
            await self._client.head_bucket(Bucket=self._bucket)  # type: ignore[arg-type]
        except Exception:
            await self._client.create_bucket(Bucket=self._bucket)  # type: ignore[arg-type]
