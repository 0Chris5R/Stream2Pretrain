"""Delete retained objects owned exclusively by the removed GitHub source.

This migration never touches shared Iceberg data files. It removes only
source-specific Bronze payloads and ingest cursors whose keys prove GitHub
ownership. The shared Kafka backlog is discarded separately at a recorded
frontier before the GitHub workloads are removed.
"""

from __future__ import annotations

import json
import os
from typing import Any

import boto3
from botocore.config import Config


def _required(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"{name} is required")
    return value


def _is_github_key(key: str) -> bool:
    lowered = key.lower()
    return (
        lowered.startswith("code/")
        or lowered.startswith("source=github-releases/")
        or lowered.startswith("source=github-release-tarballs/")
        or "/source=github-releases/" in lowered
        or "/source=github-release-tarballs/" in lowered
        or ("github-releases" in lowered and lowered.startswith("ingest-cursors/"))
        or ("github-release-tarballs" in lowered and lowered.startswith("ingest-cursors/"))
    )


def _delete_matching(client: Any, bucket: str) -> dict[str, int | str]:
    pending: list[dict[str, str]] = []
    objects = 0
    size_bytes = 0
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket):
        for item in page.get("Contents", []):
            key = str(item.get("Key") or "")
            if not _is_github_key(key):
                continue
            pending.append({"Key": key})
            objects += 1
            size_bytes += int(item.get("Size") or 0)
            if len(pending) == 1000:
                client.delete_objects(Bucket=bucket, Delete={"Objects": pending, "Quiet": True})
                pending = []
    if pending:
        client.delete_objects(Bucket=bucket, Delete={"Objects": pending, "Quiet": True})
    return {"bucket": bucket, "objects_deleted": objects, "bytes_deleted": size_bytes}


def main() -> None:
    client = boto3.client(
        "s3",
        endpoint_url=_required("MINIO_ENDPOINT"),
        aws_access_key_id=os.environ.get("MINIO_ACCESS_KEY") or _required("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.environ.get("MINIO_SECRET_KEY")
        or _required("AWS_SECRET_ACCESS_KEY"),
        region_name=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
        config=Config(signature_version="s3v4"),
    )
    buckets = list(
        dict.fromkeys(
            value
            for value in (
                os.environ.get("MINIO_BRONZE_BUCKET"),
                os.environ.get("S2P_STATE_BUCKET"),
            )
            if value
        )
    )
    if not buckets:
        raise RuntimeError("MINIO_BRONZE_BUCKET or S2P_STATE_BUCKET is required")
    print(json.dumps([_delete_matching(client, bucket) for bucket in buckets], sort_keys=True))


if __name__ == "__main__":
    main()
