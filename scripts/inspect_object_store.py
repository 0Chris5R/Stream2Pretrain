"""Report logical MinIO object growth without walking the host filesystem."""

from __future__ import annotations

import json
import os
from collections import defaultdict
from datetime import UTC, datetime, timedelta
from typing import Any

import boto3
from botocore.config import Config


def _required(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"{name} is required")
    return value


def _bucket_names() -> list[str]:
    names = [
        _required("MINIO_BRONZE_BUCKET"),
        _required("MINIO_SILVER_BUCKET"),
        _required("MINIO_GOLD_BUCKET"),
        _required("MINIO_DECON_BUCKET"),
        _required("MINIO_POSTTRAIN_BUCKET"),
    ]
    state = os.environ.get("S2P_STATE_BUCKET")
    if state:
        names.append(state)
    return list(dict.fromkeys(names))


def _empty_window() -> dict[str, int]:
    return {"objects": 0, "bytes": 0}


def _record(window: dict[str, int], size: int) -> None:
    window["objects"] += 1
    window["bytes"] += size


def inspect_bucket(client: Any, bucket: str, *, now: datetime) -> dict[str, Any]:
    total = _empty_window()
    last_hour = _empty_window()
    last_day = _empty_window()
    prefixes: dict[str, dict[str, int]] = defaultdict(_empty_window)
    hour_cutoff = now - timedelta(hours=1)
    day_cutoff = now - timedelta(hours=24)

    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket):
        for item in page.get("Contents", []):
            key = str(item.get("Key") or "")
            size = int(item.get("Size") or 0)
            modified = item.get("LastModified")
            _record(total, size)
            _record(prefixes[key.split("/", 1)[0] or "root"], size)
            if isinstance(modified, datetime):
                observed = modified if modified.tzinfo else modified.replace(tzinfo=UTC)
                if observed >= day_cutoff:
                    _record(last_day, size)
                if observed >= hour_cutoff:
                    _record(last_hour, size)

    return {
        "bucket": bucket,
        "total": total,
        "last_1h": last_hour,
        "last_24h": last_day,
        "prefixes": [
            {"prefix": prefix, **values}
            for prefix, values in sorted(
                prefixes.items(), key=lambda pair: pair[1]["bytes"], reverse=True
            )
        ],
    }


def main() -> None:
    now = datetime.now(UTC)
    client = boto3.client(
        "s3",
        endpoint_url=_required("MINIO_ENDPOINT"),
        aws_access_key_id=os.environ.get("MINIO_ACCESS_KEY")
        or _required("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.environ.get("MINIO_SECRET_KEY")
        or _required("AWS_SECRET_ACCESS_KEY"),
        region_name=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
        config=Config(signature_version="s3v4"),
    )
    report = {
        "measured_at": now.isoformat(),
        "buckets": [inspect_bucket(client, bucket, now=now) for bucket in _bucket_names()],
    }
    print(json.dumps(report, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
