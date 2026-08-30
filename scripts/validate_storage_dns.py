"""Bounded rollout probe for cluster DNS and authenticated MinIO operations."""

from __future__ import annotations

import json
import socket
import time
import uuid
from urllib.parse import urlparse

import boto3
from botocore.config import Config

from processor import common


def _host(value: str) -> str:
    parsed = urlparse(value)
    if not parsed.hostname:
        raise RuntimeError(f"endpoint has no hostname: {value!r}")
    return parsed.hostname


def main() -> None:
    cfg = common.load_config()
    hosts = sorted({_host(cfg.minio_endpoint), _host(cfg.polaris_uri)})
    resolutions: dict[str, int] = {}
    for host in hosts:
        started = time.monotonic()
        for _ in range(100):
            addresses = socket.getaddrinfo(host, None, type=socket.SOCK_STREAM)
            if not addresses:
                raise RuntimeError(f"DNS returned no addresses for {host}")
        resolutions[host] = round((time.monotonic() - started) * 1000)

    s3 = boto3.client(
        "s3",
        endpoint_url=cfg.minio_endpoint,
        aws_access_key_id=cfg.minio_access_key,
        aws_secret_access_key=cfg.minio_secret_key,
        region_name="us-east-1",
        config=Config(
            retries={"max_attempts": 5, "mode": "standard"},
            connect_timeout=10,
            read_timeout=60,
            s3={"addressing_style": "path"},
        ),
    )
    completed = 0
    for _ in range(10):
        key = f"operations/dns-storage-probe/{uuid.uuid4().hex}.txt"
        uploaded = False
        try:
            s3.put_object(Bucket=cfg.gold_bucket, Key=key, Body=b"stream2pretrain-probe")
            uploaded = True
            metadata = s3.head_object(Bucket=cfg.gold_bucket, Key=key)
            if int(metadata.get("ContentLength", -1)) != len(b"stream2pretrain-probe"):
                raise RuntimeError("MinIO HEAD returned an unexpected content length")
            completed += 1
        finally:
            if uploaded:
                s3.delete_object(Bucket=cfg.gold_bucket, Key=key)

    print(
        json.dumps(
            {
                "dns_lookups_per_host": 100,
                "dns_total_ms": resolutions,
                "minio_put_head_delete_cycles": completed,
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
