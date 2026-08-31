"""Install and verify lifecycle rules for transient object-store tiers.

Bronze source bodies and Silver scientific artifacts are replay/audit material,
not the durable training corpus.  Gold, post-training packages, and state are
deliberately outside this module.
"""

from __future__ import annotations

import argparse
import json
import os
from typing import Any

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

RULE_ID = "stream2pretrain-transient-retention-v1"


def _required(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"{name} is required")
    return value


def _retention_days() -> int:
    raw = _required("S2P_TRANSIENT_OBJECT_RETENTION_DAYS")
    try:
        days = int(raw)
    except ValueError as exc:
        raise RuntimeError("S2P_TRANSIENT_OBJECT_RETENTION_DAYS must be an integer") from exc
    if days < 1:
        raise RuntimeError("S2P_TRANSIENT_OBJECT_RETENTION_DAYS must be at least 1")
    return days


def lifecycle_rule(days: int) -> dict[str, Any]:
    return {
        "ID": RULE_ID,
        "Status": "Enabled",
        "Filter": {"Prefix": ""},
        "Expiration": {"Days": days},
        "AbortIncompleteMultipartUpload": {"DaysAfterInitiation": days},
    }


def _is_global_filter(rule: dict[str, Any]) -> bool:
    """Accept the equivalent global-filter shapes returned by S3 implementations."""
    if "Filter" in rule:
        filter_value = rule["Filter"]
        return filter_value in ({}, {"Prefix": ""})
    return rule.get("Prefix", "") == ""


def _matches_rule(rule: dict[str, Any], *, days: int) -> bool:
    return (
        rule.get("ID") == RULE_ID
        and rule.get("Status") == "Enabled"
        and rule.get("Expiration", {}).get("Days") == days
        and rule.get("AbortIncompleteMultipartUpload", {}).get("DaysAfterInitiation") == days
        and _is_global_filter(rule)
    )


def _existing_rules(client: Any, bucket: str) -> list[dict[str, Any]]:
    try:
        response = client.get_bucket_lifecycle_configuration(Bucket=bucket)
    except ClientError as exc:
        code = str(exc.response.get("Error", {}).get("Code", ""))
        if code in {"NoSuchLifecycleConfiguration", "NoSuchLifecycle"}:
            return []
        raise
    return list(response.get("Rules", []))


def desired_rules(existing: list[dict[str, Any]], *, days: int) -> list[dict[str, Any]]:
    preserved = [rule for rule in existing if rule.get("ID") != RULE_ID]
    return [*preserved, lifecycle_rule(days)]


def configure_bucket(client: Any, bucket: str, *, days: int, apply: bool) -> dict[str, Any]:
    existing = _existing_rules(client, bucket)
    desired = desired_rules(existing, days=days)
    managed = [rule for rule in existing if rule.get("ID") == RULE_ID]
    changed = len(managed) != 1 or not _matches_rule(managed[0], days=days)
    if apply and changed:
        client.put_bucket_lifecycle_configuration(
            Bucket=bucket,
            LifecycleConfiguration={"Rules": desired},
        )
    if apply:
        installed = _existing_rules(client, bucket)
        matches = [rule for rule in installed if rule.get("ID") == RULE_ID]
        if len(matches) != 1 or not _matches_rule(matches[0], days=days):
            observed = json.dumps(matches, sort_keys=True, default=str)
            raise RuntimeError(
                f"Lifecycle verification failed for {bucket}; observed managed rules: {observed}"
            )
    return {"bucket": bucket, "changed": changed, "retention_days": days}


def _client() -> Any:
    return boto3.client(
        "s3",
        endpoint_url=_required("MINIO_ENDPOINT"),
        aws_access_key_id=os.environ.get("MINIO_ACCESS_KEY") or _required("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.environ.get("MINIO_SECRET_KEY")
        or _required("AWS_SECRET_ACCESS_KEY"),
        region_name=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
        config=Config(
            signature_version="s3v4",
            connect_timeout=5,
            read_timeout=10,
            retries={"max_attempts": 3, "mode": "standard"},
        ),
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    days = _retention_days()
    client = _client()
    buckets = [_required("MINIO_BRONZE_BUCKET"), _required("MINIO_SILVER_BUCKET")]
    report = {
        "applied": args.apply,
        "tiers": [
            configure_bucket(client, bucket, days=days, apply=args.apply) for bucket in buckets
        ],
    }
    print(json.dumps(report, sort_keys=True))


if __name__ == "__main__":
    main()
