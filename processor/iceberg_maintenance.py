"""Bounded Iceberg metadata maintenance and catalog recovery.

The command is read-only unless ``--apply`` is present. It never deletes data
files, manifests, or attestations. Cleanup is limited to old Iceberg
``*.metadata.json`` objects that are not the catalog's current metadata file
and are not present in the current table metadata log.
"""

from __future__ import annotations

import argparse
import json
import os
import re
from collections.abc import Iterable
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any
from urllib.parse import urlsplit

import boto3
from botocore.config import Config

from processor import common
from processor.iceberg_catalog import load_runtime_catalog

_METADATA_FILE_RE = re.compile(r"(?:^|/)(\d+)-[^/]+\.metadata\.json$")
DEFAULT_SNAPSHOT_RETENTION_HOURS = 168
DEFAULT_METADATA_VERSIONS = 20
DEFAULT_MIN_SNAPSHOTS_TO_KEEP = 10
_DEFAULT_TABLES = (
    "curated",
    "curation_decisions",
    "license_admissions",
    "foundry_events",
    "posttrain_artifacts",
)


@dataclass(frozen=True, slots=True)
class ObjectInfo:
    """The S3 fields needed to decide whether an object is removable."""

    key: str
    size: int
    last_modified: datetime


def _maintenance_properties() -> dict[str, str]:
    retention_hours = int(
        os.environ.get(
            "S2P_ICEBERG_SNAPSHOT_RETENTION_HOURS",
            DEFAULT_SNAPSHOT_RETENTION_HOURS,
        )
    )
    metadata_versions = int(
        os.environ.get("S2P_ICEBERG_METADATA_VERSIONS", DEFAULT_METADATA_VERSIONS)
    )
    minimum_snapshots = int(
        os.environ.get("S2P_ICEBERG_MIN_SNAPSHOTS_TO_KEEP", DEFAULT_MIN_SNAPSHOTS_TO_KEEP)
    )
    if min(retention_hours, metadata_versions, minimum_snapshots) < 1:
        raise ValueError("Iceberg maintenance limits must be at least 1")
    return {
        "write.metadata.delete-after-commit.enabled": "true",
        "write.metadata.previous-versions-max": str(metadata_versions),
        "history.expire.max-snapshot-age-ms": str(retention_hours * 60 * 60 * 1000),
        "history.expire.min-snapshots-to-keep": str(minimum_snapshots),
    }


def _ensure_maintenance_properties(table: Any) -> None:
    current = getattr(table, "properties", {})
    changed = {
        key: value for key, value in _maintenance_properties().items() if current.get(key) != value
    }
    if not changed:
        return
    with table.transaction() as txn:
        txn.set_properties(**changed)


def _s3_location(value: str) -> tuple[str, str]:
    parsed = urlsplit(value)
    if parsed.scheme not in {"s3", "s3a"} or not parsed.netloc:
        raise ValueError(f"expected an s3:// location, got {value!r}")
    return parsed.netloc, parsed.path.lstrip("/")


def _metadata_key(value: str) -> tuple[str, str]:
    bucket, key = _s3_location(value)
    if not key.endswith(".metadata.json"):
        raise ValueError(f"not an Iceberg metadata JSON location: {value!r}")
    return bucket, key


def _iter_objects(s3: Any, *, bucket: str, prefix: str) -> Iterable[ObjectInfo]:
    token: str | None = None
    while True:
        request: dict[str, Any] = {"Bucket": bucket, "Prefix": prefix}
        if token:
            request["ContinuationToken"] = token
        response = s3.list_objects_v2(**request)
        for item in response.get("Contents", []):
            yield ObjectInfo(
                key=str(item["Key"]),
                size=int(item.get("Size", 0)),
                last_modified=item["LastModified"].astimezone(UTC),
            )
        if not response.get("IsTruncated"):
            return
        token = str(response["NextContinuationToken"])


def _cleanup_candidates(
    objects: Iterable[ObjectInfo],
    *,
    protected_keys: set[str],
    older_than: datetime,
) -> list[ObjectInfo]:
    return [
        item
        for item in objects
        if item.key.endswith(".metadata.json")
        and item.key not in protected_keys
        and item.last_modified < older_than
    ]


def _delete_objects(s3: Any, *, bucket: str, objects: list[ObjectInfo]) -> None:
    for offset in range(0, len(objects), 1000):
        batch = objects[offset : offset + 1000]
        response = s3.delete_objects(
            Bucket=bucket,
            Delete={"Objects": [{"Key": item.key} for item in batch], "Quiet": True},
        )
        errors = response.get("Errors", [])
        if errors:
            raise RuntimeError(f"MinIO rejected metadata deletions: {errors[:3]}")


def _latest_metadata_location(
    s3: Any,
    *,
    bucket: str,
    table_prefix: str,
) -> str | None:
    latest: tuple[int, str] | None = None
    prefix = f"{table_prefix.rstrip('/')}/metadata/"
    for item in _iter_objects(s3, bucket=bucket, prefix=prefix):
        match = _METADATA_FILE_RE.search(item.key)
        if match is None:
            continue
        candidate = (int(match.group(1)), item.key)
        if latest is None or candidate > latest:
            latest = candidate
    return f"s3://{bucket}/{latest[1]}" if latest else None


def _table_names(catalog: Any, namespace: str) -> list[str]:
    names = set(_DEFAULT_TABLES)
    with suppress(Exception):
        names.update(identifier[-1] for identifier in catalog.list_tables((namespace,)))
    return sorted(names)


def _load_or_register_table(
    catalog: Any,
    s3: Any,
    *,
    namespace: str,
    table_name: str,
    bucket: str,
    register_missing: bool,
) -> tuple[Any | None, str | None]:
    identifier = (namespace, table_name)
    try:
        return catalog.load_table(identifier), None
    except Exception as exc:
        if not register_missing:
            return None, str(exc)
    location = _latest_metadata_location(
        s3,
        bucket=bucket,
        table_prefix=f"warehouse/{namespace}/{table_name}",
    )
    if location is None:
        return None, "table is absent from the catalog and no metadata JSON exists"
    try:
        return catalog.register_table(identifier, location), None
    except Exception as exc:
        return None, f"catalog registration failed for {location}: {exc}"


def _protected_metadata(table: Any) -> tuple[str, set[str]]:
    bucket, current_key = _metadata_key(str(table.metadata_location))
    protected = {current_key}
    for entry in table.metadata.metadata_log:
        entry_bucket, entry_key = _metadata_key(str(entry.metadata_file))
        if entry_bucket != bucket:
            raise RuntimeError("one Iceberg table references metadata in multiple buckets")
        protected.add(entry_key)
    return bucket, protected


def _maintain_table(
    catalog: Any,
    s3: Any,
    *,
    namespace: str,
    table_name: str,
    bucket: str,
    apply: bool,
    register_missing: bool,
    snapshot_cutoff: datetime,
    metadata_cutoff: datetime,
    register_only: bool = False,
) -> dict[str, Any]:
    table, error = _load_or_register_table(
        catalog,
        s3,
        namespace=namespace,
        table_name=table_name,
        bucket=bucket,
        register_missing=register_missing,
    )
    if table is None:
        return {"table": f"{namespace}.{table_name}", "status": "missing", "reason": error}

    if register_only:
        return {
            "table": f"{namespace}.{table_name}",
            "status": "reconciled",
            "current_metadata": str(table.metadata_location),
        }

    snapshots_before = len(table.metadata.snapshots)
    if apply:
        _ensure_maintenance_properties(table)
        table = catalog.load_table((namespace, table_name))
        table.maintenance.expire_snapshots().older_than(snapshot_cutoff).commit()
        table = catalog.load_table((namespace, table_name))

    metadata_bucket, protected = _protected_metadata(table)
    if metadata_bucket != bucket:
        raise RuntimeError(
            f"{namespace}.{table_name} is in bucket {metadata_bucket}, expected {bucket}"
        )
    _, current_key = _metadata_key(str(table.metadata_location))
    prefix = current_key.rsplit("/", 1)[0] + "/"
    objects = list(_iter_objects(s3, bucket=bucket, prefix=prefix))
    candidates = _cleanup_candidates(
        objects,
        protected_keys=protected,
        older_than=metadata_cutoff,
    )
    if apply:
        _delete_objects(s3, bucket=bucket, objects=candidates)

    return {
        "table": f"{namespace}.{table_name}",
        "status": "applied" if apply else "dry-run",
        "snapshots_before": snapshots_before,
        "snapshots_after": len(table.metadata.snapshots),
        "current_metadata": str(table.metadata_location),
        "protected_metadata_files": len(protected),
        "unreferenced_metadata_files": len(candidates),
        "unreferenced_metadata_bytes": sum(item.size for item in candidates),
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="commit maintenance and deletions")
    parser.add_argument(
        "--register-missing",
        action="store_true",
        help="register the latest on-disk metadata when the catalog lost a table",
    )
    parser.add_argument(
        "--register-only",
        action="store_true",
        help="restore missing catalog entries without expiring snapshots or deleting metadata",
    )
    parser.add_argument(
        "--snapshot-retention-hours",
        type=int,
        default=int(
            os.environ.get(
                "S2P_ICEBERG_SNAPSHOT_RETENTION_HOURS",
                DEFAULT_SNAPSHOT_RETENTION_HOURS,
            )
        ),
    )
    parser.add_argument(
        "--metadata-minimum-age-hours",
        type=int,
        default=int(os.environ.get("S2P_ICEBERG_ORPHAN_MINIMUM_AGE_HOURS", 24)),
    )
    parser.add_argument("--table", action="append", dest="tables")
    return parser


def main() -> None:
    """Run read-only planning or explicitly requested maintenance."""
    args = _parser().parse_args()
    if args.snapshot_retention_hours < 1 or args.metadata_minimum_age_hours < 1:
        raise SystemExit("retention and minimum age values must be at least one hour")
    if args.register_missing and not args.apply:
        raise SystemExit("--register-missing changes the catalog and requires --apply")
    if args.register_only and not args.register_missing:
        raise SystemExit("--register-only requires --register-missing")
    cfg = common.load_config()
    catalog = load_runtime_catalog(cfg)
    s3 = boto3.client(
        "s3",
        endpoint_url=cfg.minio_endpoint,
        aws_access_key_id=cfg.minio_access_key,
        aws_secret_access_key=cfg.minio_secret_key,
        region_name="us-east-1",
        config=Config(s3={"addressing_style": "path"}),
    )
    namespace = os.environ.get("S2P_ICEBERG_NAMESPACE") or os.environ.get(
        "ICEBERG_NAMESPACE", "gold"
    )
    bucket = cfg.gold_bucket
    now = datetime.now(UTC)
    table_names = sorted(set(args.tables or _table_names(catalog, namespace)))
    results = [
        _maintain_table(
            catalog,
            s3,
            namespace=namespace,
            table_name=table_name,
            bucket=bucket,
            apply=bool(args.apply),
            register_missing=bool(args.register_missing),
            register_only=bool(args.register_only),
            snapshot_cutoff=now - timedelta(hours=args.snapshot_retention_hours),
            metadata_cutoff=now - timedelta(hours=args.metadata_minimum_age_hours),
        )
        for table_name in table_names
    ]
    print(
        json.dumps(
            {
                "mode": "apply" if args.apply else "dry-run",
                "register_only": bool(args.register_only),
                "snapshot_retention_hours": args.snapshot_retention_hours,
                "metadata_minimum_age_hours": args.metadata_minimum_age_hours,
                "tables": results,
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
