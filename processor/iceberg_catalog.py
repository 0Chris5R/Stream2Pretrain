"""Shared Iceberg catalog construction for cluster and laptop runtimes.

Production keeps using the Polaris REST catalog.  The local compose profile
can opt into a SQLite-backed SQL catalog with a filesystem warehouse by
setting ``S2P_ICEBERG_CATALOG_TYPE=sql``.  SQLite is deliberately a local
test substitute, not a production topology.
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pyiceberg.catalog import Catalog

    from processor.common import ProcessorConfig


DEFAULT_METADATA_VERSIONS = 20
DEFAULT_SNAPSHOT_RETENTION_HOURS = 168
DEFAULT_MIN_SNAPSHOTS_TO_KEEP = 10


def iceberg_maintenance_properties() -> dict[str, str]:
    """Return the shared table policy with scheduled cleanup as sole owner."""
    metadata_versions = _positive_int_env(
        "S2P_ICEBERG_METADATA_VERSIONS", DEFAULT_METADATA_VERSIONS
    )
    retention_hours = _positive_int_env(
        "S2P_ICEBERG_SNAPSHOT_RETENTION_HOURS",
        DEFAULT_SNAPSHOT_RETENTION_HOURS,
    )
    minimum_snapshots = _positive_int_env(
        "S2P_ICEBERG_MIN_SNAPSHOTS_TO_KEEP",
        DEFAULT_MIN_SNAPSHOTS_TO_KEEP,
    )
    return {
        # The maintenance CronJob owns physical metadata deletion. Keep MinIO
        # cleanup requests off the hot append path.
        "write.metadata.delete-after-commit.enabled": "false",
        "write.metadata.previous-versions-max": str(metadata_versions),
        "history.expire.max-snapshot-age-ms": str(retention_hours * 60 * 60 * 1000),
        "history.expire.min-snapshots-to-keep": str(minimum_snapshots),
    }


def ensure_iceberg_maintenance_properties(table: Any) -> None:
    """Reconcile one existing table to the shared retention policy."""
    current = getattr(table, "properties", {})
    changed = {
        key: value
        for key, value in iceberg_maintenance_properties().items()
        if current.get(key) != value
    }
    if not changed:
        return
    with table.transaction() as txn:
        txn.set_properties(**changed)


def load_runtime_catalog(cfg: ProcessorConfig | None = None) -> Catalog:
    """Load the configured PyIceberg catalog.

    ``sql`` mode needs only a SQLite URI and a local ``file://`` warehouse.
    Every other value selects the existing Polaris REST path.
    """
    from pyiceberg.catalog import load_catalog

    catalog_type = os.environ.get("S2P_ICEBERG_CATALOG_TYPE", "rest").strip().lower()
    catalog_name = os.environ.get(
        "S2P_ICEBERG_CATALOG_NAME",
        "local" if catalog_type == "sql" else "polaris",
    )

    if catalog_type == "sql":
        uri = _required_env("S2P_ICEBERG_CATALOG_URI")
        warehouse = _required_env("S2P_ICEBERG_WAREHOUSE")
        return load_catalog(
            catalog_name,
            **{
                "type": "sql",
                "uri": uri,
                "warehouse": warehouse,
                "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO",
            },
        )

    props = _polaris_properties(cfg)
    return load_catalog(catalog_name, **props)


def _polaris_properties(cfg: ProcessorConfig | None) -> dict[str, str]:
    uri = cfg.polaris_uri if cfg is not None else os.environ.get("POLARIS_URI", "")
    warehouse = (
        cfg.polaris_warehouse if cfg is not None else os.environ.get("POLARIS_WAREHOUSE", "")
    )
    if not uri or not warehouse:
        raise RuntimeError("POLARIS_URI and POLARIS_WAREHOUSE are required for REST mode")

    props: dict[str, str] = {
        "type": "rest",
        "uri": uri,
        "warehouse": warehouse,
        "header.X-Iceberg-Access-Delegation": os.environ.get(
            "S2P_ICEBERG_ACCESS_DELEGATION", "vended-credentials"
        ),
        "s3.endpoint": _cfg_or_env(cfg, "minio_endpoint", "MINIO_ENDPOINT"),
        "s3.access-key-id": _cfg_or_env(cfg, "minio_access_key", "MINIO_ACCESS_KEY"),
        "s3.secret-access-key": _cfg_or_env(cfg, "minio_secret_key", "MINIO_SECRET_KEY"),
        "s3.region": os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
        "s3.connect-timeout": os.environ.get("S2P_S3_CONNECT_TIMEOUT_SECONDS", "10"),
        "s3.request-timeout": os.environ.get("S2P_S3_REQUEST_TIMEOUT_SECONDS", "60"),
        "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO",
    }
    token = cfg.polaris_token if cfg is not None else os.environ.get("POLARIS_TOKEN")
    if token:
        props["token"] = token
    credential = os.environ.get("POLARIS_CREDENTIAL")
    if credential:
        props["credential"] = credential
        props["scope"] = os.environ.get("POLARIS_SCOPE", "PRINCIPAL_ROLE:ALL")
    return props


def _cfg_or_env(cfg: ProcessorConfig | None, attr: str, env: str) -> str:
    if cfg is not None:
        return str(getattr(cfg, attr))
    value = os.environ.get(env, "")
    if value:
        return value
    aws_fallbacks = {
        "MINIO_ACCESS_KEY": "AWS_ACCESS_KEY_ID",
        "MINIO_SECRET_KEY": "AWS_SECRET_ACCESS_KEY",
    }
    fallback = aws_fallbacks.get(env)
    return os.environ.get(fallback, "") if fallback else ""


def _required_env(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(f"{name} is required for the local SQL Iceberg catalog")
    return value


def _positive_int_env(name: str, default: int) -> int:
    value = int(os.environ.get(name, default))
    if value < 1:
        raise ValueError(f"{name} must be at least 1")
    return value


__all__ = [
    "DEFAULT_METADATA_VERSIONS",
    "DEFAULT_MIN_SNAPSHOTS_TO_KEEP",
    "DEFAULT_SNAPSHOT_RETENTION_HOURS",
    "ensure_iceberg_maintenance_properties",
    "iceberg_maintenance_properties",
    "load_runtime_catalog",
]
