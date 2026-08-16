"""Shared Iceberg catalog construction for cluster and laptop runtimes.

Production keeps using the Polaris REST catalog.  The local compose profile
can opt into a SQLite-backed SQL catalog with a filesystem warehouse by
setting ``S2P_ICEBERG_CATALOG_TYPE=sql``.  SQLite is deliberately a local
test substitute, not a production topology.
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyiceberg.catalog import Catalog

    from processor.common import ProcessorConfig


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


__all__ = ["load_runtime_catalog"]
