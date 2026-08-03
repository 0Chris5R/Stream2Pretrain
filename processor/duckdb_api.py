"""DuckDB HTTP API for cockpit lakehouse queries.

The service keeps Iceberg/httpfs extensions warm in one in-cluster process and
exposes only the narrow routes the UI needs. Arbitrary SQL is restricted to
read-only ``SELECT`` statements; dashboards should prefer the typed endpoints.
"""

from __future__ import annotations

import os
import re
import time
from collections.abc import Sequence
from typing import Any, Protocol
from urllib.parse import urlparse

_RELATION_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_.$]*$")


class DuckDBConnection(Protocol):
    description: Sequence[tuple[Any, ...]] | None

    def execute(self, sql: str, parameters: Sequence[Any] | None = None) -> DuckDBConnection: ...
    def fetchall(self) -> list[tuple[Any, ...]]: ...


class DuckDBQueryService:
    """Typed query helper around a DuckDB connection."""

    def __init__(
        self,
        connection: DuckDBConnection,
        *,
        gold_relation: str = "gold",
        refresh_iceberg: bool = False,
    ) -> None:
        if not _RELATION_RE.fullmatch(gold_relation):
            raise ValueError("gold_relation must be a simple DuckDB relation name")
        self._conn = connection
        self._gold = gold_relation
        self._refresh_iceberg = refresh_iceberg

    @classmethod
    def from_env(cls) -> DuckDBQueryService:
        import duckdb  # type: ignore[import-untyped]

        db_path = os.environ.get("S2P_DUCKDB_DATABASE", ":memory:")
        gold_relation = os.environ.get("S2P_DUCKDB_GOLD_RELATION", "gold")
        conn = duckdb.connect(db_path, read_only=False)
        _load_extensions(conn)
        _configure_s3(conn)
        return cls(conn, gold_relation=gold_relation, refresh_iceberg=True)

    def as_of(self, ts: str) -> list[dict[str, Any]]:
        sql = f"""
        SELECT
          source_feed,
          CAST(COALESCE(SUM(tokens), 0) AS BIGINT) AS tokens,
          CAST(COUNT(*) AS BIGINT) AS documents
        FROM {self._gold}
        WHERE valid_from <= CAST(? AS TIMESTAMP)
          AND (valid_to IS NULL OR valid_to > CAST(? AS TIMESTAMP))
        GROUP BY source_feed
        ORDER BY tokens DESC, source_feed ASC
        """
        return self._rows(sql, [ts, ts])

    def quality_histogram(self) -> dict[str, list[dict[str, Any]]]:
        sql = f"""
        SELECT
          CAST(FLOOR(quality_score * 2) / 2 AS DOUBLE) AS score,
          CAST(COUNT(*) AS BIGINT) AS count
        FROM {self._gold}
        GROUP BY score
        ORDER BY score ASC
        """
        return {"buckets": self._rows(sql, [])}

    def safe_query(self, sql: str, params: Sequence[Any]) -> dict[str, Any]:
        stripped = sql.strip().rstrip(";")
        if not stripped.lower().startswith("select"):
            raise ValueError("only SELECT statements are allowed")
        if ";" in stripped:
            raise ValueError("multiple SQL statements are not allowed")
        started = time.perf_counter()
        rows = self._rows(stripped, params)
        return {"rows": rows, "durationMs": (time.perf_counter() - started) * 1000.0}

    def _rows(self, sql: str, params: Sequence[Any]) -> list[dict[str, Any]]:
        if self._refresh_iceberg:
            _register_gold_relation(self._conn, self._gold)
        result = self._conn.execute(sql, params)
        names = [str(col[0]) for col in (result.description or [])]
        return [dict(zip(names, row, strict=True)) for row in result.fetchall()]


def _load_extensions(conn: DuckDBConnection) -> None:
    for extension in ("httpfs", "iceberg"):
        try:
            conn.execute(f"INSTALL {extension}")
            conn.execute(f"LOAD {extension}")
        except Exception:
            # Some images bake extensions in or run without network. Query
            # failures are surfaced by the routes with a 503.
            pass


def _configure_s3(conn: DuckDBConnection) -> None:
    """Configure DuckDB httpfs for the in-cluster MinIO endpoint."""
    endpoint = os.environ.get("MINIO_ENDPOINT")
    access_key = os.environ.get("MINIO_ACCESS_KEY") or os.environ.get("AWS_ACCESS_KEY_ID")
    secret_key = os.environ.get("MINIO_SECRET_KEY") or os.environ.get("AWS_SECRET_ACCESS_KEY")
    if not endpoint or not access_key or not secret_key:
        return
    parsed = urlparse(endpoint)
    host = parsed.netloc or parsed.path
    use_ssl = "true" if parsed.scheme == "https" else "false"
    settings = {
        "s3_endpoint": host,
        "s3_access_key_id": access_key,
        "s3_secret_access_key": secret_key,
        "s3_region": os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
        "s3_url_style": "path",
        "s3_use_ssl": use_ssl,
    }
    for key, value in settings.items():
        conn.execute(f"SET {key}={_sql_string(value)}")


def _register_gold_relation(conn: DuckDBConnection, relation: str) -> None:
    """Expose the Polaris Iceberg Gold table as the local DuckDB relation."""
    if not _RELATION_RE.fullmatch(relation):
        raise ValueError("gold_relation must be a simple DuckDB relation name")
    location = _load_gold_table_location()
    if location is None:
        _create_empty_gold_relation(conn, relation)
        return
    conn.execute(
        f"CREATE OR REPLACE VIEW {relation} AS "
        f"SELECT * FROM iceberg_scan({_sql_string(location)}, allow_moved_paths = true)"
    )


def _load_gold_table_location() -> str | None:
    """Resolve the Gold table location through Polaris, returning None if absent."""
    uri = os.environ.get("POLARIS_URI")
    warehouse = os.environ.get("POLARIS_WAREHOUSE")
    if not uri or not warehouse:
        return None
    from pyiceberg.catalog import load_catalog

    props: dict[str, str] = {
        "uri": uri,
        "warehouse": warehouse,
        "s3.endpoint": os.environ.get("MINIO_ENDPOINT", ""),
        "s3.access-key-id": os.environ.get("MINIO_ACCESS_KEY")
        or os.environ.get("AWS_ACCESS_KEY_ID", ""),
        "s3.secret-access-key": os.environ.get("MINIO_SECRET_KEY")
        or os.environ.get("AWS_SECRET_ACCESS_KEY", ""),
        "s3.region": os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
        "py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO",
    }
    token = os.environ.get("POLARIS_TOKEN")
    if token:
        props["token"] = token
    credential = os.environ.get("POLARIS_CREDENTIAL")
    if credential:
        props["credential"] = credential
        props["scope"] = os.environ.get("POLARIS_SCOPE", "PRINCIPAL_ROLE:ALL")
    namespace = os.environ.get("S2P_ICEBERG_NAMESPACE") or os.environ.get(
        "ICEBERG_NAMESPACE", "gold"
    )
    table_name = os.environ.get("S2P_ICEBERG_GOLD_TABLE", "curated")
    try:
        table = load_catalog("polaris", **props).load_table((namespace, table_name))
    except Exception as exc:
        if _is_missing_iceberg_table(exc):
            return None
        raise
    location = getattr(table, "location", None)
    return str(location()) if callable(location) else str(location)


def _is_missing_iceberg_table(exc: Exception) -> bool:
    name = exc.__class__.__name__.lower()
    message = str(exc).lower()
    return "nosuch" in name or "not found" in message or "does not exist" in message


def _create_empty_gold_relation(conn: DuckDBConnection, relation: str) -> None:
    """Create a zero-row Gold-shaped view until the first Iceberg commit exists."""
    conn.execute(
        f"""
        CREATE OR REPLACE VIEW {relation} AS
        SELECT
          CAST(NULL AS VARCHAR) AS doc_id,
          CAST(NULL AS VARCHAR) AS text,
          CAST(NULL AS VARCHAR) AS lang,
          CAST(NULL AS INTEGER) AS tokens,
          CAST(NULL AS DOUBLE) AS quality_score,
          CAST(NULL AS DOUBLE) AS edu_score,
          CAST(NULL AS VARCHAR) AS license,
          CAST(NULL AS VARCHAR) AS license_source,
          CAST(NULL AS INTEGER) AS risk_tier,
          CAST([] AS VARCHAR[]) AS pii_flags,
          CAST([] AS VARCHAR[]) AS contaminated_with,
          CAST(NULL AS TIMESTAMP) AS valid_from,
          CAST(NULL AS TIMESTAMP) AS valid_to,
          CAST([] AS VARCHAR[]) AS reject_reasons,
          CAST(NULL AS VARCHAR) AS scoring_version,
          CAST(NULL AS VARCHAR) AS classifier_revision,
          CAST(NULL AS VARCHAR) AS policy_revision,
          CAST(NULL AS BIGINT) AS snapshot_id,
          CAST(NULL AS VARCHAR) AS trace_id,
          CAST(NULL AS VARCHAR) AS source_feed,
          CAST(NULL AS VARCHAR) AS source_format,
          CAST(NULL AS VARCHAR) AS extraction_pipeline,
          CAST(NULL AS VARCHAR) AS spdx_license,
          CAST(NULL AS VARCHAR) AS spdx_license_source
        WHERE FALSE
        """
    )


def _sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


async def serve(service: DuckDBQueryService, *, port: int = 8090) -> None:
    from aiohttp import web  # type: ignore[import-untyped]

    async def probe(_: web.Request) -> web.Response:
        return web.Response(text="ok\n", content_type="text/plain")

    async def as_of(request: web.Request) -> web.Response:
        ts = request.query.get("ts")
        if not ts:
            return web.json_response({"detail": "missing ts"}, status=400)
        try:
            return web.json_response(service.as_of(ts))
        except Exception as exc:
            return web.json_response({"detail": str(exc)}, status=503)

    async def quality(_: web.Request) -> web.Response:
        try:
            return web.json_response(service.quality_histogram())
        except Exception as exc:
            return web.json_response({"detail": str(exc)}, status=503)

    async def query(request: web.Request) -> web.Response:
        body = await request.json()
        try:
            return web.json_response(service.safe_query(str(body.get("sql", "")), body.get("params", [])))
        except ValueError as exc:
            return web.json_response({"detail": str(exc)}, status=400)
        except Exception as exc:
            return web.json_response({"detail": str(exc)}, status=503)

    app = web.Application()
    app.router.add_get("/healthz", probe)
    app.router.add_get("/readyz", probe)
    app.router.add_get("/as-of", as_of)
    app.router.add_get("/quality-histogram", quality)
    app.router.add_post("/query", query)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, os.environ.get("S2P_BIND_HOST", "::"), port)
    await site.start()
    while True:
        import asyncio

        await asyncio.sleep(3600)


def main() -> None:
    import asyncio

    service = DuckDBQueryService.from_env()
    asyncio.run(serve(service, port=int(os.environ.get("S2P_DUCKDB_API_PORT", "8090"))))


if __name__ == "__main__":
    main()
