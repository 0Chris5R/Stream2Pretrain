/**
 * Lazy DuckDB-WASM client for in-browser queries against Parquet files served
 * from MinIO via presigned URLs.
 *
 * Two modes:
 *   - direct (default): instantiate DuckDB-WASM in the browser and run SQL on
 *     remote Parquet via the `httpfs` extension.
 *   - proxy (env DUCKDB_MODE=proxy): forward queries to the in-cluster
 *     duckdb-server pod through `/api/duckdb/query`. The proxy mode is the
 *     fallback for environments where SharedArrayBuffer is unavailable.
 *
 * The browser-side worker bundle is selected at runtime from the JSDelivr CDN
 * shipped by `@duckdb/duckdb-wasm`. We keep it lazy-imported so SSR builds
 * don't try to evaluate worker code at module init.
 */
'use client';

import type { AsyncDuckDB, AsyncDuckDBConnection } from '@duckdb/duckdb-wasm';

let dbPromise: Promise<AsyncDuckDB> | null = null;

async function instantiate(): Promise<AsyncDuckDB> {
  const duckdb = await import('@duckdb/duckdb-wasm');
  const bundles = duckdb.getJsDelivrBundles();
  const bundle = await duckdb.selectBundle(bundles);
  if (!bundle.mainWorker) {
    throw new Error('duckdb-wasm: no compatible worker bundle for this browser');
  }
  const workerUrl = URL.createObjectURL(
    new Blob([`importScripts("${bundle.mainWorker}");`], { type: 'text/javascript' }),
  );
  const worker = new Worker(workerUrl);
  const logger = new duckdb.ConsoleLogger(duckdb.LogLevel.WARNING);
  const db = new duckdb.AsyncDuckDB(logger, worker);
  await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
  URL.revokeObjectURL(workerUrl);
  return db;
}

async function getDb(): Promise<AsyncDuckDB> {
  if (!dbPromise) dbPromise = instantiate();
  return dbPromise;
}

export interface QueryResult<T> {
  rows: T[];
  durationMs: number;
}

/**
 * Run a SQL query against DuckDB-WASM. Use `parquetUrl` placeholders (`?`)
 * with `params`; positional binding is preferred over string interpolation.
 */
export async function runQuery<T = Record<string, unknown>>(
  sql: string,
  params: ReadonlyArray<string | number | boolean> = [],
): Promise<QueryResult<T>> {
  const db = await getDb();
  const conn: AsyncDuckDBConnection = await db.connect();
  const t0 = performance.now();
  try {
    const stmt = await conn.prepare(sql);
    try {
      const arrowTable = await stmt.query(...params);
      const rows = arrowTable.toArray().map((r) => r.toJSON() as T);
      return { rows, durationMs: performance.now() - t0 };
    } finally {
      await stmt.close();
    }
  } finally {
    await conn.close();
  }
}

/**
 * Common SQL fragments.
 *
 * Iceberg `as_of(timestamp)` is exposed by the DuckDB Iceberg extension as
 * `iceberg_scan(..., snapshot_from_timestamp => ?)`. We default to the gold
 * table; callers override the path for benchmarking.
 */
export const ICEBERG_GOLD_TABLE = 's3://stream2pretrain/gold/curated/';

/**
 * Bootstrap the Iceberg + httpfs extensions. Idempotent.
 *
 * Authentication: the browser MUST never see raw MinIO credentials. We
 * rely exclusively on short-lived presigned URLs returned by the server
 * (FastAPI submit-api -> /api/duckdb/presign), which already encode the
 * SigV4 signature into the URL path/query. DuckDB's httpfs extension
 * follows them transparently; no SET statements with secrets needed.
 *
 * Earlier revisions of this file read static access/secret keys from a
 * `<meta name="x-s2p-s3-creds">` tag and interpolated them into SET
 * statements - that branch was removed because (a) embedding raw keys
 * in the rendered HTML exposed them to any extension or XSS payload,
 * and (b) string interpolation into SQL was an injection footgun.
 */
export async function ensureIcebergSetup(): Promise<void> {
  const db = await getDb();
  const conn = await db.connect();
  try {
    await conn.query(`INSTALL httpfs; LOAD httpfs;`);
    await conn.query(`INSTALL iceberg; LOAD iceberg;`);
  } finally {
    await conn.close();
  }
}
