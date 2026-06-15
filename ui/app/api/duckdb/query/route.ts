/**
 * POST /api/duckdb/query
 *
 * Proxy mode for the DuckDB-WASM client. Browsers without
 * SharedArrayBuffer (some embedded webviews) cannot run DuckDB-WASM
 * locally; this route forwards the SQL to the in-cluster duckdb-server
 * pod which holds Iceberg + httpfs extensions warm.
 *
 * The route does NOT execute SQL itself - duckdb-server applies the
 * project's parameterised template allow-list. Sending arbitrary SQL is
 * still rejected upstream when the cluster runs in `safe_mode=on`.
 */
import { NextResponse } from 'next/server';
import { z } from 'zod';

import { UPSTREAM, upstreamError } from '@/lib/upstream';

export const runtime = 'nodejs';
export const dynamic = 'force-dynamic';

const RequestSchema = z.object({
  sql: z.string().min(1).max(8_192),
  params: z.array(z.union([z.string(), z.number(), z.boolean(), z.null()])).default([]),
});

const ResponseSchema = z.object({
  rows: z.array(z.record(z.string(), z.unknown())),
  durationMs: z.number().nonnegative(),
});

export async function POST(req: Request): Promise<NextResponse> {
  const body = await req.json().catch(() => null);
  const parsed = RequestSchema.safeParse(body);
  if (!parsed.success) {
    return NextResponse.json({ detail: parsed.error.message }, { status: 400 });
  }
  try {
    const resp = await fetch(`${UPSTREAM.duckdb}/query`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(parsed.data),
    });
    const text = await resp.text();
    if (!resp.ok) {
      return NextResponse.json(
        { detail: text || `duckdb_status_${resp.status}` },
        { status: resp.status >= 400 && resp.status < 500 ? resp.status : 502 },
      );
    }
    const out = ResponseSchema.safeParse(JSON.parse(text));
    if (!out.success) {
      return NextResponse.json(upstreamError('duckdb_shape_invalid'), { status: 502 });
    }
    return NextResponse.json(out.data);
  } catch (err) {
    console.warn('duckdb proxy upstream failed', (err as Error).message);
    return NextResponse.json(upstreamError('duckdb_unreachable'), { status: 502 });
  }
}
