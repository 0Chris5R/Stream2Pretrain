/**
 * GET /api/as-of?ts=<ISO-8601>
 *
 * Returns the per-source mixture (document + token counts) of the gold
 * Iceberg table valid at `ts`. Implements novelty pillar N2: typed
 * `[valid_from, valid_to)` validity intervals propagated end-to-end with
 * an `as_of(timestamp)` view.
 *
 * The route forwards the timestamp to the in-cluster duckdb-server,
 * which runs the half-open-interval predicate
 *   `valid_from <= ts AND (valid_to IS NULL OR valid_to > ts)`
 * against the retained serving projection of the Iceberg corpus. This is
 * source-validity time, not an Iceberg snapshot/processing-time query.
 */
import { NextResponse } from 'next/server';
import { z } from 'zod';

import { AsOfMixtureRowSchema } from '@/lib/schemas';
import { UPSTREAM, upstreamError } from '@/lib/upstream';

export const runtime = 'nodejs';
export const dynamic = 'force-dynamic';

const RowsSchema = z.array(AsOfMixtureRowSchema);

export async function GET(req: Request): Promise<NextResponse> {
  const url = new URL(req.url);
  const ts = url.searchParams.get('ts');
  if (!ts) {
    return NextResponse.json({ detail: 'missing ts query parameter' }, { status: 400 });
  }
  const parsedDate = new Date(ts);
  if (Number.isNaN(parsedDate.getTime())) {
    return NextResponse.json({ detail: 'ts must be an ISO-8601 timestamp' }, { status: 400 });
  }
  try {
    const resp = await fetch(
      `${UPSTREAM.duckdb}/as-of?ts=${encodeURIComponent(parsedDate.toISOString())}`,
      { cache: 'no-store', signal: AbortSignal.timeout(25_000) },
    );
    if (!resp.ok) {
      return NextResponse.json(upstreamError(`duckdb_status_${resp.status}`), { status: 502 });
    }
    const parsed = RowsSchema.safeParse(await resp.json());
    if (!parsed.success) {
      return NextResponse.json(upstreamError('as_of_shape_invalid'), { status: 502 });
    }
    return NextResponse.json(parsed.data);
  } catch (err) {
    console.warn('as-of upstream failed', (err as Error).message);
    return NextResponse.json(upstreamError('duckdb_unreachable'), { status: 502 });
  }
}
