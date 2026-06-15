/**
 * GET /api/mixture/compare?a=<recipe>&b=<recipe>
 *
 * Forwards an A/B compare request to the in-cluster mixture controller.
 * The controller owns the proxy-LM evaluation loop and returns a
 * `MixtureCompare` payload (per-step perplexity delta + tokens/hour).
 */
import { NextResponse } from 'next/server';

import { MixtureCompareSchema } from '@/lib/schemas';
import { UPSTREAM, upstreamError } from '@/lib/upstream';

export const runtime = 'nodejs';
export const dynamic = 'force-dynamic';

export async function GET(req: Request): Promise<NextResponse> {
  const url = new URL(req.url);
  const a = url.searchParams.get('a');
  const b = url.searchParams.get('b');
  if (!a || !b) {
    return NextResponse.json({ detail: 'missing a or b' }, { status: 400 });
  }
  if (!/^[a-z0-9][a-z0-9-]{0,62}$/.test(a) || !/^[a-z0-9][a-z0-9-]{0,62}$/.test(b)) {
    return NextResponse.json({ detail: 'invalid recipe name' }, { status: 400 });
  }
  try {
    const resp = await fetch(
      `${UPSTREAM.mixture}/v1/compare?a=${encodeURIComponent(a)}&b=${encodeURIComponent(b)}`,
      { cache: 'no-store' },
    );
    if (!resp.ok) {
      return NextResponse.json(upstreamError(`mixture_status_${resp.status}`), { status: 502 });
    }
    const parsed = MixtureCompareSchema.safeParse(await resp.json());
    if (!parsed.success) {
      return NextResponse.json(upstreamError('mixture_shape_invalid'), { status: 502 });
    }
    return NextResponse.json(parsed.data);
  } catch (err) {
    console.warn('mixture compare upstream failed', (err as Error).message);
    return NextResponse.json(upstreamError('mixture_unreachable'), { status: 502 });
  }
}
