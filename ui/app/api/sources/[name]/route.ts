/**
 * DELETE /api/sources/{name}
 *
 * Proxies SourceFeed deletion to the sources-api upstream. Returns
 * `{ deleted: true }` on success; mirrors the upstream's 404 when the
 * SourceFeed does not exist.
 */
import { NextResponse } from 'next/server';

import { UPSTREAM, upstreamError } from '@/lib/upstream';

export const runtime = 'nodejs';
export const dynamic = 'force-dynamic';

export async function DELETE(
  _req: Request,
  ctx: { params: Promise<{ name: string }> },
): Promise<NextResponse> {
  const { name } = await ctx.params;
  if (!/^[a-z0-9][a-z0-9-]{0,61}[a-z0-9]$/.test(name)) {
    return NextResponse.json({ detail: 'invalid source name' }, { status: 400 });
  }
  try {
    const resp = await fetch(`${UPSTREAM.sourcesApi}/v1/sources/${encodeURIComponent(name)}`, {
      method: 'DELETE',
    });
    if (resp.status === 404) {
      return NextResponse.json({ detail: 'source not found' }, { status: 404 });
    }
    if (!resp.ok) {
      return NextResponse.json(upstreamError(`sources_api_status_${resp.status}`), {
        status: 502,
      });
    }
    return NextResponse.json({ deleted: true });
  } catch (err) {
    console.warn('sources DELETE upstream failed', (err as Error).message);
    return NextResponse.json(upstreamError('sources_api_unreachable'), { status: 502 });
  }
}

export async function PATCH(
  req: Request,
  ctx: { params: Promise<{ name: string }> },
): Promise<NextResponse> {
  const { name } = await ctx.params;
  if (!/^[a-z0-9][a-z0-9-]{0,61}[a-z0-9]$/.test(name)) {
    return NextResponse.json({ detail: 'invalid source name' }, { status: 400 });
  }
  const body = await req.json().catch(() => null);
  if (typeof body?.enabled !== 'boolean') {
    return NextResponse.json({ detail: 'enabled must be a boolean' }, { status: 400 });
  }
  try {
    const resp = await fetch(`${UPSTREAM.sourcesApi}/v1/sources/${encodeURIComponent(name)}`, {
      method: 'PATCH',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ enabled: body.enabled }),
    });
    const text = await resp.text();
    if (!resp.ok) {
      return NextResponse.json(
        { detail: text || `sources_api_status_${resp.status}` },
        { status: resp.status >= 400 && resp.status < 500 ? resp.status : 502 },
      );
    }
    return NextResponse.json(JSON.parse(text));
  } catch (err) {
    console.warn('sources PATCH upstream failed', (err as Error).message);
    return NextResponse.json(upstreamError('sources_api_unreachable'), { status: 502 });
  }
}
