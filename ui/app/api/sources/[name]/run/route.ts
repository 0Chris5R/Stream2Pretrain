import { NextResponse } from 'next/server';

import { SourceFeedStatusSchema } from '@/lib/schemas';
import { UPSTREAM, upstreamError } from '@/lib/upstream';

export const runtime = 'nodejs';
export const dynamic = 'force-dynamic';

export async function POST(
  _req: Request,
  ctx: { params: Promise<{ name: string }> },
): Promise<NextResponse> {
  const { name } = await ctx.params;
  if (!/^[a-z0-9][a-z0-9-]{0,61}[a-z0-9]$/.test(name)) {
    return NextResponse.json({ detail: 'invalid source name' }, { status: 400 });
  }
  try {
    const resp = await fetch(`${UPSTREAM.sourcesApi}/v1/sources/${encodeURIComponent(name)}/run`, {
      method: 'POST',
    });
    const text = await resp.text();
    if (!resp.ok) {
      return NextResponse.json(
        { detail: text || `sources_api_status_${resp.status}` },
        { status: resp.status >= 400 && resp.status < 500 ? resp.status : 502 },
      );
    }
    const status = SourceFeedStatusSchema.safeParse(JSON.parse(text));
    if (!status.success) {
      return NextResponse.json(upstreamError('sources_shape_invalid'), { status: 502 });
    }
    return NextResponse.json(status.data, { status: 202 });
  } catch (err) {
    console.warn('sources run upstream failed', (err as Error).message);
    return NextResponse.json(upstreamError('sources_api_unreachable'), { status: 502 });
  }
}
