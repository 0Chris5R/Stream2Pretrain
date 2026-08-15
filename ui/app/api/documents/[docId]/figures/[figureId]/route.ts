import { NextResponse } from 'next/server';

import { UPSTREAM, upstreamError } from '@/lib/upstream';

export const runtime = 'nodejs';
export const dynamic = 'force-dynamic';

export async function GET(
  _req: Request,
  ctx: { params: Promise<{ docId: string; figureId: string }> },
): Promise<NextResponse> {
  const { docId, figureId } = await ctx.params;
  try {
    const response = await fetch(
      `${UPSTREAM.duckdb}/documents/${encodeURIComponent(docId)}/figures/${encodeURIComponent(figureId)}`,
      { cache: 'no-store' },
    );
    if (!response.ok) {
      return NextResponse.json(upstreamError(`figure_status_${response.status}`), {
        status: response.status === 404 ? 404 : 502,
      });
    }
    return new NextResponse(await response.arrayBuffer(), {
      headers: {
        'Content-Type': response.headers.get('content-type') ?? 'application/octet-stream',
        'Cache-Control': 'private, max-age=300',
      },
    });
  } catch {
    return NextResponse.json(upstreamError('duckdb_unreachable'), { status: 502 });
  }
}
