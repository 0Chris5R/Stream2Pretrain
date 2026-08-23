import { NextResponse } from 'next/server';

import { DocumentDetailResponseSchema } from '@/lib/schemas';
import { UPSTREAM, upstreamError } from '@/lib/upstream';

export const runtime = 'nodejs';
export const dynamic = 'force-dynamic';

export async function GET(
  _req: Request,
  ctx: { params: Promise<{ docId: string }> },
): Promise<NextResponse> {
  const { docId } = await ctx.params;
  try {
    const response = await fetch(`${UPSTREAM.duckdb}/documents/${encodeURIComponent(docId)}`, {
      cache: 'no-store',
    });
    if (response.status === 404) {
      return NextResponse.json({ detail: 'document not found' }, { status: 404 });
    }
    if (!response.ok) {
      return NextResponse.json(upstreamError(`document_status_${response.status}`), { status: 502 });
    }
    const parsed = DocumentDetailResponseSchema.safeParse(await response.json());
    if (!parsed.success) {
      return NextResponse.json(upstreamError('document_shape_invalid'), { status: 502 });
    }
    return NextResponse.json(parsed.data);
  } catch {
    return NextResponse.json(upstreamError('duckdb_unreachable'), { status: 502 });
  }
}
