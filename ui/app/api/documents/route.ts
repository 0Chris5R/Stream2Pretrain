import { NextResponse } from 'next/server';
import { DocumentPageSchema } from '@/lib/schemas';
import { UPSTREAM, upstreamError } from '@/lib/upstream';

export const runtime = 'nodejs';
export const dynamic = 'force-dynamic';

export async function GET(req: Request): Promise<NextResponse> {
  const url = new URL(req.url);
  const allowed = new Set([
    'page', 'page_size', 'search', 'route', 'source', 'source_format', 'date_from', 'date_to',
    'tag', 'rejection_reason', 'has_figures', 'has_tables', 'has_equations', 'include_fixtures',
    'min_edu', 'max_edu', 'min_quality', 'max_quality', 'sort',
  ]);
  const query = new URLSearchParams();
  for (const [key, value] of url.searchParams.entries()) {
    if (allowed.has(key)) query.append(key, value);
  }
  try {
    const response = await fetch(
      `${UPSTREAM.duckdb}/documents?${query.toString()}`,
      { cache: 'no-store' },
    );
    if (!response.ok) {
      return NextResponse.json(upstreamError(`documents_status_${response.status}`), { status: 502 });
    }
    const parsed = DocumentPageSchema.safeParse(await response.json());
    if (!parsed.success) {
      return NextResponse.json(upstreamError('documents_shape_invalid'), { status: 502 });
    }
    return NextResponse.json(parsed.data);
  } catch {
    return NextResponse.json(upstreamError('duckdb_unreachable'), { status: 502 });
  }
}
