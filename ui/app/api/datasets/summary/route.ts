import { NextResponse } from 'next/server';

import { DatasetSummarySchema } from '@/lib/schemas';
import { UPSTREAM, upstreamError } from '@/lib/upstream';

export const runtime = 'nodejs';
export const dynamic = 'force-dynamic';

export async function GET(req: Request): Promise<NextResponse> {
  const incoming = new URL(req.url);
  const allowed = new Set([
    'date_from',
    'date_to',
    'route',
    'source',
    'source_format',
    'tag',
    'min_edu',
    'min_quality',
    'include_structured',
  ]);
  const query = new URLSearchParams();
  for (const [key, value] of incoming.searchParams.entries()) {
    if (allowed.has(key)) query.append(key, value);
  }
  try {
    const response = await fetch(`${UPSTREAM.duckdb}/datasets/summary?${query.toString()}`, {
      cache: 'no-store',
    });
    if (!response.ok) {
      return NextResponse.json(upstreamError(`dataset_summary_status_${response.status}`), {
        status: response.status === 400 ? 400 : 502,
      });
    }
    const parsed = DatasetSummarySchema.safeParse(await response.json());
    if (!parsed.success) {
      return NextResponse.json(upstreamError('dataset_summary_shape_invalid'), { status: 502 });
    }
    return NextResponse.json(parsed.data);
  } catch {
    return NextResponse.json(upstreamError('duckdb_unreachable'), { status: 502 });
  }
}
