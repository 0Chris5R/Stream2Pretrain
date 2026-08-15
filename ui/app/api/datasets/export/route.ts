import { NextResponse } from 'next/server';

import { UPSTREAM, upstreamError } from '@/lib/upstream';

export const runtime = 'nodejs';
export const dynamic = 'force-dynamic';

export async function GET(req: Request): Promise<Response> {
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
    'format',
    'limit',
  ]);
  const query = new URLSearchParams();
  for (const [key, value] of incoming.searchParams.entries()) {
    if (allowed.has(key)) query.append(key, value);
  }
  try {
    const response = await fetch(`${UPSTREAM.duckdb}/datasets/export?${query.toString()}`, {
      cache: 'no-store',
    });
    if (!response.ok) {
      return NextResponse.json(upstreamError(`dataset_export_status_${response.status}`), {
        status: response.status === 400 ? 400 : 502,
      });
    }
    const format = incoming.searchParams.get('format') === 'parquet' ? 'parquet' : 'jsonl';
    return new Response(response.body, {
      headers: {
        'Content-Type':
          format === 'parquet' ? 'application/vnd.apache.parquet' : 'application/x-ndjson',
        'Content-Disposition': `attachment; filename="stream2pretrain.${format}"`,
      },
    });
  } catch {
    return NextResponse.json(upstreamError('duckdb_unreachable'), { status: 502 });
  }
}
