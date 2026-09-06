import { NextRequest, NextResponse } from 'next/server';

import { UPSTREAM, upstreamError } from '@/lib/upstream';

export const runtime = 'nodejs';
export const dynamic = 'force-dynamic';

export async function GET(request: NextRequest): Promise<Response> {
  const token = process.env.FOUNDRY_CONTROL_TOKEN;
  if (!token) {
    return NextResponse.json(upstreamError('foundry_control_not_configured'), { status: 503 });
  }
  const query = new URLSearchParams();
  const allowed = new Set(['kind', 'dataset_split', 'date_from', 'date_to']);
  for (const [key, value] of request.nextUrl.searchParams.entries()) {
    if (allowed.has(key)) query.append(key, value);
  }
  const kind = query.get('kind');
  if (kind !== 'sft_trajectory' && kind !== 'rl_environment') {
    return NextResponse.json(upstreamError('invalid_dataset_kind'), { status: 400 });
  }
  try {
    const response = await fetch(
      `${UPSTREAM.foundry}/api/foundry/datasets/export?${query.toString()}`,
      {
        cache: 'no-store',
        headers: { Authorization: `Bearer ${token}` },
      },
    );
    if (!response.ok) {
      const payload: unknown = await response
        .json()
        .catch(() => upstreamError('foundry_unavailable'));
      return NextResponse.json(payload, { status: response.status });
    }
    const fallback =
      kind === 'sft_trajectory'
        ? 'attachment; filename="stream2pretrain-sft.jsonl"'
        : 'attachment; filename="stream2pretrain-rl-environments.tar.gz"';
    return new Response(response.body, {
      headers: {
        'Content-Type': response.headers.get('content-type') ?? 'application/octet-stream',
        'Content-Disposition': response.headers.get('content-disposition') ?? fallback,
      },
    });
  } catch {
    return NextResponse.json(upstreamError('foundry_unavailable'), { status: 503 });
  }
}
