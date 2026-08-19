import { NextRequest, NextResponse } from 'next/server';

import { FoundryJobDetailSchema } from '@/lib/schemas';
import { UPSTREAM, upstreamError } from '@/lib/upstream';

export const runtime = 'nodejs';
export const dynamic = 'force-dynamic';

export async function GET(
  _request: NextRequest,
  context: { params: Promise<{ job_id: string }> },
): Promise<NextResponse> {
  const { job_id } = await context.params;
  try {
    const response = await fetch(
      `${UPSTREAM.foundry}/api/foundry/jobs/${encodeURIComponent(job_id)}`,
      { cache: 'no-store' },
    );
    if (response.status === 404) return NextResponse.json({ detail: 'job not found' }, { status: 404 });
    if (!response.ok) return NextResponse.json(upstreamError('foundry_unavailable'), { status: 503 });
    const parsed = FoundryJobDetailSchema.safeParse(await response.json());
    if (!parsed.success) return NextResponse.json(upstreamError('foundry_shape_invalid'), { status: 502 });
    return NextResponse.json(parsed.data);
  } catch {
    return NextResponse.json(upstreamError('foundry_unavailable'), { status: 503 });
  }
}
