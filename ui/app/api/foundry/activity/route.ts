import { NextRequest, NextResponse } from 'next/server';

import { ActivityWindowSchema, FoundryActivitySchema } from '@/lib/schemas';
import { UPSTREAM, upstreamError } from '@/lib/upstream';

export const runtime = 'nodejs';
export const dynamic = 'force-dynamic';

export async function GET(request: NextRequest): Promise<NextResponse> {
  const window = ActivityWindowSchema.safeParse(request.nextUrl.searchParams.get('window') ?? '5m');
  if (!window.success) {
    return NextResponse.json({ detail: 'window must be one of 5m, 1h, or 24h' }, { status: 400 });
  }
  try {
    const response = await fetch(
      `${UPSTREAM.foundry}/api/foundry/activity?window=${encodeURIComponent(window.data)}`,
      { cache: 'no-store' },
    );
    if (!response.ok) {
      return NextResponse.json(upstreamError('foundry_unavailable'), { status: 503 });
    }
    const parsed = FoundryActivitySchema.safeParse(await response.json());
    if (!parsed.success) {
      return NextResponse.json(upstreamError('foundry_shape_invalid'), { status: 502 });
    }
    return NextResponse.json(parsed.data);
  } catch {
    return NextResponse.json(upstreamError('foundry_unavailable'), { status: 503 });
  }
}
