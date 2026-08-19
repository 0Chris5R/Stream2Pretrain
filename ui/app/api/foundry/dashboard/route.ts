import { NextResponse } from 'next/server';

import { FoundryDashboardSchema } from '@/lib/schemas';
import { UPSTREAM, upstreamError } from '@/lib/upstream';

export const runtime = 'nodejs';
export const dynamic = 'force-dynamic';

export async function GET(): Promise<NextResponse> {
  try {
    const response = await fetch(`${UPSTREAM.foundry}/api/foundry/dashboard`, {
      cache: 'no-store',
    });
    if (!response.ok)
      return NextResponse.json(upstreamError('foundry_unavailable'), { status: 503 });
    const parsed = FoundryDashboardSchema.safeParse(await response.json());
    if (!parsed.success)
      return NextResponse.json(upstreamError('foundry_shape_invalid'), { status: 502 });
    return NextResponse.json(parsed.data);
  } catch {
    return NextResponse.json(upstreamError('foundry_unavailable'), { status: 503 });
  }
}
