import { NextRequest, NextResponse } from 'next/server';

import { FoundryArtifactListSchema } from '@/lib/schemas';
import { UPSTREAM, upstreamError } from '@/lib/upstream';

export const runtime = 'nodejs';
export const dynamic = 'force-dynamic';

export async function GET(request: NextRequest): Promise<NextResponse> {
  try {
    const query = request.nextUrl.searchParams.toString();
    const response = await fetch(
      `${UPSTREAM.foundry}/api/foundry/artifacts${query ? `?${query}` : ''}`,
      { cache: 'no-store' },
    );
    if (!response.ok)
      return NextResponse.json(upstreamError('foundry_unavailable'), { status: 503 });
    const parsed = FoundryArtifactListSchema.safeParse(await response.json());
    if (!parsed.success)
      return NextResponse.json(upstreamError('foundry_shape_invalid'), { status: 502 });
    return NextResponse.json(parsed.data);
  } catch {
    return NextResponse.json(upstreamError('foundry_unavailable'), { status: 503 });
  }
}
