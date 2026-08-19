import { NextResponse } from 'next/server';

import { FoundryManualRunResponseSchema } from '@/lib/schemas';
import { UPSTREAM, upstreamError } from '@/lib/upstream';

export const runtime = 'nodejs';
export const dynamic = 'force-dynamic';

export async function POST(request: Request): Promise<NextResponse> {
  const token = process.env.FOUNDRY_CONTROL_TOKEN;
  if (!token) {
    return NextResponse.json(upstreamError('foundry_control_not_configured'), { status: 503 });
  }
  try {
    const body = await request.text();
    const response = await fetch(`${UPSTREAM.foundry}/api/foundry/runs/manual`, {
      method: 'POST',
      cache: 'no-store',
      headers: {
        Authorization: `Bearer ${token}`,
        ...(body ? { 'Content-Type': 'application/json' } : {}),
      },
      ...(body ? { body } : {}),
    });
    const payload: unknown = await response.json();
    if (!response.ok) return NextResponse.json(payload, { status: response.status });
    const parsed = FoundryManualRunResponseSchema.safeParse(payload);
    if (!parsed.success) {
      return NextResponse.json(upstreamError('foundry_shape_invalid'), { status: 502 });
    }
    return NextResponse.json(parsed.data, { status: response.status });
  } catch {
    return NextResponse.json(upstreamError('foundry_unavailable'), { status: 503 });
  }
}
