import { NextRequest, NextResponse } from 'next/server';

import { FoundryArtifactInspectionSchema } from '@/lib/schemas';
import { UPSTREAM, upstreamError } from '@/lib/upstream';

export const runtime = 'nodejs';
export const dynamic = 'force-dynamic';

export async function GET(
  _request: NextRequest,
  context: { params: Promise<{ artifact_id: string }> },
): Promise<NextResponse> {
  const token = process.env.FOUNDRY_CONTROL_TOKEN;
  if (!token) {
    return NextResponse.json(upstreamError('foundry_control_not_configured'), { status: 503 });
  }
  const { artifact_id } = await context.params;
  try {
    const response = await fetch(
      `${UPSTREAM.foundry}/api/foundry/artifacts/${encodeURIComponent(artifact_id)}/inspect`,
      {
        cache: 'no-store',
        headers: { Authorization: `Bearer ${token}` },
      },
    );
    const payload: unknown = await response.json();
    if (!response.ok) return NextResponse.json(payload, { status: response.status });
    const parsed = FoundryArtifactInspectionSchema.safeParse(payload);
    if (!parsed.success) {
      return NextResponse.json(upstreamError('foundry_shape_invalid'), { status: 502 });
    }
    return NextResponse.json(parsed.data);
  } catch {
    return NextResponse.json(upstreamError('foundry_unavailable'), { status: 503 });
  }
}
