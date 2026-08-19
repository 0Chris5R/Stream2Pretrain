import { NextRequest, NextResponse } from 'next/server';

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
      `${UPSTREAM.foundry}/api/foundry/artifacts/${encodeURIComponent(artifact_id)}/package`,
      {
        cache: 'no-store',
        headers: { Authorization: `Bearer ${token}` },
      },
    );
    if (!response.ok) {
      const payload: unknown = await response.json().catch(() => upstreamError('foundry_unavailable'));
      return NextResponse.json(payload, { status: response.status });
    }
    return new NextResponse(response.body, {
      status: 200,
      headers: {
        'Content-Type': response.headers.get('content-type') ?? 'application/gzip',
        'Content-Disposition':
          response.headers.get('content-disposition') ?? 'attachment; filename="artifact.tar.gz"',
      },
    });
  } catch {
    return NextResponse.json(upstreamError('foundry_unavailable'), { status: 503 });
  }
}
