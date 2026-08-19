import { NextRequest, NextResponse } from 'next/server';

import { z } from 'zod';

import { FoundryArtifactAuditResponseSchema } from '@/lib/schemas';
import { UPSTREAM, upstreamError } from '@/lib/upstream';

export const runtime = 'nodejs';
export const dynamic = 'force-dynamic';

const AuditRequestSchema = z.object({
  decision: z.enum(['approved', 'rejected']),
  reviewer: z.string().trim().min(1).max(200),
  reason: z.string().trim().max(2_000).optional(),
});

export async function POST(
  request: NextRequest,
  context: { params: Promise<{ artifact_id: string }> },
): Promise<NextResponse> {
  const token = process.env.FOUNDRY_CONTROL_TOKEN;
  if (!token) {
    return NextResponse.json(upstreamError('foundry_control_not_configured'), { status: 503 });
  }
  const parsedBody = AuditRequestSchema.safeParse(await request.json().catch(() => null));
  if (!parsedBody.success) {
    return NextResponse.json({ detail: 'invalid artifact audit' }, { status: 400 });
  }
  const { artifact_id } = await context.params;
  try {
    const response = await fetch(
      `${UPSTREAM.foundry}/api/foundry/artifacts/${encodeURIComponent(artifact_id)}/audit`,
      {
        method: 'POST',
        cache: 'no-store',
        headers: {
          Authorization: `Bearer ${token}`,
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(parsedBody.data),
      },
    );
    const payload: unknown = await response.json();
    if (!response.ok) return NextResponse.json(payload, { status: response.status });
    const parsed = FoundryArtifactAuditResponseSchema.safeParse(payload);
    if (!parsed.success) {
      return NextResponse.json(upstreamError('foundry_shape_invalid'), { status: 502 });
    }
    return NextResponse.json(parsed.data, { status: response.status });
  } catch {
    return NextResponse.json(upstreamError('foundry_unavailable'), { status: 503 });
  }
}
