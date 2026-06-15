/**
 * GET /api/sources
 * POST /api/sources
 *
 * Proxies SourceFeed CRUD to the FastAPI submit-api. The cockpit never
 * talks to the Kubernetes API directly: the submit-api owns auth, schema
 * validation, and the Gatekeeper round-trip on POST. We only forward the
 * already-validated payload.
 */
import { NextResponse } from 'next/server';
import { z } from 'zod';

import { SourceFeedSpecSchema, SourceFeedStatusSchema } from '@/lib/schemas';
import { UPSTREAM, upstreamError } from '@/lib/upstream';

export const runtime = 'nodejs';
export const dynamic = 'force-dynamic';

const SourceListSchema = z.array(SourceFeedStatusSchema);

export async function GET(): Promise<NextResponse> {
  try {
    const resp = await fetch(`${UPSTREAM.submitApi}/v1/sources`, { cache: 'no-store' });
    if (!resp.ok) {
      return NextResponse.json(upstreamError(`submit_api_status_${resp.status}`), {
        status: 502,
      });
    }
    const parsed = SourceListSchema.safeParse(await resp.json());
    if (!parsed.success) {
      return NextResponse.json(upstreamError('sources_shape_invalid'), { status: 502 });
    }
    return NextResponse.json(parsed.data);
  } catch (err) {
    console.warn('sources GET upstream failed', (err as Error).message);
    return NextResponse.json(upstreamError('submit_api_unreachable'), { status: 502 });
  }
}

export async function POST(req: Request): Promise<NextResponse> {
  const body = await req.json().catch(() => null);
  const parsed = SourceFeedSpecSchema.safeParse(body);
  if (!parsed.success) {
    return NextResponse.json({ detail: parsed.error.message }, { status: 400 });
  }
  try {
    const resp = await fetch(`${UPSTREAM.submitApi}/v1/sources`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(parsed.data),
    });
    const text = await resp.text();
    if (!resp.ok) {
      return NextResponse.json(
        { detail: text || `submit_api_status_${resp.status}` },
        { status: resp.status >= 400 && resp.status < 500 ? resp.status : 502 },
      );
    }
    const status = SourceFeedStatusSchema.safeParse(JSON.parse(text));
    if (!status.success) {
      return NextResponse.json(upstreamError('sources_shape_invalid'), { status: 502 });
    }
    return NextResponse.json(status.data, { status: 201 });
  } catch (err) {
    console.warn('sources POST upstream failed', (err as Error).message);
    return NextResponse.json(upstreamError('submit_api_unreachable'), { status: 502 });
  }
}
