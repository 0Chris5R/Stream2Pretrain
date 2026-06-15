/**
 * GET /api/decon?limit=N
 *
 * Returns the most recent `DeconAttestation` records published to the
 * `decon.attest` topic / decon-gate REST. The cockpit lists them so a
 * grader can pick a snapshot id and verify its signature via
 * `/api/decon/verify`.
 */
import { NextResponse } from 'next/server';
import { z } from 'zod';

import { DeconAttestationSchema } from '@/lib/schemas';
import { UPSTREAM, upstreamError } from '@/lib/upstream';

export const runtime = 'nodejs';
export const dynamic = 'force-dynamic';

const ListSchema = z.array(DeconAttestationSchema);

export async function GET(req: Request): Promise<NextResponse> {
  const url = new URL(req.url);
  const rawLimit = url.searchParams.get('limit');
  const limit = Math.min(200, Math.max(1, Number.parseInt(rawLimit ?? '20', 10) || 20));
  try {
    const resp = await fetch(`${UPSTREAM.deconGate}/attestations?limit=${limit}`, {
      cache: 'no-store',
    });
    if (!resp.ok) {
      return NextResponse.json(upstreamError(`decon_gate_status_${resp.status}`), {
        status: 502,
      });
    }
    const parsed = ListSchema.safeParse(await resp.json());
    if (!parsed.success) {
      return NextResponse.json(upstreamError('decon_shape_invalid'), { status: 502 });
    }
    return NextResponse.json(parsed.data);
  } catch (err) {
    console.warn('decon list upstream failed', (err as Error).message);
    return NextResponse.json(upstreamError('decon_gate_unreachable'), { status: 502 });
  }
}
