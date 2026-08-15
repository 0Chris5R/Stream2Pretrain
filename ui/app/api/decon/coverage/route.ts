import { NextResponse } from 'next/server';

import { BenchmarkCoverageSchema } from '@/lib/schemas';
import { UPSTREAM, upstreamError } from '@/lib/upstream';

export const runtime = 'nodejs';
export const dynamic = 'force-dynamic';

export async function GET(): Promise<NextResponse> {
  try {
    const response = await fetch(`${UPSTREAM.deconGate}/coverage`, { cache: 'no-store' });
    if (!response.ok) {
      return NextResponse.json(upstreamError(`benchmark_coverage_status_${response.status}`), {
        status: 502,
      });
    }
    const parsed = BenchmarkCoverageSchema.safeParse(await response.json());
    if (!parsed.success) {
      return NextResponse.json(upstreamError('benchmark_coverage_shape_invalid'), { status: 502 });
    }
    return NextResponse.json(parsed.data);
  } catch {
    return NextResponse.json(upstreamError('decon_gate_unreachable'), { status: 502 });
  }
}
