import { NextResponse } from 'next/server';

import { DocumentFacetsSchema } from '@/lib/schemas';
import { UPSTREAM, upstreamError } from '@/lib/upstream';

export const runtime = 'nodejs';
export const dynamic = 'force-dynamic';

export async function GET(req: Request): Promise<NextResponse> {
  const includeFixtures = new URL(req.url).searchParams.get('include_fixtures') === 'true';
  try {
    const response = await fetch(
      `${UPSTREAM.duckdb}/document-facets?include_fixtures=${includeFixtures}`,
      { cache: 'no-store' },
    );
    if (!response.ok) {
      return NextResponse.json(upstreamError(`document_facets_status_${response.status}`), {
        status: 502,
      });
    }
    const parsed = DocumentFacetsSchema.safeParse(await response.json());
    if (!parsed.success) {
      return NextResponse.json(upstreamError('document_facets_shape_invalid'), { status: 502 });
    }
    return NextResponse.json(parsed.data);
  } catch {
    return NextResponse.json(upstreamError('duckdb_unreachable'), { status: 502 });
  }
}
