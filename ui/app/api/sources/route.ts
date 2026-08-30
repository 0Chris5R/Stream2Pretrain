/**
 * GET /api/sources
 *
 * Read-only source monitoring endpoint. Source configuration is deployment
 * configuration and is never mutated through the dashboard.
 */
import { NextResponse } from 'next/server';
import { z } from 'zod';

import { SourceFeedStatusSchema } from '@/lib/schemas';
import { UPSTREAM, upstreamError } from '@/lib/upstream';

export const runtime = 'nodejs';
export const dynamic = 'force-dynamic';

const SourceListSchema = z.array(SourceFeedStatusSchema);
const SourceActivitySchema = z.object({
  window_hours: z.literal(24),
  sources: z.array(
    z.object({
      source_feed: z.string(),
      documents: z.number().int().nonnegative(),
      admitted: z.number().int().nonnegative(),
      posttrain_transform_only: z.number().int().nonnegative(),
      quarantined: z.number().int().nonnegative(),
      last_observed_at: z.string().nullable(),
      license_distribution: z.array(
        z.object({
          license_id: z.string(),
          status: z.enum(['admitted', 'posttrain_transform_only', 'quarantined']),
          count: z.number().int().nonnegative(),
        }),
      ),
      license_provenance: z.array(
        z.object({
          license_source: z.string(),
          count: z.number().int().nonnegative(),
        }),
      ),
    }),
  ),
});

type SourceActivity = z.infer<typeof SourceActivitySchema>['sources'][number];

async function fetchSourceActivity(): Promise<Map<string, SourceActivity>> {
  try {
    const response = await fetch(`${UPSTREAM.duckdb}/source-activity?window_hours=24`, {
      cache: 'no-store',
    });
    if (!response.ok) return new Map();
    const parsed = SourceActivitySchema.safeParse(await response.json());
    if (!parsed.success) return new Map();
    return new Map(parsed.data.sources.map((row) => [row.source_feed, row]));
  } catch {
    return new Map();
  }
}

export async function GET(): Promise<NextResponse> {
  try {
    const resp = await fetch(`${UPSTREAM.sourcesApi}/v1/sources`, { cache: 'no-store' });
    if (!resp.ok) {
      return NextResponse.json(upstreamError(`sources_api_status_${resp.status}`), {
        status: 502,
      });
    }
    const parsed = SourceListSchema.safeParse(await resp.json());
    if (!parsed.success) {
      return NextResponse.json(upstreamError('sources_shape_invalid'), { status: 502 });
    }
    const activity = await fetchSourceActivity();
    return NextResponse.json(
      parsed.data.map((source) => {
        const observed = activity.get(source.name);
        return {
          ...source,
          last_success_at: observed?.last_observed_at ?? source.last_success_at,
          last_attempt_at: observed?.last_observed_at ?? source.last_attempt_at,
          documents_24h: observed?.documents ?? source.documents_24h,
          pretrain_documents_24h: observed?.admitted ?? 0,
          posttrain_only_documents_24h: observed?.posttrain_transform_only ?? 0,
          quarantined_documents_24h: observed?.quarantined ?? 0,
          license_distribution: observed?.license_distribution ?? [],
          license_provenance: observed?.license_provenance ?? [],
        };
      }),
    );
  } catch (err) {
    console.warn('sources GET upstream failed', (err as Error).message);
    return NextResponse.json(upstreamError('sources_api_unreachable'), { status: 502 });
  }
}
