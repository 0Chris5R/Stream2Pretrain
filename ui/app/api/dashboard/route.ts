/**
 * GET /api/dashboard
 *
 * Returns the `DashboardSummary` payload the cockpit renders. Corpus totals,
 * source acceptance, and rejection reasons come from durable Iceberg tables,
 * so replacing a worker cannot make an existing corpus appear empty.
 * Prometheus remains the source for the separate live-activity spark line.
 */
import { NextResponse } from 'next/server';
import { z } from 'zod';

import { DashboardSummarySchema } from '@/lib/schemas';
import { UPSTREAM, upstreamError } from '@/lib/upstream';

export const runtime = 'nodejs';
export const dynamic = 'force-dynamic';

const QualityHistogramRespSchema = z.object({
  buckets: z.array(z.object({ score: z.number(), count: z.number().int().nonnegative() })),
  edu_buckets: z.array(z.object({ score: z.number(), count: z.number().int().nonnegative() })),
});

const RouteSummarySchema = z.array(z.object({
  route: z.string(),
  documents: z.number().int().nonnegative(),
  source_words: z.number().int().nonnegative(),
  training_words: z.number().int().nonnegative(),
  mean_quality: z.number(),
  mean_edu: z.number(),
}));

const CorpusOverviewSchema = z.object({
  durable_decisions: z.number().int().nonnegative(),
  training_export_documents: z.number().int().nonnegative(),
  rejected_by_reason: z.record(z.string(), z.number().int().nonnegative()),
  per_source_acceptance: z.array(z.object({
    source: z.string(),
    accepted: z.number().int().nonnegative(),
    total: z.number().int().nonnegative(),
  })),
});

async function fetchQualityHistogram(): Promise<{
  buckets: Array<{ score: number; count: number }>;
  edu_buckets: Array<{ score: number; count: number }>;
}> {
  try {
    const resp = await fetch(`${UPSTREAM.duckdb}/quality-histogram`, { cache: 'no-store' });
    if (!resp.ok) return { buckets: [], edu_buckets: [] };
    const parsed = QualityHistogramRespSchema.safeParse(await resp.json());
    return parsed.success ? parsed.data : { buckets: [], edu_buckets: [] };
  } catch {
    return { buckets: [], edu_buckets: [] };
  }
}

async function fetchRouteSummary(): Promise<z.infer<typeof RouteSummarySchema>> {
  try {
    const resp = await fetch(`${UPSTREAM.duckdb}/curation-summary`, { cache: 'no-store' });
    if (!resp.ok) return [];
    const parsed = RouteSummarySchema.safeParse(await resp.json());
    return parsed.success ? parsed.data : [];
  } catch {
    return [];
  }
}

async function fetchCorpusOverview(): Promise<z.infer<typeof CorpusOverviewSchema>> {
  const resp = await fetch(`${UPSTREAM.duckdb}/corpus-overview`, { cache: 'no-store' });
  if (!resp.ok) {
    throw new Error(`durable corpus overview returned ${resp.status}`);
  }
  return CorpusOverviewSchema.parse(await resp.json());
}

export async function GET(): Promise<NextResponse> {
  let overview: z.infer<typeof CorpusOverviewSchema>;
  try {
    overview = await fetchCorpusOverview();
  } catch (err) {
    return NextResponse.json(
      upstreamError(`durable corpus overview unavailable: ${(err as Error).message}`),
      { status: 503 },
    );
  }
  const [histogram, routes] = await Promise.all([
    fetchQualityHistogram(),
    fetchRouteSummary(),
  ]);

  const summary = {
    ...overview,
    quality_histogram: histogram,
    route_distribution: routes,
  };

  const parsed = DashboardSummarySchema.safeParse(summary);
  if (!parsed.success) {
    return NextResponse.json(upstreamError('dashboard_shape_invalid'), { status: 502 });
  }
  return NextResponse.json(parsed.data);
}
