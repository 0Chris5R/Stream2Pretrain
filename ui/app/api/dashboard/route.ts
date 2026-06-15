/**
 * GET /api/dashboard
 *
 * Returns the `DashboardSummary` payload the cockpit's home renders. The
 * route fans out to Prometheus for last-hour rollups (ingested / curated
 * / rejected counters) and to the in-cluster decon-gate for the quality
 * histogram, then collates a single JSON object. All fields are filled
 * with conservative zeros if any upstream returns an error so the page
 * never renders an empty state on a transient hiccup.
 */
import { NextResponse } from 'next/server';
import { z } from 'zod';

import { DashboardSummarySchema } from '@/lib/schemas';
import { UPSTREAM, upstreamError } from '@/lib/upstream';

export const runtime = 'nodejs';
export const dynamic = 'force-dynamic';

const RANGE = process.env.DASHBOARD_RANGE ?? '1h';

interface PromInstantResp {
  status: 'success' | 'error';
  data?: {
    result: Array<{ metric: Record<string, string>; value: [number, string] }>;
  };
}

async function promInstant(query: string): Promise<PromInstantResp['data'] | null> {
  const url = `${UPSTREAM.prometheus}/api/v1/query?query=${encodeURIComponent(query)}`;
  try {
    const resp = await fetch(url, { cache: 'no-store' });
    if (!resp.ok) return null;
    const json = (await resp.json()) as PromInstantResp;
    if (json.status !== 'success') return null;
    return json.data ?? null;
  } catch (err) {
    console.warn('dashboard prometheus fetch failed', (err as Error).message);
    return null;
  }
}

function singleNumber(data: PromInstantResp['data'] | null): number {
  if (!data || !data.result.length) return 0;
  const v = Number.parseFloat(data.result[0].value[1]);
  return Number.isFinite(v) ? v : 0;
}

function rejectedByReasonFromVector(
  data: PromInstantResp['data'] | null,
): Record<string, number> {
  const out: Record<string, number> = {};
  if (!data) return out;
  for (const series of data.result) {
    const reason = series.metric.reason ?? 'unknown';
    const v = Number.parseFloat(series.value[1]);
    if (Number.isFinite(v) && v > 0) {
      out[reason] = (out[reason] ?? 0) + v;
    }
  }
  return out;
}

function perSourceFromVector(
  accepted: PromInstantResp['data'] | null,
  total: PromInstantResp['data'] | null,
): Array<{ source: string; accepted: number; total: number }> {
  const map = new Map<string, { accepted: number; total: number }>();
  for (const series of accepted?.result ?? []) {
    const src = series.metric.source ?? 'unknown';
    const v = Number.parseFloat(series.value[1]);
    if (!map.has(src)) map.set(src, { accepted: 0, total: 0 });
    map.get(src)!.accepted += Number.isFinite(v) ? v : 0;
  }
  for (const series of total?.result ?? []) {
    const src = series.metric.source ?? 'unknown';
    const v = Number.parseFloat(series.value[1]);
    if (!map.has(src)) map.set(src, { accepted: 0, total: 0 });
    map.get(src)!.total += Number.isFinite(v) ? v : 0;
  }
  return Array.from(map.entries()).map(([source, v]) => ({ source, ...v }));
}

const QualityHistogramRespSchema = z.object({
  buckets: z.array(z.object({ score: z.number(), count: z.number().int().nonnegative() })),
});

async function fetchQualityHistogram(): Promise<{
  buckets: Array<{ score: number; count: number }>;
}> {
  try {
    const resp = await fetch(`${UPSTREAM.duckdb}/quality-histogram`, { cache: 'no-store' });
    if (!resp.ok) return { buckets: [] };
    const parsed = QualityHistogramRespSchema.safeParse(await resp.json());
    return parsed.success ? parsed.data : { buckets: [] };
  } catch {
    return { buckets: [] };
  }
}

export async function GET(): Promise<NextResponse> {
  const [ingested, curated, rejectedByReason, perSourceAccepted, perSourceTotal, histogram] =
    await Promise.all([
      promInstant(`sum(increase(s2p_processor_ingested_total[${RANGE}]))`),
      promInstant(`sum(increase(s2p_processor_curated_total[${RANGE}]))`),
      promInstant(`sum by (reason) (increase(s2p_processor_dropped_total[${RANGE}]))`),
      promInstant(`sum by (source) (increase(s2p_processor_curated_total[${RANGE}]))`),
      promInstant(`sum by (source) (increase(s2p_processor_ingested_total[${RANGE}]))`),
      fetchQualityHistogram(),
    ]);

  const summary = {
    ingested_last_hour: Math.max(0, Math.round(singleNumber(ingested))),
    curated_last_hour: Math.max(0, Math.round(singleNumber(curated))),
    rejected_by_reason: Object.fromEntries(
      Object.entries(rejectedByReasonFromVector(rejectedByReason)).map(([k, v]) => [
        k,
        Math.max(0, Math.round(v)),
      ]),
    ),
    per_source_acceptance: perSourceFromVector(perSourceAccepted, perSourceTotal).map((row) => ({
      source: row.source,
      accepted: Math.max(0, Math.round(row.accepted)),
      total: Math.max(0, Math.round(row.total)),
    })),
    quality_histogram: histogram,
  };

  const parsed = DashboardSummarySchema.safeParse(summary);
  if (!parsed.success) {
    return NextResponse.json(upstreamError('dashboard_shape_invalid'), { status: 502 });
  }
  return NextResponse.json(parsed.data);
}
