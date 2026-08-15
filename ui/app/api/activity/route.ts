import { type NextRequest, NextResponse } from 'next/server';

import { ActivitySummarySchema, ActivityWindowSchema } from '@/lib/schemas';

export const runtime = 'nodejs';
export const dynamic = 'force-dynamic';

const PROMETHEUS_URL =
  process.env.PROMETHEUS_URL ?? 'http://kube-prometheus-stack-prometheus.monitoring.svc:9090';

const WINDOWS = {
  '5m': { seconds: 5 * 60, step: 10 },
  '1h': { seconds: 60 * 60, step: 60 },
  '24h': { seconds: 24 * 60 * 60, step: 300 },
} as const;

const METRICS = {
  fetched:
    'redpanda_kafka_records_produced_total{redpanda_namespace="kafka",redpanda_topic="raw.fetched"}',
  extracted:
    'redpanda_kafka_records_produced_total{redpanda_namespace="kafka",redpanda_topic="docs.normalized"}',
  decided:
    'redpanda_kafka_records_produced_total{redpanda_namespace="kafka",redpanda_topic="curation.decisions"}',
  training:
    'redpanda_kafka_records_produced_total{redpanda_namespace="kafka",redpanda_topic="docs.curated"}',
} as const;

type Stage = keyof typeof METRICS;

interface PrometheusMatrix {
  status: 'success' | 'error';
  data?: {
    result: Array<{ values?: Array<[number, string]> }>;
  };
}

function numeric(value: string | undefined): number {
  const parsed = Number.parseFloat(value ?? '0');
  return Number.isFinite(parsed) ? Math.max(0, parsed) : 0;
}

async function prometheus<T>(path: string, query: URLSearchParams): Promise<T> {
  const response = await fetch(`${PROMETHEUS_URL}${path}?${query.toString()}`, {
    cache: 'no-store',
  });
  if (!response.ok) throw new Error(`prometheus_status_${response.status}`);
  return (await response.json()) as T;
}

async function stageSeries(
  stage: Stage,
  start: number,
  end: number,
  step: number,
): Promise<{ values: Array<[number, number]>; total: number }> {
  const query = new URLSearchParams({
    query: METRICS[stage],
    // Include one sample before the visible window as an observed baseline.
    start: String(start - step),
    end: String(end),
    step: String(step),
  });
  const body = await prometheus<PrometheusMatrix>('/api/v1/query_range', query);
  if (body.status !== 'success') throw new Error('prometheus_range_error');
  const buckets = new Map<number, number>();
  for (let ts = start; ts <= end; ts += step) buckets.set(ts, 0);

  // Prometheus increase() extrapolates to the whole range when a counter was
  // created part-way through it. That is useful for rates but wrong for the
  // exact document counts shown here. Compute observed deltas per labelled
  // series and treat a decrease as a process-counter reset.
  for (const series of body.data?.result ?? []) {
    let previous: number | null = null;
    for (const [ts, raw] of series.values ?? []) {
      const current = numeric(raw);
      if (ts <= start) {
        previous = current;
        continue;
      }
      // A newly discovered target has no observation before the requested
      // window. Its first cumulative value is a baseline, not activity that
      // happened inside the window.
      if (previous === null) {
        previous = current;
        continue;
      }
      const delta = current >= previous ? current - previous : current;
      buckets.set(ts, (buckets.get(ts) ?? 0) + delta);
      previous = current;
    }
  }
  const values = [...buckets.entries()].sort(([left], [right]) => left - right);
  return { values, total: values.reduce((sum, [, value]) => sum + value, 0) };
}

export async function GET(request: NextRequest): Promise<NextResponse> {
  const parsedWindow = ActivityWindowSchema.safeParse(request.nextUrl.searchParams.get('window') ?? '5m');
  if (!parsedWindow.success) {
    return NextResponse.json({ detail: 'invalid activity window' }, { status: 400 });
  }

  const window = parsedWindow.data;
  const config = WINDOWS[window];
  const end = Math.floor(Date.now() / 1000);
  const start = end - config.seconds;
  const stages = Object.keys(METRICS) as Stage[];

  try {
    const series = await Promise.all(
      stages.map((stage) => stageSeries(stage, start, end, config.step)),
    );

    const points = new Map<
      number,
      { ts: string; fetched: number; extracted: number; decided: number; training: number }
    >();
    for (let index = 0; index < stages.length; index += 1) {
      const stage = stages[index];
      for (const [ts, value] of series[index].values) {
        const point = points.get(ts) ?? {
          ts: new Date(ts * 1000).toISOString(),
          fetched: 0,
          extracted: 0,
          decided: 0,
          training: 0,
        };
        point[stage] = value;
        points.set(ts, point);
      }
    }

    const totals = Object.fromEntries(
      stages.map((stage, index) => [stage, series[index].total]),
    ) as Record<Stage, number>;
    const payload = {
      window,
      start: new Date(start * 1000).toISOString(),
      end: new Date(end * 1000).toISOString(),
      bucket_seconds: config.step,
      totals,
      points: [...points.values()].sort((left, right) => left.ts.localeCompare(right.ts)),
    };
    return NextResponse.json(ActivitySummarySchema.parse(payload));
  } catch (error) {
    console.warn('activity query failed', error);
    return NextResponse.json({ detail: 'activity_unavailable' }, { status: 502 });
  }
}
