'use client';

import { useQuery } from '@tanstack/react-query';
import { useMemo, useState } from 'react';
import {
  CartesianGrid,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts';

import { Button } from '@/components/ui/button';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { apiFetch } from '@/lib/api';
import {
  ActivitySummarySchema,
  type ActivityPoint,
  type ActivitySummary,
  type ActivityWindow,
} from '@/lib/schemas';
import { formatInt } from '@/lib/utils';

type Stage = 'fetched' | 'extracted' | 'decided' | 'training';

const WINDOWS: Array<{ value: ActivityWindow; label: string }> = [
  { value: '5m', label: '5 min' },
  { value: '1h', label: '1 hour' },
  { value: '24h', label: '24 hours' },
];

const STAGES: Array<{ key: Stage; label: string; color: string }> = [
  { key: 'fetched', label: 'Fetched', color: '#2563eb' },
  { key: 'extracted', label: 'Extracted', color: '#0891b2' },
  { key: 'decided', label: 'Decided', color: '#7c3aed' },
  { key: 'training', label: 'Training output', color: '#059669' },
];

async function fetchActivity(window: ActivityWindow): Promise<ActivitySummary> {
  return apiFetch(`/api/activity?window=${window}`, ActivitySummarySchema);
}

export function ActivityPanel() {
  const [window, setWindow] = useState<ActivityWindow>('5m');
  const activity = useQuery({
    queryKey: ['activity', window],
    queryFn: () => fetchActivity(window),
    refetchInterval: 5_000,
  });
  const data = activity.data;
  const yMax = useMemo(() => {
    if (!data?.points.length) return 1;
    return Math.max(
      1,
      ...data.points.flatMap((point) => STAGES.map((stage) => point[stage.key])),
    );
  }, [data]);

  return (
    <Card>
      <CardHeader className="flex flex-row items-center justify-between gap-3 pb-2">
        <CardTitle className="text-sm font-medium">Live activity</CardTitle>
        <div className="flex rounded-md border p-0.5">
          {WINDOWS.map((option) => (
            <Button
              key={option.value}
              size="sm"
              variant={window === option.value ? 'secondary' : 'ghost'}
              className="h-7 px-2.5 text-xs"
              onClick={() => setWindow(option.value)}
            >
              {option.label}
            </Button>
          ))}
        </div>
      </CardHeader>
      <CardContent>
        {activity.error ? (
          <div className="flex h-36 items-center justify-center text-sm text-muted-foreground">
            Activity unavailable
          </div>
        ) : (
          <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
            {STAGES.map((stage) => (
              <StageChart
                key={stage.key}
                stage={stage}
                data={data}
                yMax={yMax}
              />
            ))}
          </div>
        )}
      </CardContent>
    </Card>
  );
}

function StageChart({
  stage,
  data,
  yMax,
}: {
  stage: (typeof STAGES)[number];
  data: ActivitySummary | undefined;
  yMax: number;
}) {
  const latest = data?.points.at(-1)?.[stage.key] ?? 0;
  return (
    <div className="min-w-0 rounded-lg border p-3">
      <div className="flex items-start justify-between gap-3">
        <div>
          <div className="flex items-center gap-2 text-xs text-muted-foreground">
            <span className="h-2 w-2 rounded-full" style={{ backgroundColor: stage.color }} />
            {stage.label}
          </div>
          <div className="mt-1 text-2xl font-semibold tabular-nums">
            {data ? formatInt(Math.round(data.totals[stage.key])) : '-'}
          </div>
        </div>
        <div className="text-right text-[11px] text-muted-foreground">
          <div>Latest</div>
          <div className="font-mono text-foreground">{formatInt(Math.round(latest))}</div>
        </div>
      </div>
      <div className="mt-2 h-24">
        {data ? (
          <ResponsiveContainer width="100%" height="100%">
            <LineChart data={data.points} margin={{ top: 4, right: 4, left: 4, bottom: 0 }}>
              <CartesianGrid vertical={false} strokeDasharray="3 3" className="stroke-border" />
              <XAxis dataKey="ts" hide />
              <YAxis hide domain={[0, yMax]} />
              <Tooltip content={<StageTooltip stage={stage} />} />
              <Line
                type="stepAfter"
                dataKey={stage.key}
                dot={false}
                isAnimationActive={false}
                stroke={stage.color}
                strokeWidth={2}
              />
            </LineChart>
          </ResponsiveContainer>
        ) : (
          <div className="h-full animate-pulse rounded bg-muted" />
        )}
      </div>
    </div>
  );
}

function StageTooltip({
  active,
  payload,
  stage,
}: {
  active?: boolean;
  payload?: Array<{ payload: ActivityPoint }>;
  stage: (typeof STAGES)[number];
}) {
  if (!active || !payload?.[0]) return null;
  const point = payload[0].payload;
  return (
    <div className="rounded-md border bg-card px-2.5 py-2 text-xs shadow-sm">
      <div className="text-muted-foreground">
        {new Date(point.ts).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })}
      </div>
      <div className="mt-0.5 font-medium" style={{ color: stage.color }}>
        {stage.label}: {formatInt(Math.round(point[stage.key]))}
      </div>
    </div>
  );
}
