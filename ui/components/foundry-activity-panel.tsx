'use client';

import { useQuery } from '@tanstack/react-query';
import * as React from 'react';
import {
  Area,
  AreaChart,
  Bar,
  BarChart,
  CartesianGrid,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts';

import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { apiFetch } from '@/lib/api';
import {
  FoundryActivitySchema,
  type ActivityWindow,
  type FoundryActivity,
  type FoundryDashboard,
} from '@/lib/schemas';
import { formatInt, relativeTime } from '@/lib/utils';

const WINDOWS: Array<{ value: ActivityWindow; label: string }> = [
  { value: '5m', label: '5 min' },
  { value: '1h', label: '1 hour' },
  { value: '24h', label: '24 hours' },
];

const CALL_SERIES = [
  { key: 'callsStarted', label: 'Started', color: '#2563eb' },
  { key: 'callsSucceeded', label: 'Succeeded', color: '#059669' },
  { key: 'callsFailed', label: 'Failed', color: '#dc2626' },
  { key: 'callsRateLimited', label: 'Rate limited', color: '#d97706' },
] as const;

const TOKEN_SERIES = [
  { key: 'inputTokens', label: 'Input', color: '#7c3aed' },
  { key: 'outputTokens', label: 'Output', color: '#0891b2' },
] as const;

const STAGE_SERIES = [
  { key: 'received', label: 'Received', color: '#64748b' },
  { key: 'graphCompiled', label: 'Evidence graph', color: '#2563eb' },
  { key: 'graphCritiqued', label: 'Graph reviewed', color: '#4f46e5' },
  { key: 'tasksProposed', label: 'Tasks', color: '#7c3aed' },
  { key: 'solutionsGenerated', label: 'Solutions', color: '#c026d3' },
  { key: 'verifiersCompiled', label: 'Verifiers', color: '#d97706' },
  { key: 'adversarialValidated', label: 'Adversarial', color: '#059669' },
] as const;

const subscribeToClient = () => () => undefined;
const clientSnapshot = () => true;
const serverSnapshot = () => false;

type ChartPoint = {
  ts: string;
  callsStarted: number;
  callsSucceeded: number;
  callsFailed: number;
  callsRateLimited: number;
  inputTokens: number;
  outputTokens: number;
  received: number;
  graphCompiled: number;
  graphCritiqued: number;
  tasksProposed: number;
  solutionsGenerated: number;
  verifiersCompiled: number;
  adversarialValidated: number;
};

async function fetchActivity(window: ActivityWindow): Promise<FoundryActivity> {
  return apiFetch(`/api/foundry/activity?window=${window}`, FoundryActivitySchema);
}

export function FoundryActivityPanel({ dashboard }: { dashboard?: FoundryDashboard }) {
  const [window, setWindow] = React.useState<ActivityWindow>('5m');
  const chartsReady = React.useSyncExternalStore(
    subscribeToClient,
    clientSnapshot,
    serverSnapshot,
  );
  const activity = useQuery({
    queryKey: ['foundry-activity', window],
    queryFn: () => fetchActivity(window),
    refetchInterval: 2_000,
  });
  const data = activity.data;
  const points = React.useMemo(() => flattenPoints(data), [data]);
  const activeRun =
    dashboard?.manual_runs.find((run) => ['pending', 'running'].includes(run.state)) ??
    dashboard?.daily_runs.find((run) => run.state === 'running') ??
    dashboard?.manual_runs[0] ??
    dashboard?.daily_runs[0];
  const processed = activeRun?.processed_count ?? 0;
  const candidates = activeRun?.candidate_count ?? 0;
  const progress = candidates ? Math.min(100, (processed / candidates) * 100) : 0;

  return (
    <Card>
      <CardHeader className="flex flex-row items-center justify-between gap-3 p-4 pb-3">
        <CardTitle className="text-base">Run monitor</CardTitle>
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
      <CardContent className="space-y-4 p-4 pt-0">
        {activity.error ? (
          <div className="rounded-lg border border-destructive/40 bg-destructive/5 p-3 text-sm text-destructive">
            Monitoring unavailable
          </div>
        ) : (
          <>
            <ActiveOperations activity={data} dashboard={dashboard} />

            <div className="grid gap-3 sm:grid-cols-4">
              <RunMetric
                label="Run progress"
                value={activeRun ? `${formatInt(processed)} / ${formatInt(candidates)}` : '-'}
              >
                <div className="mt-2 h-1.5 overflow-hidden rounded-full bg-muted">
                  <div className="h-full bg-primary" style={{ width: `${progress}%` }} />
                </div>
              </RunMetric>
              <RunMetric label="Queued papers" value={formatInt(dashboard?.queued_candidates)} />
              <RunMetric label="Completed calls" value={formatInt(data?.totals.calls.succeeded)} />
              <RunMetric
                label="Failed calls"
                value={formatInt(
                  (data?.totals.calls.failed ?? 0) + (data?.totals.calls.rate_limited ?? 0),
                )}
              />
            </div>

            <div className="grid gap-3 xl:grid-cols-3">
              <CallsChart points={points} data={data} window={window} ready={chartsReady} />
              <TokensChart points={points} data={data} window={window} ready={chartsReady} />
              <StagesChart points={points} data={data} window={window} ready={chartsReady} />
            </div>
          </>
        )}
      </CardContent>
    </Card>
  );
}

function ActiveOperations({
  activity,
  dashboard,
}: {
  activity?: FoundryActivity;
  dashboard?: FoundryDashboard;
}) {
  const calls = activity?.active_calls ?? [];
  const modelByProvider = new Map<string, string>(
    (dashboard?.models ?? []).map((snapshot) => [
      snapshot.provider,
      snapshot.configured_model_ids[0] ?? 'Unknown model',
    ]),
  );
  if (!activity) {
    return <div className="h-24 animate-pulse rounded-lg bg-muted" />;
  }
  if (!calls.length) {
    const latest = dashboard?.recent_jobs[0];
    return (
      <div className="flex items-center justify-between rounded-lg border px-4 py-3">
        <div>
          <div className="font-medium">No model call in progress</div>
          <div className="mt-0.5 text-xs text-muted-foreground">
            {latest ? `${latest.paper_id} · ${stageLabel(latest.state)}` : 'No paper selected'}
          </div>
        </div>
        {latest ? <Badge variant="secondary">{relativeTime(latest.updated_at)}</Badge> : null}
      </div>
    );
  }
  return (
    <div className="overflow-hidden rounded-lg border">
      {calls.map((call) => (
        <div
          key={`${call.job_id}:${call.attempt}`}
          className="grid gap-3 border-b px-4 py-3 last:border-b-0 md:grid-cols-[minmax(12rem,1.3fr)_minmax(10rem,1fr)_repeat(3,minmax(7rem,0.65fr))] md:items-center"
        >
          <div className="min-w-0">
            <div className="truncate font-medium">{call.paper_id}</div>
            <div className="truncate text-xs text-muted-foreground">
              {modelByProvider.get(call.provider) ?? humanize(call.provider)}
            </div>
          </div>
          <OperationValue label="Stage" value={roleLabel(call.role)} />
          <OperationValue label="Elapsed" value={elapsed(call.started_at)} />
          <OperationValue label="Streamed" value={`${formatInt(call.partial_characters)} chars`} />
          <OperationValue
            label="Checkpoint"
            value={call.checkpoint_at ? relativeTime(call.checkpoint_at) : 'Waiting'}
          />
        </div>
      ))}
    </div>
  );
}

function OperationValue({ label, value }: { label: string; value: string }) {
  return (
    <div className="min-w-0">
      <div className="text-[11px] uppercase tracking-wide text-muted-foreground">{label}</div>
      <div className="mt-0.5 truncate text-sm font-medium tabular-nums">{value}</div>
    </div>
  );
}

function RunMetric({
  label,
  value,
  children,
}: {
  label: string;
  value: string;
  children?: React.ReactNode;
}) {
  return (
    <div className="rounded-lg border bg-muted/10 p-3">
      <div className="text-xs text-muted-foreground">{label}</div>
      <div className="mt-1 text-xl font-semibold tabular-nums">{value}</div>
      {children}
    </div>
  );
}

function CallsChart({
  points,
  data,
  window,
  ready,
}: {
  points: ChartPoint[];
  data?: FoundryActivity;
  window: ActivityWindow;
  ready: boolean;
}) {
  return (
    <ChartFrame
      title="Model calls"
      summary={`${formatInt(data?.totals.calls.succeeded)} completed`}
      series={CALL_SERIES}
    >
      {ready ? (
        <ResponsiveContainer
          width="100%"
          height="100%"
          minWidth={0}
          minHeight={0}
          initialDimension={{ width: 480, height: 176 }}
        >
          <LineChart data={points} margin={{ top: 8, right: 8, left: -18, bottom: 0 }}>
            <CartesianGrid vertical={false} strokeDasharray="3 3" className="stroke-border" />
            <XAxis
              dataKey="ts"
              tickFormatter={(value) => timeTick(value, window)}
              minTickGap={24}
            />
            <YAxis allowDecimals={false} />
            <Tooltip content={<MonitorTooltip series={CALL_SERIES} />} />
            {CALL_SERIES.map((series) => (
              <Line
                key={series.key}
                type="stepAfter"
                dataKey={series.key}
                stroke={series.color}
                strokeWidth={2}
                dot={false}
                isAnimationActive={false}
              />
            ))}
          </LineChart>
        </ResponsiveContainer>
      ) : (
        <ChartSkeleton />
      )}
    </ChartFrame>
  );
}

function TokensChart({
  points,
  data,
  window,
  ready,
}: {
  points: ChartPoint[];
  data?: FoundryActivity;
  window: ActivityWindow;
  ready: boolean;
}) {
  return (
    <ChartFrame
      title="Completed tokens"
      summary={`${formatInt(data?.totals.tokens.input)} in · ${formatInt(data?.totals.tokens.output)} out`}
      series={TOKEN_SERIES}
    >
      {ready ? (
        <ResponsiveContainer
          width="100%"
          height="100%"
          minWidth={0}
          minHeight={0}
          initialDimension={{ width: 480, height: 176 }}
        >
          <AreaChart data={points} margin={{ top: 8, right: 8, left: -8, bottom: 0 }}>
            <CartesianGrid vertical={false} strokeDasharray="3 3" className="stroke-border" />
            <XAxis
              dataKey="ts"
              tickFormatter={(value) => timeTick(value, window)}
              minTickGap={24}
            />
            <YAxis tickFormatter={compactNumber} />
            <Tooltip content={<MonitorTooltip series={TOKEN_SERIES} />} />
            {TOKEN_SERIES.map((series) => (
              <Area
                key={series.key}
                type="stepAfter"
                dataKey={series.key}
                stroke={series.color}
                fill={series.color}
                fillOpacity={0.14}
                strokeWidth={2}
                isAnimationActive={false}
              />
            ))}
          </AreaChart>
        </ResponsiveContainer>
      ) : (
        <ChartSkeleton />
      )}
    </ChartFrame>
  );
}

function StagesChart({
  points,
  data,
  window,
  ready,
}: {
  points: ChartPoint[];
  data?: FoundryActivity;
  window: ActivityWindow;
  ready: boolean;
}) {
  const total = data
    ? Object.values(data.totals.stages).reduce((sum, value) => sum + value, 0)
    : undefined;
  return (
    <ChartFrame
      title="Stage transitions"
      summary={`${formatInt(total)} transitions`}
      series={STAGE_SERIES}
    >
      {ready ? (
        <ResponsiveContainer
          width="100%"
          height="100%"
          minWidth={0}
          minHeight={0}
          initialDimension={{ width: 480, height: 176 }}
        >
          <BarChart data={points} margin={{ top: 8, right: 8, left: -18, bottom: 0 }}>
            <CartesianGrid vertical={false} strokeDasharray="3 3" className="stroke-border" />
            <XAxis
              dataKey="ts"
              tickFormatter={(value) => timeTick(value, window)}
              minTickGap={24}
            />
            <YAxis allowDecimals={false} />
            <Tooltip content={<MonitorTooltip series={STAGE_SERIES} />} />
            {STAGE_SERIES.map((series) => (
              <Bar
                key={series.key}
                dataKey={series.key}
                stackId="stages"
                fill={series.color}
                isAnimationActive={false}
              />
            ))}
          </BarChart>
        </ResponsiveContainer>
      ) : (
        <ChartSkeleton />
      )}
    </ChartFrame>
  );
}

function ChartFrame({
  title,
  summary,
  series,
  children,
}: {
  title: string;
  summary: string;
  series: ReadonlyArray<{ key: string; label: string; color: string }>;
  children: React.ReactNode;
}) {
  return (
    <div className="min-w-0 rounded-lg border p-3">
      <div className="flex items-start justify-between gap-2">
        <div className="text-sm font-medium">{title}</div>
        <div className="text-xs tabular-nums text-muted-foreground">{summary}</div>
      </div>
      <div className="mt-2 flex min-h-8 flex-wrap content-start gap-x-3 gap-y-1">
        {series.map((item) => (
          <div
            key={item.key}
            className="flex items-center gap-1.5 text-[11px] text-muted-foreground"
          >
            <span className="h-2 w-2 rounded-full" style={{ backgroundColor: item.color }} />
            {item.label}
          </div>
        ))}
      </div>
      <div className="mt-1 h-44">{children}</div>
    </div>
  );
}

function ChartSkeleton() {
  return <div className="h-full animate-pulse rounded bg-muted" />;
}

function MonitorTooltip({
  active,
  payload,
  label,
  series,
}: {
  active?: boolean;
  payload?: Array<{ dataKey?: string | number; value?: number; color?: string }>;
  label?: string;
  series: ReadonlyArray<{ key: string; label: string; color: string }>;
}) {
  if (!active || !payload?.length) return null;
  const labels = new Map(series.map((item) => [item.key, item.label]));
  return (
    <div className="rounded-md border bg-card px-2.5 py-2 text-xs shadow-sm">
      <div className="mb-1 text-muted-foreground">{formatTooltipTime(label)}</div>
      {payload
        .filter((entry) => Number(entry.value ?? 0) > 0)
        .map((entry) => (
          <div key={String(entry.dataKey)} className="flex justify-between gap-5">
            <span style={{ color: entry.color }}>{labels.get(String(entry.dataKey))}</span>
            <span className="font-medium tabular-nums">{formatInt(entry.value)}</span>
          </div>
        ))}
    </div>
  );
}

function flattenPoints(data?: FoundryActivity): ChartPoint[] {
  return (data?.points ?? []).map((point) => ({
    ts: point.ts,
    callsStarted: point.calls.started,
    callsSucceeded: point.calls.succeeded,
    callsFailed: point.calls.failed,
    callsRateLimited: point.calls.rate_limited,
    inputTokens: point.tokens.input,
    outputTokens: point.tokens.output,
    received: point.stages.received,
    graphCompiled: point.stages.graph_compiled,
    graphCritiqued: point.stages.graph_critiqued,
    tasksProposed: point.stages.tasks_proposed,
    solutionsGenerated: point.stages.solutions_generated,
    verifiersCompiled: point.stages.verifiers_compiled,
    adversarialValidated: point.stages.adversarial_validated,
  }));
}

function roleLabel(role: string): string {
  const labels: Record<string, string> = {
    structure_compiler: 'Graph · Structure',
    claim_compiler: 'Graph · Claims',
    evidence_compiler: 'Graph · Evidence',
    dependency_compiler: 'Graph · Dependencies',
    canonicalization_compiler: 'Graph · Canonicalization',
    conflict_compiler: 'Graph · Conflicts',
    graph_critic: 'Graph review',
    graph_repair: 'Graph repair',
    task_designer: 'Task design',
    answerability_critic: 'Task review',
    solver_a: 'Solution A',
    solver_b: 'Solution B',
    grounding_critic: 'Grounding review',
    verifier_compiler: 'Verifier build',
    verifier_critic: 'Verifier review',
    adversary: 'Adversarial test',
    final_repair: 'Final repair',
  };
  return labels[role] ?? humanize(role);
}

function stageLabel(value: string): string {
  const labels: Record<string, string> = {
    CALL_STARTED: 'Model call in progress',
    CALL_FAILED: 'Model call failed',
    GRAPH_COMPILED: 'Evidence graph complete',
    GRAPH_CRITIQUED: 'Graph reviewed',
    TASKS_PROPOSED: 'Tasks proposed',
    SOLUTIONS_GENERATED: 'Solutions generated',
    VERIFIERS_COMPILED: 'Verifiers compiled',
    ADVERSARIAL_VALIDATED: 'Adversarial validation complete',
    ACCEPTED_SFT: 'SFT accepted',
    ACCEPTED_RL: 'RL accepted',
  };
  return labels[value] ?? humanize(value);
}

function humanize(value: string): string {
  return value
    .replaceAll('_', ' ')
    .toLowerCase()
    .replace(/\b\w/g, (char) => char.toUpperCase());
}

function elapsed(startedAt: string): string {
  const seconds = Math.max(0, Math.floor((Date.now() - new Date(startedAt).getTime()) / 1_000));
  const hours = Math.floor(seconds / 3_600);
  const minutes = Math.floor((seconds % 3_600) / 60);
  const remainder = seconds % 60;
  return hours ? `${hours}h ${minutes}m` : minutes ? `${minutes}m ${remainder}s` : `${remainder}s`;
}

function timeTick(value: string, window: ActivityWindow): string {
  const date = new Date(value);
  if (window === '24h') {
    return new Intl.DateTimeFormat(undefined, { hour: '2-digit', minute: '2-digit' }).format(date);
  }
  return new Intl.DateTimeFormat(undefined, {
    hour: '2-digit',
    minute: '2-digit',
    ...(window === '5m' ? { second: '2-digit' } : {}),
  }).format(date);
}

function formatTooltipTime(value?: string): string {
  if (!value) return '';
  return new Intl.DateTimeFormat(undefined, {
    month: 'short',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
  }).format(new Date(value));
}

function compactNumber(value: number): string {
  return new Intl.NumberFormat(undefined, { notation: 'compact', maximumFractionDigits: 1 }).format(
    value,
  );
}
