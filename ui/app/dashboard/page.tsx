'use client';

import Link from 'next/link';
import { useQuery } from '@tanstack/react-query';
import { ArrowUpRight, FlaskConical } from 'lucide-react';

import { ActivityPanel } from '@/components/activity-panel';
import { Badge } from '@/components/ui/badge';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { QualityHistogramChart } from '@/components/quality-histogram';
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table';
import { apiFetch } from '@/lib/api';
import { queryKeys } from '@/lib/query-keys';
import {
  DashboardSummarySchema,
  FoundryDashboardSchema,
  type DashboardSummary,
  type FoundryDashboard,
} from '@/lib/schemas';
import { formatInt } from '@/lib/utils';

async function fetchDashboard(): Promise<DashboardSummary> {
  return apiFetch('/api/dashboard', DashboardSummarySchema);
}

async function fetchFoundry(): Promise<FoundryDashboard> {
  return apiFetch('/api/foundry/dashboard', FoundryDashboardSchema);
}

export default function DashboardPage() {
  const dashboard = useQuery({
    queryKey: queryKeys.dashboard,
    queryFn: fetchDashboard,
    refetchInterval: 5_000,
  });
  const foundry = useQuery({
    queryKey: queryKeys.foundry,
    queryFn: fetchFoundry,
    refetchInterval: 10_000,
  });
  const data = dashboard.data;
  const scored =
    data?.quality_histogram.edu_buckets.reduce((sum, bucket) => sum + bucket.count, 0) ?? 0;

  return (
    <div className="space-y-5">
      <h1 className="text-2xl font-semibold tracking-tight">Dashboard</h1>

      <div className="grid gap-4 sm:grid-cols-2 xl:grid-cols-4">
        <Metric label="Decisions" value={data ? formatInt(data.durable_decisions) : '-'} />
        <Metric
          label="Training documents"
          value={data ? formatInt(data.training_export_documents) : '-'}
        />
        <Metric
          label="Acceptance"
          value={data ? `${(acceptanceRate(data) * 100).toFixed(1)}%` : '-'}
        />
        <Metric
          label="Post-train candidates"
          value={
            data
              ? formatInt(
                  data.route_distribution.find((row) => row.route === 'posttrain_candidate')
                    ?.documents ?? 0,
                )
              : '-'
          }
        />
      </div>

      <ActivityPanel />

      <PostTrainingSummary data={foundry.data} />

      <div className="grid gap-4 xl:grid-cols-2">
        <Card>
          <CardHeader className="flex flex-row items-center justify-between pb-2">
            <CardTitle className="text-sm font-medium">Source quality</CardTitle>
            <span className="text-xs tabular-nums text-muted-foreground">
              n={formatInt(scored)}
            </span>
          </CardHeader>
          <CardContent>
            {data ? (
              <QualityHistogramChart data={data.quality_histogram} series="edu_buckets" />
            ) : (
              <Skeleton />
            )}
          </CardContent>
        </Card>
        <Card>
          <CardHeader className="flex flex-row items-center justify-between pb-2">
            <CardTitle className="text-sm font-medium">Composite quality</CardTitle>
            <span className="text-xs tabular-nums text-muted-foreground">
              n={formatInt(scored)}
            </span>
          </CardHeader>
          <CardContent>
            {data ? <QualityHistogramChart data={data.quality_histogram} /> : <Skeleton />}
          </CardContent>
        </Card>
      </div>

      <Card>
        <CardHeader className="pb-2">
          <CardTitle className="text-sm font-medium">Corpus routes</CardTitle>
        </CardHeader>
        <CardContent className="overflow-x-auto">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Route</TableHead>
                <TableHead className="text-right">Documents</TableHead>
                <TableHead className="text-right">Source words</TableHead>
                <TableHead className="text-right">Projection words</TableHead>
                <TableHead className="text-right">Mean source quality</TableHead>
                <TableHead className="text-right">Mean composite</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {data?.route_distribution.map((row) => (
                <TableRow key={row.route}>
                  <TableCell>
                    <Link
                      href={`/documents?route=${encodeURIComponent(row.route)}&include_fixtures=true`}
                      className="inline-flex items-center gap-1 hover:underline"
                    >
                      <RouteBadge route={row.route} />
                      <ArrowUpRight className="h-3.5 w-3.5" />
                    </Link>
                  </TableCell>
                  <TableCell className="text-right font-mono">{formatInt(row.documents)}</TableCell>
                  <TableCell className="text-right font-mono">
                    {formatInt(row.source_words)}
                  </TableCell>
                  <TableCell className="text-right font-mono">
                    {formatInt(row.training_words)}
                  </TableCell>
                  <TableCell className="text-right font-mono">{row.mean_edu.toFixed(2)}</TableCell>
                  <TableCell className="text-right font-mono">
                    {row.mean_quality.toFixed(2)}
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </CardContent>
      </Card>

      <div className="grid gap-4 xl:grid-cols-[2fr_1fr]">
        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-sm font-medium">Sources</CardTitle>
          </CardHeader>
          <CardContent>
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Source</TableHead>
                  <TableHead className="text-right">Observed</TableHead>
                  <TableHead className="text-right">Training</TableHead>
                  <TableHead className="text-right">Rate</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {data?.per_source_acceptance.map((row) => (
                  <TableRow key={row.source}>
                    <TableCell>
                      <Link
                        href={`/documents?source=${encodeURIComponent(row.source)}&include_fixtures=true`}
                        className="hover:underline"
                      >
                        {row.source}
                      </Link>
                    </TableCell>
                    <TableCell className="text-right font-mono">{formatInt(row.total)}</TableCell>
                    <TableCell className="text-right font-mono">
                      {formatInt(row.accepted)}
                    </TableCell>
                    <TableCell className="text-right font-mono">
                      {row.total ? `${((row.accepted / row.total) * 100).toFixed(1)}%` : '-'}
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </CardContent>
        </Card>
        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-sm font-medium">Rejected</CardTitle>
          </CardHeader>
          <CardContent className="space-y-2">
            {data ? (
              Object.entries(data.rejected_by_reason).map(([reason, count]) => (
                <Link
                  key={reason}
                  href={`/documents?rejection_reason=${encodeURIComponent(reason)}&include_fixtures=true`}
                  className="flex items-center justify-between rounded-md border px-3 py-2 text-sm hover:bg-accent"
                >
                  <span>{humanize(reason)}</span>
                  <span className="font-mono">{formatInt(count)}</span>
                </Link>
              ))
            ) : (
              <Skeleton />
            )}
          </CardContent>
        </Card>
      </div>

      {dashboard.error ? (
        <div className="rounded-lg border border-destructive/40 bg-destructive/5 p-3 text-sm text-destructive">
          {(dashboard.error as Error).message}
        </div>
      ) : null}
    </div>
  );
}

function Metric({ label, value }: { label: string; value: string }) {
  return (
    <Card>
      <CardContent className="p-4">
        <div className="text-xs text-muted-foreground">{label}</div>
        <div className="mt-1 text-3xl font-semibold tabular-nums">{value}</div>
      </CardContent>
    </Card>
  );
}
function PostTrainingSummary({ data }: { data?: FoundryDashboard }) {
  const sft = data?.artifacts['sft_trajectory:accepted'] ?? 0;
  const rl = data?.artifacts['rl_environment:accepted'] ?? 0;
  const latest = data?.manual_runs[0] ?? data?.daily_runs[0];
  return (
    <Card>
      <CardHeader className="flex flex-row items-center justify-between pb-2">
        <CardTitle className="flex items-center gap-2 text-sm font-medium">
          <FlaskConical className="h-4 w-4" /> Post-training foundry
        </CardTitle>
        <Link
          href="/post-training"
          className="inline-flex items-center gap-1 text-sm hover:underline"
        >
          Open <ArrowUpRight className="h-3.5 w-3.5" />
        </Link>
      </CardHeader>
      <CardContent className="grid gap-3 sm:grid-cols-5">
        <SummaryValue label="SFT accepted" value={formatInt(sft)} />
        <SummaryValue label="RL accepted" value={formatInt(rl)} />
        <SummaryValue label="Human approved" value={formatInt(data?.human_audits.approved ?? 0)} />
        <SummaryValue label="Candidates queued" value={formatInt(data?.queued_candidates ?? 0)} />
        <SummaryValue label="Latest run" value={latest ? humanize(latest.state) : 'None'} />
      </CardContent>
    </Card>
  );
}
function SummaryValue({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded-md border px-3 py-2">
      <div className="text-xs text-muted-foreground">{label}</div>
      <div className="mt-1 font-medium tabular-nums">{value}</div>
    </div>
  );
}
function RouteBadge({ route }: { route: string }) {
  return (
    <Badge
      variant={route === 'quarantine' ? 'destructive' : route === 'retry' ? 'warning' : 'success'}
    >
      {route === 'posttrain_candidate' || route === 'reasoning_candidate'
        ? 'Post-training'
        : humanize(route)}
    </Badge>
  );
}
function acceptanceRate(data: DashboardSummary): number {
  return data.durable_decisions ? data.training_export_documents / data.durable_decisions : 0;
}
function humanize(value: string): string {
  return value.replaceAll('_', ' ').replace(/\b\w/g, (char) => char.toUpperCase());
}
function Skeleton() {
  return <div className="h-40 animate-pulse rounded bg-muted" />;
}
