'use client';

import { useQuery } from '@tanstack/react-query';

import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { QualityHistogramChart } from '@/components/quality-histogram';
import { ThroughputSpark } from '@/components/throughput-spark';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table';
import { Badge } from '@/components/ui/badge';
import { apiFetch } from '@/lib/api';
import { queryKeys } from '@/lib/query-keys';
import { DashboardSummarySchema, type DashboardSummary } from '@/lib/schemas';
import { formatInt } from '@/lib/utils';

async function fetchDashboard(): Promise<DashboardSummary> {
  return apiFetch<DashboardSummary>('/api/dashboard', DashboardSummarySchema);
}

export default function DashboardPage() {
  const { data, isLoading, error } = useQuery({
    queryKey: queryKeys.dashboard,
    queryFn: fetchDashboard,
    refetchInterval: 5_000,
  });

  return (
    <div className="space-y-6">
      <header className="space-y-1">
        <h1 className="text-2xl font-semibold tracking-tight">Dashboard</h1>
        <p className="text-sm text-muted-foreground">
          Last hour rollup. Auto-refreshes every 5 s; spark line is live SSE.
        </p>
      </header>

      <div className="grid grid-cols-1 gap-4 sm:grid-cols-3">
        <KpiCard
          label="Ingested last hour"
          value={data ? formatInt(data.ingested_last_hour) : '-'}
          loading={isLoading}
        />
        <KpiCard
          label="Curated last hour"
          value={data ? formatInt(data.curated_last_hour) : '-'}
          loading={isLoading}
        />
        <KpiCard
          label="Acceptance rate"
          value={data ? `${(acceptanceRate(data) * 100).toFixed(1)}%` : '-'}
          loading={isLoading}
        />
      </div>

      <Card>
        <CardHeader>
          <CardTitle>Live curated docs/min</CardTitle>
          <CardDescription>Server-Sent Events from the in-cluster Prometheus.</CardDescription>
        </CardHeader>
        <CardContent>
          <ThroughputSpark height={120} />
        </CardContent>
      </Card>

      <div className="grid grid-cols-1 gap-4 lg:grid-cols-2">
        <Card>
          <CardHeader>
            <CardTitle>Quality histogram</CardTitle>
            <CardDescription>FineWeb-Edu score over the last hour.</CardDescription>
          </CardHeader>
          <CardContent>
            {data ? (
              <QualityHistogramChart data={data.quality_histogram} />
            ) : (
              <Skeleton className="h-[220px]" />
            )}
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>Rejected by reason</CardTitle>
            <CardDescription>Aggregated from processor.dropped_total.</CardDescription>
          </CardHeader>
          <CardContent>
            {data ? (
              <div className="flex flex-wrap gap-2">
                {Object.entries(data.rejected_by_reason).map(([reason, count]) => (
                  <Badge key={reason} variant="secondary" className="font-mono">
                    {reason} {formatInt(count)}
                  </Badge>
                ))}
              </div>
            ) : (
              <Skeleton className="h-10" />
            )}
          </CardContent>
        </Card>
      </div>

      <Card>
        <CardHeader>
          <CardTitle>Per-source acceptance</CardTitle>
          <CardDescription>Curated vs ingested by source over the last hour.</CardDescription>
        </CardHeader>
        <CardContent>
          {data ? (
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Source</TableHead>
                  <TableHead className="text-right">Total</TableHead>
                  <TableHead className="text-right">Accepted</TableHead>
                  <TableHead className="text-right">Rate</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {data.per_source_acceptance.map((row) => {
                  const rate = row.total === 0 ? 0 : row.accepted / row.total;
                  return (
                    <TableRow key={row.source}>
                      <TableCell className="font-mono">{row.source}</TableCell>
                      <TableCell className="text-right font-mono">{formatInt(row.total)}</TableCell>
                      <TableCell className="text-right font-mono">
                        {formatInt(row.accepted)}
                      </TableCell>
                      <TableCell className="text-right font-mono">
                        {(rate * 100).toFixed(1)}%
                      </TableCell>
                    </TableRow>
                  );
                })}
              </TableBody>
            </Table>
          ) : (
            <Skeleton className="h-32" />
          )}
        </CardContent>
      </Card>

      {error ? (
        <p className="rounded border border-destructive/50 bg-destructive/10 p-3 text-sm text-destructive">
          {(error as Error).message}
        </p>
      ) : null}
    </div>
  );
}

function acceptanceRate(d: DashboardSummary): number {
  if (d.ingested_last_hour === 0) return 0;
  return d.curated_last_hour / d.ingested_last_hour;
}

function KpiCard({
  label,
  value,
  loading,
}: {
  label: string;
  value: string;
  loading: boolean;
}) {
  return (
    <Card>
      <CardHeader className="pb-2">
        <CardDescription>{label}</CardDescription>
      </CardHeader>
      <CardContent>
        {loading ? <Skeleton className="h-8 w-24" /> : <p className="text-3xl font-semibold tabular-nums">{value}</p>}
      </CardContent>
    </Card>
  );
}

function Skeleton({ className = '' }: { className?: string }) {
  return <div className={`animate-pulse rounded-md bg-muted ${className}`} />;
}
