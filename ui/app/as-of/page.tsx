'use client';

import { useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import {
  Bar,
  BarChart,
  CartesianGrid,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts';
import { z } from 'zod';

import { Button } from '@/components/ui/button';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
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
import { AsOfMixtureRowSchema, type AsOfMixtureRow } from '@/lib/schemas';
import { formatInt } from '@/lib/utils';

const RowsSchema = z.array(AsOfMixtureRowSchema);

async function fetchAsOf(ts: string): Promise<AsOfMixtureRow[]> {
  return apiFetch<AsOfMixtureRow[]>(
    `/api/as-of?ts=${encodeURIComponent(ts)}`,
    RowsSchema,
  );
}

function defaultIso(): string {
  const d = new Date();
  d.setMinutes(d.getMinutes() - d.getTimezoneOffset());
  return d.toISOString().slice(0, 16);
}

export default function AsOfPage() {
  const [tsInput, setTsInput] = useState<string>(defaultIso());
  const [committedTs, setCommittedTs] = useState<string>(new Date().toISOString());

  const { data, error, isFetching } = useQuery({
    queryKey: queryKeys.asOf(committedTs),
    queryFn: () => fetchAsOf(committedTs),
  });

  function submit(e: React.FormEvent) {
    e.preventDefault();
    const date = new Date(tsInput);
    if (Number.isNaN(date.getTime())) return;
    setCommittedTs(date.toISOString());
  }

  const totalTokens = data?.reduce((acc, r) => acc + r.tokens, 0) ?? 0;

  return (
    <div className="space-y-6">
      <header className="space-y-1">
        <h1 className="text-2xl font-semibold tracking-tight">As-of query</h1>
        <p className="text-sm text-muted-foreground">
          Pin the gold table to a historical Iceberg snapshot. Backed by{' '}
          <code className="font-mono">iceberg_scan(..., snapshot_from_timestamp =&gt; ?)</code>{' '}
          via the in-cluster DuckDB-server (proxy mode).
        </p>
      </header>

      <Card>
        <CardHeader>
          <CardTitle>Pick timestamp</CardTitle>
          <CardDescription>
            Local time; converted to UTC ISO-8601 before issuing the query.
          </CardDescription>
        </CardHeader>
        <CardContent>
          <form onSubmit={submit} className="flex flex-wrap items-end gap-3">
            <div className="space-y-1.5">
              <Label className="text-xs uppercase tracking-wide text-muted-foreground">
                as_of
              </Label>
              <Input
                type="datetime-local"
                value={tsInput}
                onChange={(e) => setTsInput(e.target.value)}
                className="font-mono"
                required
              />
            </div>
            <Button type="submit" disabled={isFetching}>
              {isFetching ? 'Querying...' : 'Run as_of'}
            </Button>
            <p className="text-xs font-mono text-muted-foreground">resolved: {committedTs}</p>
          </form>
        </CardContent>
      </Card>

      {error ? (
        <p className="rounded border border-destructive/50 bg-destructive/10 p-3 text-sm text-destructive">
          {(error as Error).message}
        </p>
      ) : null}

      {data ? (
        <>
          <Card>
            <CardHeader>
              <CardTitle>Token mixture by source</CardTitle>
              <CardDescription>
                Total tokens at snapshot: {formatInt(totalTokens)}
              </CardDescription>
            </CardHeader>
            <CardContent>
              <ResponsiveContainer width="100%" height={260}>
                <BarChart data={data} margin={{ top: 10, right: 16, left: 0, bottom: 0 }}>
                  <CartesianGrid strokeDasharray="3 3" className="stroke-border" />
                  <XAxis dataKey="source_feed" className="text-xs" stroke="currentColor" />
                  <YAxis className="text-xs" stroke="currentColor" />
                  <Tooltip
                    contentStyle={{
                      background: 'hsl(var(--card))',
                      border: '1px solid hsl(var(--border))',
                      borderRadius: 6,
                      fontSize: 12,
                    }}
                  />
                  <Bar dataKey="tokens" fill="hsl(var(--primary))" radius={[2, 2, 0, 0]} />
                </BarChart>
              </ResponsiveContainer>
            </CardContent>
          </Card>

          <Card>
            <CardHeader>
              <CardTitle>Detail</CardTitle>
            </CardHeader>
            <CardContent>
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Source</TableHead>
                    <TableHead className="text-right">Documents</TableHead>
                    <TableHead className="text-right">Tokens</TableHead>
                    <TableHead className="text-right">Share</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {data.map((row) => (
                    <TableRow key={row.source_feed}>
                      <TableCell className="font-mono">{row.source_feed}</TableCell>
                      <TableCell className="text-right font-mono">
                        {formatInt(row.documents)}
                      </TableCell>
                      <TableCell className="text-right font-mono">
                        {formatInt(row.tokens)}
                      </TableCell>
                      <TableCell className="text-right font-mono">
                        {totalTokens === 0
                          ? '-'
                          : `${((row.tokens / totalTokens) * 100).toFixed(1)}%`}
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </CardContent>
          </Card>
        </>
      ) : null}
    </div>
  );
}
