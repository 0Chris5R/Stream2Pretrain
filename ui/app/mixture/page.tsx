'use client';

import { useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import {
  CartesianGrid,
  Legend,
  Line,
  LineChart,
  ReferenceLine,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts';

import { Button } from '@/components/ui/button';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { apiFetch } from '@/lib/api';
import { queryKeys } from '@/lib/query-keys';
import { MixtureCompareSchema, RuntimeProfileSchema, type MixtureCompare } from '@/lib/schemas';
import { formatInt } from '@/lib/utils';

async function fetchCompare(a: string, b: string): Promise<MixtureCompare> {
  return apiFetch<MixtureCompare>(
    `/api/mixture/compare?a=${encodeURIComponent(a)}&b=${encodeURIComponent(b)}`,
    MixtureCompareSchema,
  );
}

export default function MixturePage() {
  const [a, setA] = useState('main');
  const [b, setB] = useState('shadow');
  const [committed, setCommitted] = useState<{ a: string; b: string } | null>(null);
  const profile = useQuery({
    queryKey: ['runtime-profile'],
    queryFn: () => apiFetch('/api/health', RuntimeProfileSchema),
  });

  const compare = useQuery({
    queryKey: committed ? queryKeys.mixture(committed.a, committed.b) : ['mixture', 'idle'],
    queryFn: () => (committed ? fetchCompare(committed.a, committed.b) : Promise.reject('idle')),
    enabled: committed !== null,
  });

  function submit(e: React.FormEvent) {
    e.preventDefault();
    setCommitted({ a, b });
  }

  const meanDelta =
    compare.data && compare.data.perplexity_delta.length > 0
      ? compare.data.perplexity_delta.reduce((acc, p) => acc + p.delta, 0) /
        compare.data.perplexity_delta.length
      : null;

  if (profile.data?.local_mode) {
    return <N3FutureWork />;
  }

  return (
    <div className="space-y-6">
      <header className="space-y-1">
        <h1 className="text-2xl font-semibold tracking-tight">Mixture A/B</h1>
        <p className="text-sm text-muted-foreground">
          Two MixtureRecipe CRDs write to two Iceberg branches. The shadow trainer reports
          per-step proxy-LM perplexity for both; this view plots the delta (B - A): negative is
          better for B.
        </p>
      </header>

      <Card>
        <CardHeader>
          <CardTitle>Pick branches</CardTitle>
          <CardDescription>
            Branches are Iceberg refs, not git refs. Use `main` for the production mixture.
          </CardDescription>
        </CardHeader>
        <CardContent>
          <form onSubmit={submit} className="flex flex-wrap items-end gap-3">
            <div className="space-y-1.5">
              <Label className="text-xs uppercase tracking-wide text-muted-foreground">A</Label>
              <Input value={a} onChange={(e) => setA(e.target.value)} required />
            </div>
            <div className="space-y-1.5">
              <Label className="text-xs uppercase tracking-wide text-muted-foreground">B</Label>
              <Input value={b} onChange={(e) => setB(e.target.value)} required />
            </div>
            <Button type="submit" disabled={compare.isFetching}>
              {compare.isFetching ? 'Querying...' : 'Compare'}
            </Button>
          </form>
        </CardContent>
      </Card>

      {compare.error ? (
        <p className="rounded border border-destructive/50 bg-destructive/10 p-3 text-sm text-destructive">
          {(compare.error as Error).message}
        </p>
      ) : null}

      {compare.data ? (
        <>
          <div className="grid grid-cols-1 gap-4 sm:grid-cols-3">
            <Stat
              label={`tokens/h on ${compare.data.recipe_a}`}
              value={formatInt(compare.data.tokens_per_hour_a)}
            />
            <Stat
              label={`tokens/h on ${compare.data.recipe_b}`}
              value={formatInt(compare.data.tokens_per_hour_b)}
            />
            <Stat
              label="mean perplexity delta"
              value={meanDelta === null ? '-' : meanDelta.toFixed(3)}
            />
          </div>

          <Card>
            <CardHeader>
              <CardTitle>Perplexity delta over training steps</CardTitle>
              <CardDescription>
                delta = ppl({compare.data.recipe_b}) - ppl({compare.data.recipe_a}); smaller is
                better for B.
              </CardDescription>
            </CardHeader>
            <CardContent>
              <ResponsiveContainer width="100%" height={300}>
                <LineChart
                  data={compare.data.perplexity_delta}
                  margin={{ top: 10, right: 24, left: 0, bottom: 0 }}
                >
                  <CartesianGrid strokeDasharray="3 3" className="stroke-border" />
                  <XAxis dataKey="step" className="text-xs" stroke="currentColor" />
                  <YAxis className="text-xs" stroke="currentColor" />
                  <Tooltip
                    contentStyle={{
                      background: 'hsl(var(--card))',
                      border: '1px solid hsl(var(--border))',
                      borderRadius: 6,
                      fontSize: 12,
                    }}
                  />
                  <Legend wrapperStyle={{ fontSize: 12 }} />
                  <ReferenceLine y={0} stroke="hsl(var(--muted-foreground))" strokeDasharray="3 3" />
                  <Line
                    type="monotone"
                    dataKey="delta"
                    stroke="hsl(var(--primary))"
                    strokeWidth={1.75}
                    dot={false}
                    name="ppl(B) - ppl(A)"
                  />
                </LineChart>
              </ResponsiveContainer>
            </CardContent>
          </Card>
        </>
      ) : null}
    </div>
  );
}

function N3FutureWork() {
  return (
    <div className="space-y-5">
      <header className="space-y-1">
        <div className="flex items-center gap-2">
          <h1 className="text-2xl font-semibold tracking-tight">Mixture A/B</h1>
          <span className="rounded-full bg-amber-500 px-2.5 py-0.5 text-xs font-medium text-white">future work</span>
        </div>
      </header>
      <div className="grid gap-4 lg:grid-cols-3">
        <FutureCard title="Branch A + B" text="Two recipes consume the same stream into isolated Iceberg branches." />
        <FutureCard title="Rolling evaluation" text="An identical proxy LM trains on equal token windows for each branch." />
        <FutureCard title="Promotion gate" text="Per-domain perplexity deltas produce an auditable promotion decision." />
      </div>
    </div>
  );
}

function FutureCard({ title, text }: { title: string; text: string }) {
  return <Card><CardHeader><CardTitle className="text-base">{title}</CardTitle></CardHeader><CardContent className="text-sm text-muted-foreground">{text}</CardContent></Card>;
}

function Stat({ label, value }: { label: string; value: string }) {
  return (
    <Card>
      <CardHeader className="pb-2">
        <CardDescription>{label}</CardDescription>
      </CardHeader>
      <CardContent>
        <p className="text-2xl font-semibold tabular-nums">{value}</p>
      </CardContent>
    </Card>
  );
}
