'use client';

import { useQuery } from '@tanstack/react-query';
import { z } from 'zod';

import { AttestationViewer } from '@/components/attestation-viewer';
import { Badge } from '@/components/ui/badge';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
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
  BenchmarkCoverageSchema,
  DeconAttestationSchema,
  type BenchmarkCoverage,
  type DeconAttestation,
} from '@/lib/schemas';
import { formatInt, formatTs } from '@/lib/utils';

const ListSchema = z.array(DeconAttestationSchema);

async function fetchAttestations(): Promise<DeconAttestation[]> {
  return apiFetch('/api/decon?limit=20', ListSchema);
}

async function fetchCoverage(): Promise<BenchmarkCoverage> {
  return apiFetch('/api/decon/coverage', BenchmarkCoverageSchema);
}

export default function BenchmarkSafetyPage() {
  const attestations = useQuery({
    queryKey: queryKeys.decon(20),
    queryFn: fetchAttestations,
    refetchInterval: 30_000,
  });
  const coverage = useQuery({
    queryKey: queryKeys.benchmarkCoverage,
    queryFn: fetchCoverage,
    refetchInterval: 30_000,
  });
  const latest = attestations.data?.[0];
  const previous = attestations.data?.slice(1) ?? [];

  return (
    <div className="space-y-5">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <h1 className="text-2xl font-semibold tracking-tight">Benchmark Safety</h1>
        {coverage.data ? (
          <Badge
            variant={coverage.data.corpus_kind === 'restricted_reserve' ? 'success' : 'warning'}
          >
            {coverage.data.corpus_kind === 'restricted_reserve'
              ? 'Restricted reserve'
              : coverage.data.corpus_kind === 'synthetic_canary'
                ? 'Synthetic canary reserve'
                : 'Demo canaries'}
          </Badge>
        ) : null}
      </div>

      {coverage.error ? <ErrorBox error={coverage.error} /> : null}
      <div className="grid grid-cols-2 gap-3 xl:grid-cols-5">
        <Metric
          label="Manifest items"
          value={coverage.data ? formatInt(coverage.data.item_count) : '-'}
        />
        <Metric
          label="Benchmarks covered"
          value={coverage.data ? `${coverage.data.non_empty_benchmarks.length}/5` : '-'}
        />
        <Metric
          label="Tokens scanned"
          value={coverage.data ? formatInt(coverage.data.tokens_scanned) : '-'}
        />
        <Metric
          label="Tokens flagged"
          value={coverage.data ? formatInt(coverage.data.tokens_flagged) : '-'}
        />
        <Metric
          label="Last scan"
          value={
            coverage.data?.last_successful_scan ? formatTs(coverage.data.last_successful_scan) : '-'
          }
          small
        />
      </div>

      {coverage.data ? (
        <Card>
          <CardContent className="grid gap-4 p-4 lg:grid-cols-[1fr_2fr] lg:items-center">
            <div className="min-w-0">
              <div className="text-sm font-medium">{coverage.data.benchmark_set_version}</div>
              <div
                className="mt-1 truncate font-mono text-xs text-muted-foreground"
                title={`sha256:${coverage.data.manifest_sha256}`}
              >
                sha256:{coverage.data.manifest_sha256.slice(0, 12)}…
                {coverage.data.manifest_sha256.slice(-8)}
              </div>
            </div>
            <div className="grid grid-cols-2 gap-2 sm:grid-cols-5">
              {Object.entries(coverage.data.per_benchmark_items).map(([name, count]) => (
                <div key={name} className="rounded-md border p-2 text-center">
                  <div className="text-xs text-muted-foreground">{name}</div>
                  <div className="font-mono font-semibold">{formatInt(count)}</div>
                </div>
              ))}
            </div>
          </CardContent>
        </Card>
      ) : null}

      <section className="space-y-3">
        <h2 className="text-base font-semibold">Latest signed scan</h2>
        {attestations.error ? <ErrorBox error={attestations.error} /> : null}
        {latest ? (
          <AttestationViewer attestation={latest} />
        ) : (
          <div className="grid h-32 place-items-center rounded-lg border border-dashed text-sm text-muted-foreground">
            No signed scans
          </div>
        )}
        {previous.length ? (
          <details className="overflow-hidden rounded-lg border bg-card">
            <summary className="cursor-pointer list-none px-4 py-3 text-sm font-medium">
              Previous scans ({previous.length})
            </summary>
            <div className="overflow-x-auto border-t">
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Snapshot</TableHead>
                    <TableHead>Committed</TableHead>
                    <TableHead className="text-right">Scanned</TableHead>
                    <TableHead className="text-right">Flagged</TableHead>
                    <TableHead className="text-right">Rejected</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {previous.map((attestation) => (
                    <TableRow key={attestation.snapshot_id}>
                      <TableCell className="font-mono">#{attestation.snapshot_id}</TableCell>
                      <TableCell>{formatTs(attestation.committed_at)}</TableCell>
                      <TableCell className="text-right font-mono">
                        {formatInt(attestation.tokens_scanned)}
                      </TableCell>
                      <TableCell className="text-right font-mono">
                        {formatInt(attestation.tokens_flagged)}
                      </TableCell>
                      <TableCell className="text-right font-mono">
                        {formatInt(attestation.rejected_doc_hashes.length)}
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </div>
          </details>
        ) : null}
      </section>
    </div>
  );
}

function Metric({
  label,
  value,
  small = false,
}: {
  label: string;
  value: string;
  small?: boolean;
}) {
  return (
    <Card>
      <CardHeader className="pb-2">
        <CardTitle className="text-xs font-normal text-muted-foreground">{label}</CardTitle>
      </CardHeader>
      <CardContent>
        <div className={`font-mono font-semibold ${small ? 'text-sm' : 'text-2xl'}`}>{value}</div>
      </CardContent>
    </Card>
  );
}
function ErrorBox({ error }: { error: unknown }) {
  return (
    <div className="rounded-lg border border-destructive/40 bg-destructive/5 p-3 text-sm text-destructive">
      {(error as Error).message}
    </div>
  );
}
