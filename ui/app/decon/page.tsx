'use client';

import { useQuery } from '@tanstack/react-query';
import { z } from 'zod';

import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { AttestationViewer } from '@/components/attestation-viewer';
import { apiFetch } from '@/lib/api';
import { queryKeys } from '@/lib/query-keys';
import { DeconAttestationSchema, type DeconAttestation } from '@/lib/schemas';

const ListSchema = z.array(DeconAttestationSchema);

async function fetchAttestations(limit: number): Promise<DeconAttestation[]> {
  return apiFetch<DeconAttestation[]>(`/api/decon?limit=${limit}`, ListSchema);
}

export default function DeconPage() {
  const { data, error, isLoading } = useQuery({
    queryKey: queryKeys.decon(20),
    queryFn: () => fetchAttestations(20),
    refetchInterval: 30_000,
  });

  return (
    <div className="space-y-6">
      <header className="space-y-1">
        <h1 className="text-2xl font-semibold tracking-tight">Decon attestations</h1>
        <p className="text-sm text-muted-foreground">
          Each Iceberg snapshot of `gold.curated` produces a signed attestation listing
          MMLU/GSM8K/HumanEval/MATH/GPQA n-gram hits dropped during streaming. Click verify to
          run `cosign verify-blob` server-side.
        </p>
      </header>

      {error ? (
        <p className="rounded border border-destructive/50 bg-destructive/10 p-3 text-sm text-destructive">
          {(error as Error).message}
        </p>
      ) : null}

      {isLoading ? (
        <Card>
          <CardHeader>
            <CardTitle className="text-base">Loading...</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="h-32 animate-pulse rounded bg-muted" />
          </CardContent>
        </Card>
      ) : data && data.length > 0 ? (
        <div className="grid grid-cols-1 gap-4 lg:grid-cols-2">
          {data.map((att) => (
            <AttestationViewer key={att.snapshot_id} attestation={att} />
          ))}
        </div>
      ) : (
        <Card>
          <CardHeader>
            <CardTitle className="text-base">No attestations yet</CardTitle>
          </CardHeader>
          <CardContent>
            <p className="text-sm text-muted-foreground">
              Attestations are produced after the first commit to `gold.curated`. Wait for the
              processor to flush its first snapshot.
            </p>
          </CardContent>
        </Card>
      )}
    </div>
  );
}
