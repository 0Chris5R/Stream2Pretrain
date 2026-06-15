'use client';

import { useState } from 'react';
import { ShieldCheck, ShieldAlert, Loader2 } from 'lucide-react';

import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { apiFetch } from '@/lib/api';
import { VerifyResultSchema, type DeconAttestation, type VerifyResult } from '@/lib/schemas';
import { formatInt, formatTs } from '@/lib/utils';

interface Props {
  attestation: DeconAttestation;
}

/**
 * Renders a single decon attestation and offers a "Verify" button that calls
 * the server-side `/api/decon/verify` route, which executes `cosign verify-blob`
 * with the bundled signer cert.
 */
export function AttestationViewer({ attestation }: Props) {
  const [state, setState] = useState<'idle' | 'pending' | VerifyResult>('idle');

  async function verify() {
    setState('pending');
    try {
      const result = await apiFetch<VerifyResult>('/api/decon/verify', VerifyResultSchema, {
        method: 'POST',
        body: { snapshot_id: attestation.snapshot_id },
      });
      setState(result);
    } catch (err) {
      setState({
        ok: false,
        snapshot_id: attestation.snapshot_id,
        message: (err as Error).message,
        verified_at: new Date().toISOString(),
      });
    }
  }

  const verdict =
    state === 'idle' || state === 'pending'
      ? null
      : state.ok
        ? { variant: 'success' as const, label: 'verified' }
        : { variant: 'destructive' as const, label: 'invalid' };

  return (
    <Card>
      <CardHeader className="flex flex-row items-start justify-between gap-3 space-y-0 pb-3">
        <div>
          <CardTitle className="text-base">snapshot #{attestation.snapshot_id}</CardTitle>
          <p className="font-mono text-xs text-muted-foreground">
            {formatTs(attestation.committed_at)} - benchmarks{' '}
            {attestation.benchmark_set_version}
          </p>
        </div>
        <div className="flex items-center gap-2">
          {verdict ? <Badge variant={verdict.variant}>{verdict.label}</Badge> : null}
          <Button size="sm" variant="outline" onClick={verify} disabled={state === 'pending'}>
            {state === 'pending' ? (
              <Loader2 className="mr-2 h-4 w-4 animate-spin" />
            ) : verdict?.variant === 'success' ? (
              <ShieldCheck className="mr-2 h-4 w-4" />
            ) : (
              <ShieldAlert className="mr-2 h-4 w-4" />
            )}
            Verify
          </Button>
        </div>
      </CardHeader>
      <CardContent className="space-y-3 text-sm">
        <div className="grid grid-cols-2 gap-3 sm:grid-cols-4">
          <Stat label="tokens scanned" value={formatInt(attestation.tokens_scanned)} />
          <Stat label="tokens flagged" value={formatInt(attestation.tokens_flagged)} />
          <Stat label="rejected docs" value={formatInt(attestation.rejected_doc_hashes.length)} />
          <Stat label="benchmarks" value={attestation.benchmarks.join(', ')} />
        </div>
        <div className="grid grid-cols-2 gap-2 sm:grid-cols-5">
          {Object.entries(attestation.per_benchmark_hits).map(([bench, hits]) => (
            <div
              key={bench}
              className="rounded border bg-muted/30 px-2 py-1 text-center font-mono text-xs"
            >
              <div className="text-muted-foreground">{bench}</div>
              <div className="text-sm">{formatInt(hits)}</div>
            </div>
          ))}
        </div>
        {state !== 'idle' && state !== 'pending' ? (
          <p className="rounded border bg-muted/30 p-2 font-mono text-xs">
            {state.message}
            {state.signer_subject ? ` - signer: ${state.signer_subject}` : ''}
          </p>
        ) : null}
      </CardContent>
    </Card>
  );
}

function Stat({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded border bg-muted/30 p-2">
      <div className="text-xs text-muted-foreground">{label}</div>
      <div className="font-mono text-sm">{value}</div>
    </div>
  );
}
