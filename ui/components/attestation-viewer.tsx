'use client';

import { useCallback, useEffect, useState } from 'react';
import { Loader2, ShieldAlert, ShieldCheck } from 'lucide-react';

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
 * Renders the current attestation. Verification is automatic: local mode uses
 * Ed25519 directly and production can opt into Sigstore/cosign.
 */
export function AttestationViewer({ attestation }: Props) {
  const [state, setState] = useState<'idle' | 'pending' | VerifyResult>('idle');

  const verify = useCallback(async () => {
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
  }, [attestation.snapshot_id]);

  useEffect(() => {
    const timer = window.setTimeout(() => void verify(), 0);
    return () => window.clearTimeout(timer);
  }, [verify]);

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
            {formatTs(attestation.committed_at)} - benchmarks {attestation.benchmark_set_version}
          </p>
        </div>
        <div className="flex items-center gap-2">
          <Badge variant={verdict?.variant ?? 'secondary'}>
            {state === 'pending' || state === 'idle' ? (
              <Loader2 className="mr-1 h-3.5 w-3.5 animate-spin" />
            ) : verdict?.variant === 'success' ? (
              <ShieldCheck className="mr-1 h-3.5 w-3.5" />
            ) : (
              <ShieldAlert className="mr-1 h-3.5 w-3.5" />
            )}
            {verdict?.label ?? 'verifying'}
          </Badge>
          <Button
            size="icon"
            variant="ghost"
            title="Re-verify signature"
            aria-label="Re-verify signature"
            onClick={verify}
            disabled={state === 'pending'}
          >
            <ShieldCheck className="h-4 w-4" />
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
        {state !== 'idle' && state !== 'pending' ? (
          <p className="flex items-center gap-2 text-xs text-muted-foreground">
            {state.ok ? <ShieldCheck className="h-4 w-4 text-emerald-600" /> : null}
            {state.ok ? 'Signature verified automatically' : state.message}
            {state.signer_subject ? ` · ${state.signer_subject}` : ''}
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
