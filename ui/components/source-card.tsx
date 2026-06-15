'use client';

import { Activity, AlertTriangle, CheckCircle2, Clock } from 'lucide-react';

import { Badge } from '@/components/ui/badge';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import type { SourceFeedStatus } from '@/lib/schemas';
import { formatInt, relativeTime } from '@/lib/utils';

interface Props {
  status: SourceFeedStatus;
}

const stateBadge: Record<
  SourceFeedStatus['poll_state'],
  { label: string; variant: 'default' | 'secondary' | 'destructive' | 'success' | 'warning' }
> = {
  idle: { label: 'idle', variant: 'secondary' },
  polling: { label: 'polling', variant: 'default' },
  cooldown: { label: 'cooldown', variant: 'warning' },
  error: { label: 'error', variant: 'destructive' },
};

export function SourceCard({ status }: Props) {
  const badge = stateBadge[status.poll_state];
  return (
    <Card className="flex flex-col">
      <CardHeader className="pb-3">
        <div className="flex items-start justify-between gap-3">
          <div>
            <CardTitle className="text-base">{status.name}</CardTitle>
            <CardDescription className="font-mono text-xs">
              {status.spec.protocol} - {new URL(status.spec.endpoint).host}
            </CardDescription>
          </div>
          <Badge variant={badge.variant}>{badge.label}</Badge>
        </div>
      </CardHeader>
      <CardContent className="flex flex-col gap-2 text-sm">
        <div className="flex items-center gap-2 text-muted-foreground">
          <Clock className="h-4 w-4" />
          poll every {status.spec.poll_interval_seconds}s; rate{' '}
          {status.spec.rate_limit.requests_per_second} rps
        </div>
        <div className="flex items-center gap-2">
          {status.last_error ? (
            <AlertTriangle className="h-4 w-4 text-destructive" />
          ) : (
            <CheckCircle2 className="h-4 w-4 text-emerald-600" />
          )}
          <span className="text-muted-foreground">last success</span>
          <span className="font-mono">{relativeTime(status.last_success_at)}</span>
        </div>
        <div className="flex items-center gap-2">
          <Activity className="h-4 w-4 text-muted-foreground" />
          <span className="text-muted-foreground">24h docs</span>
          <span className="font-mono">{formatInt(status.documents_24h)}</span>
          <span className="ml-auto text-xs text-muted-foreground">
            err {(status.error_rate_24h * 100).toFixed(1)}%
          </span>
        </div>
        {status.last_error ? (
          <p className="mt-1 line-clamp-2 rounded bg-destructive/10 p-2 font-mono text-xs text-destructive">
            {status.last_error}
          </p>
        ) : null}
      </CardContent>
    </Card>
  );
}
