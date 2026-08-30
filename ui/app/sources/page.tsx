'use client';

import { useQuery } from '@tanstack/react-query';
import { z } from 'zod';

import { Badge } from '@/components/ui/badge';
import { Card, CardContent } from '@/components/ui/card';
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
import { SourceFeedStatusSchema, type SourceFeedStatus } from '@/lib/schemas';
import { formatInt, relativeTime } from '@/lib/utils';

const SourceListSchema = z.array(SourceFeedStatusSchema);

async function fetchSources(): Promise<SourceFeedStatus[]> {
  return apiFetch('/api/sources', SourceListSchema);
}

export default function SourcesPage() {
  const sources = useQuery({
    queryKey: queryKeys.sources,
    queryFn: fetchSources,
    refetchInterval: 4_000,
  });

  return (
    <div className="space-y-5">
      <h1 className="text-2xl font-semibold tracking-tight">Sources</h1>

      {sources.error ? <ErrorBox error={sources.error as Error} /> : null}

      <Card>
        <CardContent className="overflow-x-auto p-0">
          <Table className="min-w-[60rem]">
            <TableHeader>
              <TableRow>
                <TableHead>Source</TableHead>
                <TableHead>Protocol</TableHead>
                <TableHead>Classifier / license</TableHead>
                <TableHead>Status</TableHead>
                <TableHead className="text-right">Observed (24h)</TableHead>
                <TableHead className="text-right">Pretrain</TableHead>
                <TableHead className="text-right">Posttrain only</TableHead>
                <TableHead className="text-right">Quarantined</TableHead>
                <TableHead>Last poll</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {sources.data?.map((source) => (
                <TableRow key={source.name}>
                  <TableCell>
                    <div className="font-medium">{source.name}</div>
                    <div className="max-w-64 truncate text-xs text-muted-foreground">
                      {new URL(source.spec.endpoint).host}
                    </div>
                  </TableCell>
                  <TableCell className="font-mono text-xs">{source.spec.protocol}</TableCell>
                  <TableCell>
                    <div className="flex flex-col items-start gap-1">
                      <Badge variant="outline">{source.quality_policy}</Badge>
                      <span
                        className="text-xs text-muted-foreground"
                        title={licenseProvenanceSummary(source)}
                      >
                        {licenseSummary(source)}
                      </span>
                    </div>
                  </TableCell>
                  <TableCell>
                    <StatusBadge source={source} />
                  </TableCell>
                  <TableCell className="text-right font-mono">
                    {formatInt(source.documents_24h)}
                  </TableCell>
                  <TableCell className="text-right font-mono text-emerald-700 dark:text-emerald-400">
                    {formatInt(source.pretrain_documents_24h)}
                  </TableCell>
                  <TableCell className="text-right font-mono text-amber-700 dark:text-amber-400">
                    {formatInt(source.posttrain_only_documents_24h)}
                  </TableCell>
                  <TableCell className="text-right font-mono text-muted-foreground">
                    {formatInt(source.quarantined_documents_24h)}
                  </TableCell>
                  <TableCell className="text-sm">{relativeTime(source.last_attempt_at)}</TableCell>
                </TableRow>
              ))}
              {sources.data?.length === 0 ? (
                <TableRow>
                  <TableCell colSpan={9} className="h-32 text-center text-muted-foreground">
                    No sources
                  </TableCell>
                </TableRow>
              ) : null}
            </TableBody>
          </Table>
        </CardContent>
      </Card>
    </div>
  );
}

function ErrorBox({ error }: { error: Error }) {
  return (
    <div className="rounded-md border border-destructive/40 bg-destructive/5 p-3 text-sm text-destructive">
      {error.message}
    </div>
  );
}

function licenseSummary(source: SourceFeedStatus): string {
  if (source.license_distribution.length === 0) return source.license_resolver;
  const values = source.license_distribution
    .slice(0, 3)
    .map((entry) => `${entry.license_id} ${formatInt(entry.count)}`);
  const remaining = source.license_distribution.length - values.length;
  return remaining > 0 ? `${values.join(' · ')} · +${remaining}` : values.join(' · ');
}

function licenseProvenanceSummary(source: SourceFeedStatus): string {
  if (source.license_provenance.length === 0) return source.license_resolver;
  return source.license_provenance
    .map((entry) => `${entry.license_source}: ${formatInt(entry.count)}`)
    .join(' · ');
}

function StatusBadge({ source }: { source: SourceFeedStatus }) {
  if (!source.spec.enabled) return <Badge variant="secondary">Disabled</Badge>;
  if (source.poll_state === 'polling') return <Badge>Running</Badge>;
  if (source.poll_state === 'error') return <Badge variant="destructive">Failed</Badge>;
  return <Badge variant="success">Ready</Badge>;
}
