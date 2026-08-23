'use client';

import { useState } from 'react';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { Edit3, Play, Plus, RefreshCcw, Trash2 } from 'lucide-react';
import { z } from 'zod';

import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { Card, CardContent } from '@/components/ui/card';
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from '@/components/ui/dialog';
import { Input } from '@/components/ui/input';
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
  SourceFeedSpecSchema,
  SourceFeedStatusSchema,
  type SourceFeedSpec,
  type SourceFeedStatus,
} from '@/lib/schemas';
import { formatInt, relativeTime } from '@/lib/utils';

const SourceListSchema = z.array(SourceFeedStatusSchema);
const runnableProtocols = ['rss', 'atom', 'oai-pmh', 'sitemap'] as const;

async function fetchSources(): Promise<SourceFeedStatus[]> {
  return apiFetch('/api/sources', SourceListSchema);
}

async function saveSource(spec: SourceFeedSpec): Promise<SourceFeedStatus> {
  return apiFetch('/api/sources', SourceFeedStatusSchema, { method: 'POST', body: spec });
}

async function deleteSource(name: string): Promise<{ deleted: boolean }> {
  return apiFetch(`/api/sources/${encodeURIComponent(name)}`, z.object({ deleted: z.boolean() }), {
    method: 'DELETE',
  });
}

async function setSourceEnabled(name: string, enabled: boolean): Promise<SourceFeedStatus> {
  return apiFetch(`/api/sources/${encodeURIComponent(name)}`, SourceFeedStatusSchema, {
    method: 'PATCH',
    body: { enabled },
  });
}

async function runSource(name: string): Promise<SourceFeedStatus> {
  return apiFetch(`/api/sources/${encodeURIComponent(name)}/run`, SourceFeedStatusSchema, {
    method: 'POST',
  });
}

export default function SourcesPage() {
  const queryClient = useQueryClient();
  const sources = useQuery({
    queryKey: queryKeys.sources,
    queryFn: fetchSources,
    refetchInterval: 4_000,
  });
  const refresh = () => queryClient.invalidateQueries({ queryKey: queryKeys.sources });
  const save = useMutation({ mutationFn: saveSource, onSuccess: refresh });
  const remove = useMutation({ mutationFn: deleteSource, onSuccess: refresh });
  const toggle = useMutation({
    mutationFn: ({ name, enabled }: { name: string; enabled: boolean }) =>
      setSourceEnabled(name, enabled),
    onSuccess: refresh,
  });
  const run = useMutation({ mutationFn: runSource, onSuccess: refresh });
  const [editing, setEditing] = useState<SourceFeedSpec | null>(null);
  const [createOpen, setCreateOpen] = useState(false);

  return (
    <div className="space-y-5">
      <div className="flex items-center justify-between gap-3">
        <h1 className="text-2xl font-semibold tracking-tight">Sources</h1>
        <div className="flex gap-2">
          <Button
            variant="outline"
            size="sm"
            onClick={() => sources.refetch()}
            disabled={sources.isFetching}
          >
            <RefreshCcw className="mr-2 h-4 w-4" /> Refresh
          </Button>
          <Dialog open={createOpen} onOpenChange={setCreateOpen}>
            <DialogTrigger asChild>
              <Button size="sm">
                <Plus className="mr-2 h-4 w-4" /> Add source
              </Button>
            </DialogTrigger>
            <DialogContent>
              <DialogHeader>
                <DialogTitle>Add source</DialogTitle>
              </DialogHeader>
              <SourceForm
                pending={save.isPending}
                onSubmit={async (spec) => {
                  await save.mutateAsync(spec);
                  setCreateOpen(false);
                }}
              />
            </DialogContent>
          </Dialog>
        </div>
      </div>

      {sources.error ? <ErrorBox error={sources.error as Error} /> : null}
      {save.error ? <ErrorBox error={save.error as Error} /> : null}
      {toggle.error ? <ErrorBox error={toggle.error as Error} /> : null}
      {run.error ? <ErrorBox error={run.error as Error} /> : null}

      <Card>
        <CardContent className="overflow-x-auto p-0">
          <Table className="min-w-[68rem]">
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
                <TableHead className="w-[190px] text-right">Actions</TableHead>
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
                      <Badge variant="outline">{qualityPolicyFor(source)}</Badge>
                      <span className="text-xs text-muted-foreground">
                        {licensePolicyFor(source.spec.license_default)}
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
                  <TableCell>
                    <div className="flex justify-end gap-1">
                      <Button
                        title={source.spec.enabled ? 'Disable' : 'Enable'}
                        variant="ghost"
                        size="sm"
                        onClick={() =>
                          toggle.mutate({ name: source.name, enabled: !source.spec.enabled })
                        }
                      >
                        {source.spec.enabled ? 'Disable' : 'Enable'}
                      </Button>
                      <Button
                        title="Run once"
                        variant="ghost"
                        size="icon"
                        disabled={
                          !source.spec.enabled ||
                          source.poll_state === 'polling' ||
                          !runnableProtocols.includes(
                            source.spec.protocol as (typeof runnableProtocols)[number],
                          )
                        }
                        onClick={() => run.mutate(source.name)}
                      >
                        <Play className="h-4 w-4" />
                      </Button>
                      <Button
                        title="Edit"
                        variant="ghost"
                        size="icon"
                        onClick={() => setEditing(source.spec)}
                      >
                        <Edit3 className="h-4 w-4" />
                      </Button>
                      <Button
                        title="Delete"
                        variant="ghost"
                        size="icon"
                        className="hover:text-destructive"
                        onClick={() => {
                          if (window.confirm(`Delete ${source.name}?`)) remove.mutate(source.name);
                        }}
                      >
                        <Trash2 className="h-4 w-4" />
                      </Button>
                    </div>
                  </TableCell>
                </TableRow>
              ))}
              {sources.data?.length === 0 ? (
                <TableRow>
                  <TableCell colSpan={10} className="h-32 text-center text-muted-foreground">
                    No sources
                  </TableCell>
                </TableRow>
              ) : null}
            </TableBody>
          </Table>
        </CardContent>
      </Card>

      <Dialog
        open={editing !== null}
        onOpenChange={(open) => {
          if (!open) setEditing(null);
        }}
      >
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Edit source</DialogTitle>
          </DialogHeader>
          {editing ? (
            <SourceForm
              initial={editing}
              pending={save.isPending}
              onSubmit={async (spec) => {
                await save.mutateAsync(spec);
                setEditing(null);
              }}
            />
          ) : null}
        </DialogContent>
      </Dialog>
    </div>
  );
}

function SourceForm({
  initial,
  pending,
  onSubmit,
}: {
  initial?: SourceFeedSpec;
  pending: boolean;
  onSubmit: (spec: SourceFeedSpec) => Promise<void>;
}) {
  const [form, setForm] = useState({
    name: initial?.name ?? '',
    protocol: initial?.protocol ?? 'rss',
    endpoint: initial?.endpoint ?? '',
    poll_interval_seconds: String(initial?.poll_interval_seconds ?? 7200),
    requests_per_second: String(initial?.rate_limit.requests_per_second ?? 1),
    burst: String(initial?.rate_limit.burst ?? 2),
    license_default: initial?.license_default ?? 'unknown',
  });
  const [error, setError] = useState<string | null>(null);
  const update = (key: keyof typeof form, value: string) =>
    setForm((current) => ({ ...current, [key]: value }));

  async function submit(event: React.FormEvent) {
    event.preventDefault();
    const endpoint = form.endpoint.trim();
    const parsed = SourceFeedSpecSchema.safeParse({
      name: form.name.trim(),
      protocol: form.protocol,
      endpoint,
      enabled: initial?.enabled ?? true,
      poll_interval_seconds: Number(form.poll_interval_seconds),
      rate_limit: {
        requests_per_second: Number(form.requests_per_second),
        burst: Number(form.burst),
        respect_x_poll_interval: false,
      },
      auth: { type: 'none' },
      accept_content_types: initial?.accept_content_types ?? [],
      egress_allow: initial?.egress_allow ?? [safeHost(endpoint)].filter(Boolean),
      license_default: form.license_default.trim(),
    });
    if (!parsed.success) {
      setError(parsed.error.issues[0]?.message ?? 'Invalid source');
      return;
    }
    setError(null);
    await onSubmit(parsed.data);
  }

  return (
    <form className="space-y-4" onSubmit={submit}>
      <div className="grid gap-3 sm:grid-cols-2">
        <Field label="Name">
          <Input
            value={form.name}
            disabled={Boolean(initial)}
            onChange={(event) => update('name', event.target.value)}
            required
          />
        </Field>
        <Field label="Protocol">
          <select
            className="flex h-10 w-full rounded-md border border-input bg-background px-3 text-sm"
            value={form.protocol}
            onChange={(event) => update('protocol', event.target.value)}
          >
            {runnableProtocols.map((protocol) => (
              <option key={protocol}>{protocol}</option>
            ))}
          </select>
        </Field>
      </div>
      <Field label="Endpoint">
        <Input
          type="url"
          value={form.endpoint}
          onChange={(event) => update('endpoint', event.target.value)}
          required
        />
      </Field>
      <Field label="Default license">
        <Input
          list="source-license-defaults"
          value={form.license_default}
          onChange={(event) => update('license_default', event.target.value)}
          required
        />
        <datalist id="source-license-defaults">
          <option value="unknown" />
          <option value="per-record" />
          <option value="arxiv-non-exclusive-distribution" />
          <option value="CC-BY-4.0" />
          <option value="CC-BY-SA-4.0" />
          <option value="CC0-1.0" />
          <option value="Apache-2.0" />
          <option value="MIT" />
          <option value="BSD-3-Clause" />
          <option value="ODC-By-1.0" />
        </datalist>
        <p className="text-xs text-muted-foreground">
          Unknown, arXiv distribution-only, and ODC-By sources are retained for transformed
          post-training but excluded from verbatim pretraining.
        </p>
      </Field>
      <div className="grid gap-3 sm:grid-cols-3">
        <Field label="Poll interval">
          <Input
            type="number"
            min="60"
            value={form.poll_interval_seconds}
            onChange={(event) => update('poll_interval_seconds', event.target.value)}
          />
        </Field>
        <Field label="Requests / sec">
          <Input
            type="number"
            min="0.1"
            step="0.1"
            value={form.requests_per_second}
            onChange={(event) => update('requests_per_second', event.target.value)}
          />
        </Field>
        <Field label="Burst">
          <Input
            type="number"
            min="1"
            value={form.burst}
            onChange={(event) => update('burst', event.target.value)}
          />
        </Field>
      </div>
      {error ? <p className="text-sm text-destructive">{error}</p> : null}
      <div className="flex justify-end">
        <Button disabled={pending}>{pending ? 'Saving' : 'Save source'}</Button>
      </div>
    </form>
  );
}

function Field({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <label className="space-y-1.5">
      <span className="text-xs font-medium">{label}</span>
      {children}
    </label>
  );
}
function ErrorBox({ error }: { error: Error }) {
  return (
    <div className="rounded-md border border-destructive/40 bg-destructive/5 p-3 text-sm text-destructive">
      {error.message}
    </div>
  );
}
function safeHost(value: string): string {
  try {
    return new URL(value).hostname;
  } catch {
    return '';
  }
}
function qualityPolicyFor(source: SourceFeedStatus): string {
  const host = new URL(source.spec.endpoint).hostname;
  if (source.spec.protocol === 'oai-pmh' || source.spec.protocol === 'rest-json') {
    return 'Structured metadata';
  }
  return host.endsWith('arxiv.org') ? 'FinePDFs scientific' : 'FineWeb web';
}
function licensePolicyFor(value: string | null | undefined): string {
  if (!value || ['unknown', 'arxiv-non-exclusive-distribution', 'ODC-By-1.0'].includes(value)) {
    return 'Posttrain transform only';
  }
  if (value === 'per-record') return 'Per-record admission';
  return `Pretrain allowlist: ${value}`;
}
function StatusBadge({ source }: { source: SourceFeedStatus }) {
  if (!source.spec.enabled) return <Badge variant="secondary">Disabled</Badge>;
  if (source.poll_state === 'polling') return <Badge>Running</Badge>;
  if (source.poll_state === 'error') return <Badge variant="destructive">Failed</Badge>;
  return <Badge variant="success">Ready</Badge>;
}
