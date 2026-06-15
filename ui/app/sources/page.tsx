'use client';

import { useState } from 'react';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { z } from 'zod';
import { Plus, RefreshCcw, Trash2 } from 'lucide-react';

import { Button } from '@/components/ui/button';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from '@/components/ui/dialog';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { SourceCard } from '@/components/source-card';
import { apiFetch } from '@/lib/api';
import { queryKeys } from '@/lib/query-keys';
import {
  SourceFeedProtocols,
  SourceFeedSpecSchema,
  SourceFeedStatusSchema,
  type SourceFeedSpec,
  type SourceFeedStatus,
} from '@/lib/schemas';

const SourceListSchema = z.array(SourceFeedStatusSchema);

async function fetchSources(): Promise<SourceFeedStatus[]> {
  return apiFetch<SourceFeedStatus[]>('/api/sources', SourceListSchema);
}

async function createSource(spec: SourceFeedSpec): Promise<SourceFeedStatus> {
  return apiFetch<SourceFeedStatus>('/api/sources', SourceFeedStatusSchema, {
    method: 'POST',
    body: spec,
  });
}

async function deleteSource(name: string): Promise<{ deleted: boolean }> {
  return apiFetch(`/api/sources/${encodeURIComponent(name)}`, z.object({ deleted: z.boolean() }), {
    method: 'DELETE',
  });
}

export default function SourcesPage() {
  const queryClient = useQueryClient();
  const sources = useQuery({
    queryKey: queryKeys.sources,
    queryFn: fetchSources,
    refetchInterval: 10_000,
  });

  const create = useMutation({
    mutationFn: createSource,
    onSuccess: () => queryClient.invalidateQueries({ queryKey: queryKeys.sources }),
  });
  const remove = useMutation({
    mutationFn: deleteSource,
    onSuccess: () => queryClient.invalidateQueries({ queryKey: queryKeys.sources }),
  });

  const [open, setOpen] = useState(false);

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-semibold tracking-tight">Source feeds</h1>
          <p className="text-sm text-muted-foreground">
            Backed by SourceFeed CRDs. CRUD here proxies through `/api/sources` to the cluster API.
          </p>
        </div>
        <div className="flex gap-2">
          <Button
            variant="outline"
            size="sm"
            onClick={() => sources.refetch()}
            disabled={sources.isFetching}
          >
            <RefreshCcw className="mr-2 h-4 w-4" /> Refresh
          </Button>
          <Dialog open={open} onOpenChange={setOpen}>
            <DialogTrigger asChild>
              <Button size="sm">
                <Plus className="mr-2 h-4 w-4" /> New source
              </Button>
            </DialogTrigger>
            <DialogContent>
              <DialogHeader>
                <DialogTitle>Create SourceFeed</DialogTitle>
                <DialogDescription>
                  Validated against `schemas/source_feed_spec.schema.json` before submit.
                </DialogDescription>
              </DialogHeader>
              <NewSourceForm
                onSubmit={async (spec) => {
                  await create.mutateAsync(spec);
                  setOpen(false);
                }}
                pending={create.isPending}
                error={create.error as Error | null}
              />
            </DialogContent>
          </Dialog>
        </div>
      </div>

      {sources.error ? (
        <p className="rounded border border-destructive/50 bg-destructive/10 p-3 text-sm text-destructive">
          {(sources.error as Error).message}
        </p>
      ) : null}

      {sources.data ? (
        sources.data.length === 0 ? (
          <Card>
            <CardHeader>
              <CardTitle>No SourceFeed CRDs found</CardTitle>
            </CardHeader>
            <CardContent>
              <p className="text-sm text-muted-foreground">
                Apply the seed manifests with `kubectl apply -f infra/k8s/sourcefeeds-seed.yaml`,
                or create one with the button above.
              </p>
            </CardContent>
          </Card>
        ) : (
          <div className="grid grid-cols-1 gap-4 md:grid-cols-2 lg:grid-cols-3">
            {sources.data.map((status) => (
              <div key={status.name} className="relative">
                <SourceCard status={status} />
                <Button
                  variant="ghost"
                  size="icon"
                  className="absolute right-2 top-2 h-7 w-7 text-muted-foreground hover:text-destructive"
                  onClick={() => {
                    if (window.confirm(`Delete SourceFeed/${status.name}?`)) {
                      remove.mutate(status.name);
                    }
                  }}
                  aria-label="delete"
                >
                  <Trash2 className="h-4 w-4" />
                </Button>
              </div>
            ))}
          </div>
        )
      ) : (
        <p className="text-sm text-muted-foreground">Loading sources...</p>
      )}
    </div>
  );
}

interface NewSourceFormProps {
  onSubmit: (spec: SourceFeedSpec) => void | Promise<void>;
  pending: boolean;
  error: Error | null;
}

function NewSourceForm({ onSubmit, pending, error }: NewSourceFormProps) {
  const [form, setForm] = useState({
    name: '',
    protocol: 'rss',
    endpoint: '',
    poll_interval_seconds: '900',
    requests_per_second: '0.5',
    burst: '5',
  });
  const [validation, setValidation] = useState<string | null>(null);

  function update<K extends keyof typeof form>(key: K, value: string) {
    setForm((prev) => ({ ...prev, [key]: value }));
  }

  async function submit(e: React.FormEvent) {
    e.preventDefault();
    const candidate = {
      name: form.name.trim(),
      protocol: form.protocol,
      endpoint: form.endpoint.trim(),
      enabled: true,
      poll_interval_seconds: Number(form.poll_interval_seconds),
      rate_limit: {
        requests_per_second: Number(form.requests_per_second),
        burst: Number(form.burst),
        respect_x_poll_interval: false,
      },
      accept_content_types: [],
      egress_allow: [],
    };
    const parsed = SourceFeedSpecSchema.safeParse(candidate);
    if (!parsed.success) {
      setValidation(parsed.error.issues.map((i) => `${i.path.join('.')}: ${i.message}`).join('; '));
      return;
    }
    setValidation(null);
    await onSubmit(parsed.data);
  }

  return (
    <form onSubmit={submit} className="space-y-3">
      <div className="grid grid-cols-2 gap-3">
        <Field label="name">
          <Input value={form.name} onChange={(e) => update('name', e.target.value)} required />
        </Field>
        <Field label="protocol">
          <select
            className="flex h-10 w-full rounded-md border border-input bg-background px-3 py-2 text-sm"
            value={form.protocol}
            onChange={(e) => update('protocol', e.target.value)}
          >
            {SourceFeedProtocols.map((p) => (
              <option key={p} value={p}>
                {p}
              </option>
            ))}
          </select>
        </Field>
      </div>
      <Field label="endpoint">
        <Input
          value={form.endpoint}
          onChange={(e) => update('endpoint', e.target.value)}
          placeholder="https://example.org/feed.xml"
          required
        />
      </Field>
      <div className="grid grid-cols-3 gap-3">
        <Field label="poll interval (s)">
          <Input
            type="number"
            min={60}
            value={form.poll_interval_seconds}
            onChange={(e) => update('poll_interval_seconds', e.target.value)}
          />
        </Field>
        <Field label="rps">
          <Input
            type="number"
            min={0.1}
            step={0.1}
            value={form.requests_per_second}
            onChange={(e) => update('requests_per_second', e.target.value)}
          />
        </Field>
        <Field label="burst">
          <Input
            type="number"
            min={1}
            value={form.burst}
            onChange={(e) => update('burst', e.target.value)}
          />
        </Field>
      </div>
      {validation ? (
        <p className="rounded border border-destructive/50 bg-destructive/10 p-2 text-xs text-destructive">
          {validation}
        </p>
      ) : null}
      {error ? (
        <p className="rounded border border-destructive/50 bg-destructive/10 p-2 text-xs text-destructive">
          {error.message}
        </p>
      ) : null}
      <div className="flex justify-end">
        <Button type="submit" disabled={pending}>
          {pending ? 'Creating...' : 'Create'}
        </Button>
      </div>
    </form>
  );
}

function Field({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <div className="space-y-1.5">
      <Label className="text-xs uppercase tracking-wide text-muted-foreground">{label}</Label>
      {children}
    </div>
  );
}
