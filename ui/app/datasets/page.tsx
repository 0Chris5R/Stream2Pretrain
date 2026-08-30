'use client';

import { useMemo, useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { Download, FileJson, Layers3 } from 'lucide-react';

import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Input } from '@/components/ui/input';
import { apiFetch } from '@/lib/api';
import { queryKeys } from '@/lib/query-keys';
import {
  DatasetSummarySchema,
  DocumentFacetsSchema,
  type DatasetSummary,
  type DocumentFacets,
} from '@/lib/schemas';
import { formatInt } from '@/lib/utils';

function day(offset: number): string {
  const value = new Date();
  value.setUTCDate(value.getUTCDate() + offset);
  return value.toISOString().slice(0, 10);
}

async function fetchSummary(query: string): Promise<DatasetSummary> {
  return apiFetch(`/api/datasets/summary?${query}`, DatasetSummarySchema);
}

async function fetchFacets(): Promise<DocumentFacets> {
  return apiFetch('/api/documents/facets?include_fixtures=false', DocumentFacetsSchema);
}

export default function DatasetsPage() {
  const [dateFrom, setDateFrom] = useState(day(-30));
  const [dateTo, setDateTo] = useState(day(0));
  const [routes, setRoutes] = useState(['pretrain', 'posttrain_candidate']);
  const [source, setSource] = useState('');
  const [contentTag, setContentTag] = useState('');
  const [includeStructured, setIncludeStructured] = useState(true);
  const [format, setFormat] = useState<'jsonl' | 'parquet'>('jsonl');
  const query = useMemo(() => {
    const value = new URLSearchParams({
      date_from: new Date(`${dateFrom}T00:00:00Z`).toISOString(),
      date_to: new Date(`${dateTo}T23:59:59Z`).toISOString(),
      include_structured: String(includeStructured),
    });
    routes.forEach((route) => value.append('route', route));
    if (contentTag) value.set('tag', contentTag);
    if (source) value.set('source', source);
    return value.toString();
  }, [dateFrom, dateTo, routes, source, contentTag, includeStructured]);
  const summary = useQuery({
    queryKey: queryKeys.dataset(query),
    queryFn: () => fetchSummary(query),
    enabled: routes.length > 0,
  });
  const facets = useQuery({ queryKey: queryKeys.documentFacets(false), queryFn: fetchFacets });

  function toggleRoute(route: string) {
    setRoutes((current) =>
      current.includes(route) ? current.filter((item) => item !== route) : [...current, route],
    );
  }
  function downloadManifest() {
    if (!summary.data) return;
    const blob = new Blob(
      [
        JSON.stringify(
          { generated_at: new Date().toISOString(), export_format: format, ...summary.data },
          null,
          2,
        ),
      ],
      { type: 'application/json' },
    );
    const url = URL.createObjectURL(blob);
    const anchor = document.createElement('a');
    anchor.href = url;
    anchor.download = 'stream2pretrain-manifest.json';
    anchor.click();
    URL.revokeObjectURL(url);
  }
  function exportDataset() {
    if (!summary.data?.documents) return;
    window.location.assign(`/api/datasets/export?${query}&format=${format}&limit=5000`);
  }

  return (
    <div className="space-y-5">
      <h1 className="text-2xl font-semibold tracking-tight">Datasets</h1>

      <Card>
        <CardContent className="space-y-4 p-5">
          <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-4">
            <Field label="From">
              <Input
                type="date"
                value={dateFrom}
                onChange={(event) => setDateFrom(event.target.value)}
              />
            </Field>
            <Field label="To">
              <Input
                type="date"
                value={dateTo}
                onChange={(event) => setDateTo(event.target.value)}
              />
            </Field>
            <Field label="Source">
              <select
                className="h-10 w-full rounded-md border bg-background px-3 text-sm"
                value={source}
                onChange={(event) => setSource(event.target.value)}
              >
                <option value="">All sources</option>
                {facets.data?.sources.map((item) => <option key={item}>{item}</option>)}
              </select>
            </Field>
            <Field label="Content tag">
              <select
                className="h-10 w-full rounded-md border bg-background px-3 text-sm"
                value={contentTag}
                onChange={(event) => setContentTag(event.target.value)}
              >
                <option value="">All content</option>
                {facets.data?.content_tags.map((item) => (
                  <option key={item} value={item}>
                    {item.replaceAll('_', ' ')}
                  </option>
                ))}
              </select>
            </Field>
          </div>
          <div>
            <ChipField
              label="Routes"
              values={['pretrain', 'posttrain_candidate']}
              selected={routes}
              toggle={toggleRoute}
            />
          </div>
          <div className="flex flex-wrap items-center justify-between gap-3 border-t pt-4">
            <div className="flex flex-wrap gap-4">
              <label className="flex items-center gap-2 text-sm">
                <input
                  type="checkbox"
                  checked={includeStructured}
                  onChange={(event) => setIncludeStructured(event.target.checked)}
                />{' '}
                Include tables, equations, and figure captions
              </label>
            </div>
            <div className="flex gap-2">
              <select
                className="h-10 rounded-md border bg-background px-3 text-sm"
                value={format}
                onChange={(event) => setFormat(event.target.value as 'jsonl' | 'parquet')}
              >
                <option value="jsonl">JSONL</option>
                <option value="parquet">Parquet</option>
              </select>
              <Button disabled={!summary.data?.documents} onClick={exportDataset}>
                <Download className="mr-2 h-4 w-4" /> Export
              </Button>
            </div>
          </div>
        </CardContent>
      </Card>

      {summary.error ? (
        <div className="rounded-lg border border-destructive/40 bg-destructive/5 p-3 text-sm text-destructive">
          {(summary.error as Error).message}
        </div>
      ) : null}

      <div className="grid gap-4 sm:grid-cols-2 xl:grid-cols-5">
        <Metric label="Documents" value={summary.data ? formatInt(summary.data.documents) : '-'} />
        <Metric label="Tokens" value={summary.data ? formatInt(summary.data.tokens) : '-'} />
        <Metric
          label="Source words"
          value={summary.data ? formatInt(summary.data.source_words) : '-'}
        />
        <Metric
          label="Projection words"
          value={summary.data ? formatInt(summary.data.projection_words) : '-'}
        />
        <Metric label="Sources" value={summary.data ? formatInt(summary.data.source_count) : '-'} />
      </div>

      <div className="grid gap-4 lg:grid-cols-2">
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2 text-base">
              <Layers3 className="h-4 w-4" /> Selection
            </CardTitle>
          </CardHeader>
          <CardContent className="space-y-3 text-sm">
            <SelectionRow label="Window" value={`${dateFrom} to ${dateTo}`} />
            <SelectionRow label="Routes" value={routes.join(', ') || 'none'} />
            <SelectionRow label="Licences" value="Strict allowlist" />
          </CardContent>
        </Card>
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2 text-base">
              <FileJson className="h-4 w-4" /> Manifest
            </CardTitle>
          </CardHeader>
          <CardContent>
            <Button variant="outline" onClick={downloadManifest} disabled={!summary.data}>
              <Download className="mr-2 h-4 w-4" /> Download manifest
            </Button>
          </CardContent>
        </Card>
      </div>
    </div>
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
function ChipField({
  label,
  values,
  selected,
  toggle,
}: {
  label: string;
  values: string[];
  selected: string[];
  toggle: (value: string) => void;
}) {
  return (
    <div className="space-y-1.5">
      <span className="text-xs font-medium">{label}</span>
      <div className="flex max-h-24 min-h-10 flex-wrap gap-1 overflow-y-auto">
        {values.map((value) => (
          <button key={value} type="button" onClick={() => toggle(value)}>
            <Badge variant={selected.includes(value) ? 'default' : 'outline'}>
              {value.replaceAll('_', ' ')}
            </Badge>
          </button>
        ))}
      </div>
    </div>
  );
}
function Metric({ label, value }: { label: string; value: string }) {
  return (
    <Card>
      <CardContent className="p-4">
        <div className="text-xs text-muted-foreground">{label}</div>
        <div className="mt-1 font-mono text-2xl font-semibold">{value}</div>
      </CardContent>
    </Card>
  );
}
function SelectionRow({ label, value }: { label: string; value: string }) {
  return (
    <div className="flex justify-between gap-4 border-b pb-2 last:border-0">
      <span className="text-muted-foreground">{label}</span>
      <span className="text-right font-medium">{value}</span>
    </div>
  );
}
