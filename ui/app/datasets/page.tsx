'use client';

import { useMemo, useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { Download } from 'lucide-react';

import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { Card, CardContent } from '@/components/ui/card';
import { Input } from '@/components/ui/input';
import { apiFetch } from '@/lib/api';
import { queryKeys } from '@/lib/query-keys';
import { DocumentFacetsSchema, type DocumentFacets } from '@/lib/schemas';

type Corpus = 'pretrain' | 'sft' | 'rl';
type DatasetSplit = 'all' | 'train' | 'benchmark';
type PretrainFormat = 'jsonl' | 'parquet';

function day(offset: number): string {
  const value = new Date();
  value.setUTCDate(value.getUTCDate() + offset);
  return value.toISOString().slice(0, 10);
}

async function fetchFacets(): Promise<DocumentFacets> {
  return apiFetch('/api/documents/facets?include_fixtures=false', DocumentFacetsSchema);
}

export default function DatasetsPage() {
  const [corpus, setCorpus] = useState<Corpus>('pretrain');
  const [dateFrom, setDateFrom] = useState(day(-30));
  const [dateTo, setDateTo] = useState(day(0));
  const [source, setSource] = useState('');
  const [datasetSplit, setDatasetSplit] = useState<DatasetSplit>('all');
  const [includeStructured, setIncludeStructured] = useState(true);
  const [pretrainFormat, setPretrainFormat] = useState<PretrainFormat>('jsonl');
  const facets = useQuery({ queryKey: queryKeys.documentFacets(false), queryFn: fetchFacets });

  const exportUrl = useMemo(() => {
    const query = new URLSearchParams({
      date_from: new Date(`${dateFrom}T00:00:00Z`).toISOString(),
      date_to: new Date(`${dateTo}T23:59:59Z`).toISOString(),
    });
    if (corpus === 'pretrain') {
      ['pretrain', 'broad_pretraining', 'posttrain_candidate', 'reasoning_candidate'].forEach(
        (route) => query.append('route', route),
      );
      query.set('include_structured', String(includeStructured));
      query.set('format', pretrainFormat);
      if (source) query.set('source', source);
      return `/api/datasets/export?${query.toString()}`;
    }
    query.set('kind', corpus === 'sft' ? 'sft_trajectory' : 'rl_environment');
    if (datasetSplit !== 'all') query.set('dataset_split', datasetSplit);
    return `/api/foundry/datasets/export?${query.toString()}`;
  }, [corpus, datasetSplit, dateFrom, dateTo, includeStructured, pretrainFormat, source]);

  return (
    <div className="space-y-5">
      <h1 className="text-2xl font-semibold tracking-tight">Datasets</h1>

      <Card>
        <CardContent className="space-y-5 p-5">
          <ChipField
            label="Corpus"
            values={['pretrain', 'sft', 'rl']}
            selected={corpus}
            select={(value) => setCorpus(value as Corpus)}
          />

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
            {corpus === 'pretrain' ? (
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
            ) : (
              <Field label="Split">
                <select
                  className="h-10 w-full rounded-md border bg-background px-3 text-sm"
                  value={datasetSplit}
                  onChange={(event) => setDatasetSplit(event.target.value as DatasetSplit)}
                >
                  <option value="all">Train and benchmark</option>
                  <option value="train">Train</option>
                  <option value="benchmark">Benchmark</option>
                </select>
              </Field>
            )}
            <Field label="Format">
              {corpus === 'pretrain' ? (
                <select
                  className="h-10 w-full rounded-md border bg-background px-3 text-sm"
                  value={pretrainFormat}
                  onChange={(event) => setPretrainFormat(event.target.value as PretrainFormat)}
                >
                  <option value="jsonl">JSONL</option>
                  <option value="parquet">Parquet</option>
                </select>
              ) : (
                <Input value={corpus === 'sft' ? 'JSONL' : 'Environment archive'} disabled />
              )}
            </Field>
          </div>

          <div className="flex flex-wrap items-center justify-between gap-3 border-t pt-4">
            {corpus === 'pretrain' ? (
              <label className="flex items-center gap-2 text-sm">
                <input
                  type="checkbox"
                  checked={includeStructured}
                  onChange={(event) => setIncludeStructured(event.target.checked)}
                />
                Include tables, equations, and figure captions
              </label>
            ) : (
              <span />
            )}
            <Button asChild>
              <a href={exportUrl}>
                <Download className="mr-2 h-4 w-4" /> Export {corpus.toUpperCase()}
              </a>
            </Button>
          </div>
        </CardContent>
      </Card>
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
  select,
}: {
  label: string;
  values: string[];
  selected: string;
  select: (value: string) => void;
}) {
  return (
    <div className="space-y-1.5">
      <span className="text-xs font-medium">{label}</span>
      <div className="flex flex-wrap gap-1">
        {values.map((value) => (
          <button key={value} type="button" onClick={() => select(value)}>
            <Badge variant={selected === value ? 'default' : 'outline'}>
              {value.toUpperCase()}
            </Badge>
          </button>
        ))}
      </div>
    </div>
  );
}
