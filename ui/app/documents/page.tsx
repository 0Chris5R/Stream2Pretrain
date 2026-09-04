'use client';

import { useDeferredValue, useEffect, useMemo, useState } from 'react';
import { useInfiniteQuery, useQuery } from '@tanstack/react-query';
import {
  Check,
  ChevronRight,
  ExternalLink,
  Filter,
  Search,
  X,
} from 'lucide-react';

import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { Card, CardContent } from '@/components/ui/card';
import { Input } from '@/components/ui/input';
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog';
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import { apiFetch } from '@/lib/api';
import { queryKeys } from '@/lib/query-keys';
import {
  CorpusRouteSchema,
  DocumentDetailResponseSchema,
  DocumentFacetsSchema,
  DocumentPageSchema,
  type CorpusRoute,
  type CuratedDocumentDetail,
  type DocumentDetail,
  type DocumentFacets,
  type DocumentPage,
  type DocumentSummary,
} from '@/lib/schemas';
import { formatInt } from '@/lib/utils';

const ROUTES = CorpusRouteSchema.options;

interface Filters {
  search: string;
  routes: CorpusRoute[];
  source: string;
  sourceFormat: string;
  tags: string[];
  rejectionReason: string;
  dateFrom: string;
  dateTo: string;
  hasFigures: boolean;
  hasTables: boolean;
  hasEquations: boolean;
  includeFixtures: boolean;
  minEdu: string;
  minQuality: string;
  sort: string;
}

const EMPTY_FILTERS: Filters = {
  search: '',
  routes: [],
  source: '',
  sourceFormat: '',
  tags: [],
  rejectionReason: '',
  dateFrom: '',
  dateTo: '',
  hasFigures: false,
  hasTables: false,
  hasEquations: false,
  includeFixtures: false,
  minEdu: '',
  minQuality: '',
  sort: 'newest',
};

async function fetchDocuments(query: string): Promise<DocumentPage> {
  return apiFetch(`/api/documents?${query}`, DocumentPageSchema);
}

async function fetchFacets(includeFixtures: boolean): Promise<DocumentFacets> {
  return apiFetch(
    `/api/documents/facets?include_fixtures=${includeFixtures}`,
    DocumentFacetsSchema,
  );
}

async function fetchDocument(docId: string): Promise<DocumentDetail> {
  return apiFetch(`/api/documents/${encodeURIComponent(docId)}`, DocumentDetailResponseSchema);
}

export default function DocumentsPage() {
  const [filters, setFilters] = useState<Filters>(EMPTY_FILTERS);
  const [selected, setSelected] = useState('');
  const deferredSearch = useDeferredValue(filters.search);
  const query = useMemo(
    () => buildQuery({ ...filters, search: deferredSearch }),
    [filters, deferredSearch],
  );
  const list = useInfiniteQuery({
    queryKey: queryKeys.documents(query),
    queryFn: ({ pageParam }) =>
      fetchDocuments(pageParam ? `${query}&cursor=${encodeURIComponent(pageParam)}` : query),
    initialPageParam: '',
    getNextPageParam: (lastPage) =>
      lastPage.has_more && lastPage.next_cursor ? lastPage.next_cursor : undefined,
    refetchInterval: 10_000,
  });
  const facets = useQuery({
    queryKey: queryKeys.documentFacets(filters.includeFixtures),
    queryFn: () => fetchFacets(filters.includeFixtures),
  });
  const selectedId = selected;
  const detail = useQuery({
    queryKey: queryKeys.document(selectedId),
    queryFn: () => fetchDocument(selectedId),
    enabled: Boolean(selectedId),
  });

  function update(patch: Partial<Filters>) {
    setFilters((current) => ({ ...current, ...patch }));
  }

  const items = Array.from(
    new Map(
      (list.data?.pages.flatMap((value) => value.items) ?? []).map((item) => [item.doc_id, item]),
    ).values(),
  );
  const total = list.data?.pages[0]?.total;

  const activeFilters = countActiveFilters(filters);

  useEffect(() => {
    const values = new URLSearchParams(window.location.search);
    const route = values.get('route');
    const timer = window.setTimeout(() => {
      setFilters((current) => ({
        ...current,
        routes: route && CorpusRouteSchema.safeParse(route).success ? [route as CorpusRoute] : [],
        source: values.get('source') ?? '',
        rejectionReason: values.get('rejection_reason') ?? '',
        includeFixtures: values.get('include_fixtures') === 'true',
      }));
    }, 0);
    return () => window.clearTimeout(timer);
  }, []);

  return (
    <div className="space-y-5">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <h1 className="text-2xl font-semibold tracking-tight">Documents</h1>
        <div className="text-sm tabular-nums text-muted-foreground">
          {total !== undefined ? `${formatInt(total)} results` : 'Loading'}
        </div>
      </div>

      <div className="rounded-xl border bg-card p-3 shadow-sm">
        <div className="flex flex-wrap items-center gap-2">
          <div className="relative min-w-[16rem] flex-1">
            <Search className="absolute left-3 top-2.5 h-4 w-4 text-muted-foreground" />
            <Input
              value={filters.search}
              onChange={(event) => update({ search: event.target.value })}
              placeholder="Search title, source, or id"
              className="pl-9"
            />
          </div>
          <FilterMenu
            label="Route"
            values={ROUTES}
            selected={filters.routes}
            onChange={(routes) => update({ routes: routes as CorpusRoute[] })}
          />
          <SelectFilter
            label="Source"
            value={filters.source}
            values={facets.data?.sources ?? []}
            onChange={(source) => update({ source })}
          />
          <SelectFilter
            label="Format"
            value={filters.sourceFormat}
            values={facets.data?.source_formats ?? []}
            onChange={(sourceFormat) => update({ sourceFormat })}
          />
          <FilterMenu
            label="Tags"
            values={facets.data?.content_tags ?? []}
            selected={filters.tags}
            onChange={(tags) => update({ tags })}
          />
          <details className="relative">
            <summary className="flex h-10 cursor-pointer list-none items-center gap-2 rounded-md border px-3 text-sm hover:bg-accent">
              <Filter className="h-4 w-4" /> More
              {activeFilters > 0 ? <Badge variant="secondary">{activeFilters}</Badge> : null}
            </summary>
            <AdvancedFilters filters={filters} facets={facets.data} update={update} />
          </details>
          {activeFilters > 0 ? (
            <Button variant="ghost" size="sm" onClick={() => update(EMPTY_FILTERS)}>
              <X className="mr-1 h-4 w-4" /> Clear
            </Button>
          ) : null}
        </div>
      </div>

      {list.error ? <ErrorBox error={list.error} /> : null}

      <DocumentTable
        items={items}
        loading={list.isLoading}
        selected={selectedId}
        select={setSelected}
      />

      <Dialog open={Boolean(selectedId)} onOpenChange={(open) => !open && setSelected('')}>
        <DialogContent className="max-h-[92vh] max-w-6xl overflow-y-auto p-0">
          <DialogHeader className="border-b px-6 py-4">
            <DialogTitle>Document details</DialogTitle>
            <DialogDescription>
              Training projection, evidence, and processing audit.
            </DialogDescription>
          </DialogHeader>
          <div className="p-5">
            {detail.data ? <DocumentPanel document={detail.data} /> : null}
            {detail.isLoading ? <PanelLoading /> : null}
            {detail.error ? <ErrorBox error={detail.error} /> : null}
          </div>
        </DialogContent>
      </Dialog>

      {list.hasNextPage ? (
        <div className="flex justify-center">
          <Button
            variant="outline"
            size="sm"
            disabled={list.isFetchingNextPage}
            onClick={() => list.fetchNextPage()}
          >
            {list.isFetchingNextPage ? 'Loading' : 'Show more'}
          </Button>
        </div>
      ) : null}
    </div>
  );
}

function DocumentTable({
  items,
  loading,
  selected,
  select,
}: {
  items: DocumentPage['items'];
  loading: boolean;
  selected: string;
  select: (docId: string) => void;
}) {
  return (
    <div className="min-w-0 overflow-x-auto rounded-xl border bg-card shadow-sm">
      <Table className="min-w-[48rem]">
        <TableHeader>
          <TableRow>
            <TableHead>Document</TableHead>
            <TableHead>Route</TableHead>
            <TableHead className="text-right">Source quality</TableHead>
            <TableHead className="text-right">Composite</TableHead>
            <TableHead className="text-right">Sections</TableHead>
            <TableHead className="w-8" />
          </TableRow>
        </TableHeader>
        <TableBody>
          {loading ? <LoadingRows /> : null}
          {items.map((item) => (
            <TableRow
              key={item.doc_id}
              data-state={selected === item.doc_id ? 'selected' : undefined}
              className="cursor-pointer"
              onClick={() => select(item.doc_id)}
            >
              <TableCell className="max-w-[28rem]">
                <div className="truncate font-medium">{item.title || item.doc_id}</div>
                <div className="mt-1 flex items-center gap-2 text-xs text-muted-foreground">
                  <span className="truncate">{item.source_feed}</span>
                  <span>{item.source_format.toUpperCase()}</span>
                  <span>{new Date(item.valid_from).toLocaleDateString()}</span>
                </div>
              </TableCell>
              <TableCell>
                <RouteBadge route={item.route} />
              </TableCell>
              <TableCell className="text-right font-mono">
                {item.admission_only ? '-' : item.edu_score.toFixed(2)}
              </TableCell>
              <TableCell className="text-right font-mono">
                {item.admission_only ? '-' : item.quality_score.toFixed(2)}
              </TableCell>
              <TableCell className="text-right font-mono">
                {item.admission_only
                  ? '-'
                  : `${item.included_section_count}/${item.included_section_count + item.excluded_section_count}`}
              </TableCell>
              <TableCell>
                <ChevronRight className="h-4 w-4 text-muted-foreground" />
              </TableCell>
            </TableRow>
          ))}
          {!loading && items.length === 0 ? (
            <TableRow>
              <TableCell colSpan={6} className="h-40 text-center text-muted-foreground">
                No documents match these filters.
              </TableCell>
            </TableRow>
          ) : null}
        </TableBody>
      </Table>
    </div>
  );
}

function DocumentPanel({ document }: { document: DocumentDetail }) {
  if (document.admission_only) return <AdmissionOnlyPanel document={document} />;
  const artifact = document.scientific_artifact;
  const classifier = document.quality_diagnostics
    ? (document.source_feed === 'arxiv-html-fetcher' ? 'arXiv quality' : 'HF quality')
    : document.classifier_revision.includes('finepdfs')
    ? 'FinePDFs Edu v2'
    : document.classifier_revision.startsWith('not-run:')
      ? 'Not run'
      : 'Quality score';
  return (
    <Card className="overflow-hidden shadow-sm">
      <div className={`h-1 ${routeColor(document.route)}`} />
      <CardContent className="space-y-5 p-5">
        <div className="space-y-3">
          <div className="flex items-start justify-between gap-3">
            <div className="min-w-0">
              <div className="mb-2 flex flex-wrap gap-2">
                <RouteBadge route={document.route} />
                <Badge variant="outline">{document.source_format.toUpperCase()}</Badge>
                {document.quality_diagnostics?.mode === 'diagnostic' ? <Badge variant="outline">Diagnostic scoring</Badge> : null}
              </div>
              <h2 className="text-xl font-semibold leading-tight">
                {artifact?.title ?? document.title}
              </h2>
            </div>
            {artifact?.source_url ? (
              <Button asChild variant="outline" size="sm">
                <a href={artifact.source_url} target="_blank" rel="noreferrer">
                  Original <ExternalLink className="ml-1 h-3.5 w-3.5" />
                </a>
              </Button>
            ) : null}
          </div>
          <div className="grid grid-cols-3 gap-2">
            <Score label={classifier} value={document.edu_score.toFixed(2)} />
            <Score label="Structure" value={document.structural_quality_score.toFixed(2)} />
            <Score label="Reasoning evidence" value={percent(document.reasoning_score)} />
          </div>
          {Object.keys(document.quality_diagnostics?.classifiers ?? {}).length > 0 ? (
            <div className="grid grid-cols-2 gap-2">
              {Object.entries(document.quality_diagnostics!.classifiers).map(([task, result]) => (
                <div key={task} className="rounded-lg border p-3 text-sm">
                  <div className="font-medium">{classifierLabel(task)}</div>
                  <div>Max {result.score.toFixed(2)} · Mean {result.weighted_mean.toFixed(2)}</div>
                </div>
              ))}
            </div>
          ) : null}
          {document.quality_diagnostics?.confidence != null ? (
            <div className="text-sm">Model confidence {percent(document.quality_diagnostics.confidence)}</div>
          ) : null}
          <div className="flex flex-wrap gap-1.5">
            {document.content_tags.map((tag) => (
              <Badge key={tag} variant="secondary">
                {humanize(tag)}
              </Badge>
            ))}
          </div>
          <DecisionStrip document={document} />
        </div>

        <Tabs defaultValue="sections">
          <TabsList className="grid h-10 w-full grid-cols-3">
            <TabsTrigger value="sections">Sections</TabsTrigger>
            <TabsTrigger value="projection">Projection</TabsTrigger>
            <TabsTrigger value="assets">Assets</TabsTrigger>
          </TabsList>
          <TabsContent value="sections" className="mt-4">
            {document.quality_diagnostics ? <ClassifierSections document={document} /> : <SectionView document={document} />}
          </TabsContent>
          <TabsContent value="projection" className="mt-4">
            <ProjectionView document={document} />
          </TabsContent>
          <TabsContent value="assets" className="mt-4">
            <AssetView document={document} />
          </TabsContent>
        </Tabs>

        <details className="rounded-lg border">
          <summary className="cursor-pointer list-none px-4 py-3 text-sm font-medium">
            Advanced audit
          </summary>
          <AuditView document={document} />
        </details>
      </CardContent>
    </Card>
  );
}

function classifierLabel(task: string) {
  return task === 'arxiv-math-reasoning' ? 'Math reasoning' : 'Post-training fit';
}

function ClassifierSections({ document }: { document: CuratedDocumentDetail }) {
  return <div className="space-y-2">
    {document.quality_diagnostics?.sections.map((section) => (
      <details key={section.section_id} className="rounded-lg border">
        <summary className="flex cursor-pointer items-center justify-between gap-3 px-3 py-2.5">
          <span className="min-w-0 font-medium">{section.title}</span>
          <span className="flex shrink-0 gap-2">
            <Badge variant="outline">{section.score.toFixed(2)} / 5</Badge>
            {section.confidence != null ? <Badge variant="secondary">{percent(section.confidence)}</Badge> : null}
          </span>
        </summary>
        <div className="space-y-3 border-t px-3 py-3 text-sm">
          <div className="text-muted-foreground">{humanize(section.section_type)} · {formatInt(section.tokens)} tokens · {section.chunks} chunks</div>
          {Object.entries(section.classifiers).map(([task, result]) => (
            <div key={task} className="flex items-center gap-2">
              <span>{classifierLabel(task)}</span>
              <Badge variant="outline">{result.edu_score.toFixed(2)} / 5</Badge>
              {result.confidence != null ? <Badge variant="secondary">{percent(result.confidence)}</Badge> : null}
            </div>
          ))}
          <p className="whitespace-pre-wrap leading-relaxed">{section.text}</p>
        </div>
      </details>
    ))}
  </div>;
}

function AdmissionOnlyPanel({
  document,
}: {
  document: Extract<DocumentDetail, { admission_only: true }>;
}) {
  const admission = document.license_admission;
  return (
    <Card className="overflow-hidden shadow-sm">
      <div className={`h-1 ${routeColor(document.route)}`} />
      <CardContent className="space-y-5 p-5">
        <div className="flex items-start justify-between gap-3">
          <div className="min-w-0">
            <div className="mb-2 flex flex-wrap gap-2">
              <RouteBadge route={document.route} />
              <Badge variant="outline">NOT FETCHED</Badge>
            </div>
            <h2 className="break-all text-xl font-semibold leading-tight">{document.title}</h2>
          </div>
          <Button asChild variant="outline" size="sm">
            <a href={document.source_url} target="_blank" rel="noreferrer">
              Source <ExternalLink className="ml-1 h-3.5 w-3.5" />
            </a>
          </Button>
        </div>
        <div className="rounded-lg border border-red-300 bg-red-50 p-3 text-sm dark:bg-red-950/20">
          <div className="flex items-center gap-2 font-medium">
            <X className="h-4 w-4 text-red-600" />
            {admission.reason}
          </div>
        </div>
        <dl className="grid gap-px overflow-hidden rounded-lg border bg-border sm:grid-cols-2">
          {[
            ['Licence', admission.license_id],
            ['Resolver', admission.resolver ?? admission.license_source],
            ['Evidence scope', admission.evidence_scope ?? 'unknown'],
            ['Evidence revision', admission.evidence_revision ?? 'not available'],
            ['Evidence URL', admission.evidence_url ?? 'not available'],
            ['Policy', admission.policy_revision ?? 'not available'],
            ['Observed', document.valid_from],
            ['Document id', document.doc_id],
          ].map(([label, value]) => (
            <div key={label} className="bg-card p-3">
              <dt className="text-xs text-muted-foreground">{label}</dt>
              <dd className="mt-1 break-all font-mono text-xs">{value}</dd>
            </div>
          ))}
        </dl>
      </CardContent>
    </Card>
  );
}

function DecisionStrip({ document }: { document: CuratedDocumentDetail }) {
  const blocked = document.reject_reasons.length > 0;
  return (
    <div
      className={`rounded-lg border p-3 ${blocked ? 'border-red-300 bg-red-50 dark:bg-red-950/20' : 'border-emerald-300 bg-emerald-50 dark:bg-emerald-950/20'}`}
    >
      <div className="flex items-center gap-2 text-sm font-medium">
        {blocked ? (
          <X className="h-4 w-4 text-red-600" />
        ) : (
          <Check className="h-4 w-4 text-emerald-600" />
        )}
        {blocked ? document.reject_reasons.map(humanize).join(', ') : 'Passed all blocking gates'}
      </div>
    </div>
  );
}

function SectionView({ document }: { document: CuratedDocumentDetail }) {
  const artifact = document.scientific_artifact;
  if (!artifact) return <Empty label="No structured artifact" />;
  return (
    <div className="max-h-[34rem] space-y-2 overflow-y-auto pr-1">
      {artifact.sections.map((section) => {
        const score = document.segment_scores.find(
          (item) => item.segment_id === section.section_id,
        );
        return (
          <details
            key={section.section_id}
            className="rounded-lg border"
            open={section.role === 'abstract'}
          >
            <summary className="flex cursor-pointer list-none items-center justify-between gap-3 px-3 py-2.5">
              <div className="min-w-0">
                <span className="font-medium">{section.title}</span>
                <span className="ml-2 text-xs text-muted-foreground">{humanize(section.role)}</span>
              </div>
              <div className="flex shrink-0 items-center gap-2">
                {score?.finepdfs_edu_score != null ? (
                  <Badge variant="outline">FinePDFs {score.finepdfs_edu_score.toFixed(2)}</Badge>
                ) : null}
                <Badge variant={section.include_in_training ? 'success' : 'destructive'}>
                  {section.include_in_training ? 'kept' : 'removed'}
                </Badge>
              </div>
            </summary>
            <div className="border-t px-3 py-3 text-sm leading-relaxed">
              {section.exclusion_reason ? (
                <p className="mb-2 font-medium text-destructive">{section.exclusion_reason}</p>
              ) : null}
              <p className="whitespace-pre-wrap">{section.text}</p>
            </div>
          </details>
        );
      })}
    </div>
  );
}

function ProjectionView({ document }: { document: CuratedDocumentDetail }) {
  return (
    <div className="space-y-3">
      <div className="grid grid-cols-3 gap-2">
        <Score label="Source words" value={formatInt(document.source_word_count)} />
        <Score label="Projection words" value={formatInt(document.training_word_count)} />
        <Score
          label="Sections kept"
          value={`${document.included_section_count}/${document.included_section_count + document.excluded_section_count}`}
        />
      </div>
      <pre className="max-h-[30rem] overflow-auto whitespace-pre-wrap rounded-lg bg-muted/60 p-4 text-xs leading-relaxed">
        {document.text}
      </pre>
    </div>
  );
}

function AssetView({ document }: { document: CuratedDocumentDetail }) {
  const artifact = document.scientific_artifact;
  if (!artifact) return <Empty label="No scientific assets" />;
  return (
    <div className="space-y-4">
      <div className="grid grid-cols-3 gap-2">
        <Score label="Figures" value={String(artifact.figures.length)} />
        <Score label="Tables" value={String(artifact.tables.length)} />
        <Score label="Equations" value={String(artifact.equations.length)} />
      </div>
      <div className="grid max-h-[31rem] gap-3 overflow-y-auto sm:grid-cols-2">
        {artifact.figures.map((figure) => (
          <div key={figure.figure_id} className="overflow-hidden rounded-lg border">
            {figure.asset_s3_uri ? (
              // eslint-disable-next-line @next/next/no-img-element
              <img
                src={`/api/documents/${encodeURIComponent(document.doc_id)}/figures/${encodeURIComponent(figure.figure_id)}`}
                alt={figure.alt_text ?? figure.caption ?? figure.figure_id}
                className="h-44 w-full bg-white object-contain"
              />
            ) : null}
            <div className="space-y-2 p-3 text-xs">
              <div className="flex items-center justify-between gap-2">
                <span className="font-medium">{humanize(figure.figure_type)}</span>
                <Badge variant={figure.ocr_training_eligible ? 'success' : 'secondary'}>
                  OCR {figure.ocr_training_eligible ? 'accepted' : 'audit only'}
                </Badge>
              </div>
              {figure.caption ? <p>{figure.caption}</p> : null}
              {figure.ocr_text ? (
                <details>
                  <summary className="cursor-pointer font-medium">OCR text</summary>
                  <p className="mt-2 whitespace-pre-wrap font-mono text-muted-foreground">
                    {figure.ocr_text}
                  </p>
                </details>
              ) : null}
            </div>
          </div>
        ))}
        {artifact.tables.map((table) => (
          <div key={table.table_id} className="overflow-auto rounded-lg border p-3 text-xs">
            <div className="mb-2 font-medium">{table.caption ?? table.table_id}</div>
            <table className="w-full border-collapse">
              <tbody>
                {table.rows.slice(0, 20).map((row, rowIndex) => (
                  <tr key={rowIndex}>
                    {row.slice(0, 10).map((cell, cellIndex) => (
                      <td key={cellIndex} className="border px-1.5 py-1">
                        {cell}
                      </td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        ))}
      </div>
    </div>
  );
}

function AuditView({ document }: { document: CuratedDocumentDetail }) {
  const rows = [
    ['Quality classifier', `${document.classifier_backend} · ${document.classifier_revision}`],
    [
      'KenLM',
      `${document.perplexity.toFixed(1)} · ${document.perplexity_bucket} · ${document.perplexity_scorer}`,
    ],
    [
      'Language',
      `${document.lang} ${(document.lang_score * 100).toFixed(1)}% · ${document.lang_detector_revision}`,
    ],
    ['Gopher', `${document.gopher_pass ? 'pass' : 'fail'} · ${document.gopher_word_count} words`],
    ['C4', document.c4_nopunc_pass && document.c4_lorem_ipsum_pass ? 'pass' : 'fail'],
    ['PII', `${humanize(document.pii_action)} · ${document.pii_scanner_revision}`],
    [
      'Deduplication',
      `${document.near_duplicate ? 'near duplicate' : 'unique'} · ${document.minhash_backend} + ${document.lsh_backend}`,
    ],
    [
      'Licence',
      `${document.spdx_license ?? document.license} · ${document.spdx_license_source || document.license_source}`,
    ],
    ['Training use', humanize(document.training_usage)],
    [
      'Licence evidence',
      document.license_admission
        ? `${document.license_admission.evidence_scope ?? 'unknown'} · ${document.license_admission.resolver ?? document.license_admission.license_source}`
        : 'No admission record',
    ],
    ['Policy', `${document.policy_revision} · ${document.scoring_version}`],
    ['Document id', document.doc_id],
  ];
  return (
    <dl className="grid gap-px border-t bg-border sm:grid-cols-2">
      {rows.map(([label, value]) => (
        <div key={label} className="bg-card p-3">
          <dt className="text-xs text-muted-foreground">{label}</dt>
          <dd className="mt-1 break-all font-mono text-xs">{value}</dd>
        </div>
      ))}
    </dl>
  );
}

function AdvancedFilters({
  filters,
  facets,
  update,
}: {
  filters: Filters;
  facets?: DocumentFacets;
  update: (patch: Partial<Filters>) => void;
}) {
  return (
    <div className="bg-popover absolute right-0 top-12 z-20 w-[22rem] space-y-4 rounded-xl border p-4 shadow-xl">
      <div className="grid grid-cols-2 gap-2">
        <Field label="Published from">
          <Input
            type="date"
            value={filters.dateFrom}
            onChange={(event) => update({ dateFrom: event.target.value })}
          />
        </Field>
        <Field label="Published to">
          <Input
            type="date"
            value={filters.dateTo}
            onChange={(event) => update({ dateTo: event.target.value })}
          />
        </Field>
        <Field label="Min source quality">
          <Input
            type="number"
            min="0"
            max="5"
            step="0.1"
            value={filters.minEdu}
            onChange={(event) => update({ minEdu: event.target.value })}
          />
        </Field>
        <Field label="Min composite">
          <Input
            type="number"
            min="0"
            max="5"
            step="0.1"
            value={filters.minQuality}
            onChange={(event) => update({ minQuality: event.target.value })}
          />
        </Field>
      </div>
      <Field label="Rejection reason">
        <select
          className="h-10 w-full rounded-md border bg-background px-3 text-sm"
          value={filters.rejectionReason}
          onChange={(event) => update({ rejectionReason: event.target.value })}
        >
          <option value="">Any</option>
          {facets?.rejection_reasons.map((value) => (
            <option key={value} value={value}>
              {humanize(value)}
            </option>
          ))}
        </select>
      </Field>
      <div className="grid grid-cols-2 gap-2">
        <Toggle
          label="Figures"
          checked={filters.hasFigures}
          onChange={(hasFigures) => update({ hasFigures })}
        />
        <Toggle
          label="Tables"
          checked={filters.hasTables}
          onChange={(hasTables) => update({ hasTables })}
        />
        <Toggle
          label="Equations"
          checked={filters.hasEquations}
          onChange={(hasEquations) => update({ hasEquations })}
        />
        <Toggle
          label="Demo controls"
          checked={filters.includeFixtures}
          onChange={(includeFixtures) => update({ includeFixtures })}
        />
      </div>
      <Field label="Sort">
        <select
          className="h-10 w-full rounded-md border bg-background px-3 text-sm"
          value={filters.sort}
          onChange={(event) => update({ sort: event.target.value })}
        >
          <option value="newest">Newest</option>
          <option value="oldest">Oldest</option>
          <option value="quality_desc">Composite quality</option>
          <option value="edu_desc">Source quality</option>
          <option value="perplexity_asc">Lowest perplexity</option>
        </select>
      </Field>
    </div>
  );
}

function FilterMenu({
  label,
  values,
  selected,
  onChange,
}: {
  label: string;
  values: readonly string[];
  selected: readonly string[];
  onChange: (values: string[]) => void;
}) {
  return (
    <details className="relative">
      <summary className="flex h-10 cursor-pointer list-none items-center gap-2 rounded-md border px-3 text-sm hover:bg-accent">
        {label}
        {selected.length ? <Badge variant="secondary">{selected.length}</Badge> : null}
      </summary>
      <div className="bg-popover absolute left-0 top-12 z-20 max-h-72 min-w-60 overflow-y-auto rounded-lg border p-2 shadow-xl">
        {values.map((value) => {
          const checked = selected.includes(value);
          return (
            <label
              key={value}
              className="flex cursor-pointer items-center gap-2 rounded px-2 py-2 text-sm hover:bg-accent"
            >
              <input
                type="checkbox"
                checked={checked}
                onChange={() =>
                  onChange(
                    checked ? selected.filter((item) => item !== value) : [...selected, value],
                  )
                }
              />
              <span>{humanize(value)}</span>
            </label>
          );
        })}
      </div>
    </details>
  );
}

function SelectFilter({
  label,
  value,
  values,
  onChange,
}: {
  label: string;
  value: string;
  values: string[];
  onChange: (value: string) => void;
}) {
  return (
    <select
      aria-label={label}
      className="h-10 max-w-48 rounded-md border bg-background px-3 text-sm"
      value={value}
      onChange={(event) => onChange(event.target.value)}
    >
      <option value="">{label}</option>
      {values.map((item) => (
        <option key={item} value={item}>
          {item}
        </option>
      ))}
    </select>
  );
}

function Toggle({
  label,
  checked,
  onChange,
}: {
  label: string;
  checked: boolean;
  onChange: (checked: boolean) => void;
}) {
  return (
    <label className="flex cursor-pointer items-center gap-2 rounded-md border px-3 py-2 text-sm">
      <input
        type="checkbox"
        checked={checked}
        onChange={(event) => onChange(event.target.checked)}
      />
      {label}
    </label>
  );
}

function Field({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <label className="space-y-1 text-xs font-medium">
      <span>{label}</span>
      {children}
    </label>
  );
}

function Score({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded-lg border bg-muted/20 p-2.5">
      <div className="text-xs text-muted-foreground">{label}</div>
      <div className="mt-1 font-mono text-lg font-semibold tabular-nums">{value}</div>
    </div>
  );
}

function RouteBadge({ route }: { route: CorpusRoute }) {
  const variant =
    route === 'quarantine' ? 'destructive' : route === 'retry' ? 'warning' : 'success';
  return <Badge variant={variant}>{routeLabel(route)}</Badge>;
}

function routeLabel(route: CorpusRoute): string {
  if (route === 'posttrain_candidate' || route === 'reasoning_candidate') return 'Post-training';
  return humanize(route);
}

function routeColor(route: CorpusRoute): string {
  if (route === 'quarantine') return 'bg-red-500';
  if (route === 'retry') return 'bg-amber-500';
  return 'bg-emerald-500';
}

function buildQuery(filters: Filters): string {
  const query = new URLSearchParams({ page_size: '50', sort: filters.sort });
  if (filters.search.trim()) query.set('search', filters.search.trim());
  filters.routes.forEach((value) => query.append('route', value));
  filters.tags.forEach((value) => query.append('tag', value));
  if (filters.source) query.set('source', filters.source);
  if (filters.sourceFormat) query.set('source_format', filters.sourceFormat);
  if (filters.rejectionReason) query.set('rejection_reason', filters.rejectionReason);
  if (filters.dateFrom)
    query.set('date_from', new Date(`${filters.dateFrom}T00:00:00Z`).toISOString());
  if (filters.dateTo) query.set('date_to', new Date(`${filters.dateTo}T23:59:59Z`).toISOString());
  if (filters.hasFigures) query.set('has_figures', 'true');
  if (filters.hasTables) query.set('has_tables', 'true');
  if (filters.hasEquations) query.set('has_equations', 'true');
  if (filters.includeFixtures) query.set('include_fixtures', 'true');
  if (filters.minEdu) query.set('min_edu', filters.minEdu);
  if (filters.minQuality) query.set('min_quality', filters.minQuality);
  return query.toString();
}

function countActiveFilters(filters: Filters): number {
  return (
    filters.routes.length +
    filters.tags.length +
    [
      filters.source,
      filters.sourceFormat,
      filters.rejectionReason,
      filters.dateFrom,
      filters.dateTo,
      filters.minEdu,
      filters.minQuality,
    ].filter(Boolean).length +
    [filters.hasFigures, filters.hasTables, filters.hasEquations, filters.includeFixtures].filter(
      Boolean,
    ).length
  );
}

function humanize(value: string): string {
  return value.replaceAll('_', ' ').replace(/\b\w/g, (char) => char.toUpperCase());
}

function percent(value: number): string {
  return `${Math.round(value * 100)}%`;
}

function LoadingRows() {
  return (
    <>
      {Array.from({ length: 8 }, (_, index) => (
        <TableRow key={index}>
          <TableCell colSpan={6}>
            <div className="h-10 animate-pulse rounded bg-muted" />
          </TableCell>
        </TableRow>
      ))}
    </>
  );
}
function PanelLoading() {
  return <div className="h-[42rem] animate-pulse rounded-xl border bg-muted" />;
}
function EmptyPanel() {
  return (
    <div className="grid h-72 place-items-center rounded-xl border border-dashed text-sm text-muted-foreground">
      Select a document
    </div>
  );
}
function Empty({ label }: { label: string }) {
  return (
    <div className="grid h-32 place-items-center rounded-lg border border-dashed text-sm text-muted-foreground">
      {label}
    </div>
  );
}
function ErrorBox({ error }: { error: unknown }) {
  return (
    <div className="rounded-lg border border-destructive/40 bg-destructive/5 p-3 text-sm text-destructive">
      {(error as Error).message}
    </div>
  );
}
