'use client';

import { useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { Check, CircleDot, Clock3, ExternalLink, Eye, FlaskConical, X } from 'lucide-react';

import { ArtifactInspector } from '@/components/artifact-inspector';
import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { FoundryActivityPanel } from '@/components/foundry-activity-panel';
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
import { apiFetch } from '@/lib/api';
import { queryKeys } from '@/lib/query-keys';
import {
  FoundryDashboardSchema,
  FoundryArtifactListSchema,
  FoundryJobDetailSchema,
  type FoundryDashboard,
  type FoundryArtifact,
  type FoundryJobDetail,
} from '@/lib/schemas';
import { formatInt } from '@/lib/utils';

async function fetchDashboard(): Promise<FoundryDashboard> {
  return apiFetch('/api/foundry/dashboard', FoundryDashboardSchema);
}

async function fetchJob(jobId: string): Promise<FoundryJobDetail> {
  return apiFetch(`/api/foundry/jobs/${encodeURIComponent(jobId)}`, FoundryJobDetailSchema);
}

async function fetchArtifacts(): Promise<FoundryArtifact[]> {
  return (await apiFetch('/api/foundry/artifacts?limit=100', FoundryArtifactListSchema)).items;
}

export default function PostTrainingPage() {
  const [selected, setSelected] = useState('');
  const [inspectionTarget, setInspectionTarget] = useState<FoundryArtifact | null>(null);
  const dashboard = useQuery({
    queryKey: queryKeys.foundry,
    queryFn: fetchDashboard,
    refetchInterval: 10_000,
  });
  const selectedId = selected;
  const job = useQuery({
    queryKey: queryKeys.foundryJob(selectedId),
    queryFn: () => fetchJob(selectedId),
    enabled: Boolean(selectedId),
    refetchInterval: 10_000,
  });
  const artifacts = useQuery({
    queryKey: queryKeys.foundryArtifacts,
    queryFn: fetchArtifacts,
    refetchInterval: 10_000,
  });
  const data = dashboard.data;
  const acceptedSft = data?.artifacts['sft_trajectory:accepted'] ?? 0;
  const acceptedRl = data?.artifacts['rl_environment:accepted'] ?? 0;
  const rejected = Object.entries(data?.artifacts ?? {}).reduce(
    (sum, [key, value]) => sum + (key.endsWith(':rejected') ? value : 0),
    0,
  );
  const acceptance =
    acceptedRl + acceptedSft + rejected
      ? (acceptedRl + acceptedSft) / (acceptedRl + acceptedSft + rejected)
      : 0;

  return (
    <div className="space-y-5">
      <div className="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
        <h1 className="text-2xl font-semibold tracking-tight">Post-training</h1>
        <Badge variant="outline">
          Daily cohort · {String(data?.daily_run_hour_utc ?? 0).padStart(2, '0')}:
          {String(data?.daily_run_minute_utc ?? 0).padStart(2, '0')} UTC
        </Badge>
      </div>

      {dashboard.error ? <ErrorBox error={dashboard.error} /> : null}

      <div className="grid gap-4 sm:grid-cols-2 xl:grid-cols-5">
        <Metric label="SFT trajectories" value={formatInt(acceptedSft)} />
        <Metric label="RL environments" value={formatInt(acceptedRl)} />
        <Metric label="Acceptance" value={`${Math.round(acceptance * 100)}%`} />
        <Metric
          label="Papers / queued"
          value={`${formatInt(Object.values(data?.jobs ?? {}).reduce((sum, value) => sum + value, 0))} / ${formatInt(data?.queued_candidates ?? 0)}`}
        />
        <Metric label="Human approved" value={formatInt(data?.human_audits.approved ?? 0)} />
      </div>

      <FoundryActivityPanel dashboard={data} />

      <div className="grid gap-4 xl:grid-cols-[1.2fr_0.8fr_0.8fr]">
        <ProviderCapacity data={data} />
        <FamilyMix families={data?.families ?? {}} />
        <SplitAllocation data={data} />
      </div>

      <JobsTable jobs={data?.recent_jobs ?? []} selected={selectedId} onSelect={setSelected} />

      <Dialog open={Boolean(selectedId)} onOpenChange={(open) => !open && setSelected('')}>
        <DialogContent className="max-h-[92vh] max-w-5xl overflow-y-auto p-0">
          <DialogHeader className="border-b px-6 py-4">
            <DialogTitle>Post-training job</DialogTitle>
            <DialogDescription>
              Tasks, model calls, validation, and generated artifacts.
            </DialogDescription>
          </DialogHeader>
          <div className="p-5">
            {job.data ? <JobPanel job={job.data} onInspect={setInspectionTarget} /> : null}
            {job.isLoading ? <EmptyPanel loading /> : null}
            {job.error ? <ErrorBox error={job.error} /> : null}
          </div>
        </DialogContent>
      </Dialog>

      <ArtifactTable
        artifacts={artifacts.data ?? []}
        onSelectJob={setSelected}
        onInspect={setInspectionTarget}
      />

      <ArtifactInspector
        artifact={inspectionTarget}
        open={Boolean(inspectionTarget)}
        onOpenChange={(open) => {
          if (!open) setInspectionTarget(null);
        }}
      />
    </div>
  );
}

function ArtifactTable({
  artifacts,
  onSelectJob,
  onInspect,
}: {
  artifacts: FoundryArtifact[];
  onSelectJob: (jobId: string) => void;
  onInspect: (artifact: FoundryArtifact) => void;
}) {
  return (
    <Card className="overflow-hidden">
      <CardHeader className="p-4 pb-2">
        <CardTitle className="text-base">Dataset artifacts</CardTitle>
      </CardHeader>
      <Table>
        <TableHeader>
          <TableRow>
            <TableHead>Paper / task</TableHead>
            <TableHead>Artifact</TableHead>
            <TableHead>Dataset</TableHead>
            <TableHead>Validation</TableHead>
            <TableHead>Human audit</TableHead>
            <TableHead>Created</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {artifacts.map((artifact) => (
            <TableRow
              key={artifact.artifact_id}
              className="cursor-pointer"
              onClick={() => onSelectJob(artifact.job_id)}
            >
              <TableCell>
                <div className="max-w-[24rem] truncate font-medium">{artifact.paper_id}</div>
                <div className="max-w-[24rem] truncate text-xs text-muted-foreground">
                  {artifact.task_id}
                </div>
              </TableCell>
              <TableCell>
                <Badge variant={artifact.status === 'accepted' ? 'success' : 'destructive'}>
                  {humanize(artifact.kind)}
                </Badge>
                <div className="mt-1 text-xs text-muted-foreground">
                  {humanize(artifact.family)}
                </div>
              </TableCell>
              <TableCell>
                {artifact.dataset_split === 'none'
                  ? '-'
                  : `${humanize(artifact.pool)} · ${humanize(artifact.dataset_split)}`}
              </TableCell>
              <TableCell>
                <ValidationStrip artifact={artifact} />
              </TableCell>
              <TableCell>
                <div className="flex items-center gap-2">
                  {artifact.human_audit ? (
                    <Badge
                      variant={
                        artifact.human_audit.decision === 'approved' ? 'success' : 'destructive'
                      }
                    >
                      {humanize(artifact.human_audit.decision)} · {artifact.human_audit.reviewer}
                    </Badge>
                  ) : (
                    <Badge variant="secondary">Pending</Badge>
                  )}
                  <Button
                    size="sm"
                    variant="outline"
                    onClick={(event) => {
                      event.stopPropagation();
                      onInspect(artifact);
                    }}
                  >
                    <Eye className="mr-2 h-4 w-4" /> Inspect
                  </Button>
                </div>
              </TableCell>
              <TableCell className="whitespace-nowrap text-xs tabular-nums">
                {formatTime(artifact.created_at)}
              </TableCell>
            </TableRow>
          ))}
          {!artifacts.length ? (
            <TableRow>
              <TableCell colSpan={6} className="h-28 text-center text-muted-foreground">
                No artifacts yet
              </TableCell>
            </TableRow>
          ) : null}
        </TableBody>
      </Table>
    </Card>
  );
}

function Metric({ label, value }: { label: string; value: string }) {
  return (
    <Card>
      <CardContent className="p-4">
        <div className="text-xs text-muted-foreground">{label}</div>
        <div className="mt-1 text-3xl font-semibold tabular-nums">{value}</div>
      </CardContent>
    </Card>
  );
}

function ProviderCapacity({ data }: { data?: FoundryDashboard }) {
  const providers = ['hetzner'] as const;
  return (
    <Card>
      <CardHeader className="p-4 pb-3">
        <CardTitle className="text-base">Model capacity</CardTitle>
      </CardHeader>
      <CardContent className="grid gap-3 p-4 pt-0">
        {providers.map((provider) => {
          const minute = data?.quotas.find(
            (value) => value.provider === provider && value.window === 'minute',
          );
          const model = data?.models.find((value) => value.provider === provider);
          const providerStatus = data?.provider_statuses[provider];
          const budgetExhausted =
            providerStatus?.state === 'CALL_RATE_LIMITED' &&
            providerStatus.reason?.toLowerCase().includes('budget exhausted');
          const calls = data?.providers[provider]?.calls ?? 0;
          const remaining = minute?.estimated_remaining_output;
          return (
            <div key={provider} className="rounded-lg border p-3">
              <div className="flex items-center justify-between gap-2">
                <span className="font-medium">{humanize(provider)}</span>
                <Badge
                  variant={budgetExhausted ? 'destructive' : model ? 'success' : 'secondary'}
                  title={providerStatus?.reason ?? undefined}
                >
                  {budgetExhausted
                    ? 'Budget exhausted'
                    : model?.drifted
                      ? 'Changed'
                      : model
                        ? 'Available'
                        : 'Pending'}
                </Badge>
              </div>
              <div className="mt-3 text-2xl font-semibold tabular-nums">
                {budgetExhausted || remaining === null || remaining === undefined
                  ? '-'
                  : formatInt(remaining)}
              </div>
              <div className="text-xs text-muted-foreground">
                {budgetExhausted
                  ? 'provider balance unavailable'
                  : 'output tokens left this minute'}
              </div>
              <div className="mt-3 truncate text-xs">
                {model?.configured_model_ids[0] ?? 'Awaiting discovery'}
              </div>
              <div className="mt-1 text-xs tabular-nums text-muted-foreground">
                {formatInt(calls)} successful calls
              </div>
            </div>
          );
        })}
      </CardContent>
    </Card>
  );
}

function FamilyMix({ families }: { families: Record<string, number> }) {
  const values = Object.entries(families).sort((a, b) => b[1] - a[1]);
  const maximum = Math.max(1, ...values.map(([, count]) => count));
  return (
    <Card>
      <CardHeader className="p-4 pb-3">
        <CardTitle className="text-base">Accepted task mix</CardTitle>
      </CardHeader>
      <CardContent className="space-y-2 p-4 pt-0">
        {values.length ? (
          values.map(([family, count]) => (
            <div key={family} className="grid grid-cols-[9rem_1fr_2rem] items-center gap-2 text-xs">
              <span className="truncate">{humanize(family)}</span>
              <div className="h-2 overflow-hidden rounded-full bg-muted">
                <div
                  className="h-full rounded-full bg-primary"
                  style={{ width: `${(count / maximum) * 100}%` }}
                />
              </div>
              <span className="text-right tabular-nums">{count}</span>
            </div>
          ))
        ) : (
          <div className="py-8 text-center text-sm text-muted-foreground">No accepted tasks</div>
        )}
      </CardContent>
    </Card>
  );
}

function SplitAllocation({ data }: { data?: FoundryDashboard }) {
  const latest = data?.daily_runs[0];
  const manual = data?.manual_runs[0];
  const rows = [
    ['SFT train', data?.splits['sft:train'] ?? 0],
    ['SFT benchmark', data?.splits['sft:benchmark'] ?? 0],
    ['RL train', data?.splits['rl:train'] ?? 0],
    ['RL benchmark', data?.splits['rl:benchmark'] ?? 0],
  ] as const;
  return (
    <Card>
      <CardHeader className="p-4 pb-3">
        <CardTitle className="text-base">Dataset allocation</CardTitle>
      </CardHeader>
      <CardContent className="space-y-2 p-4 pt-0">
        {rows.map(([label, count]) => (
          <div key={label} className="flex items-center justify-between text-sm">
            <span>{label}</span>
            <span className="font-medium tabular-nums">{formatInt(count)}</span>
          </div>
        ))}
        <div className="border-t pt-2 text-xs text-muted-foreground">
          {latest
            ? `${latest.run_date}: ${humanize(latest.state)} · ${latest.processed_count}/${latest.candidate_count}`
            : 'No daily run yet'}
        </div>
        {manual ? (
          <div className="text-xs text-muted-foreground">
            Manual: {humanize(manual.state)} · {manual.processed_count}/{manual.candidate_count}
          </div>
        ) : null}
      </CardContent>
    </Card>
  );
}

function JobsTable({
  jobs,
  selected,
  onSelect,
}: {
  jobs: FoundryDashboard['recent_jobs'];
  selected: string;
  onSelect: (value: string) => void;
}) {
  return (
    <Card className="overflow-hidden">
      <Table>
        <TableHeader>
          <TableRow>
            <TableHead>Paper</TableHead>
            <TableHead>State</TableHead>
            <TableHead>Updated</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {jobs.map((job) => (
            <TableRow
              key={job.job_id}
              data-state={job.job_id === selected ? 'selected' : undefined}
              className="cursor-pointer"
              onClick={() => onSelect(job.job_id)}
            >
              <TableCell>
                <div className="max-w-[28rem] truncate font-medium">{job.paper_id}</div>
                <div className="max-w-[28rem] truncate text-xs text-muted-foreground">
                  {job.doc_id}
                </div>
              </TableCell>
              <TableCell>
                <StateBadge state={job.state} reason={job.reason} />
              </TableCell>
              <TableCell className="whitespace-nowrap text-xs tabular-nums">
                {formatTime(job.updated_at)}
              </TableCell>
            </TableRow>
          ))}
          {!jobs.length ? (
            <TableRow>
              <TableCell colSpan={3} className="h-28 text-center text-muted-foreground">
                No foundry runs
              </TableCell>
            </TableRow>
          ) : null}
        </TableBody>
      </Table>
    </Card>
  );
}

function JobPanel({
  job,
  onInspect,
}: {
  job: FoundryJobDetail;
  onInspect: (artifact: FoundryArtifact) => void;
}) {
  const semanticEvents = job.events.filter(
    (event) =>
      !event.state.startsWith('CALL_') &&
      !event.state.startsWith('QUOTA_') &&
      event.state !== 'STREAM_CHECKPOINTED',
  );
  return (
    <Card>
      <CardContent className="space-y-5 p-5">
        <div className="flex items-start justify-between gap-3">
          <div className="min-w-0">
            <StateBadge state={job.state} reason={job.reason} />
            <h2 className="mt-2 truncate text-lg font-semibold">{job.paper_id}</h2>
          </div>
          <Badge variant="outline">{job.artifacts.length} artifacts</Badge>
        </div>

        <div className="grid grid-cols-2 gap-2">
          <PanelMetric label="Provider calls" value={formatInt(job.provider_traces.length)} />
          <PanelMetric
            label="Output tokens"
            value={formatInt(
              job.provider_traces.reduce((sum, value) => sum + value.output_tokens, 0),
            )}
          />
        </div>

        <div className="space-y-2">
          <div className="text-xs font-medium uppercase tracking-wide text-muted-foreground">
            Artifacts
          </div>
          {job.artifacts.map((artifact) => (
            <div key={artifact.artifact_id} className="rounded-lg border p-3">
              <div className="flex items-center justify-between gap-2">
                <div className="font-medium">{humanize(artifact.family)}</div>
                <div className="flex gap-1">
                  {artifact.dataset_split !== 'none' ? (
                    <Badge variant="outline">
                      {humanize(artifact.pool)} · {humanize(artifact.dataset_split)}
                    </Badge>
                  ) : null}
                  <Badge variant={artifact.status === 'accepted' ? 'success' : 'destructive'}>
                    {humanize(artifact.kind)}
                  </Badge>
                  {artifact.human_audit ? (
                    <Badge
                      variant={
                        artifact.human_audit.decision === 'approved' ? 'success' : 'destructive'
                      }
                    >
                      Human {humanize(artifact.human_audit.decision)}
                    </Badge>
                  ) : (
                    <Badge variant="secondary">Audit pending</Badge>
                  )}
                </div>
              </div>
              <ValidationStrip artifact={artifact} />
              {artifact.package_uri ? (
                <div className="mt-2 flex items-center gap-1 truncate text-xs text-muted-foreground">
                  <ExternalLink className="h-3 w-3" /> {artifact.package_uri}
                </div>
              ) : null}
              <Button
                className="mt-3"
                size="sm"
                variant="outline"
                onClick={() => onInspect(artifact)}
              >
                <Eye className="mr-2 h-4 w-4" /> Inspect
              </Button>
            </div>
          ))}
          {!job.artifacts.length ? (
            <div className="text-sm text-muted-foreground">No artifacts yet</div>
          ) : null}
        </div>

        <div className="space-y-2">
          <div className="text-xs font-medium uppercase tracking-wide text-muted-foreground">
            Timeline
          </div>
          {semanticEvents.map((event) => (
            <div key={event.event_id} className="flex items-start gap-2 text-sm">
              <CircleDot className="mt-0.5 h-4 w-4 shrink-0 text-primary" />
              <div className="min-w-0 flex-1">
                <div className="font-medium">{stageLabel(event.state)}</div>
                {event.reason ? (
                  <div className="truncate text-xs text-destructive">{event.reason}</div>
                ) : null}
              </div>
              <div className="whitespace-nowrap text-xs tabular-nums text-muted-foreground">
                {formatTime(event.occurred_at)}
              </div>
            </div>
          ))}
        </div>
      </CardContent>
    </Card>
  );
}

function ValidationStrip({ artifact }: { artifact: FoundryJobDetail['artifacts'][number] }) {
  const values = [
    ['Positive', artifact.validation.positive_pass],
    ['Adversarial', artifact.validation.adversarial_pass],
    ['Replay', artifact.validation.replay_pass],
    ['Security', artifact.validation.security_pass],
  ] as const;
  return (
    <div className="mt-3 flex flex-wrap gap-1.5">
      {values.map(([label, passed]) => (
        <Badge key={label} variant={passed ? 'secondary' : 'destructive'}>
          {passed ? <Check className="mr-1 h-3 w-3" /> : <X className="mr-1 h-3 w-3" />}
          {label}
        </Badge>
      ))}
      {artifact.validation.mutation_total ? (
        <Badge variant="secondary">
          {artifact.validation.mutation_killed}/{artifact.validation.mutation_total} mutations
        </Badge>
      ) : null}
    </div>
  );
}

function PanelMetric({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded-lg border bg-muted/20 p-3">
      <div className="text-xs text-muted-foreground">{label}</div>
      <div className="mt-1 text-lg font-semibold tabular-nums">{value}</div>
    </div>
  );
}

function StateBadge({ state, reason }: { state: string; reason?: string | null }) {
  const budgetExhausted =
    state === 'CALL_RATE_LIMITED' && reason?.toLowerCase().includes('budget exhausted');
  const variant =
    state === 'REJECTED' || budgetExhausted
      ? 'destructive'
      : state.startsWith('ACCEPTED')
        ? 'success'
        : 'secondary';
  return (
    <Badge variant={variant} title={reason ?? undefined}>
      {budgetExhausted ? 'Budget exhausted' : stageLabel(state)}
    </Badge>
  );
}

function EmptyPanel({ loading }: { loading: boolean }) {
  return (
    <Card>
      <CardContent className="flex h-48 items-center justify-center text-sm text-muted-foreground">
        {loading ? (
          <>
            <Clock3 className="mr-2 h-4 w-4" />
            Loading
          </>
        ) : (
          <>
            <FlaskConical className="mr-2 h-4 w-4" />
            Select a run
          </>
        )}
      </CardContent>
    </Card>
  );
}

function ErrorBox({ error }: { error: unknown }) {
  const message = error instanceof Error ? error.message : 'request_failed';
  return (
    <div className="rounded-lg border border-destructive/40 bg-destructive/5 p-3 text-sm text-destructive">
      {message === 'foundry_unavailable'
        ? 'Post-training service unavailable. Retrying automatically.'
        : message}
    </div>
  );
}

function stageLabel(value: string): string {
  const labels: Record<string, string> = {
    PROVIDER_CAPACITY_RESERVED: 'Capacity reserved',
    GRAPH_COMPILED: 'Evidence graph',
    GRAPH_CRITIQUED: 'Graph reviewed',
    TASKS_PROPOSED: 'Tasks proposed',
    TASKS_ROUTED: 'Tasks routed',
    SOLUTIONS_GENERATED: 'Solutions',
    VERIFIERS_COMPILED: 'Verifiers',
    ADVERSARIAL_VALIDATED: 'Adversarial tests',
    ACCEPTED_SFT: 'SFT accepted',
    ACCEPTED_RL: 'RL accepted',
  };
  return labels[value] ?? humanize(value);
}

function humanize(value: string): string {
  return value
    .replaceAll('_', ' ')
    .toLowerCase()
    .replace(/\b\w/g, (char) => char.toUpperCase());
}

function formatTime(value: string): string {
  return new Intl.DateTimeFormat(undefined, {
    month: 'short',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
  }).format(new Date(value));
}
