'use client';

import { useState } from 'react';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { Check, Download, FileArchive, ShieldCheck, X } from 'lucide-react';

import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog';
import { Input } from '@/components/ui/input';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import { apiFetch } from '@/lib/api';
import { queryKeys } from '@/lib/query-keys';
import {
  FoundryArtifactAuditResponseSchema,
  FoundryArtifactInspectionSchema,
  type FoundryArtifact,
  type FoundryArtifactInspection,
} from '@/lib/schemas';
import { formatInt } from '@/lib/utils';

async function fetchInspection(artifactId: string): Promise<FoundryArtifactInspection> {
  return apiFetch(
    `/api/foundry/artifacts/${encodeURIComponent(artifactId)}/inspect`,
    FoundryArtifactInspectionSchema,
  );
}

async function auditArtifact(
  artifactId: string,
  decision: 'approved' | 'rejected',
  reviewer: string,
  reason: string,
): Promise<void> {
  await apiFetch(
    `/api/foundry/artifacts/${encodeURIComponent(artifactId)}/audit`,
    FoundryArtifactAuditResponseSchema,
    {
      method: 'POST',
      body: { decision, reviewer, reason: reason || undefined },
    },
  );
}

export function ArtifactInspector({
  artifact,
  open,
  onOpenChange,
}: {
  artifact: FoundryArtifact | null;
  open: boolean;
  onOpenChange: (open: boolean) => void;
}) {
  const queryClient = useQueryClient();
  const [reviewer, setReviewer] = useState('');
  const [reason, setReason] = useState('');

  const inspection = useQuery({
    queryKey: queryKeys.foundryArtifactInspection(artifact?.artifact_id ?? ''),
    queryFn: () => fetchInspection(artifact!.artifact_id),
    enabled: open && Boolean(artifact),
  });
  const audit = useMutation({
    mutationFn: (decision: 'approved' | 'rejected') => {
      if (!artifact) throw new Error('No artifact selected');
      return auditArtifact(artifact.artifact_id, decision, reviewer, reason);
    },
    onSuccess: async () => {
      setReason('');
      await Promise.all([
        queryClient.invalidateQueries({ queryKey: queryKeys.foundry }),
        queryClient.invalidateQueries({ queryKey: queryKeys.foundryArtifacts }),
        queryClient.invalidateQueries({
          queryKey: queryKeys.foundryArtifactInspection(artifact?.artifact_id ?? ''),
        }),
        artifact
          ? queryClient.invalidateQueries({ queryKey: queryKeys.foundryJob(artifact.job_id) })
          : Promise.resolve(),
      ]);
    },
  });
  const data = inspection.data;

  return (
    <Dialog
      open={open}
      onOpenChange={(next) => {
        if (!next) setReason('');
        onOpenChange(next);
      }}
    >
      <DialogContent className="flex h-[92vh] max-w-[96vw] flex-col gap-0 overflow-hidden p-0 xl:max-w-7xl">
        <DialogHeader className="border-b px-6 py-5 pr-14">
          <div className="flex flex-wrap items-center gap-2">
            <DialogTitle>{data?.task ? humanize(data.task.family) : 'Artifact inspection'}</DialogTitle>
            {artifact ? (
              <>
                <Badge variant={artifact.status === 'accepted' ? 'success' : 'destructive'}>
                  {humanize(artifact.status)}
                </Badge>
                <Badge variant="outline">{artifact.pool.toUpperCase()}</Badge>
                {artifact.dataset_split !== 'none' ? (
                  <Badge variant="outline">{humanize(artifact.dataset_split)}</Badge>
                ) : null}
              </>
            ) : null}
          </div>
          <DialogDescription className="font-mono text-xs">
            {artifact?.paper_id} · {artifact?.artifact_id}
          </DialogDescription>
        </DialogHeader>

        <div className="min-h-0 flex-1 overflow-y-auto">
          {inspection.isLoading ? (
            <div className="flex h-full items-center justify-center text-sm text-muted-foreground">
              Loading artifact
            </div>
          ) : inspection.error ? (
            <ErrorBox error={inspection.error} />
          ) : data ? (
            <Tabs defaultValue="task" className="p-6">
              <TabsList className="grid h-auto w-full grid-cols-5">
                <TabsTrigger value="task">Task</TabsTrigger>
                <TabsTrigger value="output">
                  {data.artifact.pool === 'sft' ? 'SFT data' : 'RL task'}
                </TabsTrigger>
                <TabsTrigger value="validation">Validation</TabsTrigger>
                <TabsTrigger value="environment">Environment</TabsTrigger>
                <TabsTrigger value="provenance">Provenance</TabsTrigger>
              </TabsList>
              <TabsContent value="task" className="mt-5">
                <TaskView data={data} />
              </TabsContent>
              <TabsContent value="output" className="mt-5">
                <OutputView data={data} />
              </TabsContent>
              <TabsContent value="validation" className="mt-5">
                <ValidationView data={data} />
              </TabsContent>
              <TabsContent value="environment" className="mt-5">
                <EnvironmentView data={data} />
              </TabsContent>
              <TabsContent value="provenance" className="mt-5">
                <ProvenanceView data={data} />
              </TabsContent>
            </Tabs>
          ) : null}
        </div>

        <div className="border-t bg-background px-6 py-4">
          <div className="flex flex-col gap-3 md:flex-row md:items-center">
            <div className="flex min-w-0 flex-1 gap-2">
              <Input
                className="max-w-56"
                value={reviewer}
                onChange={(event) => setReviewer(event.target.value)}
                placeholder="Reviewer name"
                autoComplete="name"
              />
              <Input
                value={reason}
                onChange={(event) => setReason(event.target.value)}
                placeholder="Audit note (optional)"
              />
            </div>
            {data?.artifact.human_audit ? (
              <Badge
                variant={
                  data.artifact.human_audit.decision === 'approved' ? 'success' : 'destructive'
                }
              >
                {humanize(data.artifact.human_audit.decision)} by{' '}
                {data.artifact.human_audit.reviewer}
              </Badge>
            ) : null}
            <Button
              variant="destructive"
              disabled={!reviewer.trim() || audit.isPending}
              onClick={() => audit.mutate('rejected')}
            >
              Reject
            </Button>
            <Button
              disabled={!reviewer.trim() || audit.isPending}
              onClick={() => audit.mutate('approved')}
            >
              Approve
            </Button>
          </div>
          {audit.error ? <div className="mt-2"><ErrorBox error={audit.error} /></div> : null}
        </div>
      </DialogContent>
    </Dialog>
  );
}

function TaskView({ data }: { data: FoundryArtifactInspection }) {
  const task = data.task;
  if (!task) {
    return (
      <Section title="Task unavailable">
        <p className="text-sm">The durable cache contains the failed generation attempts below.</p>
      </Section>
    );
  }
  return (
    <div className="space-y-5">
      <Section title="Instruction">
        <p className="whitespace-pre-wrap text-base leading-7">{task.public_instruction}</p>
      </Section>
      <div className="grid gap-4 md:grid-cols-3">
        <Fact label="Route" value={task.route.toUpperCase()} />
        <Fact label="Difficulty" value={`${task.difficulty.estimated} / 5`} />
        <Fact label="Verifier" value={task.verifier_class} />
      </div>
      <Section title="Reasoning operations">
        <div className="flex flex-wrap gap-2">
          {task.reasoning_operations.map((operation) => (
            <Badge key={operation} variant="secondary">{humanize(operation)}</Badge>
          ))}
          {!task.reasoning_operations.length ? <span className="text-sm">None</span> : null}
        </div>
      </Section>
      <Section title={`Evidence (${data.public_context.spans.length} spans)`}>
        <div className="space-y-3">
          {data.public_context.spans.map((span, index) => (
            <div key={String(span.span_id ?? index)} className="rounded-md border p-4">
              <div className="mb-2 flex flex-wrap items-center gap-2 text-xs">
                <span className="font-mono font-medium">{String(span.span_id ?? `span-${index + 1}`)}</span>
                {span.section_role ? <Badge variant="outline">{humanize(String(span.section_role))}</Badge> : null}
              </div>
              <p className="whitespace-pre-wrap text-sm leading-6">{String(span.text ?? '')}</p>
            </div>
          ))}
        </div>
      </Section>
      <Section title="Hidden audit targets">
        <JsonBlock value={task.hidden_targets} />
      </Section>
      {task.ambiguity_risks.length ? (
        <Section title="Ambiguity review">
          <ul className="list-disc space-y-1 pl-5 text-sm">
            {task.ambiguity_risks.map((finding, index) => <li key={index}>{finding}</li>)}
          </ul>
        </Section>
      ) : null}
    </div>
  );
}

function OutputView({ data }: { data: FoundryArtifactInspection }) {
  return (
    <div className="space-y-5">
      {data.trajectories.length ? (
        <Section title={`${data.artifact.pool === 'sft' ? 'Training trajectories' : 'Reference trajectories'} (${data.trajectories.length})`}>
          <div className="space-y-4">
            {data.trajectories.map((trajectory, index) => (
              <div
                key={trajectory.trajectory_id}
                className={`rounded-lg border p-4 ${trajectory.trajectory_id === data.artifact.artifact_id ? 'border-primary' : ''}`}
              >
                <div className="mb-3 flex flex-wrap items-center gap-2">
                  <span className="font-medium">Solution {index + 1}</span>
                  <Badge variant={trajectory.accepted ? 'success' : 'destructive'}>
                    {trajectory.accepted ? 'Accepted' : 'Rejected'}
                  </Badge>
                  <Badge variant="outline">Reward {trajectory.reward.toFixed(2)}</Badge>
                  <span className="ml-auto font-mono text-xs text-muted-foreground">
                    {trajectory.trajectory_id}
                  </span>
                </div>
                <p className="whitespace-pre-wrap text-sm leading-6">{trajectory.answer.report}</p>
                <details className="mt-4 rounded-md border">
                  <summary className="cursor-pointer px-3 py-2 text-sm font-medium">Answer manifest</summary>
                  <div className="border-t p-3"><JsonBlock value={trajectory.answer.answer_manifest} /></div>
                </details>
                {trajectory.tool_calls.length ? (
                  <details className="mt-2 rounded-md border">
                    <summary className="cursor-pointer px-3 py-2 text-sm font-medium">
                      Tool calls ({trajectory.tool_calls.length})
                    </summary>
                    <div className="border-t p-3"><JsonBlock value={trajectory.tool_calls} /></div>
                  </details>
                ) : null}
                <details className="mt-2 rounded-md border">
                  <summary className="cursor-pointer px-3 py-2 text-sm font-medium">Exact trajectory JSON</summary>
                  <div className="border-t p-3"><JsonBlock value={trajectory} /></div>
                </details>
              </div>
            ))}
          </div>
        </Section>
      ) : null}

      <Section title={`Generation attempts (${data.generation_attempts.length})`}>
        <div className="space-y-3">
          {data.generation_attempts.map((attempt, index) => (
            <details key={`${attempt.call_key}-${index}`} className="rounded-md border" open={data.trajectories.length === 0 && index === 0}>
              <summary className="flex cursor-pointer list-none items-center gap-2 px-4 py-3">
                <Badge variant="outline">{humanize(attempt.stage)}</Badge>
                <span className="min-w-0 flex-1 truncate font-mono text-xs">{attempt.call_key}</span>
                <span className="text-xs text-muted-foreground">{formatTime(attempt.created_at)}</span>
              </summary>
              <div className="border-t p-4"><StructuredResponse value={attempt.response} /></div>
            </details>
          ))}
          {!data.generation_attempts.length ? <div className="text-sm">No model attempts recorded.</div> : null}
        </div>
      </Section>
    </div>
  );
}

function ValidationView({ data }: { data: FoundryArtifactInspection }) {
  const report = data.validation.report ?? data.artifact.validation;
  const gates = [
    ['Positive', report.positive_pass],
    ['Equivalent', report.equivalent_pass],
    ['Adversarial', report.adversarial_pass],
    ['Metamorphic', report.metamorphic_pass],
    ['Replay', report.replay_pass],
    ['Security', report.security_pass],
  ] as const;
  const cases = [
    ['Valid solutions', data.validation.valid],
    ['Equivalent solutions', data.validation.equivalent],
    ['Adversarial solutions', data.validation.adversarial],
    ['Mutations', data.validation.mutations],
    ['Metamorphic tests', data.validation.metamorphic],
  ] as const;
  return (
    <div className="space-y-5">
      <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-3">
        {gates.map(([label, passed]) => (
          <div key={label} className="flex items-center gap-3 rounded-lg border p-4">
            {passed ? <Check className="h-5 w-5 text-emerald-600" /> : <X className="h-5 w-5 text-destructive" />}
            <div><div className="font-medium">{label}</div><div className="text-xs">{passed ? 'Passed' : 'Failed'}</div></div>
          </div>
        ))}
      </div>
      <div className="grid gap-4 sm:grid-cols-3">
        <Fact label="Mutations killed" value={`${report.mutation_killed} / ${report.mutation_total}`} />
        <Fact label="False positives" value={formatInt(report.false_positive_count)} />
        <Fact label="False negatives" value={formatInt(report.false_negative_count)} />
      </div>
      <Section title="Validation findings"><JsonBlock value={report.details} /></Section>
      <Section title="Test cases">
        <div className="space-y-2">
          {cases.map(([label, values]) => (
            <details key={label} className="rounded-md border">
              <summary className="cursor-pointer px-4 py-3 text-sm font-medium">{label} ({values.length})</summary>
              {values.length ? <div className="border-t p-4"><JsonBlock value={values} /></div> : null}
            </details>
          ))}
        </div>
      </Section>
    </div>
  );
}

function EnvironmentView({ data }: { data: FoundryArtifactInspection }) {
  const predicates = arrayField(data.verifier, 'predicates');
  const tools = data.task?.public_context_policy.tool_access ?? [];
  return (
    <div className="space-y-5">
      <div className="grid gap-4 sm:grid-cols-3">
        <Fact label="Artifact" value={humanize(data.artifact.kind)} />
        <Fact label="Network" value="Disabled" />
        <Fact label="Files" value={formatInt(data.files.length)} />
      </div>
      <Section title="Public tools">
        <div className="flex flex-wrap gap-2">
          {tools.map((tool) => <Badge key={tool} variant="secondary">{humanize(tool)}</Badge>)}
          {!tools.length ? <span className="text-sm">No tools</span> : null}
        </div>
      </Section>
      {data.verifier ? (
        <Section title={`Verifier predicates (${predicates.length})`}>
          <div className="space-y-2">
            {predicates.map((predicate, index) => (
              <div key={String(asRecord(predicate)?.id ?? index)} className="rounded-md border p-3">
                <div className="flex flex-wrap items-center gap-2">
                  <ShieldCheck className="h-4 w-4" />
                  <span className="font-medium">{humanize(String(asRecord(predicate)?.type ?? 'predicate'))}</span>
                  <Badge variant="outline">weight {String(asRecord(predicate)?.weight ?? '-')}</Badge>
                  {asRecord(predicate)?.required === false ? <Badge variant="secondary">Optional</Badge> : null}
                </div>
                <div className="mt-2"><JsonBlock value={predicate} /></div>
              </div>
            ))}
          </div>
        </Section>
      ) : (
        <Section title="Verifier"><p className="text-sm">SFT artifacts use deterministic dataset gates and do not ship an RL reward verifier.</p></Section>
      )}
      {data.manifest ? <Section title="Environment manifest"><JsonBlock value={data.manifest} /></Section> : null}
      <Section title="Package files">
        {data.package_available ? (
          <div className="mb-4">
            <Button asChild variant="outline" size="sm">
              <a href={`/api/foundry/artifacts/${encodeURIComponent(data.artifact.artifact_id)}/package`}>
                <Download className="mr-2 h-4 w-4" /> Download package
              </a>
            </Button>
          </div>
        ) : null}
        {data.package_error ? <ErrorBox error={new Error(data.package_error)} /> : null}
        <div className="divide-y rounded-md border">
          {data.files.map((file) => (
            <div key={file.path} className="flex items-center gap-3 px-3 py-2 text-sm">
              <FileArchive className="h-4 w-4 text-muted-foreground" />
              <span className="min-w-0 flex-1 truncate font-mono text-xs">{file.path}</span>
              <Badge variant="outline">{humanize(file.category)}</Badge>
              <span className="w-20 text-right tabular-nums text-muted-foreground">{formatBytes(file.size)}</span>
            </div>
          ))}
          {!data.files.length ? <div className="p-4 text-sm">Rejected artifacts have no immutable package.</div> : null}
        </div>
      </Section>
    </div>
  );
}

function ProvenanceView({ data }: { data: FoundryArtifactInspection }) {
  return (
    <div className="space-y-5">
      <div className="grid gap-4 lg:grid-cols-2">
        <HashFact label="Paper hash" value={data.artifact.paper_hash} />
        <HashFact label="Environment hash" value={data.artifact.environment_hash} />
        <HashFact label="Package hash" value={data.artifact.package_hash} />
        <HashFact label="Signature" value={data.artifact.signature_backend ?? 'Not packaged'} />
      </div>
      <Section title={`Model calls (${data.provenance.length})`}>
        <div className="space-y-2">
          {data.provenance.map((trace, index) => (
            <div key={String(trace.trace_id ?? index)} className="grid gap-2 rounded-md border p-3 text-sm md:grid-cols-[1fr_1fr_auto_auto]">
              <div><div className="text-xs text-muted-foreground">Role</div><div>{humanize(String(trace.role ?? '-'))}</div></div>
              <div><div className="text-xs text-muted-foreground">Model</div><div className="truncate">{String(trace.returned_model ?? '-')}</div></div>
              <div><div className="text-xs text-muted-foreground">Tokens</div><div className="tabular-nums">{formatInt(numberField(trace, 'input_tokens'))} in · {formatInt(numberField(trace, 'output_tokens'))} out</div></div>
              <div><div className="text-xs text-muted-foreground">Latency</div><div className="tabular-nums">{formatInt(numberField(trace, 'latency_ms'))} ms</div></div>
            </div>
          ))}
          {!data.provenance.length ? <p className="text-sm">Provider traces remain attached to the rejected attempts in the output tab.</p> : null}
        </div>
      </Section>
      {data.artifact.human_audit_history.length ? (
        <Section title="Human audit history">
          <div className="space-y-2">
            {data.artifact.human_audit_history.map((audit) => (
              <div key={audit.audit_id} className="flex flex-wrap items-center gap-2 rounded-md border p-3 text-sm">
                <Badge variant={audit.decision === 'approved' ? 'success' : 'destructive'}>{humanize(audit.decision)}</Badge>
                <span className="font-medium">{audit.reviewer}</span>
                {audit.reason ? <span>{audit.reason}</span> : null}
                <span className="ml-auto text-xs text-muted-foreground">{formatTime(audit.created_at)}</span>
              </div>
            ))}
          </div>
        </Section>
      ) : null}
    </div>
  );
}

function StructuredResponse({ value }: { value: unknown }) {
  const record = asRecord(value);
  const report = typeof record?.report === 'string' ? record.report : null;
  const findings = Array.isArray(record?.findings) ? record.findings : [];
  return (
    <div className="space-y-3">
      {typeof record?.accepted === 'boolean' ? (
        <Badge variant={record.accepted ? 'success' : 'destructive'}>{record.accepted ? 'Accepted' : 'Rejected'}</Badge>
      ) : null}
      {report ? <p className="whitespace-pre-wrap text-sm leading-6">{report}</p> : null}
      {findings.length ? (
        <ul className="list-disc space-y-1 pl-5 text-sm">{findings.map((finding, index) => <li key={index}>{String(finding)}</li>)}</ul>
      ) : null}
      <details open={!report && !findings.length} className="rounded-md border">
        <summary className="cursor-pointer px-3 py-2 text-sm font-medium">Structured response</summary>
        <div className="border-t p-3"><JsonBlock value={value} /></div>
      </details>
    </div>
  );
}

function Section({ title, children }: { title: string; children: React.ReactNode }) {
  return <section className="rounded-lg border p-5"><h3 className="mb-4 font-semibold">{title}</h3>{children}</section>;
}

function Fact({ label, value }: { label: string; value: string }) {
  return <div className="rounded-lg border p-4"><div className="text-xs text-muted-foreground">{label}</div><div className="mt-1 break-words font-medium">{value}</div></div>;
}

function HashFact({ label, value }: { label: string; value: string }) {
  return <div className="rounded-lg border p-4"><div className="text-xs text-muted-foreground">{label}</div><div className="mt-1 break-all font-mono text-xs">{value}</div></div>;
}

function JsonBlock({ value }: { value: unknown }) {
  return <pre className="max-h-[32rem] overflow-auto whitespace-pre-wrap break-words rounded-md bg-muted/50 p-3 text-xs leading-5">{JSON.stringify(value, null, 2) ?? String(value)}</pre>;
}

function ErrorBox({ error }: { error: unknown }) {
  return <div className="m-4 rounded-lg border border-destructive/40 bg-destructive/5 p-3 text-sm text-destructive">{(error as Error).message}</div>;
}

function asRecord(value: unknown): Record<string, unknown> | null {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : null;
}

function arrayField(value: unknown, field: string): unknown[] {
  const candidate = asRecord(value)?.[field];
  return Array.isArray(candidate) ? candidate : [];
}

function numberField(value: Record<string, unknown>, field: string): number {
  return typeof value[field] === 'number' ? value[field] : 0;
}

function humanize(value: string): string {
  return value.replaceAll('_', ' ').toLowerCase().replace(/\b\w/g, (char) => char.toUpperCase());
}

function formatTime(value: string): string {
  return new Intl.DateTimeFormat(undefined, { month: 'short', day: '2-digit', hour: '2-digit', minute: '2-digit' }).format(new Date(value));
}

function formatBytes(value: number): string {
  if (value < 1024) return `${value} B`;
  if (value < 1024 * 1024) return `${(value / 1024).toFixed(1)} KiB`;
  return `${(value / 1024 / 1024).toFixed(1)} MiB`;
}
