# Pipeline remediation contract

Status: binding implementation contract, approved by the project owner on
2026-08-23.

This document governs the remediation of `fix/deployment-pipeline`. Work is
complete only when the implementation, tests, deployment, and normal UI agree
with this contract. Requirements must not be silently reduced because they are
expensive or inconvenient.

## 1. Per-item purpose-aware licence admission

Every content-bearing item receives an immutable licence decision before its
body is fetched. There are exactly three outcomes:

1. `pretrain_and_posttrain`: a permissive content licence allows the retained
   projection to enter pretraining and allows the paper to become a
   post-training candidate.
2. `posttrain_transform_only`: a reviewed grey-area licence or no stated item
   licence allows the item to ground derived SFT or RL artifacts, but forbids
   verbatim pretraining export.
3. `quarantined`: an explicit restrictive, contradictory, no-derivatives, or
   otherwise incompatible licence prevents body retrieval and processing.

The outcome is always based on the individual content item. A SourceFeed
default, dataset wrapper licence, repository topic, or venue must not silently
establish rights for all contained content. A versioned source-wide grant may
apply only to the exact projection it covers, such as a public Hugging Face
repository README.

Each source family must implement the metadata lookup appropriate to it:

- arXiv RSS and OAI-PMH are internal scheduling messages. The full-text worker
  resolves the individual paper licence before body retrieval.
- GitHub release discovery is internal. The tarball worker retains repository,
  exact ref, path, licence blob SHA, and any per-file SPDX provenance.
- Hugging Face model and dataset cards use an exact commit and either explicit
  README rights or the versioned public-repository terms. This never grants
  rights in weights, dataset rows, or linked artifacts.

Discovery envelopes create no licence or curation decision and do not appear
in Sources, Documents, acceptance, or quarantine statistics. The Sources and
Documents interfaces report only content-bearing item decisions and their
provenance.

## 2. Bytewax is the production stream engine

The production fetch and curation paths run as Bytewax dataflows, consistent
with the frozen Kappa architecture and course documentation. Restoring Bytewax
must retain:

- at-least-once processing;
- output-before-offset-commit behavior;
- deterministic replay from retained Redpanda data;
- bounded retries without silent record loss;
- coordinated rescaling from durable recovery state;
- isolated deployment canaries that never contaminate production tables;
- durable state and recovery for deduplication and source cursors.

Standard Kafka-lag KEDA is deliberately not attached to the two core Bytewax
executions because Bytewax recovery, not broker commits, owns their source
progress. KEDA remains required on independently committing ingest consumers
and stateless model services. Scaling a core Bytewax flow is a coordinated
stop-and-start operation over pre-created recovery partitions.

Starting a new consumer group at `latest`, committing failed records, or using
a deployment canary to skip production backlog is forbidden. A deviation from
Bytewax requires an explicit project-owner decision backed by a reproducible
test showing that Bytewax cannot provide a required guarantee.

## 3. Grounded source-aware curation

Classifier and heuristic choices must be researched per source family and
grounded in current public papers, official model cards, or established open
source training-data pipelines. The implementation document must record the
exact model/revision, licence, input projection, CPU runtime, output scale,
threshold rationale, and fallback behavior.

Distinct policies are required for the active content families:

- scientific papers and peer-reviewed full text;
- source code and repository documentation;
- peer reviews;
- model and dataset cards.

FinePDFs Edu v2 remains the scientific-quality default unless same-sample
evidence justifies a change. FineWeb-Edu must not score code or structured
metadata as if it were web prose. Every profile retains the shared privacy,
deduplication, decontamination, provenance, and audit contracts where those
operations are meaningful.

OAI-PMH metadata may discover and schedule an arXiv full-text item, but it must
not create a separate broken body-curation decision. Metadata-only records are
either handled by the explicit metadata policy or kept outside training-body
curation.

## 4. Ingest coverage and replay

All configured sources run through a dedicated, tested path:

- four arXiv RSS categories and current-frontier OAI-PMH schedule one canonical
  arXiv full-text artifact with bounded CPU PDF fallback;
- GitHub Releases Atom schedules exact-ref release tarball code and docs;
- Hugging Face Hub model and dataset cards retain exact-revision README prose;

Redundant discovery sources, sources without a useful corpus projection, blog
feeds without an audited reusable-content grant, and historical backfill-only
workloads are removed rather than shown as permanently failing sources.

After the explicitly approved 2026-08-25 backlog reset, every source starts at
the current live frontier. Rate limits and backpressure then delay work through
durable state rather than dropping new records.

## 5. Complete source cockpit

The Sources page is a unified operational inventory. It includes controller
managed SourceFeeds, Deployments, CronJobs, and downstream full-text workers.
For every source it shows:

- enabled and runtime state;
- last attempt, last success, and actionable error;
- fetched, admitted, posttrain-only, and quarantined item counts;
- resolved licence distribution and provenance summary;
- extraction and classifier policy;
- pipeline lag or queued work where applicable.

Quarantined content items remain inspectable in Documents. Discovery envelopes
do not. Dataset export applies the strict pretraining licence filter.

## 6. Configurable storage retention

Iceberg snapshot expiration, orphan cleanup, LSH compaction, and related
maintenance remain available because the current cluster is storage
constrained. Their retention windows and enablement are configuration, not
hard-coded product behavior. Defaults must be conservative, dry-run or guarded
where practical, observable, and documented. A larger deployment can retain
more history without code changes.

## 7. Principled post-training Foundry

The changes on `fix/deployment-pipeline` must be audited against
`docs/POSTTRAIN_FOUNDRY.md` and the public systems on which the design is based.
Reactive changes made only to pass deployment are retained only when they
preserve the intended SFT/RL semantics.

The live acceptance gate requires:

- the Foundry API and worker remain available after restart;
- eligible papers are processed automatically in daily rank order;
- SFT and RL artifacts contain inspectable tasks, inputs, outputs, validators,
  environments, and success/failure evidence;
- valid RL artifacts can pass automated validation;
- invalid artifacts fail with specific, auditable reasons;
- human approval or rejection is per artifact with a manually entered reviewer;
- the 20 percent benchmark split is assigned after accepted SFT/RL artifacts
  exist, independently for each pool;
- exact Hetzner model identity and provider responses are logged without
  committing credentials or output fixtures.

## 8. UI truthfulness

The normal UI is concise and self-explanatory, but every displayed state is
derived from real runtime data. Specifically:

- source licence labels use observed item decisions, not source defaults;
- route filters distinguish the primary route from additional eligible uses;
- quarantined documents are visible for audit;
- scientific artifact counts are backed by retrievable tables, equations,
  figures, OCR, and extraction records;
- post-training renders an explicit unavailable state instead of an empty or
  apparently healthy dashboard;
- synthetic benchmark canaries are labelled as canaries and never presented as
  comprehensive benchmark protection.

## 9. Validation and deployment order

Work may be bundled, but each bundle is validated at small scale before the
next one:

1. Contract and research record.
2. Licence adapters and source inventory.
3. Bytewax and ingest durability.
4. Source-aware extraction and curation.
5. Storage and core UI corrections.
6. Remote CI unit, integration, schema, image-build, and render validation.
7. Kubernetes deployment plus live end-to-end and browser validation across
   every available source and stage. No local container, model, pipeline, or
   end-to-end runtime is used for this remediation.
8. Foundry refinements and their separate remote acceptance gate only after
   the deployed core pipeline passes the preceding steps.

HTTP 200 responses and synthetic canaries are necessary but not sufficient.
Completion requires observable real records traversing ingestion, licence
admission, extraction, classification, curation, Iceberg, serving, and, for
selected candidates, the post-training Foundry.
