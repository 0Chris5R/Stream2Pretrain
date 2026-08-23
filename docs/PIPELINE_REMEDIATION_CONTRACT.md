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
2. `posttrain_transform_only`: a reviewed grey-area licence permits the item to
   ground derived SFT or RL artifacts, but forbids verbatim pretraining export.
3. `quarantined`: a restrictive, contradictory, or unresolved licence prevents
   body retrieval and all downstream processing.

The outcome is always based on the individual content item. A SourceFeed
default, dataset wrapper licence, repository topic, venue, or hosting platform
must not silently establish rights for all contained content.

Each source family must implement the metadata lookup appropriate to it:

- arXiv RSS, OAI-PMH, HTML, and Hugging Face Daily Papers resolve the licence
  of the individual arXiv paper before full-text retrieval.
- GitHub events, releases, and tarball files resolve the repository/release
  licence and retain repository, ref, path, and licence provenance.
- Hugging Face model, dataset, and Space records use the individual card/API
  metadata at an immutable revision; a dataset wrapper licence does not
  licence referenced external content.
- OpenReview live and backfill records resolve paper and review licences at the
  individual note or artifact level.
- RSS, sitemap, and lab-blog pages use item/page metadata or an explicitly
  audited source-wide content licence. An unknown default does not permit a
  body fetch.

The Sources and Documents interfaces report observed item-level decisions and
their provenance. They must not infer a fixed training policy from a feed
default.

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

At minimum, distinct policies are required for:

- scientific papers and peer-reviewed full text;
- general web and lab-blog prose;
- source code and repository documentation;
- peer reviews;
- model, dataset, and release metadata.

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

All configured Phase-1 sources must run through a dedicated, tested path:

- arXiv OAI-PMH and four arXiv RSS categories;
- arXiv full-text HTML with bounded PDF fallback;
- GitHub Events, Releases, and release tarball code;
- Hugging Face Hub model, dataset, and Space cards plus Daily Papers;
- configured AI-lab blogs;
- OpenReview live papers, public review threads, and the configured backfill;
- the peS2o, RedPajama arXiv, FineWeb-Edu, Stack-Edu, and Wayback seed
  components, each under the same per-item admission contract as live ingest.

Arbitrary page caps, new-group `latest` offsets, and other changes that can
silently reduce coverage are removed. Rate limits and backpressure remain, but
they delay work through durable state rather than dropping it.

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

Quarantined items remain inspectable in Documents. Dataset export alone applies
the strict pretraining licence filter.

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
