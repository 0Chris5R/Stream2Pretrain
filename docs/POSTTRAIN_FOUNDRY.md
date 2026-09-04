# Scientific Paper to SFT and RL Environment Foundry

The Foundry converts curated scientific papers into grounded SFT trajectories
and standalone verifier-backed RL environments. It generates data, not model
weights. Its upstream input is an eligible Gold record and durable scientific
evidence, not a second source fetch or extraction pipeline.

## 1. Integration contract

- All model-authored roles use Hetzner Experiments Inference and exact
  `Qwen3.8-27B`, with authenticated catalogue discovery and returned-model logs.
- The existing Gold record and scientific artifact form an immutable PaperBundle.
- The daily scheduler freezes all candidates received in the preceding 24 hours
  and processes them in rank order, without a fixed daily paper cap.
- The task designer receives the complete existing paper input. High
  classifier section scores add optional hints, not context replacement.
- Validation decisions apply to individual SFT trajectories and RL environments.
  A named human audit is a separate append-only record.
- Generated Prime Intellect packages use `verifiers==0.3.1` interfaces.
- Official-artifact oracles are optional, isolated and disabled in the current
  deployment. Recorded-provider replay is an explicit deterministic test mode.

## 2. End-to-end data flow

```mermaid
flowchart LR
    curated["Gold posttrain eligibility"] --> queue["Durable ranked daily snapshot"]
    queue --> bundle["Immutable PaperBundle"]
    bundle --> oracle["Optional isolated official oracles"]
    bundle --> graph["Six-pass evidence graph"]
    oracle --> graph
    graph --> tasks["Task proposal and answerability audit"]
    tasks --> solve["Independent-prompt solver trajectories"]
    solve --> verifier["Deterministic verifier compiler"]
    verifier --> attack["Adversarial and mutation validation"]
    attack --> split["Leakage-safe SFT and RL 80/20 pools"]
    split --> package["Signed SFT and RL packages"]
    package --> lake["MinIO plus Iceberg"]
    package --> bus["Redpanda foundry topics"]
    lake --> api["Foundry API"]
    bus --> metrics["Prometheus and Grafana"]
    api --> ui["Post-training cockpit"]
```

The foundry consumes `docs.curated` rather than polling arXiv itself. Every
upstream source can therefore participate if curation assigns
`posttrain_candidate`. Pre-fetch admission and pretraining curation are the
single licence authority; the foundry does not repeat or reinterpret it. The current
adapter requires a `ScientificDocument`, so the first production cohort is
scientific papers. Web and code-specific foundries require separate future
contracts rather than forcing those formats through paper assumptions.

## 3. Eligibility and capacity policy

A record enters the durable candidate queue only when all of these are true:

- `posttrain_candidate` is its primary route or one of its durable eligible
  routes;
- it has a structured scientific artifact in MinIO;
- the Gold and scientific document identities match;
- at least one training-retained stable body span exists.

These conditions rely on the upstream guarantee that only licence-admitted
Gold records reach `docs.curated`. There is no post-training licence stage,
licence hash, licence ledger, or second legal decision.

At `S2P_FOUNDRY_DAILY_RUN_HOUR_UTC` and
`S2P_FOUNDRY_DAILY_RUN_MINUTE_UTC`, a one-replica scheduler commits exactly one
cohort, including an empty cohort. It first drops unprocessed entries at or
before the prior 24-hour boundary, then freezes all queued candidates received
after that boundary and no later than the current boundary. Papers are ranked
by token-weighted mean `arxiv-posttrain-suitability` score divided by five.
Ties use reasoning, composite quality, recency and document ID. API cost and
context length are not ranking inputs. The structural fallback for records
without an active learned score is defined in `processor/foundry/worker.py`.

The production schedule is 08:30 UTC, corresponding to 10:30 Europe/Berlin
while daylight-saving time is active. The deployment is anchored by
`S2P_FOUNDRY_DAILY_NOT_BEFORE_UTC=2026-08-28T08:30:00Z`, so changing the run
hour cannot accidentally back-run the preceding day's cohort. The worker processes the entire frozen rank order serially until the next boundary. A provider-capacity stop
does not admit older papers at the next boundary: any unfinished cohort members
are removed, and only the newly completed 24-hour arrival window may compete.
New arrivals after the cutoff wait for the next cohort. An authenticated
control API remains available for operator diagnostics, but the monitoring UI
does not expose pipeline-control actions.

The control store resets interrupted `processing` rows to `queued` after
restart and resumes the same daily cutoff. A serialized background drain loop
rechecks independently of source arrivals. Its 60-second poll interval and the
00:00 UTC run-hour default are operational starting values marked
`needs-measurement`; neither is a throughput claim.

Candidate admission reads and validates the exact structured scientific JSON
referenced by Gold, then snapshots those bytes beside the Gold payload in the
durable queue. Provider work therefore does not depend on later MinIO reads of
an artifact that was already acknowledged at admission. Unavailable source evidence prevents candidate generation; transient object-store
failures remain retryable. Missing historical evidence does not become a
fabricated content-quality rejection or a reconstructed paper from flat text.

Every accepted paper-family package is assigned independently within its SFT
or RL pool. Four consecutive paper packages go to `train` and the fifth goes
to `benchmark`, giving an exact 80/20 allocation in each complete block of
five. The assignment is durable and retry-stable. Every task, trajectory,
verifier, and environment derived from the same paper and pool shares the same
split, preventing same-paper leakage between training and benchmark output.
This 20 percent split is held out from SFT/RL training. Under the requested
full-feed pretraining policy, its source paper may still be present in the
pretraining corpus, so it is a post-training holdout rather than a guaranteed
pretraining-unseen benchmark. Pretraining itself does not assign a benchmark
route or split.

The current defaults propose six task specifications and retain at most three
accepted tasks per paper. They are scheduling configuration, not claims about
expected acceptance. Actual yield is exposed in the UI and metrics.

## 4. Immutable contracts and provenance

`schemas/foundry.py` defines strict frozen Pydantic contracts and checked-in JSON
Schemas for:

- `PaperBundle`, stable spans, equations, tables, figures, official artifacts,
  oracle recipes, and oracle results;
- evidence nodes, edges, graph versions, compiler runs, and graph criticism;
- task specifications, public context policy, hidden targets, task family,
  difficulty, and route;
- verifier predicates and normalized verifier specifications;
- answers, manifests, tool calls, turns, full trajectories, and loss masking;
- provider model snapshots, traces, and quota states;
- validation reports, append-only foundry events, artifacts, and signed package
  manifests.

The complete bundle is persisted unchanged. Model prompts use a deterministic
projection that retains every training span and scientific object, selects
LaTeX over duplicate MathML when both encode the same equation, selects table
rows over duplicate cell records, and removes duplicate caption and binary
asset-address fields. This preserves the paper's reasoning evidence while
keeping unusually equation-heavy papers inside the model context window.

Every provider trace preserves the provider, credential label, role, base URL,
requested model, returned model, upstream route when exposed, request ID when
exposed, exact model family and licence, prompt version, request and response
hashes, usage, latency, time to first token, output rate, sampling parameters,
terms snapshot hash, and timestamps.

The Hetzner provider-terms snapshot in `docs/provider-terms/` is hashed into
every live trace. It is audit evidence, not a legal opinion. Source-paper,
official-artifact, model-weight, service-terms, and output-distribution rights
remain separate decisions.

## 5. Provider discovery and role routing

At startup the worker authenticates to Hetzner's `/models` endpoint and stores
the returned catalogue. The configured, licence-recorded model must be present.
There is no provider qualification benchmark, score threshold, availability
heartbeat, maximum-gap rule, or provider approval step. Normal request errors
use bounded retries and durable checkpoints. Every call records the requested
and returned model, provider, route, usage, latency, and terms snapshot.

Role routing is deliberately uniform:

| Work | Route |
| --- | --- |
| Every model-authored compiler, designer, solver, critic, repair, and adversarial role | Hetzner `Qwen3.8-27B` |
| Deterministic normalization, security, replay, packaging | Local CPU code |

Roles retain separate prompts, call keys, outputs, and traces. This provides
procedural cross-checking but does not claim model-family independence.
Deterministic checks override model agreement wherever an exact check exists.

## 6. Quotas, retries, and resumability

The SQLite WAL control plane is the single-writer stateful core locally and in
the one-replica Kubernetes StatefulSet. It stores jobs, events, provider
results, partial streams, traces, catalogues, artifacts, append-only human
artifact audits, and the candidate queue.

Quota reservations cover Hetzner's published per-key 60-second limits: 10
requests, 4,000,000 input tokens, and 100,000 output tokens. No daily provider
cap or artificial reserve is configured because neither is published.

Each call follows `CALL_PLANNED`, `QUOTA_RESERVED`, `CALL_STARTED`, streamed
checkpoints, terminal call state, and `QUOTA_RECONCILED`. Capacity is reserved
before transmission and reconciled with actual provider usage. A completed
call is cached by job, stable call key, prompt version, and full request hash.
Every possible transport attempt is reserved up front. Successful calls
reconcile their exact attempt count and reported usage; failed calls or
missing usage reconcile conservatively at the reserved maximum.
Transport retry keeps the same route and semantic request. Provider errors and
minute-window exhaustion leave the current cohort resumable while it is still
current. If provider capacity prevents completion before the next cohort
boundary, unfinished papers expire with the old cohort rather than competing
against newly arrived research. Semantic failures produce explicit rejection
states.

On worker restart, abandoned quota reservations are charged conservatively.
Every prior `CALL_PLANNED` or `CALL_STARTED` event without a matching terminal
call event is then closed with an auditable restart-recovery `CALL_FAILED`
event before the queue resumes. Cached complete provider results remain
idempotent, while an actually interrupted request receives a new call attempt;
the UI no longer presents the prior process's call as live indefinitely.

The API client performs real SSE streaming and saves reconstructable partial
text hashes. It rejects invalid JSON, an unapproved exact returned route, a
missing exact model licence record, and authenticated catalogue drift.

## 7. Evidence graph compiler

The compiler uses bounded passes rather than one unconstrained generation:

1. extract claims and contributions tied to stable body spans;
2. extract methods, algorithms, assumptions, inputs, and outputs;
3. extract equations, quantities, tables, figures, results, and dependencies;
4. extract limitations, qualifications, contradictions, and negative evidence;
5. canonicalize equations, units, identifiers, methods, table values, and
   accepted equivalence classes;
6. identify caveats, changing definitions, conflicts, negative evidence, and
   unresolved ambiguity.

Each pass is a prioritized incremental delta of at most 24 nodes and 40 edges.
This keeps structured JSON inside the output window on unusually dense papers
without collapsing the six distinct extraction objectives into one prompt.

Qwen3.8 critiques the merged graph under a dedicated critic prompt. A bounded
repair pass may add, replace, or remove nodes and edges before a second critic
pass. References to nonexistent spans or objects, dangling edges, invalid
paper identity, and unsupported graph regions fail before task generation.
Official oracle results remain construction-private and never leak into public
task context.

## 8. Task and trajectory construction

The implemented task families are:

- grounded technical explanation;
- claim and evidence reconstruction;
- derivation completion;
- method-DAG reconstruction;
- figure and table reasoning;
- corruption diagnosis;
- assumption and limitation consequence analysis;
- long single-paper research with frozen tools;
- official-artifact experiment configuration;
- official-artifact result reproduction.

Task proposal uses graph evidence and stable IDs. Content policy
`scientific-reasoning-v2` prioritizes, in order, multi-step derivations,
scaling-law or regime inference, multi-result numerical synthesis, assumption
consequences, and algorithm analysis. Derivation and assumption families are
capped at two tasks each; every other family is capped at one. Within a family,
difficulty, distinct reasoning operations, answer-facing values, relations,
and graph nodes determine priority. Corruption diagnosis is last. The selector
does not backfill unused slots with shallow tasks.

An independent answerability audit rejects missing targets, unresolved public
context, oracle-only answers, external knowledge, answer leakage, ambiguous
outcomes, and tasks that cannot be checked from the packaged environment. Both
proposal and answerability prompts explicitly reject one-value lookup, one
arithmetic operation, internal-ID listing, simple edge reversal, and
manifest-format compliance. RL requires difficulty 4 or 5, at least two linked
reasoning operations, and an answer-facing numeric, symbolic, or discrete
outcome. Valuable open-ended synthesis remains SFT. A public task never becomes
hard merely by demanding more schema fields.

Each selected task receives bounded multi-turn solutions from separate Qwen3.8
solver calls and prompts. The agents can request only frozen paper-local tools. Tool requests
are executed before the final answer and all turns are retained. Tool result
turns are explicitly loss-masked for SFT. The final answer separates readable
report text from an exact machine-checkable manifest of claims, evidence,
equations, numeric results, qualifications, and tool use.

The solver receives only the public instruction, public paper context, allowed
node types, and output identifiers. Hidden targets and canonical answers never
enter an SFT prompt. Its prompt says that the readable report is the scientific
answer and must show intermediate reasoning, calculations, assumptions, and
the final requested values or expressions. It says that the manifest is
machine-readable provenance, not a substitute for reasoning. Derivation tasks
ask for an ordinary step-by-step mathematical argument and final expression;
they do not require the learner to return evidence-span citations or internal
IDs. The hidden evidence links remain provenance for construction and audit.

The grounding critic returns exactly one decision per trajectory. It may reject
only a concrete unsupported claim, contradiction, wrong calculation, or
materially incomplete scientific conclusion. Manifest shape, node IDs,
relation labels, ordering, and configuration keys are owned by executable
checks. A proven format-only negative vote does not discard a scientifically
sound trajectory; an empty or scientifically substantive negative vote still
fails closed. Every trajectory then runs the complete deterministic suite
independently. A good SFT trajectory is retained even if its paired solution
fails, and accepted packages contain only accepted trajectories. Rejected
siblings remain separate inspectable artifact records with their trajectory ID,
critic decision, validation report, and provider-trace links.

The exact versioned prompt text is generated by `_designer_system`,
`_answerability_system`, `_solver_system`, and `_grounding_system` in
`processor/foundry/tasking.py`; each function appends the live strict JSON
Schema to the literal policy above. Prompt version
`paper-foundry-prompts-v4` invalidates the prior provider-call cache, so no v3
response can be reused under this contract.

## 9. Frozen tools and verifier construction

Packaged tools have no network access and operate only on bundled public data:

- lexical search over stable spans;
- exact span opening;
- literal find;
- an allowlisted AST calculator;
- restricted symbolic simplify, expand, factor, solve, and equivalent checks.

Tool count, result size, and expression structure are bounded. Imports, file
access, attribute traversal, arbitrary Python execution, and shell execution
are rejected. Malformed tool arguments are returned as tool errors, and a
repeated identical invalid request or eight tool-request turns rejects that
solver trajectory rather than pinning the paper or daily queue.

For `scientific-reasoning-v2`, the authoritative `VerifierSpec` is compiled
locally from the reviewed `TaskSpec` hidden targets. Qwen3.8 performs an
audit-only critique and cannot add qualifications, targets, predicate types, or
schema fields. The verifier can only enforce requirements in the reviewed task.
The v1 package reader retains its bounded model-rubric contract for inspection
of persisted artifacts. Predicate types cover required nodes, evidence
membership and coverage, exact equations, numeric tolerances, required
qualifications, contradiction flags, report-manifest consistency, and tool
budget. The standalone verifier is copied into every package and imports
neither network clients nor model SDKs.

A derivation enters the RL route only when its target is a bounded numeric value
or a canonical LaTeX expression that the packaged symbolic runtime can parse.
Under v2 it must also bind at least two equation nodes through a real equation
dependency. The verifier checks submitted target-specific intermediate and
final equations for symbolic equivalence, checks required derivation order,
and requires the readable report itself to expose the checked final expression.
Numeric and required configuration outcomes must likewise appear in the
readable report, not only in the hidden manifest. A report that merely lists
internal IDs or says the answer is in the manifest fails. The packaged
standalone reward implements the same checks as the construction-time suite and
is regression-tested against the signed archive. A
valuable derivation without such a deterministic oracle remains eligible for
SFT but cannot be mislabeled as a machine-verifiable RL environment.

## 10. Acceptance suite

No SFT or RL artifact is accepted from model consensus alone. The suite runs
separately for each generated trajectory:

- positive tests against independent trajectories;
- equivalent-answer tests;
- malformed, unsupported, evidence-swapped, and contradiction adversaries;
- targeted verifier mutations with mutation-kill accounting;
- metamorphic tests for order and equivalent representation changes;
- deterministic replay;
- security tests for tool budget and code-execution attempts.

The package includes the actual test-case and mutation JSONL, not a summary
placeholder. V2 mutations also remove answer-facing scientific results from
the readable report while leaving the manifest intact. Validation reports
retain false-positive and false-negative counts, every hard gate, mutation
totals, per-trajectory critic decisions, and details. A failed gate rejects
that trajectory rather than weakening the verifier or discarding a sound
sibling. An RL environment is accepted when at least one independently
grounded reference trajectory passes every gate; only those passing references
enter its package.

Task, prompt, verifier and policy revisions are retained in every package so
outputs from different configurations remain distinguishable.

## 11. Official-artifact oracles

Oracle execution is disabled in both the default Podman and Kubernetes
configuration. It is later optional work and does not block the current
pipeline or acceptance gate.

An operator can add an audited JSON manifest under
`s3://posttrain/oracle-manifests/<paper-family>.json`. Each item records its
licence, source, content hash, optional asset locations, and optional
`OracleRecipe`.

`s2p-foundry-build-oracle` produces this manifest from a local audited build
context. It rejects symlinked artifact trees, hashes every embedded file, builds
with Podman network disabled and pulls disabled, requires explicit build and
runtime resource limits, resolves the final image content digest, and binds the
embedded artifact hash to the recipe. Cloud promotion still requires publishing
that exact digest through the team's image registry; the helper never pushes.

Local recipes run as Podman containers with no network, a read-only root,
dropped capabilities, no-new-privileges, PID/CPU/RAM/time limits, a bounded
tmpfs, and a digest-pinned image. Cloud recipes become one-shot Kubernetes
Jobs with the same limits, no service-account token, optional sandbox runtime
class, zero retry, active deadline, and a selector-specific deny-all
NetworkPolicy. The foundry service account has only create/get/delete Job and
read Pod/log permissions in its namespace.

An oracle emits JSON on stdout. The result and stdout hashes enter private
provenance. Official code, data, checkpoints, generated output, and paper text
retain separate licence snapshots. Absence of an official artifact is normal
and does not force a proxy oracle.

## 12. Package format and signatures

Accepted artifacts are deterministic gzip/tar archives in MinIO. Timestamps,
member order, permissions, and gzip metadata are normalized before hashing.
Each archive is stored under
`s3://posttrain/<sft|rl>/<train|benchmark>/revisions/<content-policy>/environments/...`.
The revision segment is mandatory, and the signed manifest repeats the same
revision. Each archive
contains:

```text
paper_environment/
  manifest.json
  prompt.json
  public_context/
    paper.txt
    span_index.json
    equations.json
    figures/
    tables/
  public_tools/
    runtime.py
    search.py
    open.py
    find.py
    calculator.py
    symbolic.py
  hidden/
    evidence_graph.json
    reference_state.json
    accepted_equivalences.json
    tolerances.json
    verifier_spec.json
    verifier.py
    oracle_results.json
  validation/
    valid_solutions.jsonl
    equivalent_solutions.jsonl
    adversarial_solutions.jsonl
    mutations.jsonl
    metamorphic_tests.jsonl
    replay_report.json
  trajectories/
    accepted.jsonl
    rejected.jsonl
  legal/
    paper_license.json
    artifact_licenses.json
    model_provider_audit.json
  prime_verifiers/
    pyproject.toml
    paper_foundry/
      __init__.py
      taskset.py
  lock/
    requirements.lock
```

The Prime Verifiers package exposes real paper-local tools, a `Taskset`, and a
deterministic reward backed by the frozen verifier. Dependencies are pinned.
The package hash is signed with Ed25519 and the detached signature and
certificate are stored beside the archive. Production reuses the externally
managed signing key. Local development can generate an ephemeral signer for
mechanical tests, which is never a production identity.

## 13. Storage, API, UI, and observability

Redpanda adds `foundry.jobs`, `foundry.events`, and `foundry.artifacts`.
MinIO adds the `posttrain` bucket. Iceberg adds append-only event and artifact
tables. The local SQL catalogue and cloud Polaris catalogue use the same record
contracts.

The foundry API exposes dashboard, jobs, job detail, artifacts, quotas, model
catalogues, health, and Prometheus metrics plus authenticated manual-run,
artifact-inspection, immutable-package download, and per-artifact audit
endpoints. The main dashboard carries
a compact foundry summary, and the Next.js `/post-training` page shows accepted SFT and RL counts,
acceptance, papers and queue, stage flow, provider capacity, catalogue changes,
accepted task mix, daily-run status, per-pool train/benchmark allocation, jobs,
artifacts, human audit status, validation, traces, and timeline. `Inspect`
opens the actual task, public evidence, hidden audit targets, exact accepted SFT
trajectories or RL verifier, failed generation attempts, validation cases,
package inventory, hashes, and provider traces before the reviewer can approve
or reject it. Accepted package content comes directly from the immutable MinIO
archive. Rejected attempts are reconstructed from the durable bundle, graph,
and structured provider-result cache. The inspector never exposes provider
credentials or private oracle results.

Prometheus and Grafana cover stage outcomes, artifact families, provider roles,
usage, call failures, latency, time to first token, output rate, quota
remaining, validation gates, mutation kill rate, security failures, queue
depth, and human audit decisions. Alerts fire for worker unavailability and
provider rate limiting.

## 14. Kubernetes deployment

The chart adds a one-replica foundry StatefulSet with worker and control API
sidecar, a ReadWriteOnce control-plane PVC, ClusterIP service, provider Secret,
signing-key mount, exact provider egress class, ServiceMonitor, PrometheusRule,
Grafana panels, oracle RBAC, and oracle deny-all network policy.

The StatefulSet is intentionally single-writer until a distributed control
store is designed and measured. Provider work is naturally expensive and is
serialized against shared quotas. Horizontal generation can later shard by paper family only
after idempotency and quota ownership move to a shared transactional backend.
The current CPU and memory requests and the 5 GiB control PVC are
`needs-measurement` for sustained cloud operation.

Required externally managed Secrets are:

- `stream2pretrain-foundry-providers` with
  `HETZNER_INFERENCE_API_KEY` and a random `controlToken`;
- `stream2pretrain-foundry-signing` with `ed25519.key` and `ed25519.crt`,
  created once by deployment bootstrap unless it was pre-provisioned;
- the existing MinIO and Polaris Secrets.

The worker starts after `Qwen3.8-27B` is visible through authenticated model
discovery. No separate provider test or approval ceremony is required.

## 15. Podman local workflow

The base pipeline, foundry API, Post-training UI, and metrics can start without
model credentials. The model worker starts after the Hetzner key is set and
the configured model can be discovered.

```bash
./scripts/foundry_local.sh base
```

Set credentials locally without committing them:

```bash
export HETZNER_INFERENCE_API_KEY='...'
```

```bash
./scripts/foundry_local.sh worker
./scripts/foundry_local.sh status
./scripts/foundry_local.sh logs
```

Open `http://localhost:3100/post-training` to monitor the scheduled queue and
inspect artifacts. Generated artifacts can be approved or rejected in the UI
by entering the current reviewer name and an optional reason. Every action is
retained. The script does not delete images,
volumes, lakehouse data, provider traces, or credentials. `down` stops only the
foundry worker and API while preserving state.

Local official-oracle execution is a host-side Podman test because mounting the
Podman control socket inside the worker would expand its privilege boundary.
Cloud execution uses the Kubernetes Job runner. Both use the same recipe and
result contracts.

After one live paper job completes, export its real structured responses for
offline deterministic regression:

```bash
./scripts/foundry_local.sh export-replay JOB_ID
```

Replay output is private test data and must be reviewed before committing.

## 16. Acceptance and rename gate

Implementation tests currently cover contracts, exact licensed route handling,
SSE checkpoints, call idempotency, published quota enforcement, deterministic validation,
mutation tests, package reproducibility, signature verification, Prime tool
export, frozen-tool security, and Kubernetes oracle hardening. Python type,
lint, schema, Helm, Compose, UI lint, UI type, and production UI build checks
are part of the handoff.

The foundry is not operationally accepted until all of these are evidenced:

- the authenticated Hetzner catalogue is stored and contains `Qwen3.8-27B`;
- every live model trace records the exact Hetzner route and model;
- at least one SFT and one RL package pass every deterministic gate;
- package signatures verify after MinIO download;
- one interrupted call and one interrupted paper resume without duplicate
  billing or artifacts;
- the Post-training UI and Grafana panels match durable records;
- artifact approval and rejection both preserve reviewer identity and history;
- actual provider usage, acceptance yield, local resource use, and cloud
  resource use are measured rather than estimated.

Only after this live acceptance should the team consider renaming the project
to Stream2Train.

## 17. Credential and decision checklist

To begin the live test, the project owner must provide or choose:

1. a Hetzner Experiments Inference token for a project account;
2. confirmation that `Qwen3.8-27B` appears in the authenticated catalogue, or
   approval for a separately licensed replacement route;
3. the production Ed25519 signing Secret for cloud testing;
4. optional audited official-artifact manifests and digest-pinned oracle images.

Reviewer names are entered manually in the Post-training UI for each artifact
audit and are never configured as deployment settings.

No personal Z.AI or GLM credential is accepted. The configuration fails if
`ZAI_API_KEY` or `GLM_API_KEY` is present.
