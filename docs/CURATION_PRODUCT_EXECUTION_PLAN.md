# Stream2Pretrain Curation Product Execution Plan

Status: locked implementation contract
Created: 2026-08-15
Scope owner: project team

This file is the durable source of truth for the curation-policy and product-UI
work agreed after the first faithful local pilot. It is not a suggestion list.
An item may be changed, deferred, or removed only through an explicit team
decision recorded in this file. Engineering effort alone is not a reason to
silently simplify an item, replace it with a proxy, or omit it.

N3 shadow-mode mixture training remains the only explicitly deferred feature.
Everything else below is part of the current implementation and validation
scope.

Execution snapshot (2026-08-15): the source-aware section pipeline, strict CPU
models, scientific artifacts/OCR policy, durable routing, SourceFeed controls,
benchmark isolation and signing, filtered review UI, dataset exports,
Kubernetes reconciliation, schemas, and local nine-document replay are
implemented and validated. The repository also contains the 37-paper reviewed
label protocol and strict five-benchmark reserve builder. Completing human
labels requires team reviewers; producing the real GPQA-inclusive reserve
requires the team's authorised Hugging Face token; measuring remote capacity
requires the DHBW cluster. These are evidence, credential, and target-environment
steps, not permission to substitute synthetic labels, partial benchmark
coverage, or invented capacity numbers.

Post-training amendment (2026-08-19):
[`POSTTRAIN_FOUNDRY.md`](POSTTRAIN_FOUNDRY.md) is the binding contract for the
implemented scientific-paper to SFT and RL environment foundry. It extends this
curation contract without weakening any curation requirement. The durable
eligibility route is now `posttrain_candidate`; `reasoning_candidate` is a
legacy read alias only. The foundry consumes structured, licence-cleared
scientific artifacts after curation, uses exactly two approved provider routes,
discovers their configured models at worker startup, and records exact returned
model provenance on every call. Provider qualification benchmarks, score
thresholds, availability heartbeats, and provider approvals were explicitly
removed by the team on 2026-08-19. Human approve/reject decisions instead apply
to individual generated SFT/RL artifacts and record the reviewer manually. The
same decision removed copy-ratio, shared-word, and minimum-answer-length checks
entirely. Optional official-artifact oracles remain disabled future work. The
current name remains Stream2Pretrain until the live acceptance and rename gate
in that document is complete.

Deployment-policy amendment (2026-08-19): every licence-admitted clean training
document is eligible for pretraining through `eligible_routes`,
independent of its primary inspection route. A document with a missing or
excluded licence is durably quarantined before its body is fetched or any
extraction, OCR, model, or curation stage runs. Each decision is first written
as a pre-fetch event and exposed as part of the same corpus route ledger.
Dataset wrapper licences are not inherited by contained documents.
Once per UTC day, the foundry freezes and ranks the
current post-training candidates, runs them serially until measured provider
quota is reached, and leaves the remainder durable for the next ranking.
Accepted paper-family packages are assigned separately within the SFT and RL
pools in retry-stable blocks of five: four train and one benchmark. A paper can
never contribute to train and benchmark within the same pool. The default
pretraining export enforces the same strict allowlist and includes exact
licence provenance; all post-training derivatives use that gate again as a
defensive check. The 20 percent
post-training benchmark split is an SFT/RL holdout. Pretraining never creates a
benchmark split; the 80/20 allocation occurs only after accepted SFT/RL output
exists. The foundry trusts upstream licence admission and performs no second
licence check, hash, or ledger operation.

## 1. Product principles

1. The default UI is a curation product, not an implementation report.
2. Pages must be self-explanatory through layout, labels, controls, and state.
   Do not add long explanatory subtitles, repeated implementation caveats, or
   paragraph-length descriptions below controls.
3. Technical provenance remains available, but under compact expandable audit
   details rather than dominating the normal workflow.
4. Model limitations, calibration notes, and experimental interpretation belong
   in repository documentation and evaluation reports, not as permanent warning
   prose in the primary UI.
5. Every displayed score has one meaning. Model outputs, deterministic
   heuristics, and composite policy scores must never be visually conflated.
6. Every route and category can be filtered, inspected, and exported.
7. Test fixtures are hidden by default and can be enabled deliberately.
8. No proxy classifier is permitted in the faithful local or Kubernetes
   profiles. Missing required artifacts fail startup.
9. A passing pipeline test is not evidence that score thresholds are correct.
   Scientific validity requires a labelled evaluation set.
10. Preserve source structure and source assets even when a derived training
    projection excludes them.

## 2. Source scope and policy shape

Stream2Pretrain is not arXiv-only.

Current and planned source formats already represented in the repository are:

- native arXiv HTML and ar5iv fallback;
- arXiv/OpenReview PDF and review material;
- curated AI-lab and project RSS/Atom feeds;
- Hugging Face model and daily-paper feeds;
- curated GitHub releases/events and release-tarball code;
- historical seed sources: peS2o, RedPajama arXiv, FineWeb-Edu slices,
  Stack-Edu, and selected Wayback content.

Therefore the pipeline keeps a source-aware policy:

- scientific HTML/PDF uses structure-first extraction and section-level
  scoring;
- web/blog content uses stronger web-quality and boilerplate checks;
- code uses language/license/path/content-specific checks;
- reviews and metadata never inherit scientific-paper assumptions blindly;
- all formats share privacy, deduplication, provenance, validity, benchmark
  decontamination, and durable routing contracts.

The UI must make source format and policy profile filterable without presenting
different products for every source.

## 3. Scoring and routing corrections

### 3.1 Scientific section sampling

Replace the current global priority sort with deterministic role-stratified
sampling. The sampler must:

- operate only on sections retained for training;
- reserve coverage for abstract, introduction/background, methods,
  results/discussion, conclusion/limitations, and other/appendix when present;
- fill unused capacity with the most informative remaining sections;
- remain bounded by configuration;
- expose sampled section ids in the durable decision record;
- avoid letting many methods/results subsections crowd out every other role;
- test short papers, very long papers, missing-role papers, and ties.

### 3.2 FinePDFs Edu v2

`HuggingFaceFW/finepdfs_edu_classifier_v2_eng_Latn` is the target scientific
educational-quality classifier.

Implementation requirements:

- download and pin an immutable revision and hashes;
- provide real CPU inference in the faithful profile;
- prefer a verified ONNX/OpenVINO CPU artifact when conversion is correct;
- preserve the raw 0..5 score and exact model/backend revision;
- keep the current FineWeb-Edu checkpoint temporarily for a controlled A/B
  comparison on the same labelled sections;
- select the production scientific classifier from measured results, with
  FinePDFs Edu v2 as the expected default;
- do not show both models as equal primary scores after selection;
- retain source-aware support for a web-specific classifier on non-scientific
  web/blog sources.

### 3.3 Score semantics

Create one canonical scoring and routing document containing:

- every input signal and its range;
- aggregation and sampling rules;
- structural completeness formula;
- reasoning-evidence formula;
- benchmark-evidence formula;
- composite formula;
- every threshold and route precedence rule;
- risk-tier mapping;
- content-tag rules or model revisions;
- examples calculated from real pilot records;
- versioning and calibration procedure.

Until trained/validated alternatives exist, rename UI-facing heuristic concepts:

- `reasoning_score` -> Reasoning evidence;
- `benchmark_score` -> Benchmark evidence;
- risk tier remains audit metadata, not a headline product label.

The durable schema may retain backwards-compatible field names while the UI and
documentation use correct terminology.

### 3.4 Content taxonomy

The multi-label taxonomy is:

- mathematical reasoning;
- empirical evidence;
- methods and procedures;
- benchmark or dataset;
- survey synthesis;
- systems implementation;
- visual evidence;
- general scientific.

Requirements:

- show every assigned tag in document detail;
- never truncate tags silently;
- filter the document collection by one or more tags;
- document whether each tag is rule-derived or model-derived;
- preserve multi-label behavior;
- evaluate whether a lightweight trained section/tag classifier materially
  improves the current deterministic baseline before replacing it.

The current tags are evidence/usefulness labels, not research-topic labels.
They must become better grounded through a reviewed paper/section label set,
per-tag precision/recall reporting, explicit rule provenance, and a measured
comparison between the deterministic baseline and a lightweight trained
multi-label classifier. A trained replacement may be adopted only when it
improves the reviewed holdout set and preserves auditable per-tag evidence.

### 3.4.1 Research-topic classification

Add a separate hierarchical, multi-label `research_topics` taxonomy. It must
not be conflated with educational quality, routing, or the evidence/usefulness
tags above. The initial taxonomy must cover at least:

- model architecture and representation learning;
- optimization and training dynamics;
- data, pretraining, and curation;
- evaluation and benchmarking;
- inference efficiency and systems;
- interpretability and mechanistic analysis;
- robustness, safety, and alignment;
- theory and learning foundations;
- agents, planning, and tool use;
- retrieval and knowledge systems;
- multimodal learning and generation;
- domain applications.

Before freezing the taxonomy, audit recent AI/ML papers for missing or
overlapping classes and document the hierarchy and multi-label rules. Establish
a deterministic/embedding baseline using arXiv metadata and scientific-text
embeddings, then compare it with a small CPU-capable classifier trained on
reviewed labels. Candidate backbones include SPECTER2/SciBERT-style scientific
embeddings with nearest-neighbour or linear classification. Record macro/micro
F1, per-class support, calibration, and abstention behavior. Do not surface the
topic labels as reliable product metadata until that evaluation is complete.

### 3.5 Scientific validation set

Build a checked-in manifest and reproducible evaluation workflow for 30-60
diverse papers. It must cover at least:

- theory/proof;
- empirical systems/ML;
- survey/review;
- short paper/note;
- dataset/benchmark paper;
- biomedical or other specialized scientific prose;
- native HTML;
- clean PDF;
- scanned/degraded PDF;
- malformed or incomplete extraction;
- papers with tables, equations, figures, and references.

Labels are recorded at paper and section level for extraction correctness,
training usefulness, reasoning evidence, benchmark evidence, OCR correctness,
and expected route. Splits are by paper. Report MAE/rank correlation for scores
and precision/recall/confusion matrices for routes. Thresholds and weights are
changed only from this evidence.

An API LLM may create first-pass labels, but reviewed human labels are the
evaluation authority.

## 4. Figures, tables, and OCR

1. Always retain original figure assets, hashes, captions, alt text, dimensions,
   source location, nearby section, classifier output, and OCR output in the
   scientific artifact.
2. Structured HTML/Docling tables use structured cells. Do not substitute OCR
   when structured cells exist.
3. Raw Tesseract OCR does not automatically enter the training projection.
4. The default text projection may include source captions and reliable source
   alt text.
5. OCR remains searchable/auditable metadata until it passes a documented
   quality policy.
6. Add OCR evaluation using character error rate, word error rate, and numeric
   exact match on manually transcribed figure crops.
7. Figure-type confidence is displayed compactly. Low-confidence labels remain
   `unknown` or audit-only rather than pretending certainty.
8. Image-only tables and plots remain preserved for a future multimodal export.
9. Generated image descriptions or visual questions are restricted to selected
   benchmark candidates and require evidence links and review. They do not run
   over every live figure.

## 5. Benchmark safety and evolving reserve

The decontamination mechanism and its signed attestations remain core. The demo
canary corpus must not be represented as complete benchmark coverage.

Requirements:

- rename the UI page to `Benchmark Safety`;
- auto-verify Ed25519 attestations when displayed;
- keep a compact `Re-verify` action for independent checking;
- display benchmark manifest version, hash, item count, non-empty coverage,
  scanned documents/tokens, hits, and last successful update;
- label the one-sentence local corpus explicitly as demo canaries;
- support a versioned secret/PVC-mounted benchmark manifest for Kubernetes;
- load a real restricted benchmark reserve before making coverage claims;
- keep benchmark candidates physically separate from training Gold;
- retain the freshness cutoff and support a future API-assisted, evidence-backed
  evolving benchmark-item workflow;
- expose candidate/review/published/rejected states for generated benchmark
  items when that workflow is enabled.

## 6. UI information architecture

The final navigation story is:

1. Dashboard
2. Documents
3. Sources
4. Benchmark Safety
5. Datasets
6. Mixture (future work)

### 6.1 Dashboard

- concise corpus state and live activity;
- route counts and training/reserve/quarantine outcomes;
- projection size and source acceptance;
- score distributions with sample counts and source/profile filters;
- direct links from route/source summaries into filtered Documents;
- no paragraph-length helper copy;
- no metric presented without its population and unit.

### 6.2 Documents

The collection view is a compact, paginated/filterable dataset browser inspired
by Hugging Face Dataset Viewer, Argilla, and Lilac.

Required controls:

- search;
- route;
- source and source format;
- publication/validity date range;
- content tags;
- rejection reasons;
- figure/table/equation presence;
- real data versus fixtures;
- score ranges;
- sort and pagination.

Default columns/cards:

- title;
- source;
- date;
- route;
- selected scientific-quality score;
- composite score;
- retained sections;
- concise warning/reject status.

Document detail is organized as:

1. decision summary and route reasons;
2. retained/removed section comparison;
3. training projection preview;
4. scientific artifact with figures/tables/equations;
5. collapsed advanced provenance with hashes, revisions, raw stage results, and
   attestations.

Do not expose a giant all-stage table by default. Do not place the document hash
as a prominent subtitle. Do not silently hide content tags.

### 6.3 Sources

- list configured sources and current state;
- Add source;
- edit;
- enable/disable;
- Run once;
- last poll, new documents, failures, and rate-limit state;
- Kubernetes actions create/update real SourceFeed CRDs;
- local actions use a real local configuration/run-once backend, not fake CRUD;
- source-specific policy profile is visible and selectable where appropriate.

### 6.4 Benchmark Safety

- one current reserve summary;
- automatic verification state;
- compact snapshot history;
- filters for hits/failures/version;
- exact affected documents and benchmark identifiers on drill-down;
- raw certificate/signature details only under advanced audit.

The current five-family reserve is a starting point, not the desired final
coverage. A future benchmark-catalogue audit must inventory accessible public
AI/ML, reasoning, coding, scientific, safety, and domain benchmarks, including
their versions, splits, mirrors, licensing/access constraints, canonical text,
and known derivatives. The resulting reserve must be versioned and reproducible
and should cover every appropriate public benchmark that the team can lawfully
and technically obtain.

The audit must also measure decontamination cost before enabling broad coverage.
Exact hashes and normalized n-grams can be applied cheaply to every document;
embedding or other semantic comparisons must be staged behind cheap candidate
generation, batched, cached, and bounded. Report reserve size, index size,
per-document latency, throughput, memory, and false-positive review load. These
values remain `needs-measurement` until tested on the real corpus and cluster.
Do not silently enable an unbounded all-pairs semantic scan as the corpus or
benchmark catalogue grows.

### 6.5 Datasets

Preserve point-in-time validity queries and add a dataset builder/export surface.

Dataset builder filters:

- as-of timestamp or publication/validity date range;
- source/source format;
- route;
- content tags;
- score ranges;
- include/exclude structured surrogates;
- output format.

Licence policy is not an optional dataset filter. Every export is restricted to
the shared strict content-licence allowlist and includes `spdx_license` and
`spdx_license_source` in each record and its manifest.

It previews document/token counts and produces an export manifest containing all
model, extractor, policy, benchmark, and table snapshot revisions. Implement an
actual bounded JSONL/Parquet export path. Benchmark-reserve rows are never
included in training exports.

### 6.6 Mixture

N3 remains future work. Keep a polished, concise description of the planned two
MixtureRecipe shadow branches and measured promotion gate. Do not imply that the
proxy-LM training loop is already implemented.

## 7. Open-source UI references to inspect and adapt

Inspect source, interaction patterns, and layouts from every project below.
Reuse compatible open-source code or components when this lowers risk and fits
the existing Next.js architecture; otherwise reproduce the proven interaction
pattern cleanly. Record any copied/adapted code and its license/attribution.

- DataFlow WebUI: visual pipeline topology and execution states.
  https://github.com/OpenDCAI/DataFlow-WebUI
- Data-Juicer demos: before/after processing, operator effects, scientific-data
  processing, classifier comparison, and mixtures.
  https://github.com/datajuicer/data-juicer/tree/main/demos
- Hugging Face Dataset Viewer: browsing, filters, pagination, statistics, and
  data slices.
  https://github.com/huggingface/dataset-viewer
- Argilla: review queues, metadata filters, status, and human decisions.
  https://github.com/argilla-io/argilla
- Lilac: signals, reversible curation, clustering, diff-style inspection, and
  export.
  https://github.com/lilacai/lilac
- Renumics Spotlight: multimodal assets, predictions, confidence, and compact
  inspection layouts.
  https://github.com/Renumics/spotlight

The Stream2Pretrain UI remains one coherent product. Do not embed six unrelated
applications or add their runtime stacks merely to claim reuse.

## 8. Source-aware classifier strategy

Do not apply one universal document-quality classifier to every source.

- scientific HTML/PDF: FinePDFs Edu v2 primary candidate plus structure and
  extraction signals;
- general web/blog: FineWeb-Edu or a measured web-quality alternative;
- code: code-specific path/content/license/quality signals;
- all formats: language, privacy, exact/near duplicate, validity, and benchmark
  decontamination;
- E5 remains semantic decontamination, not general quality;
- KenLM remains a typicality signal, not a direct quality truth.

Evaluate but do not automatically deploy:

- DCLM fastText as a cheap custom scientific-quality baseline;
- NVIDIA DeBERTa quality as a general-quality comparison;
- QuRating dimensions as a labelling taxonomy, not a live 1.3B CPU model;
- a lightweight multi-label scientific content classifier if it beats the
  deterministic taxonomy on the labelled set.

## 9. Kubernetes capacity and deployment correction

- measure the actual `k8s.node` flavor, quota, allocatable resources, PVCs,
  Redpanda drain rate, and MinIO throughput;
- correct the curator resource request/limit to reflect real strict-model RSS;
- do not advertise KEDA curator replica counts the cluster cannot schedule;
- keep cheap ingestion/extraction and heavy classifier scaling independently
  configurable;
- account for per-replica model duplication;
- keep every unmeasured throughput/capacity value marked `needs-measurement`;
- run the existing capacity probe and commit its generated report only from the
  actual cluster.

## 10. Implementation order

1. Lock this plan and align primary documentation terminology.
2. Correct OCR projection and figure-confidence behavior.
3. Implement role-stratified section sampling and tests.
4. Add/pin FinePDFs Edu v2 with source-aware classifier selection.
5. Add classifier comparison and labelled-evaluation scaffolding/results.
6. Complete real benchmark-manifest coverage and honest attestation summaries.
7. Extend APIs for document filters, dataset ranges, exports, benchmark summary,
   and source actions.
8. Rebuild the complete UI information architecture using the open-source
   references above.
9. Correct Helm resources/scaling defaults from local measurements, then finish
   the cluster measurement path.
10. Run unit, integration, schema, UI, container, and local end-to-end tests.
11. Visually inspect and iterate every page at desktop and narrow viewport.
12. Re-run a diverse real-paper pilot and manually compare extracted sections,
    figures/OCR, scores, routes, and exports against expected results.

## 11. Definition of done

This work is complete only when all statements below are true:

- FinePDFs Edu v2 runs as a pinned real CPU model for scientific sources and is
  compared against FineWeb-Edu on the same labelled sample.
- The selected scientific score is not conflated with composite quality.
- Expensive section sampling is demonstrably role-stratified.
- Raw OCR cannot enter training text without the documented acceptance policy.
- Every taxonomy label is visible and filterable.
- Reasoning and benchmark values are presented as evidence heuristics until a
  trained/validated replacement exists.
- A labelled scientific evaluation report exists and drives thresholds.
- Benchmark Safety accurately distinguishes demo canaries from real reserve
  coverage and automatically verifies attestations.
- Documents is a compact filtered review workflow with advanced details hidden
  by default.
- Sources has real add/edit/enable/run behavior for its active environment.
- Datasets supports point-in-time inspection, range selection, and actual
  bounded export with a manifest.
- The UI has been visually reviewed and iterated at multiple viewport sizes.
- Kubernetes resource settings match measured model memory and cluster capacity,
  or remain explicitly `needs-measurement` without misleading replica promises.
- Tests pass and no proxy model is accepted by faithful profiles.
- Documentation, schemas, UI labels, and runtime behavior agree.
- N3 is the only explicitly deferred product feature.
