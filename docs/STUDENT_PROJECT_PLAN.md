# Stream2Pretrain student project: audited state, focused plan, and local test

Status: local CPU pilot implemented and validated on 2026-08-15; Kubernetes
deployment work remains

This document records the project decisions from the current discussion, the
repository audit, the comparison with public LLM data-curation pipelines, the
CPU-only scientific-document options, and the exact local testing approach.
It is intentionally candid. "Local validated" below means that the code path
was exercised in the Podman profile on this laptop. It does not imply that the
same behavior has already been demonstrated on the target Kubernetes cluster.

## Current execution amendment (2026-08-15)

`docs/CURATION_PRODUCT_EXECUTION_PLAN.md` is the binding implementation
contract for the current iteration and supersedes older FineWeb-only and
read-only-local-UI statements later in this historical audit. The implemented
target now has:

- FinePDFs Edu v2 as the pinned primary scientific classifier, with FineWeb-Edu
  retained as the same-section comparison and as the primary general-web
  classifier;
- code-specific quality rules that bypass prose-only FinePDF/FineWeb, KenLM,
  and Gopher assumptions;
- role-stratified section scoring, training-body projection, source metadata
  removal, reference/acknowledgement exclusion, and OCR audit-only defaults;
- compact Dashboard, Documents, Sources, Benchmark Safety, Datasets, and
  future-work Mixture pages;
- real local SourceFeed persistence, automatic scheduling, and bounded runs;
  Kubernetes SourceFeed-to-CronJob reconciliation and run-once jobs;
- server-side document filters/facets/pagination and bounded JSONL/Parquet
  dataset exports with revision and Iceberg snapshot manifests;
- a pinned benchmark-reserve builder for MMLU, GSM8K, HumanEval, MATH, and GPQA;
- a 37-paper calibration/holdout manifest and reproducible label/evaluation
  contract. Human-reviewed labels remain a team evaluation activity and must
  never be fabricated by an automated run;
- conservative default KEDA maxima until the actual DHBW `m1.xlarge` capacity
  is measured. The lecture repository gives the flavor name but no numeric
  remote specification.

N3 proxy-LM mixture training remains the sole product feature explicitly
deferred. Measured results from the new strict-model replay are recorded in
`docs/LOCAL_PILOT_REPORT.md`; older numeric FineWeb-only tables below are
historical evidence, not the selected scientific policy.

## 1. Executive decision

Stream2Pretrain should be presented primarily as a Kubernetes-native,
streaming scientific-data curation system for a Big Data course. It does not
need to claim that a frontier laboratory could directly train its next model
on the resulting corpus. A small team can still build something technically
serious and distinctive by proving these properties:

1. Real, continuously arriving scientific data is ingested through a message
   bus rather than by a one-off notebook.
2. Raw evidence, derived records, quality signals, reject reasons, versions,
   and timestamps are retained with reproducible provenance.
3. CPU-compatible extraction, cleaning, classification, duplicate detection,
   privacy checks, and benchmark decontamination run as independently
   observable stages.
4. Accepted data and quarantined data cannot be confused.
5. Gold data is queryable through a versioned lakehouse and visible in a UI.
6. Kubernetes features are demonstrated where they add course value:
   deployments, services, CRDs, policies, autoscaling, observability,
   persistence, recovery, and constrained egress.
7. Scientific structure is preserved: sections, equations, tables, citations,
   figures, captions, and links to the original media. Text-only flattening is
   not treated as the final scientific representation.
8. A benchmark reserve is isolated before any training-data export. A small,
   optional API-assisted agent flow may generate and verify evolving benchmark
   items, but this is the final enhancement, not the foundation.

The strongest project story is therefore:

> A live, Kubernetes-native pipeline that turns newly published scientific
> artifacts into an auditable, versioned, quality-scored corpus while keeping
> benchmark material quarantined and continuously refreshable.

## 2. Constraints and decisions already made

### 2.1 Project constraints

- Team size: four or five students.
- Context: Cloud Computing and Big Data course project.
- Primary evaluation: system design and Kubernetes/streaming competence.
- Compute: CPU-only Kubernetes cluster, no GPU assumption.
- Local development: Podman preferred, Docker acceptable.
- Local installation burden: no additional host applications beyond the
  container engine and normal command-line tools.
- Local data volume: a few papers and deterministic fixtures, not a historical
  corpus.
- Model training: excluded from the course-project critical path.
- Licensing enforcement: deferred for now, but source and license metadata
  must not be discarded.
- Remote changes: no push until the team explicitly decides to do so.

### 2.2 Product decisions

- Live arXiv ingestion is the center of the demonstration.
- Native arXiv HTML is the preferred representation.
- The initial prefill should stay small and recent. The large historical seed
  mixture currently described in the repository is not required for the
  student-project success criteria.
- Quality should be a vector of explainable signals, not one opaque number.
- Different downstream uses should be expressible as policies over that
  vector, for example broad pretraining, high-confidence reasoning material,
  or benchmark candidates.
- Benchmark isolation and contamination evidence are core, not optional.
- LLM or multimodal APIs are acceptable only for a small benchmark-candidate
  flow where their cost and output can be controlled and reviewed.
- Every expensive or uncertain enrichment must degrade gracefully. It must not
  block normal live HTML ingestion.

## 3. Current repository state

Implementation status on 2026-08-15: the complete local CPU pilot described
below was built, reset to empty state, and exercised with six controlled
fixtures plus three genuinely fetched arXiv HTML papers. The strict model gate,
all service health checks, durable table counts, routes, signatures, APIs, and
rendered UI were verified. Kubernetes-only behavior and PDF-fallback quality
still require their separate demonstrations. Exact evidence is recorded in
`docs/LOCAL_PILOT_REPORT.md`.

### 3.1 What is genuinely implemented

The repository already contains a substantial skeleton:

- Redpanda topics for `raw.fetched`, `docs.normalized`, `curation.decisions`,
  `docs.curated`, and `decon.attest`.
- MinIO Bronze object storage and an arXiv HTML fetcher with native arXiv HTML
  first and ar5iv fallback.
- Bytewax processor stages for Bronze to Silver and Silver to accepted Gold.
- Resiliparse text extraction.
- Language identification.
- Gopher-style heuristics.
- C4-style punctuation, brace, and placeholder checks.
- KenLM integration, with pinned English Wikipedia and SentencePiece artifacts
  prepared for the local CPU profile.
- MinHash signatures and a stateful LSHBloom near-duplicate index.
- FinePDFs Edu v2 and FineWeb-Edu integration, with pinned official
  Safetensors checkpoints prepared for local Transformers CPU inference.
- Regex/Luhn PII detection plus Presidio with a bundled spaCy English model.
- An exact n-gram benchmark Decon-Gate plus a pinned E5-small-v2 ONNX path.
- Validity intervals and source/provenance fields propagated into Gold.
- An Iceberg Gold writer.
- A durable Iceberg decision table containing both accepted and rejected
  records, including the complete curation signal vector and reasons.
- Native HTML scientific artifacts containing sections, MathML/TeX equations,
  tables, citations, figures, captions, nearby text, extraction warnings, and
  provenance. Figure bytes and canonical JSON are retained in MinIO.
- Tesseract English OCR and the pinned 26-class Docling figure classifier on
  CPU. Original figures, captions, labels, and OCR are retained; raw OCR is
  audit-only unless it passes the explicit eligibility policy.
- A bounded Docling 2.114.0 CPU PDF fallback with prefetched layout,
  TableFormer, and formula artifacts for papers lacking native HTML/ar5iv.
- Signed per-snapshot decontamination attestation objects stored in MinIO and
  announced on Redpanda.
- A DuckDB HTTP query service.
- A Next.js cockpit with dashboard, compact Documents review, durable
  accept/reject details, figures/OCR/tables/equations/citations, sources,
  Benchmark Safety, Sources, Datasets/export, and Mixture pages.
- Helm templates, CRDs, KEDA objects, NetworkPolicies, Gatekeeper-related
  configuration, and observability configuration.

This is enough code to support a meaningful project, but some documentation
currently describes the intended design as if every part were already
complete and verified.

### 3.2 Important gaps and claim mismatches

These should be fixed or described honestly before the final presentation.

| Area | Repository reality | Required response |
|---|---|---|
| Scientific extraction | Locally validated for native arXiv HTML, controlled figure/table/equation fixtures, and a bounded real-PDF Docling path. A nine-page arXiv PDF produced its title, 19 sections, 4 figures, OCR on 2 figures, and a 3,467-word projection in 31.46 seconds on 2 CPUs. Structured JSON and figure assets are retained in MinIO and referenced from Silver/decision/Gold. | Expand the PDF sample before making general accuracy or throughput claims. |
| Bronze/Silver/decision/Gold storage | Bronze is raw MinIO plus a typed event; Silver is a typed topic record; structured scientific artifacts are MinIO JSON/assets; Iceberg contains the durable decision audit and accepted-only Gold. | The current README and architecture now describe those actual boundaries. Bronze/Silver Iceberg tables are not required for the local course pilot. |
| Iceberg version | The writer creates tables with `format-version=2`; `_row_id` is reserved and null. | Public local-pilot documentation now uses V2 terminology. Do not claim V3 row lineage unless it is implemented and tested later. |
| `as_of` semantics | The API applies `[valid_from, valid_to)` predicates to the current Gold relation; it does not select an Iceberg snapshot by timestamp. | UI and primary documentation now call this a validity-time query and explicitly distinguish snapshot time travel. |
| Rejected records | Locally validated: every scored record reaches `curation.decisions` and `gold.curation_decisions`; only clean training rows also reach `docs.curated` and `gold.curated`. The clean replay produced 9 decisions, 4 Gold rows, and 1 separate benchmark-candidate row. A simultaneous restart of all three stream workers preserved those exact counts. | Cross-table commits are not one distributed transaction, so cluster recovery semantics must still be measured and described as at-least-once. |
| Decon attestation | Locally validated against the authoritative decision batch, including a contaminated rejected hash, per-benchmark hits, and scanned/flagged totals. Strict mode fails on attestation persistence/publication errors. The contaminated snapshot's Ed25519 signature verified in the UI. | Test key rotation and replay/recovery on Kubernetes. |
| Benchmark corpus | The gate supports a JSON corpus path, but a complete pinned benchmark reserve and its operational lifecycle are not present. | Define versioned benchmark manifests, strict access, canaries, update flow, and tests. Do not bundle benchmark answers casually into general fixtures. |
| Real model packaging | Locally validated: strict startup loaded the official FineWeb-Edu Safetensors model through Transformers CPU, E5-small-v2 through ONNX Runtime CPU, English Wikipedia KenLM with SentencePiece, Docling 2.114.0 artifacts, the Docling 26-class figure ONNX model, Tesseract English, Presidio with `en_core_web_sm`, Rensa, Plyvel, fastlangid, Resiliparse, and tiktoken. No proxy was accepted. | Threshold calibration on a labeled scientific sample is still required. A later FineWeb-Edu ONNX export is an optimization, not a condition for real CPU inference. [FineWeb-Edu classifier files](https://huggingface.co/HuggingFaceFW/fineweb-edu-classifier/tree/284663cbb2dabf9bda30d8f8cc49601251ee1631) |
| FineWeb-Edu threshold | The repository gate accepts scores at `>=1.0`. The official model card recommends an integer score threshold of at least 3 for its web-data use and warns that specialized higher-education material is out of distribution. | Calibrate on scientific documents. Do not copy the web threshold blindly. Preserve raw score and policy revision. [FineWeb-Edu model card](https://huggingface.co/HuggingFaceFW/fineweb-edu-classifier) |
| PII | Locally validated with regex/Luhn plus Presidio and `en_core_web_sm`. Metadata and body are scanned separately: author email metadata is removed from the training projection without rejecting a clean paper, while a high-confidence unsafe body section is isolated and can quarantine the result. Ambiguous numeric scientific patterns remain audit-only. | Calibrate entity confidence and policy on a labeled scientific sample; retain restricted spans only where operationally necessary. |
| Mixture controller | The proxy bigram signal and CRD controller skeleton exist. The repository describes two real Iceberg branches and continuous proxy-model training, but the implementation does not materialize that full data path. | Treat this as a stretch demo or reduce the claim. Do not let it displace core extraction and audit work. |
| Local Polaris | The Helmfile and docs refer to a local `polaris-lite` chart that is not present. The old compose stack explicitly omits Polaris. | Use the new SQLite PyIceberg local substitute for laptop mechanics; test real Polaris in Kubernetes. |
| Local smoke test | The Podman-first compose profile covers ingestion, structured extraction, all real CPU signals, durable decisions, clean Gold, benchmark isolation, attestations, DuckDB, Prometheus, and the UI. Six deterministic fixtures cover accept, duplicate, heuristic, PII, contamination, and benchmark-reserve outcomes. | Validated on 2026-08-15 as described in section 9 and `docs/LOCAL_PILOT_REPORT.md`. |
| Recovery/exactly-once claims | The original local profile omitted Bytewax recovery configuration and replayed all nine events after a worker restart. This was found during validation, fixed with durable per-flow recovery snapshots, and retested from a clean state: force-recreating the fetcher, curator, and writer together preserved 9 decisions, 4 Gold rows, and 1 benchmark row. | Document the local result as process-restart recovery, not exactly-once. Test crash timing, multi-partition workers, and pod/node failure with real Polaris on Kubernetes. |

### 3.3 What is valuable about the current design

The project already differs from a normal offline corpus-cleaning script in
ways that are relevant to the lecture:

- Live sources and a Kafka-compatible event bus make velocity visible.
- Raw evidence in object storage plus typed events demonstrates variety and
  veracity.
- Stateful deduplication and benchmark checks demonstrate nontrivial stream
  processing.
- Iceberg and validity metadata make the result queryable and version-aware.
- Signed attestations are a good systems concept even though the current data
  flow feeding them must be corrected.
- CRDs, KEDA, policies, and observability provide a legitimate Kubernetes
  control-plane story.

The main value is not inventing a new heuristic filter. It is integrating
fresh scientific acquisition, transparent curation, benchmark isolation, and
cluster operation into one auditable system.

## 4. What current public LLM data pipelines usually do

No public source reveals every detail of a current proprietary frontier-lab
pipeline. The comparison therefore uses official papers, model reports, model
cards, documentation, and open-source implementations. Claims about closed
systems are limited to what their creators published.

### 4.1 Common pipeline shape

A mature text pretraining pipeline usually contains these layers:

1. Source acquisition and immutable raw retention.
2. Format-aware extraction and normalization.
3. Language and script identification.
4. Cheap document and line heuristics.
5. Exact, fuzzy, and sometimes semantic deduplication.
6. Model-based quality, domain, toxicity, and safety scoring.
7. PII detection, redaction, access control, or exclusion.
8. Benchmark decontamination before training export.
9. Source-aware mixing and sampling.
10. Tokenization, shuffling, shard construction, manifests, and reproducible
    export.
11. Statistical QA and small training ablations to evaluate pipeline choices.

Meta's public Llama 3 description explicitly mentions heuristic filters, NSFW
filters, semantic deduplication, text-quality classifiers trained from LLM
annotations, and experiments over source mixtures. It is useful evidence for
the shape of a frontier pipeline, but not a reproducible recipe.
[Meta Llama 3 data description](https://ai.meta.com/blog/meta-llama-3/)

FineWeb and FineWeb2 provide a more reproducible open reference. FineWeb2
documents language identification, global per-language deduplication,
filtering, PII anonymization, retained removed subsets, and ablation-trained
models to validate choices.
[FineWeb2 dataset and pipeline](https://huggingface.co/datasets/HuggingFaceFW/fineweb-2/blob/main/README.md)

DCLM is particularly important because it evaluates data recipes by training
standardized proxy models and measuring many downstream tasks, rather than
assuming a plausible filter improves data. Its public baseline uses
Resiliparse, local filters/classifiers, and global fuzzy deduplication.
[DCLM repository](https://github.com/mlfoundations/dclm)

Dolma is a useful diverse-corpus reference with open tooling and published
analyses of intermediate curation decisions.
[Dolma paper](https://arxiv.org/abs/2402.00159)

### 4.2 Open-source systems worth borrowing from

| Project | Strength relevant to Stream2Pretrain | Recommendation |
|---|---|---|
| DataTrove | Modular readers, extractors, filters, stats, tokenization, and multiple exact/fuzzy dedup flows; local and distributed execution. It is the implementation base around FineWeb. | Reuse individual tested logic or use it for offline comparison runs. Replacing Bytewax would weaken the streaming course story. [DataTrove](https://github.com/huggingface/datatrove) |
| DCLM | Reproducible filtering recipes, global deduplication, and standardized data-quality evaluation through model training. | Borrow its evaluation discipline and compare a sample against its filters. Do not add Ray just for the small live path. [DCLM baseline docs](https://github.com/mlfoundations/dclm/blob/main/baselines/README.md) |
| Dolma toolkit | Open multi-source corpus recipes and intermediate-state analysis. | Use as a reference for source-specific cleaning and audit statistics. [Dolma](https://arxiv.org/abs/2402.00159) |
| NeMo Curator | Broad text pipeline including rule/model filtering and exact, fuzzy, and semantic dedup; strong multimodal support. | Do not adopt its GPU semantic-dedup path in this CPU project. Its public docs explicitly require GPU acceleration for semantic dedup. [NeMo Curator overview](https://docs.nvidia.com/nemo/curator/latest/home/welcome), [semantic dedup requirements](https://docs.nvidia.com/nemo/curator/latest/curate-text/process-data/deduplication/semdedup) |
| Data-Juicer | Large catalog of CPU/GPU-tagged filters, mappers, and deduplicators across text and media. | Use its operator catalog as a checklist and selectively port a small number of CPU metrics. Do not integrate dozens of overlapping filters. [Data-Juicer operators](https://github.com/datajuicer/data-juicer/blob/main/docs/Operators.md) |
| IBM Data Prep Kit | Modular transforms for enterprise data preparation, including dedup and PII-related work. | Secondary reference. Adding another execution framework is not core to this project. [Data Prep Kit](https://github.com/data-prep-kit/data-prep-kit) |

### 4.3 Comparative assessment of Stream2Pretrain

#### Strong or distinctive

- Live scientific freshness rather than another static Common Crawl build.
- A clear Kubernetes-native teaching story.
- Validity metadata useful for time-aware filtering.
- A benchmark-isolation concept tied to stream processing.
- Signed evidence tied to lakehouse commits, once the rejected-record wiring is
  corrected.
- Source provenance carried across records.

#### Reasonable foundation, but not yet best-in-class

- Resiliparse extraction.
- Gopher and C4 heuristics.
- Language identification.
- MinHash/LSHBloom near-duplicate detection.
- KenLM-style typicality.
- FineWeb-Edu-style educational quality.
- Regex PII checks.

These are good building blocks, but a list of filters is not proof of data
quality. Public pipelines such as FineWeb and DCLM earned their claims through
ablation training, distribution analysis, and careful threshold selection.
Without local statistics or model ablations, Stream2Pretrain should say that a
stage is implemented, not that it improves final model quality.

#### Materially behind mature pipelines

- Scientific metadata beyond the retained sections, equations, tables,
  citations, figures, captions, coordinates, OCR, and raw extractor output can
  still be enriched further.
- Cross-table replay is at-least-once rather than transactionally exactly-once.
- Exact/global and sentence-level dedup coverage is incomplete.
- Model artifacts are runtime-verified, but thresholds are not calibrated for
  this domain.
- Safety/toxicity signals are minimal.
- PII is segment-aware, but its confidence policy still needs labeled-domain
  calibration.
- The production benchmark-corpus lifecycle is unfinished; the local canary
  reserve and complete decision-based attestation path are implemented.
- Mixture optimization is mostly a stub.
- No training-based data-quality ablations have been run.
- Tokenized training exports and manifests are not the current project focus.

### 4.4 What not to copy

- Do not deploy every open-source curation framework.
- Do not add GPU-only semantic dedup just to match a feature list.
- Do not run a large language model over every document.
- Do not use dozens of correlated heuristic filters with unexplained
  thresholds.
- Do not collapse accept/reject into a single score and lose the raw signals.
- Do not claim "frontier quality" without training or other outcome-based
  evaluation.
- Do not make the large historical seed corpus a prerequisite for the class
  demonstration.

## 5. Focused scope for the student project

### 5.1 Priority 0: correctness and truthful observability

These items should be completed before adding novel classifiers.

1. Make the small real pipeline reproducibly runnable.
2. Retain accepted and rejected decisions durably.
3. Correct the Decon-Gate attestation input so rejected contamination events
   are included.
4. Correct UI/documentation terminology for Iceberg V2 and validity-time
   queries unless true snapshot-time behavior is implemented.
5. Expose per-stage counts, latency, errors, reject reasons, and model/policy
   revisions.
6. Add controlled fixtures for accept, heuristic reject, PII reject,
   duplicate reject, and decontamination reject.
7. Measure throughput and memory on the target CPU cluster. Until measured,
   every capacity claim is `needs-measurement`.

### 5.2 Priority 1: scientific artifact quality

This is the most valuable quality improvement for the data itself.

Add a versioned structured-document model containing at least:

- document title, authors, abstract, source identifier, and publication/version
  dates;
- ordered section hierarchy;
- paragraphs with stable local IDs;
- equations with MathML and/or TeX representation;
- tables with caption, headers, cells, footnotes, and source position;
- figures with asset URI, content hash, caption, alt text, figure number, and
  nearby paragraph references;
- bibliographic entries and in-text citation links;
- extraction warnings and quality flags;
- original raw-object URI and extraction pipeline revision.

Store image/table assets in MinIO and send only bounded metadata and references
through Redpanda. Generate a plain-text training view from this structure, but
keep the structure so future reasoning, retrieval, and multimodal datasets do
not need to re-fetch the paper.

### 5.3 Priority 1: useful quality vector

The minimum useful quality vector should include:

- language and confidence;
- word/token count;
- extraction completeness;
- section count and abstract presence;
- equation, table, figure, and citation counts;
- Gopher rule outcomes, not only their aggregate;
- C4 rule outcomes and punctuation fraction;
- perplexity score, bucket, and scorer revision;
- educational-quality score and classifier revision;
- exact-content hash;
- near-duplicate cluster and method revision;
- PII types and counts, ideally spans kept only in restricted audit storage;
- decontamination matches and benchmark-set revision;
- source and document-version freshness;
- final route: broad corpus, reasoning candidate, benchmark candidate,
  quarantine, or retry.

Policies then select records by use case. For example:

- Broad pretraining: valid extraction, low risk, deduplicated, no benchmark
  hit, moderate quality threshold.
- Reasoning candidate: strong structure, equations or procedural argument,
  high extraction confidence, high quality, no PII, no benchmark hit.
- Benchmark candidate: new after a cutoff, strong evidence and answerability,
  quarantined from all training exports, human/API verification required.

The project does not need a dedicated classifier for every dimension. Several
of these are deterministic metadata or structural features and are more
auditable than another black-box model.

### 5.4 Priority 1: benchmark reserve

Use a physically and logically separate benchmark path:

```text
new paper
  -> normal extraction and quality signals
  -> benchmark-candidate policy
  -> restricted candidate store
  -> optional LLM/multimodal generation
  -> evidence verification
  -> human approval
  -> versioned benchmark release
  -> immediate Decon-Gate update

all training exports
  -> exact/fuzzy benchmark scan
  -> quarantine on hit
  -> signed snapshot evidence
```

Required controls:

- Benchmark manifests are versioned and immutable after release.
- Questions, answers, supporting passages, paper version, figure/table IDs,
  generator model, prompt revision, and verification status are recorded.
- Benchmark objects and training objects use separate buckets/tables and
  credentials.
- Candidate papers are excluded from training export as soon as they enter the
  reserve, not only after question generation.
- Exact n-gram matching is the required CPU baseline.
- MinHash or exact-substring overlap can add fuzzy text coverage.
- Embedding/VLM checks are optional and should not be the only barrier.
- Synthetic canaries prove the path without distributing real benchmark
  answers in normal test data.
- A signed attestation must be computed from the actual scanned and rejected
  event stream.

The project's strongest benchmark claim is freshness and controlled isolation,
not that a newly generated question is automatically excellent or impossible
for every model to have seen.

### 5.5 Priority 1: UI and course demonstration

The UI should make system behavior explainable:

- Live stage throughput and lag.
- Accepted and rejected counts by reason and source.
- A document inspector showing raw source, extracted structure, derived text,
  signal vector, and route decision.
- A paper view with sections, equations, tables, figures, captions, OCR, and
  provenance.
- A quarantine view for PII, duplicate, extraction, and benchmark failures.
- Signed-attestation verification tied to a real snapshot and complete scan
  statistics.
- Validity-time and, if implemented, Iceberg snapshot-time queries shown as
  distinct controls.
- Model, policy, extractor, and benchmark-set revisions.
- Kubernetes operational panels: consumer lag, replicas, restarts, resource
  use, and one controlled failure-recovery demonstration.

The existing sources and mixture pages can remain, but they should not be the
headline until their controller behavior is real.

## 6. CPU-only OCR, figures, and image understanding

### 6.1 Conclusion

A realistic CPU-only route exists if three goals are separated:

1. Preserving figures and scientific context is practical for every native
   HTML paper.
2. OCR and lightweight figure classification are practical on CPU.
3. Deep semantic interpretation of every figure is not practical at live
   stream scale on the planned cluster. It is realistic only for a small
   benchmark reserve using a tiny local VLM or a remote multimodal API.

### 6.2 Tier 1: native arXiv HTML

arXiv introduced native HTML generated from TeX/LaTeX specifically to improve
structured and accessible rendering. Use it before any PDF/OCR path.
[arXiv HTML project paper](https://arxiv.org/abs/2402.08954)

For each native HTML paper:

- Parse the real section hierarchy.
- Preserve MathML and TeX when available.
- Parse HTML tables into rows/cells.
- Download linked figures into MinIO.
- Record caption, alt text, figure number, source element ID, nearby paragraph
  IDs, source URL, image hash, MIME type, and dimensions.
- Create a bounded text surrogate for text-only exports, while retaining the
  original image asset.

Example surrogate:

```text
[FIGURE]
Type: line_chart
Caption: ...
Visible text: ...
Referenced by: section-3-paragraph-7
[/FIGURE]
```

### 6.3 Tier 2: cheap CPU enrichment

Use OCR only on extracted figure crops likely to contain labels, axes, or
legends. Tesseract is CPU-native and Apache-2.0.
[Tesseract](https://github.com/tesseract-ocr/tesseract)

Use a tiny ONNX classifier to route figure types. Docling's
`DocumentFigureClassifier-v2.5` is a strong fit: its model card describes a
4.08M-parameter model with 26 document-figure categories and ONNX weights.
[DocumentFigureClassifier-v2.5](https://huggingface.co/docling-project/DocumentFigureClassifier-v2.5)

This supports search, routing, and UI display. It does not establish the
scientific meaning of a plot.

### 6.4 Tier 3: selective CPU PDF fallback

When native HTML is missing or structurally degraded, one Docling CPU service
is the leading option. Docling supports PDF layout, reading order, tables,
figure crops, caption association, OCR engines, page coordinates, and explicit
CPU selection.
[Docling options](https://docling-project.github.io/docling/reference/pipeline_options/),
[Docling technical report](https://arxiv.org/html/2501.17887),
[Docling Serve](https://github.com/docling-project/docling-serve)

Recommended policy:

1. Run the PDF text layer, layout, tables, and figure extraction without
   full-page OCR.
2. Invoke OCR only when the text layer is absent/broken or for selected figure
   crops.
3. Enforce document/page limits, one in-flight document initially, CPU/memory
   limits, timeout, retry, and dead-letter behavior.
4. Cache by paper version plus extractor/model version.
5. Mark failures degraded instead of blocking the main HTML stream.

Docling's published CPU measurements show a large long tail and that OCR is a
major cost. They are not a prediction for this cluster. Native HTML plus figure
OCR was exercised locally. One nine-page Docling PDF completed in 31.46 seconds
on 2 CPUs; this single measurement does not predict the long tail. Cluster
throughput remains `needs-measurement`.

### 6.5 Tier 4: limited figure descriptions

Two small CPU-capable experiments are plausible:

- SmolVLM-256M for selected general figure descriptions or visual questions.
  Its official model card provides CPU examples and Apache-2.0 weights.
  [SmolVLM-256M](https://huggingface.co/HuggingFaceTB/SmolVLM-256M-Instruct)
- Granite-Docling-258M for constrained document conversion such as formulas,
  tables, and chart-to-table. Its model card warns that it is not a general
  image-understanding model and output may be inaccurate.
  [Granite-Docling-258M](https://huggingface.co/ibm-granite/granite-docling-258M)

Neither should run over every live figure. Use concurrency one, strict timeout,
and only benchmark candidates or a tiny demonstration sample. Treat output as
generated metadata, record the image hash/model/prompt/revision, and require
verification before including an answer in a benchmark.

A remote multimodal API is likely more useful for the benchmark cherry on top.
Docling can use a remote OpenAI-compatible picture-description endpoint, which
allows one interface for local and API modes.
[Docling REST API](https://docling-project.github.io/docling/usage/api_server/rest_api/)

### 6.6 Alternatives not selected

| Tool | Assessment |
|---|---|
| GROBID | Excellent CPU scholarly metadata/citations and TEI. It does not OCR scanned papers and is weaker for table/figure understanding. Running it with Docling duplicates much of the stack. [GROBID](https://grobid.readthedocs.io/en/latest/getting_started/) |
| OCRmyPDF | Good CPU scanned-PDF text layer, but no scientific structure. Docling already supports selective Tesseract OCR. [OCRmyPDF](https://github.com/ocrmypdf/ocrmypdf) |
| PDFFigures2 | CPU figure/table/caption extraction and transparent output, but older Scala integration and layout limitations make it a fallback choice. [PDFFigures2](https://github.com/allenai/pdffigures2) |
| Marker | CPU mode exists, but its strongest OCR/equation path increasingly relies on a VLM and its model licensing/integration is less attractive here. [Marker](https://github.com/datalab-to/marker) |
| MinerU | Broad capability, but its official CPU storage/RAM expectations and integration size are too high for the minimal student deployment. [MinerU](https://github.com/opendatalab/MinerU) |
| PaddleOCR PP-StructureV3 | Capable CPU OCR/layout/table/formula stack, but more components and tuning work than one Docling fallback. Keep as a replacement if Docling fails evaluation. [PaddleOCR](https://github.com/PaddlePaddle/PaddleOCR) |

### 6.7 Minimal visual implementation decision

For this project, implement only:

1. Native HTML figures/tables/equations and MinIO asset references.
2. Tiny CPU figure classification.
3. Selective Tesseract OCR on relevant figure crops.
4. One bounded Docling CPU PDF fallback with selective Tesseract OCR, explicit
   CPU execution, page/byte/time limits, and cached extractor artifacts.
5. Optional small-VLM/API descriptions only for benchmark candidates.
6. UI display of source image, caption, class, OCR, generated description,
   provenance, and verified/unverified state.

## 7. Implementation plan

### Phase A: establish the trustworthy baseline

Implementation status: complete for the local baseline. The profile was
rebuilt from current sources, reset, replayed, measured, and visually inspected
on 2026-08-15. The cluster measurements belong to Phase E.

- Keep the validated local profile reproducible.
- Capture build time, image sizes, steady CPU/RAM, per-paper latency, topic
  counts, accepted/rejected reasons, Gold query, and signature verification.
- Fix only blockers discovered by the controlled fixtures.
- Correct public docs/UI copy that contradicts behavior.
- Maintain a short, repeatable five-minute demo script.

Exit criteria:

- A real arXiv document reaches Bronze and Silver.
- The deterministic clean fixture reaches Gold.
- PII and benchmark canaries are rejected for the intended reason.
- Gold is visible through DuckDB and the UI.
- A signed attestation is produced and verified.
- Every stage shows a revision and measurable status.

### Phase B: make decisions auditable

Implementation status: every scored document is published to
`curation.decisions`, written to an append-only decision table with its full
signal vector and reasons, and only accepted rows are additionally published to
`docs.curated` and Gold. Signed attestations are built from the complete
decision batch. The one-partition local profile now uses durable Bytewax
recovery snapshots; a simultaneous restart of all three stream workers
preserved the exact 9/4/1 decision, Gold, and benchmark counts.

- Introduce a versioned `CurationDecision` or rejected-record schema.
- Publish every scored document to either accepted or quarantine storage.
- Keep raw signal vector and reason list.
- Build the snapshot attestation from actual scan decision events.
- Add exact-content dedup before expensive stages.
- Extend the validated local restart test to crash timing, multiple partitions,
  and the Kubernetes deployment.

Exit criteria:

- No scored document disappears without an auditable outcome.
- Decon attestation counts equal the corresponding decision events.
- Replaying an event produces a documented idempotent result.

### Phase C: preserve scientific structure

Implementation status: native arXiv HTML and controlled structured artifacts
are locally validated end to end. The versioned scientific-document schema,
figures/tables/equations, MinIO assets, deterministic text projection,
extraction warnings, Docling package, and document inspector are wired into the
same path. A complete nine-page real arXiv PDF also passed the isolated strict
Docling fallback; a broader PDF set and visual quality scoring remain open.

- Add the structured scientific-document schema.
- Upgrade native arXiv HTML extraction.
- Store figure assets and table structures.
- Generate the plain-text view from the structured document.
- Add extraction completeness metrics and UI inspector.

Exit criteria:

- A selected paper shows sections, equations, at least one table/figure when
  present, citations, and the matching source provenance.
- The flattened text can be regenerated deterministically.
- Missing elements create warnings instead of silent loss.

### Phase D: validate and calibrate CPU quality signals

Implementation status: the pinned real FineWeb-Edu, KenLM, E5, Presidio,
Docling, Tesseract, figure-classifier, Rensa, Plyvel, fastlangid, Resiliparse,
and tiktoken paths loaded in strict mode and processed the complete local
replay. Image, model-volume, runtime-volume, and steady container memory were
measured. Per-stage latency and threshold calibration remain open.

- Build and verify the prepared pinned FineWeb-Edu, KenLM, E5, and Presidio
  artifacts on the target CPU architectures.
- Calibrate thresholds on a labeled scientific sample.
- Measure whether KenLM's quality lift justifies its roughly 4.44 GB artifact.
- Segment metadata/body before applying PII policy.
- Add a small safety/toxicity signal only if it is CPU-feasible and changes a
  clear route decision.
- Add per-signal distributions and source drift monitoring.

Exit criteria:

- Model artifacts are pinned, checksum-verified, and load successfully.
- Proxy and real modes cannot be confused in records or UI.
- Thresholds have a labeled evaluation table rather than intuition only.

### Phase E: Kubernetes demonstration

- Deploy the real Polaris/MinIO/Redpanda/processor/UI path.
- Validate CRDs and Gatekeeper admission.
- Validate NetworkPolicy behavior.
- Show KEDA scale-up from controlled backlog.
- Kill a processor pod and show measured recovery/replay semantics.
- Show Prometheus/Grafana metrics and one trace across stages.
- Run the capacity probe and replace capacity guesses with measurements.

Exit criteria:

- The team can reproduce the cluster from documented commands.
- The five-minute course demo has no manual database edits.
- Recovery and scaling claims match observed behavior.

### Phase F: benchmark and visual cherry on top

- Add the restricted benchmark candidate store.
- Add one verified text-only question flow first.
- Add figure-aware generation only after evidence links are stable.
- Evaluate Docling CPU fallback on a deliberately chosen small PDF set.
- If useful, connect a remote LLM/VLM API with strict budget, cache, prompt
  version, and human verification.

Exit criteria:

- A new paper can produce one evidence-backed, reviewed benchmark item.
- That paper is excluded from every training export before generation begins.
- The item links back to exact text/table/figure evidence.
- The generator output is labeled generated and cannot become ground truth
  without verification.

### Phase G: final showpiece, real shadow-mode mixture evaluation

This is the last optional milestone, attempted only after Phases A-F are
stable. It turns the existing N3 skeleton into a real closed-loop experiment:
two `MixtureRecipe` CRDs consume the same live `SourceFeed`, create two actual
Iceberg branches with different curation or sampling policies, and train the
same small language-model architecture on equal, bounded rolling windows. A
separate sealed and decontaminated evaluation slice measures per-domain loss
and perplexity. The controller then emits an auditable promotion recommendation
or, after the shadow policy is proven safe, gates promotion using a minimum
sample count and configured improvement margin.

The GPU work can sit behind the existing proxy-LM interface as a remote runner.
A short-lived provider such as Modal is a plausible demonstration backend if
credits are available at implementation time; the provider, current credit
terms, image, model, seed, token budget, maximum cost, timeout, and artifact
retention must all be pinned. Redpanda, the CRDs, Iceberg branches, decision
logic, metrics, and UI remain in the project Kubernetes cluster. Only the
bounded training/evaluation job leaves the cluster. Cached inputs and outputs
make the live demo independent of a last-minute external-provider failure.

Why this is unusually strong: it evaluates a data recipe by measured model
behavior rather than by classifier scores alone. FineWeb-Edu, KenLM, heuristics,
and deduplication remain useful document-level signals; N3 tests whether one
complete mixture actually improves a small held-out model by domain. It is not
evidence that the same ranking will hold at frontier scale, but it is a serious
and visually compelling data-centric feedback loop.

Minimum scope:

- Materialize two real, reproducible Iceberg branches from one event stream.
- Use one fixed tiny model and identical tokenizer, initialization, steps,
  batch size, token count, and hardware for both branches.
- Keep the evaluation corpus sealed from both branches and report per-domain
  as well as aggregate metrics.
- Persist training manifests, dataset snapshot IDs, model artifacts, costs,
  logs, scores, controller decision, and reason.
- Show branch lag, tokens trained, evaluation deltas, confidence or repeat
  status, and promotion state in `ui/app/mixture`.
- Default to shadow recommendation. Enable automatic promotion only after a
  deterministic end-to-end fixture proves the guards and rollback path.

Exit criteria:

- One live source event is traceable into both distinct Iceberg branches.
- Two equal-budget remote jobs finish and produce reproducible manifests.
- The UI shows measured per-domain deltas and the exact policy decision.
- A failed, timed-out, or over-budget remote run leaves the active recipe
  unchanged.
- The entire optional path can be disabled without affecting ingestion,
  curation, benchmark isolation, storage, or the core Kubernetes demo.

## 8. Suggested team split

For four people:

1. Ingestion and scientific extraction: arXiv HTML, structured schema, assets,
   Docling experiment.
2. Curation and benchmark isolation: quality vector, quarantine, dedup, PII,
   complete attestations.
3. Platform: compose, Helm, Polaris, Redpanda, MinIO, KEDA, policies, recovery,
   observability.
4. UI and evaluation: document inspector, dashboards, fixture suite, labeled
   sample, demo and measurements.

For five people, split evaluation/benchmark generation from the UI work.

Every workstream should own tests and documentation for its schema boundaries.
Avoid one person becoming the only one who can operate Kubernetes.

## 9. Local test and validated result

The laptop profile is defined in `compose.local.yml`, with commands in the
top-level `Makefile` and details in `local/README.md`. It was exercised from a
clean project state on 2026-08-15. No remote repository was changed and the
unrelated `open-webui` and `sap-ai-proxy` containers were left running on their
existing ports.

### 9.1 Services

- Single-node Redpanda with internal and host listeners.
- Redpanda Console.
- MinIO plus bucket bootstrap.
- Bronze-to-Silver processor.
- Silver-to-Gold curator.
- Gold Iceberg writer.
- Decon attestation API.
- DuckDB API.
- Prometheus.
- Next.js UI.
- Manual one-shot arXiv HTML/PDF ingest service.
- Manual controlled-fixture ingest service.

### 9.2 Local lakehouse substitution

The local writer and DuckDB service use PyIceberg's SQLite SQL catalog with a
shared filesystem warehouse. This is the official PyIceberg development
pattern and avoids inventing the missing local Polaris chart.
[PyIceberg local SQL catalog example](https://py.iceberg.apache.org/)

This tests table creation, append, metadata, Parquet, and DuckDB access. It does
not test Polaris authentication, REST behavior, concurrent distributed
commits, or Gold data stored in MinIO. Those remain Kubernetes tests.

### 9.3 Local resource envelope

Allocate 4 CPUs and 10-12 GB RAM to the Podman machine; the validated run used
a 16 GB VM to leave comfortable headroom. Keep 20-25 GB of host disk free
before the first build because build layers coexist temporarily with model
artifacts and final images. Measured after the clean nine-document replay:

- shared pinned model volume: 7.3 GB;
- processor image: 2.38 GB;
- UI image: 311 MB;
- arXiv fetcher image: 351 MB;
- disposable runtime volumes: about 419 MB total, dominated by 390 MB of
  Redpanda data;
- steady cgroup working sets: curator 857 MB, Redpanda 419 MB, MinIO 255 MB,
  fetcher 220 MB, writer 185 MB, DuckDB API 175 MB, Sources API 77 MB, and
  every remaining long-running service below 60 MB at the measurement instant.

These are one laptop observation, not production capacity claims. The
curator's Linux process RSS can be much larger because the KenLM artifact is
mmap-backed even though `podman stats` reported an 857 MB cgroup working set.
That distinction is why 10-12 GB remains the recommended practical minimum.

`compose.local.yml` caps every long-running container. The curator receives the
largest limit at 2 CPUs and 8 GB RAM, Redpanda is limited to 1 CPU and 1.5 GB,
the PDF/HTML fetcher is limited to 2 CPUs and 4 GB, and lighter services receive
0.5-1 CPU and 0.5-1.5 GB. The Podman VM is the aggregate RAM/CPU ceiling. Image
and named-volume disk quotas are not portable through Compose, so disk is
guarded by the host allocation rather than a false per-container promise.

### 9.4 Classifier behavior in the minimal profile

Real in the minimal local profile:

- Resiliparse.
- structured scientific HTML extraction for sections, references, tables,
  equations, figures, and captions;
- bounded CPU Docling PDF fallback with layout, TableFormer, formula, and raw
  Docling-JSON preservation;
- Tesseract OCR plus a pinned 26-class ONNX figure router on extracted figures;
- language identification when its runtime package loads;
- Gopher and C4 rules;
- MinHash and LSHBloom;
- official FinePDFs Edu v2 Safetensors inference through PyTorch CPU for
  scientific sources;
- official FineWeb-Edu Safetensors inference through PyTorch CPU for
  same-section comparison and general web sources;
- English Wikipedia KenLM with the publisher's SentencePiece preprocessing;
- regex/Luhn PII plus Presidio and `en_core_web_sm`;
- exact n-gram plus E5-small-v2 ONNX Decon-Gate;
- token counting;
- validity enrichment;
- Ed25519 signing;
- Iceberg write and DuckDB query.

The validated local MinHash and LSHBloom implementations use `rensa` and
`plyvel` (LevelDB), respectively.

The profile pins artifact revisions and sets `S2P_REQUIRE_REAL_MODELS=1`, so a
missing or unloadable FinePDFs Edu v2, FineWeb-Edu, KenLM, E5, Presidio,
Docling, Tesseract, or figure-classifier artifact stops its service instead of
silently using a proxy. Strict validation returned `transformers-cpu`,
`kenlm-sentencepiece:en.arpa.bin`, `onnxruntime-cpu`,
`regex-luhn-v1+presidio-en_core_web_sm`, `rensa`, `plyvel`, `tiktoken`,
`fastlangid-1`, and `resiliparse-0.14`, with Docling, figure ONNX, and
Tesseract artifacts available. The remaining local infrastructure substitution
is the SQLite Iceberg catalog. Podman now has a persistent SourceFeed control
plane with add/edit/enable/delete/run-once actions and an interval scheduler;
Kubernetes uses the SourceFeed CRD controller and per-feed CronJobs.

Backend availability and the measured pilot resource behavior are validated.
Classifier accuracy, policy thresholds, per-stage latency, and production
capacity still require a labeled evaluation and cluster measurements.

### 9.5 Validated input

- Three stable arXiv IDs in `local/arxiv_ids.txt`.
- A controlled clean fixture expected to reach Gold.
- A byte-equivalent clean copy expected to exercise the near-duplicate route.
- A C4 curly-brace canary expected to exercise a heuristic rejection route.
- A synthetic benchmark-canary fixture expected to be rejected by the exact
  Decon-Gate.
- A clean benchmark-reserve fixture expected to be physically isolated from
  every training export.
- A synthetic `.invalid` email fixture expected to be rejected by PII.
- A generated chart fixture with caption text, OCR text, a table, and an
  equation for the structured-document inspector.
- A local benchmark manifest containing no real benchmark content.

The controlled cases matter because a real paper can legitimately fail due to
unsafe body PII, extracted boilerplate, benchmark overlap, quality policy, or
duplicate state. Author metadata is now separated and excluded rather than
used as a whole-paper rejection trigger.

### 9.6 Commands for a repeatable run

```bash
make local-up
make local-ingest-fixtures
make local-ingest-arxiv
make local-status
```

Then inspect:

- Cockpit: `http://localhost:3100`
- Document inspector: `http://localhost:3100/documents`
- Redpanda Console: `http://localhost:8080`
- MinIO Console: `http://localhost:9001`
- Prometheus: `http://localhost:9091`

Stop with `make local-down`. `make local-reset` is intentionally destructive
only to this compose profile's named volumes and should be used when a clean
dedup/catalog state is desired.

### 9.7 What the local run can prove

- Networked service wiring.
- Real arXiv acquisition and raw storage.
- Event movement through `raw.fetched`, `docs.normalized`,
  `curation.decisions`, `docs.curated`, and `decon.attest`.
- Native scientific HTML extraction and bounded Docling PDF fallback.
- Figure asset storage, Tesseract OCR, 26-class routing, and UI inspection.
- Actual full CPU classifier/filter invocation with no proxy mode.
- Deterministic accept, duplicate, heuristic, benchmark, and PII branches.
- Stateful near-duplicate behavior across repeated input.
- Append-only decision audit and accepted-only Gold Iceberg append.
- Complete signed attestation object and UI verification.
- DuckDB quality/as-of API mechanics.
- Prometheus scrape and UI integration.
- Full UI build and server-side API proxying.

The clean replay produced these concrete results:

- 9 durable decisions, 4 accepted Gold rows, and 1 physically separate
  benchmark-candidate row;
- routes: 4 quarantine, 4 reasoning candidate, 1 benchmark candidate;
- three real arXiv papers fetched as native HTML with `fallback=False`;
- the real FineWeb paper retained 27 of 28 sections, 17 figures, 13 tables, and
  34 equations while excluding its references;
- the real Dolma paper retained 117 of 120 sections, 5 figures, 28 tables, and
  36 equations;
- the controlled clean fixture retained 4 of 6 sections plus its figure,
  table, equation, caption, Docling class, and Tesseract text, while excluding
  acknowledgements, references, and author email metadata;
- the dashboard reported no unexplained `unknown` rejection;
- the contaminated decision reported its MMLU canary hit and its Ed25519
  signature verified from the canonical payload.

### 9.8 What it cannot prove

- Kubernetes scheduling, KEDA, NetworkPolicies, Gatekeeper, CRDs, or pod
  recovery.
- Real Polaris behavior.
- Distributed MinIO/Iceberg commit behavior.
- GPU pipelines.
- Calibrated FineWeb-Edu/KenLM/E5 accuracy before evaluation on a labeled
  scientific sample.
- Production capacity or daily volume.
- The quality of an eventual training run.
- Every existing UI page: SourceFeed and mixture operations are Kubernetes
  control-plane features.

## 10. Test matrix

| Capability | Unit/fixture | Local compose | Kubernetes |
|---|---:|---:|---:|
| arXiv HTML fetch | yes | yes | yes |
| raw MinIO object and Bronze event | yes | yes | yes |
| structured scientific extraction | yes | yes | yes |
| Gopher/C4/lang/MinHash | yes | yes | yes |
| real FineWeb-Edu/KenLM/E5 | yes | validated | required |
| PII regex | yes | yes | yes |
| benchmark exact-match reject | yes | yes | yes |
| durable reject quarantine | yes | yes | yes |
| complete signed attestation | yes | yes | yes |
| Iceberg append/query | yes | SQLite substitute | Polaris/MinIO |
| UI | component/tests/build | all local data pages validated | all pages |
| KEDA scale | no | no | yes |
| policy/egress controls | no | no | yes |
| failure recovery | process tests | three-worker restart passed | pod/node test |
| Docling PDF fallback | sample fixture | real 9-page strict-path validation | one constrained worker |
| benchmark agent/API | mocked | small opt-in | small opt-in |

## 11. Definition of done for the course project

The project is done when:

1. One command deploys the documented Kubernetes stack after secrets are
   supplied.
2. New arXiv data is visibly ingested through Redpanda.
3. Raw data and provenance are retained.
4. At least the required CPU curation signals run and expose metrics.
5. Accepted and rejected decisions are durable and explainable.
6. Benchmark-canary material is quarantined and included in a correct signed
   attestation.
7. Gold data is queryable through Iceberg/DuckDB.
8. The UI shows a complete document journey and current cluster state.
9. KEDA scale-up and one failure-recovery case are demonstrated.
10. Claims in README, slides, UI, and report match measured behavior.
11. Capacity figures are measured or marked `needs-measurement`.
12. Optional visual/agent features are labeled optional and cannot break the
    base pipeline.

## 12. Explicitly deferred work

- Training a language model on the corpus.
- Large historical corpus ingestion.
- Full semantic deduplication.
- GPU OCR or VLM services.
- Deep figure interpretation for every paper.
- Automated mixture optimization as a core requirement; the bounded N3
  experiment in Phase G remains an optional final showpiece.
- A large taxonomy of toxicity/safety classifiers.
- Full legal/license enforcement.
- Production-grade multi-region operation.
- Claiming compatibility with a particular frontier lab's undisclosed data
  format.

These can be future work without weakening the current project. The core
project becomes stronger by completing fewer paths with accurate evidence.

## 13. Immediate next actions after the validated local run

1. Freeze the nine-record local replay as the presentation regression set and
   keep `docs/LOCAL_PILOT_REPORT.md` synchronized with intentional changes.
2. Expand the bounded Docling evaluation beyond the validated nine-page sample
   and score sections, tables, equations, figures, OCR, warnings, latency, and
   peak memory across several PDF layouts.
3. Label a small scientific validation set and calibrate routing thresholds;
   do not infer accuracy from the nine demonstration records.
4. Deploy the same images to Kubernetes with real Polaris, then test
   SourceFeed CRDs, KEDA backlog scaling, NetworkPolicies, Gatekeeper, pod
   restart/replay, and cluster observability.
5. Reconcile decision, Gold, benchmark-reserve, and attestation counts after a
   controlled Kubernetes pod/node failure. The equivalent local three-worker
   restart already preserves the 9/4/1 regression counts.
6. Add the optional evidence-backed benchmark-item generator only after access
   controls and review state are clear.
7. Attempt the Phase G real shadow-mode mixture experiment last, with a hard
   external-GPU budget and cached demo results.

This sequence protects the course deliverable while still leaving room for the
scientific-quality and evolving-benchmark features that make Stream2Pretrain
interesting beyond the course.
