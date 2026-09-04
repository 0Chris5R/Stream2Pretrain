# Working agreement

README.md is the examination report. Component guides describe operation;
[the implementation reference](docs/PIPELINE_IMPLEMENTATION_REFERENCE.md)
records the exact processing rules and model prompts.

## Style and verification

- No emojis or em dashes. Use direct prose.
- Use `uv` for Python. Use `apply_patch` for source edits.
- Validate deterministic correctness before deploying. Do not run local
  containers, models, or paid provider calls without an explicit request.
- Do not use the workstation's default Kubernetes context. Cloud diagnostics
  use the configured GitHub Actions VPN workflow.
- Preserve unrelated files and running services. Never stop another project
  to free a port.
- Measure throughput, resource consumption and quality. Unknown values are
  `needs-measurement`.
- Do not weaken classifiers, truncate source coverage or remove safety checks
  to address CPU, RAM or disk pressure. Optimize demonstrated inefficiencies
  or request capacity.
- Do not create scheduled monitoring unless explicitly requested.
- Never commit credentials, source-corpus exports, teacher labels or optimizer
  checkpoints. Final classifier weights are public release artifacts.

## Product contract

- Name: Stream2Pretrain. Kappa architecture: Redpanda, Bytewax, MinIO,
  Iceberg V2 tables, Polaris and DuckDB.
- Active content sources: arXiv papers, Hugging Face model-card READMEs and
  Hugging Face dataset-card READMEs. Discovery envelopes only schedule work.
- Item-level licence policy: permissive rights allow both training uses;
  missing or reviewed grey-area rights allow derived post-training only;
  explicit incompatible rights quarantine before body retrieval. HF public
  README projections use the policy in
  [the licence matrix](docs/SOURCE_LICENSE_ADMISSION_MATRIX.md).
- Remove author blocks, references, navigation and repeated extraction content.
  Preserve substantive prose, math, structured tables, captions and evidence.
- Apply deterministic extraction, language, privacy and duplicate checks before
  learned scoring. Exact and near-duplicate state survives worker restarts.
- Four independent ModernBERT models use full retained sections with
  overlapping windows. See [classifiers](docs/CLASSIFIERS.md).
- Token-weighted quality gates: arXiv >=3.0, HF >=3.5. Confidence is not a gate.
  Do not remove individual sections based on score.
- Only quality-passing arXiv papers run mathematical-reasoning and
  post-training-suitability heads. Licence eligibility is independent.
- Mean post-training suitability ranks the daily queue. High-scoring sections
  only add optional task-designer hint sentences. Keep the existing paper input;
  no section-only input, class-5 requirement or forced task allocation.
- Persist scientific evidence in Gold before publishing a candidate. Transient
  source-object retention must not invalidate admitted Foundry work.
- Corpus totals include all policy generations with latest decision per document.
  Activity charts measure worker events, not additional durable documents.
- The cockpit is monitoring/export only, except named human approval or
  rejection per generated SFT/RL artifact. Details open in dialogs.

## Post-training contract

- [Foundry guide](docs/POSTTRAIN_FOUNDRY.md) defines the paper-to-SFT/RL pipeline.
- Hetzner Experiments Inference and exact `Qwen3.8-27B` serve all model roles.
  Retain catalogue identity and returned-model provenance.
- Each daily boundary freezes and ranks candidates received during the preceding
  24 hours. Process serially until the next boundary, cohort exhaustion or
  provider capacity exhaustion. No fixed daily paper cap.
- Upstream curation is the licence authority. The Foundry does not recheck it.
- Accept or reject SFT trajectories individually. RL acceptance requires
  executable positive, negative, adversarial and mutation checks.
- Split validated paper families independently within each SFT/RL pool:
  four to train, one to benchmark. Pretraining has no benchmark route.
- Human reviewer names are entered at audit time. Preserve all artifact audits.
- No provider qualification, availability heartbeat, arbitrary copy-ratio or
  answer-length gate, or W&B integration.
- N3 mixture comparison remains in the project. Its proxy-LM training interface
  is a scaffold, not a claim of measured GPU training or automatic quality gain.

## Examination requirements

The submission is one ZIP. No external files or running deployment are inspected.
README.md is the sole report and must cover:

1. Use case and motivation.
2. Data characteristics and Big Data V's.
3. Kappa architecture justification and diagram.
4. Components and end-to-end data flow.
5. Transformations, state, windows and late data.
6. Storage format, partitions, schema and lakehouse rationale.
7. Real UI connection and operating flow.
8. Kubernetes workloads, persistence and horizontal scaling.
9. Reproducible deployment and prerequisites.
10. Central code and manifest links with explanations.
11. Embedded UI, pod, serving and pipeline-output evidence.
12. Honest prototype limits and outlook.

Explain team contributions without inventing work or measurements. Preserve
the Redpanda/Bytewax lecture-stack justification. Distinguish demonstrated
horizontal scaling from stateful components that still require coordination.
