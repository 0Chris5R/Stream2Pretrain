# Local end-to-end test

This profile is prepared for a small Podman-first run of the actual
Stream2Pretrain data path. Docker Compose is also supported. It does not start
Kubernetes and it does not modify a remote repository.

## What the profile runs

```text
arXiv HTML, CPU PDF fallback, or controlled fixture
  -> MinIO Bronze + raw.fetched
  -> processor-fetcher
  -> Resiliparse/Docling + structured science + figures + Tesseract + figure routing
  -> lang ID + MinHash + validity
  -> docs.normalized
  -> processor-curate
  -> source-aware FinePDFs/FineWeb/code quality + KenLM + LSHBloom + PII + Decon-Gate
  -> curation.decisions (every outcome) + docs.curated (accepted subset)
  -> Iceberg decision audit + clean Gold + signed attestation
  -> local Iceberg warehouse + MinIO scientific assets + decon.attest
  -> DuckDB API + Prometheus + Next.js cockpit
  -> optional foundry -> signed SFT/RL packages + Post-training UI
```

The six controlled fixtures prove separate outcomes:

- `clean`: expected to reach `gold.curated`.
- `duplicate-clean`: byte-equivalent extracted content expected to exercise
  the durable near-duplicate rejection path after `clean`.
- `heuristic-canary`: expected to be rejected by the placeholder-boilerplate
  rule. Scientific braces are retained because they commonly encode valid
  formulas and set notation.
- `benchmark-canary`: expected to be rejected by the local MMLU canary Bloom.
- `benchmark-reserve`: expected to enter the separate benchmark-candidate
  Iceberg table and remain absent from clean Gold.
- `pii-canary`: expected to be rejected by the email detector.

They are synthetic and contain neither real benchmark material nor personal
information.

## Commands for a local run

Podman is selected automatically when it is installed. Force an engine with
`CONTAINER_ENGINE=podman` or `CONTAINER_ENGINE=docker`.

```bash
# Build and start the services, but do not ingest yet.
make local-up

# Ingest three real arXiv papers listed in local/arxiv_ids.txt.
make local-ingest-arxiv

# Inject six deterministic accept/duplicate/heuristic/decon/PII/reserve cases.
make local-ingest-fixtures

# Inspect containers, topic partitions, health endpoints, and UI URLs.
make local-status

# Follow service logs if a stage needs diagnosis.
make local-logs

# Stop while preserving state.
make local-down

# Explicitly destructive: stop and remove only this profile's named volumes.
make local-reset
```

## Resource budget

Use a Podman machine with 4 CPUs and 10-12 GB RAM; the validated pilot used a
16 GB VM for additional headroom. Keep 25-30 GB of free disk before the first
build because transient build layers coexist with the pinned FinePDFs v2,
FineWeb-Edu, KenLM, E5, Docling, and figure-classifier artifacts. The current
warm model volume is 7.3 GB; detailed measurements are recorded in
`docs/LOCAL_PILOT_REPORT.md`. The processor image was
2.38 GB, the UI image was 311 MB, the arXiv fetcher image was 351 MB, and the
six disposable runtime volumes together were about 419 MB after nine
documents.
Warm reruns reuse the model volume and images. The status script does not
pretend Compose can enforce a portable image/volume quota.

The compose profile applies per-container CPU and RAM caps. The curator has the
largest allowance at 2 CPUs and 8 GB because KenLM is mmap-backed and the
FinePDFs/FineWeb checkpoints run through PyTorch on CPU. The Podman machine remains
the aggregate hard ceiling. Compose does not impose a reliable cross-platform
disk quota on image storage or named volumes, so free disk must be checked on
the host before the build.

## Deliberate local substitutions

- Polaris is replaced by PyIceberg's SQLite SQL catalog and a shared local
  filesystem warehouse. This is the official PyIceberg development pattern,
  but it does not test Polaris, distributed object-store writes, or catalog
  concurrency.
- The profile downloads the pinned official FinePDFs Edu v2 and FineWeb-Edu
  Safetensors checkpoints, pinned E5-small-v2 ONNX graph, and pinned English Wikipedia KenLM binary plus
  its matching SentencePiece model. It also bakes Docling 2.114.0 layout,
  TableFormer, and formula artifacts plus the pinned 26-class Docling figure
  ONNX model. FinePDFs, FineWeb-Edu, and Docling use PyTorch CPU inference; E5 and figure
  routing use ONNX Runtime CPU inference; KenLM uses the publisher's
  normalization and SentencePiece recipe. Torch/TorchVision resolve from the
  official CPU wheel index, not CUDA. `S2P_REQUIRE_REAL_MODELS=1` makes startup
  fail instead of silently selecting a proxy. The same strict mode requires
  Resiliparse, fastlangid, Tesseract English, tiktoken, a real MinHash
  implementation, and a durable LSH backend.
- Presidio and `en_core_web_sm` are installed and required in addition to the
  regex/Luhn layer.
- MinHash uses `rensa`, and LSHBloom persists its index through `plyvel`
  (LevelDB). Strict startup rejects fallback state backends.
- The local SourceFeed service persists source definitions, schedules enabled
  feeds, and runs bounded RSS/Atom, OAI-PMH, sitemap, and native arXiv ingestion
  against the same Redpanda/MinIO data plane. Kubernetes reconciles SourceFeed
  CRDs into per-feed CronJobs; Gatekeeper, NetworkPolicies, KEDA, and
  MixtureRecipe control remain Kubernetes demonstrations.
- The local UI dashboard uses a small Prometheus instance. Loki, Tempo, and
  Grafana remain Kubernetes deployment tests.

## Post-training foundry

The base profile starts the foundry control API and Post-training page without
making model calls. The worker is behind the `foundry` profile and starts after
the Hetzner key is set and `Qwen3.8-27B` is visible through
authenticated discovery. The full contract and credential checklist are in
[`../docs/POSTTRAIN_FOUNDRY.md`](../docs/POSTTRAIN_FOUNDRY.md).

```bash
./scripts/foundry_local.sh base
./scripts/foundry_local.sh worker
./scripts/foundry_local.sh status
```

After the worker starts, open `http://localhost:3100/post-training` and select
`Run now`. This freezes the
current ranked candidate queue and uses the production worker path immediately,
without waiting for the daily UTC schedule. The button cannot bypass quota
reservation, licence checks, or artifact validation. Reviewers approve or
reject each generated artifact in the same page and enter their name manually.

The script does not delete local volumes. Official oracle execution is disabled
and remains later optional work.

## Validated pilot behavior

The profile was rebuilt, reset, and replayed on 2026-08-15. It produced nine
durable decisions: four clean training records, four quarantines, and one
physically separate benchmark candidate. All three real arXiv inputs used
native HTML without fallback. Exact Iceberg row counts were 4 in `curated`, 9
in `curation_decisions`, and 1 in `benchmark_candidates`. Every strict CPU
backend loaded successfully, and a contaminated decision's Ed25519 attestation
verified in the UI. See `docs/LOCAL_PILOT_REPORT.md` for the measured results.

Container image pulls and the processor/UI builds need network access and may
take time. The arXiv fetcher is deliberately polite and sleeps between
requests. A paper can be rejected without indicating a broken pipeline:
section-level PII, boilerplate, benchmark contamination, deduplication, or a
quality rule may legitimately fire. Author metadata and references are audited
separately and excluded from the training projection rather than automatically
rejecting an otherwise clean paper. That is why the controlled clean fixture
is the deterministic Gold-path test.

The local state is durable. The fixture batch itself includes a deliberate
second clean copy for the near-duplicate branch; rerunning it adds further
duplicate decisions. Use `make local-reset` only when a clean slate is
intentionally required.

Bytewax source offsets are checkpointed once per second beneath
`/var/lib/s2p/bytewax` on the project-owned `curator-state` volume for the
fetcher, curator, and Iceberg writer. The validated recovery test force-recreated
all three workers together without changing the 9 decision / 4 Gold / 1
benchmark counts. This proves the one-partition local restart path, not a
distributed exactly-once transaction across Kafka and Iceberg.
