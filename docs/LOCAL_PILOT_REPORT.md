# Stream2Pretrain local CPU pilot report

Date: 2026-08-15
Runtime: Podman machine on Apple Silicon, 16 GB VM allocation
Scope: six controlled fixtures and three real arXiv HTML papers
Result: passed

## 1. Outcome

The local profile was rebuilt from the current working tree, reset to empty
project state, and run end to end. It exercised MinIO Bronze storage,
Redpanda, structured scientific extraction, real CPU model inference,
section-aware curation, deduplication, privacy checks, benchmark
decontamination and isolation, Iceberg writes, DuckDB queries, Prometheus, and
the Next.js cockpit.

The final durable state is:

| Iceberg table | Rows | Meaning |
|---|---:|---|
| `gold.curation_decisions` | 9 | Every accepted, reserved, or quarantined outcome |
| `gold.curated` | 4 | Training-export records only |
| `gold.benchmark_candidates` | 1 | Physically separate benchmark reserve |

Final routes were four `reasoning_candidate`, four `quarantine`, and one
`benchmark_candidate`. The dashboard contained no unexplained `unknown`
rejection. Benchmark-reserve material was excluded from the training-export
count even though it passed its quality gates.

No remote repository was changed. The unrelated `open-webui` container on
port 3000 and `sap-ai-proxy` container on port 9090 were not stopped, rebuilt,
or reconfigured. Stream2Pretrain remained on its own ports, with the cockpit
at `http://localhost:3100`.

## 2. Input and routing results

| Input | Route | Composite | FinePDFs Edu v2 | Sections kept | Structured evidence | Blocking result |
|---|---|---:|---:|---:|---|---|
| Clean scientific fixture | reasoning candidate | 4.07 | 3.06 | 4/6 | 1 figure, 1 table, 1 equation | none |
| Byte-equivalent clean copy | quarantine | 4.07 | 3.06 | 4/6 | 1 figure, 1 table, 1 equation | `near_duplicate` |
| Privacy fixture | quarantine | 0.99 | 0.00 | 0/1 | none | `pii_detected`, insufficient body, low quality |
| Heuristic fixture | quarantine | 0.93 | 0.00 | 0/1 | none | C4 punctuation, insufficient body, low quality |
| Benchmark contamination canary | quarantine | 2.99 | 2.97 | 1/1 | none | exact MMLU decontamination hit |
| Fresh benchmark reserve fixture | benchmark candidate | 3.63 | 2.47 | 4/6 | 1 figure, 1 table, 1 equation | reserved from all training exports |
| arXiv HTML project paper | reasoning candidate | 3.80 | 2.96 | 16/18 | 4 figures, 1 equation, 27 citations | none |
| FineWeb paper | reasoning candidate | 4.59 | 4.38 | 27/28 | 17 figures, 13 tables, 34 equations, 243 citations | none |
| Dolma paper | reasoning candidate | 4.40 | 3.87 | 117/120 | 5 figures, 28 tables, 36 equations, 585 citations | none |

The three arXiv inputs were fetched from their native HTML endpoints with
`fallback=False`:

- `2402.08954`, the arXiv HTML project paper;
- `2406.17557`, the FineWeb paper;
- `2402.00159`, the Dolma paper.

This proves the run did not consist only of a custom local document.

The PDF fallback was then forced independently without changing the nine-row
demo state. The strict processor downloaded the complete nine-page PDF for
`2402.08954`, enforced a ten-page ceiling, and ran Docling, the figure router,
and Tesseract with two CPUs and an 8 GB container cap. After adding the tested
fallback for titles that Docling labels as ordinary text, it completed in 31.46
seconds and produced the correct paper title, 19 sections, 4 figure crops,
visible OCR text for 2 figures, a 3,467-word projection, and no document-level
extraction warnings.
The first deliberately too-small two-page ceiling rejected the nine-page input
instead of silently truncating it, confirming the guard behavior. A disposable
in-memory object sink kept this validation from adding a tenth decision or
changing the presentation dataset.

## 3. Faithful CPU backend validation

Strict mode used `S2P_REQUIRE_REAL_MODELS=1`. Startup would fail if a required
artifact silently fell back to a proxy. The validation command returned:

| Stage | Runtime backend or revision |
|---|---|
| FinePDFs Edu v2 | `transformers-cpu`, pinned official checkpoint, primary for scientific sources |
| FineWeb-Edu | `transformers-cpu`, pinned official checkpoint, same-section comparison/general-web primary |
| KenLM | `kenlm-sentencepiece:en.arpa.bin` |
| E5 decontamination | `onnxruntime-cpu` |
| PII | `regex-luhn-v1+presidio-en_core_web_sm` |
| MinHash | `rensa` |
| Durable LSH | `plyvel` / LevelDB |
| Language ID | `fastlangid-1` |
| HTML extraction | `resiliparse-0.14` |
| Tokenizer | `tiktoken` |
| Scientific PDF artifacts | Docling models present |
| Figure routing | pinned Docling 26-class ONNX model loaded |
| OCR | Tesseract English available |

The UI exposes these revisions per paper. FinePDFs Edu v2 is displayed
separately from the composite policy score; the two are not aliases. FinePDFs,
FineWeb-Edu comparison, and KenLM run over the exact same bounded,
role-stratified section sample. Every section still receives its cheap C4 and
PII checks, and unsampled expensive fields are omitted rather than fabricated.

The v1/v2 comparison used both real pinned checkpoints on the same 30 sampled
sections from the three real papers. FinePDFs v1 produced mean 0.8165 and
median 0.7409; v2 produced mean 3.6458 and median 3.9645. V2 was higher on all
30 sections, with a mean delta of 2.8293. This establishes that the selected
model version materially changes this scientific sample; it does not claim
accuracy because human labels are still pending. Exact per-section results are
in `validation/finepdfs-v1-v2-pilot.json`.

## 4. Scientific-document behavior

The controlled accepted fixture was inspected in the rendered UI and through
the API. The following behavior was verified:

- author data is retained as source metadata but excluded from the training
  projection;
- an author email produces a metadata-removal action rather than rejecting an
  otherwise clean paper;
- acknowledgements and references remain auditable but are excluded from the
  training projection;
- sections and paragraphs have stable local identifiers and include/exclude
  decisions;
- the structured table, equation, citation, figure, caption, original image,
  image hash, and source provenance remain available;
- the figure was classified as a line chart with 94.2% confidence by the real
  Docling ONNX model;
- Tesseract extracted the visible chart labels;
- the final projection includes bounded table, equation, and figure
  surrogates but no author email, acknowledgement section, or reference
  section.

The FineWeb paper demonstrated why whole-paper C4 rejection is inappropriate.
Its section named `3.5 Adding C4's filters` and an appendix containing valid
scientific braces were retained. The raw brace outcome remains visible as
`raw warning`, while the section decision correctly says `included`.

High-confidence body PII can isolate the affected part. Ambiguous phone-like
or IPv4-like numeric scientific strings remain audit signals and do not by
themselves destroy a paper. The final generated projection is scanned again
before routing.

## 5. Benchmark isolation and attestations

Two independent behaviors were exercised:

1. A synthetic exact n-gram canary matched the local MMLU demo reserve and was
   quarantined with `decontamination_hit`.
2. A clean, fresh benchmark candidate passed quality checks but was written to
   `gold.benchmark_candidates`, not `gold.curated`.

Every decision generated a decontamination attestation. The API returned them
in descending commit time. The contaminated attestation recorded 152 tokens
scanned, 152 flagged, one rejected document, and one MMLU hit. The cockpit
reconstructed the canonical payload and reported:

```text
ed25519 verify: signature valid
```

This is an actual local Ed25519 verification, not a decorative UI status.
The page accurately reports the local manifest as one demo canary with 1/5
non-empty benchmark families. The pinned real-reserve builder refuses partial
coverage and requires an authorised `HF_TOKEN` for GPQA before it will produce
a restricted MMLU/GSM8K/HumanEval/MATH/GPQA manifest. Sigstore/cosign remains
an optional deployment backend.

## 6. UI inspection

The final production image was reviewed in the in-app browser after the clean
replay.

- Dashboard: 9 durable decisions, 4 training-export papers, 44.4% export rate,
  and 1 benchmark-reserve item; distinct composite and source-quality
  histograms; explicit route/projection
  table; rejection reasons with no `unknown` bucket; per-source acceptance.
  Headline, rejection, and source counts are queried from Iceberg rather than
  process-local counters, so they remain correct after a worker restart. The
  separate Prometheus spark line is explicitly labeled as live activity.
- Documents: the three real papers by default and all nine durable decisions
  after explicitly enabling `Demo controls`; dashboard quarantine/reserve
  drill-downs enable those controls automatically. Compact route/source/tag,
  rejection, evidence, date, and score filters lead to the score vector,
  section pruning, privacy actions, per-section FinePDF/FineWeb/KenLM signals,
  structured artifacts, OCR, and final projection. Advanced provenance is
  collapsed.
- Sources: persistent add/edit/enable/delete/run-once behavior plus an automatic
  interval scheduler in Podman. The enabled arXiv SourceFeed executed
  successfully and emitted zero entries because the official feed skips
  Saturday; the independent bounded arXiv run then fetched all three native
  HTML papers with `fallback=False`. Kubernetes manifests reconcile each
  SourceFeed into a per-feed CronJob and support bounded run-once Jobs.
- Benchmark Safety: compact manifest coverage, automatic Ed25519 verification,
  one optional re-verify control, the latest signed scan, and collapsed history.
- Datasets: date-range, route, source, format, tag, and score selection; 3 real
  papers, 46,731 tokens, 44,737 source words, and 32,596 projection words in
  the default training export. JSONL returned three valid rows; Parquet had
  valid `PAR1` header/trailer bytes. The manifest preserved the exact Iceberg
  snapshot id as a string and every model/policy/extractor revision.
- Mixture: clearly labeled future work. It does not present the N3 stub as a
  measured local experiment.

Desktop and 390-pixel layouts were reviewed after the production-image build.
The navigation, document table, source table, filter controls, benchmark cards,
dataset form, modal, and long manifest identifiers remain contained without a
page-level horizontal overflow. The final dashboard and Documents pages were
inspected again against the clean nine-record state.

## 7. Tests and static verification

The final source tree passed:

```text
359 tests passed, including the live-stack integration checks
ruff: all checks passed
strict mypy: no issues in 172 source files
generated JSON Schemas: deterministic and current
Next.js TypeScript check: passed
Next.js ESLint: passed
Next.js production build: passed
Helm lint/template: passed
Podman Compose rendering: passed
strict CPU backend load check: passed
all local HTTP health surfaces: passed
```

The Iceberg table counts were read through PyIceberg after processing, rather
than inferred only from UI cards.

### 7.1 Restart and replay recovery

The first clean worker replacement exposed an actual defect: the Kafka group
names looked durable, but Bytewax deliberately disables Kafka auto-commit and
expects its own recovery configuration. Without that configuration, restarting
the three stream workers replayed all nine source events and doubled the
durable decision view. The contaminated disposable pilot state was removed and
the workers were wired to one-second Bytewax SQLite recovery snapshots on the
project's persistent `curator-state` volume.

The pilot was then reset and replayed from zero. After the snapshot files had
settled, all three streaming workers (`fetcher`, `curate`, and
`iceberg-writer`) were force-recreated together. Fifteen seconds after restart,
the result remained exactly:

```text
documents API:            9
gold.curated:             4
gold.curation_decisions:  9
gold.benchmark_candidates: 1
```

All workers were healthy and each had a non-empty recovery database. This is
evidence for clean local process-restart recovery with one partition and one
worker per stage. It is not a claim of distributed exactly-once commits:
Iceberg table updates and Kafka publications are still separate operations,
and a crash inside the one-second snapshot interval can require idempotent
handling. Pod/node failure with real Polaris remains a Kubernetes test.

## 8. Measured local resources

### 8.1 Disk

| Item | Measured size |
|---|---:|
| Shared model volume | 7.3 GB |
| Processor image | 2.38 GB |
| UI image | 311 MB |
| arXiv fetcher image | 351 MB |
| Redpanda runtime volume | 390 MB |
| MinIO pilot data | 8.7 MB |
| Curator state | 18 MB |
| Local Iceberg catalog/warehouse | 1.4 MB |
| Prometheus pilot data | 0.7 MB |
| Source control state | 4 KB |

Keep 20-25 GB free before the first build because transient build layers and
download caches temporarily coexist with the final images. Warm replays reuse
the 7.3 GB model volume. A source-only incremental processor Dockerfile avoids
duplicating the 2+ GB CPU dependency layer for ordinary code edits.
Superseded Stream2Pretrain processor and UI images
from the previous run were removed after their replacements built; no global
prune touched unrelated images.

### 8.2 Memory

Observed steady `podman stats` working sets after processing:

| Service | Working set | Configured cap |
|---|---:|---:|
| Curator | 857 MB | 8 GB |
| Redpanda | 419 MB | 1.5 GB |
| MinIO | 255 MB | 1 GB |
| Fetcher | 220 MB | 4 GB |
| Iceberg writer | 185 MB | 1.5 GB |
| DuckDB API | 175 MB | 1.5 GB |
| Decon API | 58 MB | 512 MB |
| Sources API | 77 MB | 512 MB |
| UI | 50 MB | 1 GB |
| Prometheus | 34 MB | 512 MB |
| Redpanda Console | 22 MB | 512 MB |

The curator can show a much larger process RSS inside the Linux VM because
KenLM is mmap-backed. `podman stats` and process RSS answer different questions, so a
10-12 GB Podman VM remains the practical minimum; 16 GB is comfortable for
the pilot. Per-container CPU and memory caps are enforced by Compose. Portable
per-container image/volume disk quotas are not promised.

## 9. Known limits

This run does not prove:

- Kubernetes CRDs, scheduling, KEDA scale-out, NetworkPolicies, Gatekeeper,
  pod/node recovery with real Polaris, or cluster observability;
- real Polaris authentication and concurrent distributed catalog commits;
- calibrated scientific-domain accuracy for FinePDFs Edu v2, FineWeb-Edu,
  KenLM, E5, or PII until the team completes the reviewed labels;
- full local real-benchmark coverage until the authorised GPQA token is
  supplied to the strict reserve builder;
- production throughput or daily volume;
- general Docling PDF accuracy, long-tail latency, or peak memory beyond the
  single validated nine-page sample;
- deep semantic understanding of every figure;
- training-data quality through an actual model ablation;
- the N3 two-branch proxy-LM experiment.

Those are deliberately not disguised as local successes. Human review of the
prepared 37-paper calibration/holdout manifest and the authorised GPQA reserve
build are external evidence/credential steps, not lighter substitute
implementations. The next systems demonstration is Kubernetes. N3 remains the
only deferred product feature and the final optional showpiece.

## 10. Reproduce and inspect

```bash
make local-up
make local-ingest-arxiv
make local-ingest-fixtures
make local-status
```

Primary pages:

- cockpit: `http://localhost:3100`
- dashboard: `http://localhost:3100/dashboard`
- documents and artifacts: `http://localhost:3100/documents`
- sources: `http://localhost:3100/sources`
- benchmark safety/signatures: `http://localhost:3100/decon`
- dataset builder/export: `http://localhost:3100/datasets`
- Redpanda Console: `http://localhost:8080`
- MinIO Console: `http://localhost:9001`
- Prometheus: `http://localhost:9091`

`make local-down` stops this project while preserving its volumes.
`make local-reset` intentionally removes only this compose profile's
disposable volumes and preserves the shared model volume.
