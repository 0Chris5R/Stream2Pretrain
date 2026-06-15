# Stream2Pretrain — Research & Implementation Plan

## 1. Executive Summary

Stream2Pretrain is a Kubernetes-native, streaming-first curation pipeline for LLM pretraining data. It ingests live web documents (RSS, sitemaps, manual submissions), runs FineWeb-class curation operators (HTML extraction, language ID, Gopher/C4 heuristics, MinHash near-deduplication, classifier-based quality scoring, PII filtering, benchmark decontamination) as long-running stateful stream operators on event-time semantics, and lands curated tokens into an Apache Iceberg lakehouse. Where every existing OSS curator (DataTrove, NeMo Curator, Dolma, data-juicer, data-prep-kit, DCLM, Pleias, MixtureVitae) is fundamentally batch-oriented and re-runs over fixed snapshots, Stream2Pretrain is built around a Redpanda+Bytewax data plane on k3s and emits incremental Iceberg snapshots gated by document publication-time watermarks.

The strongest novel angle that survived adversarial review is the integration shape rather than any single algorithm. Three differentiators came through: (a) a streaming Decon-Gate sidecar that emits a per-Iceberg-snapshot signed contamination attestation against MMLU/GSM8K/HumanEval/MATH/GPQA with replayable contamination bisect; (b) a per-document validity-interval column ([valid_from, valid_to)) propagated all the way into the token-shard manifest with an `as_of(timestamp)` query view, enabling deterministic contamination replay and time-conditioned training; and (c) a shadow-mode A/B mixture comparison primitive where two `MixtureRecipe` CRDs read the same live SourceFeed and a small proxy LM continuously trains on both branches with auto-promotion on perplexity-delta gates. These three are the demo headliners; the rest of the system is a faithful FineWeb recipe wired onto a streaming K8s substrate.

Mapped against the exam Bewertungsschema, Stream2Pretrain covers all required sections cleanly. It is a multi-component cloud system (ingestion, stream processor, lakehouse serving, UI) running on Kubernetes, hits all five V's of Big Data (volume via web crawl, velocity via streaming, variety via heterogeneous source schemas, veracity via quality+contamination scoring, value via curated training shards), uses justified lecture-aligned tech (k3s, Helm, KEDA, Prometheus, Loki, Traefik, FastAPI, Next.js) with a single begründete Abweichung (Redpanda + Bytewax for streaming, justified by Kafka-API compatibility and footprint on a 2-worker cluster), and produces three named novel features for the Bonus-Punkte category (Decon-Gate, validity intervals, shadow A/B).

The prototype is sized for a 4-6 week sprint on a single k3s cluster (1 control + 2 workers on DHBWCloud), targeting a sub-million-document end-to-end demo with all observability, autoscaling, and lakehouse semantics functional. Numbers like throughput targets and dedup recall are marked needs-measurement throughout.

## 2. State of the Art (June 2026)

| Name | What it does | Streaming? | K8s-native? | Gap left for Stream2Pretrain |
|---|---|---|---|---|
| HuggingFace DataTrove | FineWeb reference pipeline (extraction, MinHash, quality, tokenize) | No (Local/Slurm/Ray batch) | Via KubeRay only | No streaming runtime, no event-time, no lakehouse sink |
| NVIDIA NeMo Curator | GPU-accelerated curation (RAPIDS/Ray) with classifier zoo | No (intra-job pipelining only) | Via KubeRay | Re-runs per snapshot, no continuous ingestion, no Iceberg, GPU-required |
| AllenAI Dolma toolkit | Rust+Python taggers, mixers, Bloom dedup; built Dolma 3T | No | No | Pure CLI, no streaming, no per-doc license/temporal columns |
| Alibaba data-juicer | 100+ ops, Ray Data, streaming JSON reader | No (intra-job lazy I/O only) | Via KubeRay | "Streaming" is lazy file I/O, not Kafka-style ingestion |
| IBM data-prep-kit | KFP-on-Ray transforms, Parquet-centric | No (KFP DAGs over snapshots) | Yes (most K8s-native) | Scheduled batch DAGs, not streaming dataflows |
| DCLM (mlfoundations) | Benchmark + Resiliparse + fastText recipe | No | No | Time-frozen benchmark, not a runtime |
| Cerebras Model Zoo data prep | SlimPajama preprocessing, tokenflow CLI | No | No | Vendor batch CLI |
| Pleias Open Data Toolkit | OCRonos, Celadon, Topical, OCR-quality | No | No | Library of standalone scripts, no runtime |
| MixtureVitae | License-aware Tier 1/2/3 risk tiering, 422B tokens | No | No | Static curation scripts, batch-only |
| Common Pile v0.1 | 8TB public-domain corpus, per-doc license metadata | No | No | One-shot v0.1 release, no continuous pipeline |
| Bytewax | Python streaming dataflow (Rust core), Helm chart | Yes | Yes | No LLM-curation operators shipped |
| Apache Flink + flink-k8s-operator | JVM stream engine, RocksDB checkpointing | Yes | Yes | No FineWeb templates, JVM ops footprint |
| Pathway | Rust+Python ETL, llm-app live indexing | Yes | Docker only | RAG-focused, no pretraining curation, no Iceberg |
| RisingWave | Streaming SQL DB, Iceberg sink, MCP server | Yes | Yes | SQL UDFs awkward for MinHash/HTML extraction |
| Redpanda Connect (Benthos) | YAML connectors, AI processors | Yes | Yes | No FineWeb ops, no MinHash, no quality classifier |
| Crawl4AI / Firecrawl | LLM-friendly crawlers, clean Markdown | Per-page only | Docker | Crawl tier only, no dedup/PII/quality/lakehouse |
| Apple ml-tic-lm (TiC-LM) | 114-month CC benchmark, replay schedules | No (static slices) | No | Benchmark release, not a service |
| Mixtera (ETH) | Declarative versioned mixture queries, dynamic ADO | Train-time only | No | Read plane; no ingestion, no curation operators |

The space splits cleanly into four camps. (1) **Curation toolkits** (DataTrove, NeMo Curator, Dolma, data-juicer, data-prep-kit) ship every operator Stream2Pretrain needs but execute as discrete batch jobs over snapshots; none has Kafka, event-time watermarks, or stateful operators across job boundaries. (2) **Streaming engines** (Bytewax, Flink, Pathway, RisingWave) ship the runtime but no LLM-curation operator library. (3) **Crawlers** (Crawl4AI, Firecrawl, Scrapy Cluster) produce clean text and stop. (4) **Lakehouse substrate** (Iceberg V3, Polaris) provides row lineage and deletion vectors but knows nothing about AI semantics.

The empty quadrant is a streaming, K8s-native, opinionated implementation of the FineWeb stage graph that lands on a curated Iceberg lakehouse with first-class temporal and contamination metadata. Every component exists; nobody has integrated them into a single deployable curator with a cockpit UI. This is the Stream2Pretrain wedge. Honest caveat: HuggingFace adding a streaming executor to DataTrove, or NVIDIA bolting Iceberg + license signals onto NeMo Curator, would collapse two of the four moat legs - so the streaming-K8s-lakehouse axis must remain a first-class differentiator alongside the legal/temporal/contamination signals.

## 3. Where Stream2Pretrain is Genuinely New

Of the 14 novelty candidates put through adversarial verification, 7 survived. Ranked by demo + exam value:

### TOP 3 (build these)

**N1. Streaming Decon-Gate with per-snapshot signed contamination attestation**
- One-liner: Inline 13-gram Bloom + embedding-sketch contamination scan during ingestion, emitting a signed certificate per Iceberg snapshot.
- Why-novel: NeMo Curator has the scan logic but as a batch CLI; no project ships it as a streaming sidecar bound to snapshot commits with Sigstore-style attestation.
- Refined surviving claim: An Iceberg-snapshot-bound, signed contamination attestation artifact (per-benchmark hit counts, rejected-document hashes, benchmark-set version pin) emitted by an event-sourced inline streaming operator, enabling "contamination bisect" via Kafka-offset replay.
- Evidence of absence: Surveyed NeMo Curator, LLMSanitize, OpenCompass contamination_eval, Datatrove, Dolma, data-juicer, awesome-data-contamination index, 2025 NAACL contamination survey - all post-hoc/batch.

**N2. Per-document validity-interval column with as_of() temporal query view**
- One-liner: Each curated document carries a typed [valid_from, valid_to) interval propagated to the token-shard manifest, with an Iceberg view returning the deterministic token mixture for any timestamp.
- Why-novel: Hindsight Corpus proposes per-doc temporal annotation conceptually; Time-Aware LMs (arXiv 2106.15110) prepends a single timestamp. No system propagates intervals to token shards or exposes as_of() at the curator level.
- Refined surviving claim: A curator that writes a typed validity interval populated by ingest operators (HTTP Last-Modified, schema.org datePublished, Wayback first-seen, license effective date, retraction date), propagates it to the token-shard Parquet manifest as a column on token-id ranges, and exposes as_of(timestamp) for deterministic training selection and post-hoc contamination replay.
- Evidence of absence: Searched Hindsight Corpus, Time-Aware LMs, MixtureVitae, Common Corpus, TelaMentis, GraphRAG-temporal, Time-Travel-in-LLMs, Dated Data, USENIX OpML20.

**N3. Shadow-mode A/B mixture comparison via two MixtureRecipe CRDs**
- One-liner: Two MixtureRecipe CRDs subscribe to the same live SourceFeed, materialize separate lakehouse branches, a small proxy LM continuously trains on each branch on a rolling window, and per-domain perplexity deltas gate promotion.
- Why-novel: Mixtera + ADO does dynamic mixture but at train-time read plane; Argo Rollouts/Flagger do shadow but for inference services not data recipes.
- Refined surviving claim: Argo-Rollouts/Flagger progressive-delivery transplanted onto a streaming data-curation substrate, with proxy-LM perplexity-delta as the AnalysisTemplate signal. The composite primitive of live-stream forked recipes with continuous proxy-LM gating has no published or OSS instance.
- Evidence of absence: Argo Rollouts, Flagger, Mercari shadow A/B, Mixtera, ADO, DoReMi, Olmix, CLIMB, RegMix, DCLM, FineWeb, LakeFS, Pachyderm, DVC.

### Surviving but lower priority (nice-to-mention, build if time)

**N4. Watermarked event-time curation with Iceberg snapshots-as-deltas** (partially-novel; Apache Amoro and Iceberg issue #6514 anticipate the watermark-in-snapshot-property mechanism). Refined kernel: publication-time watermarks (distinct from crawl-time/processing-time) plus TiC-LM-style replay-mixture recipes embedded in snapshot properties.

**N5. End-to-end OpenTelemetry trace from URL discovery to token byte-offset, trace_id materialized into the Iceberg manifest** (partially-novel; pattern documented in 2026 observability blogs but not shipped in any OSS LLM curator). Refined kernel: shipping K8s-streaming curator that binds spans to GitOps revisions for one-click forensic drilldown.

**N6. Argo-Rollouts canary of the quality classifier with corpus-shape KPI gates** (partially-novel; canary-on-data-drift is established, but classifier-as-the-gated-component for curation pipelines with FineWeb-Edu retention / language-mix / near-dup-density / decontamination-overlap KPIs is not). 

**N7. MixtureController + SourceFeed CRDs for ingestion-time throttling** (partially-novel; Mixtera/ADO controls train-time read, the surviving kernel is shifting feedback upstream to the ingest layer as K8s reconciled state with workqueue rate limiters).

### Refuted (do not claim)

- Verifiable Crawl-Compliance Receipts (PEAC Protocol does this verbatim).
- Replayable Mixture Ledger (Unlearning at Scale arXiv 2508.12220 already does bit-identical WAL replay with versioned manifests).
- Rolling drift detector + per-source half-life (Velocitune, ADO, TiKMiX cover the algorithmic kernel).
- Declarative quality SLOs as K8s objects (Sloth, Keptn, Acceldata adaptive thresholds, Delta Live Tables expectations cover this).
- Contamination risk-tier passport (Yang et al. arXiv 2406.14644 already defines clean/not-clean/dirty tiers; LabelSets ships them as dataset-passport fields).
- SourceFeed Operator + per-source NetworkPolicy egress jail (Strimzi+Camel KafkaConnector RSS source + Tigera NetworkPolicy auto-gen + OPA Gatekeeper data CRDs cover the constituent parts).

## 4. Architecture

```
                        +------------------+
                        |  Next.js UI      |  (dashboard, curation cockpit, Decon-Gate viewer)
                        +--------+---------+
                                 |
                          REST/WebSocket
                                 |
+----------------+      +--------v---------+      +--------------------+
| RSS / Sitemap  +----->+ FastAPI Submit   +----->+  Redpanda          |
| Pollers (CronJob) |   | (validation,     |      |  topics:           |
| Manual /submit |      |  rate-limit,     |      |  raw.fetched       |
+----------------+      |  license tag)    |      |  docs.normalized   |
                        +------------------+      |  docs.curated      |
                                                   |  decon.attest      |
                                                   +---------+----------+
                                                             |
                                                             | Kafka API
                                                             v
+--------------------------------------------------------------------+
|  Bytewax Stream Processors (KEDA-scaled by topic lag)              |
|                                                                    |
|  fetcher  -> trafilatura/resiliparse extract -> langID (fastText)  |
|     |                                                              |
|     v                                                              |
|  Gopher/C4 stateless taggers -> MinHash signature (Rensa)          |
|     |                                                              |
|     v                                                              |
|  LSHBloom near-dup (band-partitioned, RocksDB-checkpointed)        |
|     |                                                              |
|     v                                                              |
|  FineWeb-Edu classifier (ONNX INT8) + KenLM perplexity             |
|     |                                                              |
|     v                                                              |
|  PII regex + Decon-Gate (13-gram Bloom + E5 embedding sketch)      |
|     |                                                              |
|     v                                                              |
|  validity-interval enricher  ->  Iceberg writer                    |
+--------------------------------------------------------------------+
                                                             |
                                                             v
                                  +--------------------------+--------------+
                                  |  MinIO (S3) + Apache Iceberg V3        |
                                  |  - bronze (raw fetched)                |
                                  |  - silver (normalized + tagged)        |
                                  |  - gold   (curated, mixture-ready)     |
                                  |  - decon_attestations (signed)         |
                                  +-------------------+--------------------+
                                                      |
                                                      v
                                       DuckDB serving (UI queries)
                                       Iceberg REST catalog (Polaris-lite)

Cross-cutting: Prometheus + ServiceMonitor, Loki labels, Traefik IngressRoute,
cert-manager, NetworkPolicies, OPA Gatekeeper for SourceFeed admission.
```

### Component-to-exam-section mapping

| Exam README section | Component(s) |
|---|---|
| Use Case | RSS/sitemap pollers + FastAPI submit + curated Iceberg shards |
| Five V's (Volumen/Vielfalt/Geschwindigkeit/Wahrhaftigkeit/Wert) | Crawl scale (V), heterogeneous schemas (V), Redpanda streaming (G), quality+decon scoring (W), curated training shards (W) |
| Architecture | Mermaid above + textual breakdown |
| Components | Each named box: RSS poller, FastAPI, Redpanda, Bytewax workers, Iceberg writer, MinIO, DuckDB, Next.js UI, Decon-Gate sidecar |
| Processing | Bytewax dataflow + stateful operators + windowing |
| Storage | MinIO + Iceberg + DuckDB + Redpanda topics |
| UI | Next.js dashboard with shadcn/ui, live curation rates, Decon-Gate certificate viewer |
| K8s | k3s on DHBWCloud, Helm releases, KEDA, ServiceMonitor, NetworkPolicies, Gatekeeper |
| Deployment | Single `helm install` with Helmfile, GitOps via Argo CD optional |
| Code | Monorepo with /charts, /ingest, /processor, /ui, /infra |
| Screenshots | Demo Story section |
| Limits | Risks section |

## 5. Technology Stack (Final Picks With Reasoning)

| Layer | Pick | Runner-up | Justification |
|---|---|---|---|
| K8s distribution | k3s on DHBWCloud OpenStack | kind (laptop) | Lecture stack, single-binary, fits 1+2 worker VMs |
| Packaging | Helm + Helmfile | Kustomize | Lecture default; GitOps-ready |
| Event log | Redpanda (single-binary, Kafka API) | Strimzi/Kafka | Begründete Abweichung: 3-4x lower RAM than JVM Kafka, Kafka API preserves Iceberg connector compatibility, KEDA kafka trigger works unchanged |
| Stream engine | Bytewax (Python, Rust core) | Apache Flink + PyFlink | Python-native operators, Helm chart, no JVM heap tuning on 2 workers; Flink is heavier and less educational for a demo |
| Object store | MinIO | Ceph RGW | Lecture default, single chart, S3-compatible |
| Table format | Apache Iceberg V3 | Delta Lake | Row lineage (_row_id), deletion vectors, vendor-neutral catalog (Polaris); Delta is more Databricks-centric |
| Catalog | Apache Polaris (lite mode) | Nessie | REST catalog, RBAC, Iceberg-native |
| HTML extraction | Resiliparse | Trafilatura | DCLM-Baseline default, +2.5pt Core lift, ~8x faster (per arXiv 2602.19548) |
| MinHash compute | Rensa (Rust) | datasketch (pure Python) | 16-40x faster signature compute |
| Near-dup index | LSHBloom (band-partitioned Bloom) | datasketch MinHashLSH | Append-only, constant memory, fits Bytewax stateful operator partitioned by band-key (per arXiv 2411.04257) |
| Exact dedup | rbloom (Rust mmap Bloom) | pybloomfiltermmap3 | Rust speed, mmap-shareable across workers |
| Quality classifier | FineWeb-Edu (snowflake-arctic-embed-m + linear head, ONNX INT8) | NVIDIA Nemotron-CC classifier | Open weights on HF, CPU-runnable, ~1k docs/sec/core INT8 |
| Perplexity scorer | KenLM Python bindings + edugp/kenlm pretrained .bin | CCNet from scratch | mmap-based, ~10MB/s/core, multilingual |
| Heuristic taggers | Dolma Rust taggers (via PyO3) | Pure-Python re-impl | Production Gopher/C4 implementation, hundreds of MB/s/core |
| Validity-interval enricher | Custom Bytewax operator | None | Novel; populates [valid_from, valid_to) from HTTP Last-Modified + schema.org + Wayback |
| Contamination guard | Custom Decon-Gate sidecar (13-gram Bloom + E5-small ONNX) | Datatrove decontaminator (batch) | Novel streaming variant; signs attestations with cosign |
| Lakehouse query | DuckDB (with iceberg extension) | Trino/Athena | Single binary, fast for prototype, fits one Pod |
| Submit API | FastAPI + Pydantic | Flask | Type-safe, auto-OpenAPI, async |
| UI | Next.js 14 (App Router) + shadcn/ui + TanStack Query | SvelteKit | Lecture-aligned ecosystem, rich component lib |
| Autoscaling | KEDA (Kafka lag trigger) | HPA on CPU | Lecture default, native consumer-group lag scaling |
| Observability | kube-prometheus-stack + Grafana + Loki + Tempo | Datadog | Lecture default; Tempo enables OTel trace_id-in-Iceberg novelty |
| Ingress + TLS | Traefik + cert-manager | Nginx + cert-manager | Lecture default |
| Policy | OPA Gatekeeper | Kyverno | More expressive for SourceFeed CRD constraints |
| GitOps | Argo CD (optional bonus) | Flux | Better UI for demo |

## 6. Data Model

### Bronze (raw fetched)
```json
{
  "doc_id": "sha256:e3b0c44...",
  "url": "https://example.com/post/2026-06-12",
  "fetched_at": "2026-06-15T10:23:11Z",
  "http_status": 200,
  "http_last_modified": "2026-06-12T08:00:00Z",
  "content_type": "text/html",
  "raw_html_s3_uri": "s3://bronze/2026/06/15/e3b0c44.html.gz",
  "source_feed": "rss-arxiv-cs",
  "trace_id": "4bf92f3577b34da6a3ce929d0e0e4736"
}
```

### Silver (normalized + tagged)
```json
{
  "doc_id": "sha256:e3b0c44...",
  "url": "https://example.com/post/2026-06-12",
  "title": "...",
  "text": "...",
  "lang": "en",
  "lang_score": 0.97,
  "extracted_with": "resiliparse-0.14",
  "tags": {
    "gopher_pass": true,
    "c4_nopunc_pass": true,
    "perplexity": 142.7,
    "perplexity_bucket": "head"
  },
  "minhash_sig": "<binary, 112 perms>",
  "near_dup_cluster_id": null,
  "valid_from": "2026-06-12T08:00:00Z",
  "valid_to": null,
  "valid_from_source": "http_last_modified",
  "trace_id": "4bf92f3577b34da6a3ce929d0e0e4736"
}
```

### Gold (curated, mixture-ready) — the data passport
```json
{
  "doc_id": "sha256:e3b0c44...",
  "text": "...",
  "lang": "en",
  "tokens": 1843,
  "quality_score": 4.2,
  "edu_score": 4.0,
  "license": "CC-BY-4.0",
  "license_source": "html_meta",
  "risk_tier": 1,
  "pii_flags": [],
  "contaminated_with": [],
  "valid_from": "2026-06-12T08:00:00Z",
  "valid_to": null,
  "reject_reasons": [],
  "scoring_version": "v0.4.2",
  "classifier_revision": "fineweb-edu-onnx-int8-2026-05-31",
  "policy_revision": "git:7a3b21c",
  "snapshot_id": 84219315,
  "trace_id": "4bf92f3577b34da6a3ce929d0e0e4736",
  "_row_id": 18374
}
```

### Decon attestation (signed, one per snapshot)
```json
{
  "snapshot_id": 84219315,
  "committed_at": "2026-06-15T10:30:00Z",
  "benchmark_set_version": "v2026-06-01",
  "benchmarks": ["MMLU", "GSM8K", "HumanEval", "MATH", "GPQA"],
  "tokens_scanned": 4823910,
  "tokens_flagged": 217,
  "rejected_doc_hashes": ["sha256:..."],
  "per_benchmark_hits": {"MMLU": 41, "GSM8K": 12, "HumanEval": 0, "MATH": 164, "GPQA": 0},
  "signature": "<cosign Ed25519 over canonical JSON>",
  "signer_cert": "<x509 PEM>"
}
```

### Partitioning scheme
- Bronze: `s3://bronze/year=YYYY/month=MM/day=DD/source=<feed>/...html.gz` (Hive-style for cheap pruning)
- Silver Iceberg: PARTITION BY `lang`, `bucket(16, doc_id)`
- Gold Iceberg: PARTITION BY `lang`, `risk_tier`, `month(valid_from)` (enables temporal as_of() pruning)
- Decon attestations: separate Iceberg table partitioned by `month(committed_at)`

## 7. Implementation Plan (4-6 week sprint)

### Week 1 — Bootstrap

Deliverables: k3s cluster (1+2) on DHBWCloud, Helm chart skeleton, MinIO, Redpanda, Prometheus, Loki, Traefik all green.

Files to create:
- `infra/terraform/openstack.tf` (3 VMs)
- `infra/k3s-install.sh`
- `charts/stream2pretrain/Chart.yaml`, `values.yaml`
- `charts/stream2pretrain/templates/{minio,redpanda,monitoring}.yaml`
- `helmfile.yaml`

Demo commands:
```
helmfile -f helmfile.yaml apply
kubectl get pods -A
kubectl port-forward svc/redpanda-console 8080
```

### Week 2 — Ingestion

Deliverables: RSS poller CronJob, sitemap poller CronJob, FastAPI submit endpoint, fetcher Bytewax dataflow with trafilatura/resiliparse, raw docs landing in `raw.fetched` Redpanda topic, MinIO bronze bucket populated.

Files:
- `ingest/rss_poller/main.py` + Dockerfile + CronJob
- `ingest/sitemap_poller/main.py`
- `ingest/submit_api/main.py` (FastAPI)
- `processor/fetcher.py` (Bytewax dataflow)
- `charts/stream2pretrain/templates/{ingest,processor-fetcher}.yaml`

Demo:
```
curl -X POST https://stream2pretrain.demo/submit -d '{"url":"..."}'
kubectl logs -l app=fetcher -f
rpk topic consume raw.fetched
mc ls minio/bronze/
```

### Week 3 — Stream processor

Deliverables: full curation Bytewax dataflow (Gopher/C4 taggers, MinHash via Rensa, LSHBloom near-dup with RocksDB checkpointing, FineWeb-Edu ONNX classifier sidecar, KenLM perplexity, PII regex), windowing on event-time, watermarks from `http_last_modified`, validity-interval enricher.

Files:
- `processor/curate.py` (main Bytewax dataflow)
- `processor/operators/{minhash,lshbloom,quality,kenlm,pii,validity}.py`
- `processor/models/fineweb-edu.onnx` (downloaded in init container)
- `charts/stream2pretrain/templates/processor-curate.yaml` + KEDA ScaledObject on `docs.normalized` lag

Demo:
```
kubectl get scaledobject
kubectl get hpa
rpk topic consume docs.curated | jq .quality_score
```

### Week 4 — Lakehouse + UI

Deliverables: Iceberg writer Bytewax sink, Polaris-lite REST catalog, DuckDB query pod with iceberg extension, Next.js dashboard with live throughput, per-source rates, quality histogram, validity-interval timeline view, Decon-Gate attestation viewer.

Files:
- `processor/iceberg_writer.py`
- `charts/stream2pretrain/templates/{polaris,duckdb-server}.yaml`
- `ui/app/{dashboard,sources,decon,as-of}/page.tsx`
- `ui/components/{quality-histogram,timeline,attestation-viewer}.tsx`
- `ui/lib/duckdb-client.ts`

Demo:
```
duckdb> SELECT lang, COUNT(*), AVG(quality_score) FROM gold GROUP BY lang;
duckdb> SELECT * FROM gold_as_of('2026-05-01') LIMIT 10;
open https://stream2pretrain.demo/dashboard
```

### Week 5 — K8s hardening

Deliverables: KEDA scalers on every consumer, ServiceMonitor + Grafana dashboards, Loki labels (source_feed, snapshot_id), NetworkPolicies (default-deny + per-source egress), OPA Gatekeeper SourceFeed CRD validation, cert-manager TLS on Traefik.

Files:
- `charts/stream2pretrain/templates/{networkpolicies,gatekeeper-constraints}.yaml`
- `charts/stream2pretrain/templates/grafana-dashboards/stream2pretrain.json`
- `charts/stream2pretrain/crds/sourcefeed.yaml`

Demo: kill a pod, watch KEDA scale; apply a SourceFeed without license field, watch Gatekeeper reject.

### Week 6 — Bonus features (top novelty pick)

Deliverables: Decon-Gate sidecar shipping signed attestations to `decon.attest` topic, attestation viewer in UI, MixtureRecipe CRD + shadow A/B controller (if time) OR validity-interval `as_of()` query view fully wired (priority).

Files:
- `processor/decon_gate.py` + `processor/sign.py` (cosign integration)
- `charts/stream2pretrain/templates/decon-gate-sidecar.yaml`
- `processor/mixture_controller/` (if time)
- Final README + screenshots

Demo:
```
curl -X POST /submit -d '{"url":"<known MMLU stem>"}'
# Decon-Gate viewer shows the rejection + signed certificate
cosign verify-blob --certificate cert.pem --signature sig.bin attestation.json
```

## 8. Repo Layout

```
stream2pretrain-curator/
├── README.md                    # Use case, V's, screenshots, limits
├── CLAUDE.md                    # Dev notes, decision log
├── charts/stream2pretrain/             # Single Helm chart, all components
│   ├── Chart.yaml
│   ├── values.yaml
│   ├── crds/                    # SourceFeed, MixtureRecipe CRDs
│   └── templates/
├── helmfile.yaml                # Top-level deploy: this chart + deps
├── infra/                       # Terraform for OpenStack VMs, k3s install
├── ingest/
│   ├── rss_poller/              # CronJob
│   ├── sitemap_poller/          # CronJob
│   └── submit_api/              # FastAPI Deployment
├── processor/                   # Bytewax dataflows
│   ├── fetcher.py
│   ├── curate.py
│   ├── iceberg_writer.py
│   ├── decon_gate.py
│   ├── operators/               # Reusable Bytewax operators
│   └── models/                  # ONNX classifier, KenLM bin (downloaded)
├── ui/                          # Next.js 14 app
│   ├── app/
│   ├── components/
│   └── lib/
├── docs/                        # Architecture diagram source, design notes
├── scripts/                     # Demo helpers (load_seed_feeds.sh, etc.)
└── tests/                       # pytest for operators, k6 for API
```

## 9. Demo Story / Screenshot Plan

The grader sees, in 5 minutes from screenshots alone:

1. **Architecture overview** (`docs/architecture.png`): the Mermaid diagram with all components labeled.
2. **`kubectl get pods -A`** showing all 18-22 pods running across 3 nodes, including Redpanda, Bytewax workers, MinIO, Polaris, UI, Decon-Gate sidecar.
3. **Grafana dashboard** showing throughput per stage (docs/sec at fetch, normalize, curate, write), KEDA replica counts, Redpanda lag per topic.
4. **Next.js cockpit** with live counters (last hour: N docs ingested, N curated, N rejected by reason), quality-score histogram, per-source acceptance rates.
5. **Decon-Gate attestation viewer**: click a snapshot, see the signed certificate (benchmarks scanned, hits per benchmark, signature verification status).
6. **as_of() temporal view**: a date picker; the table shows the deterministic token mixture as it would have been at that timestamp.
7. **DuckDB query**: `SELECT lang, COUNT(*), SUM(tokens) FROM gold WHERE risk_tier=1 GROUP BY lang;`
8. **Submit-and-trace flow**: POST a URL via `curl`, then jump to Tempo/Jaeger and watch the trace propagate from FastAPI through Redpanda, fetcher, curator, into the Iceberg write span.
9. **KEDA scale-up**: a load generator pumps 10k URLs; the curator pod count climbs from 1 to 6 in real time.
10. **Failure recovery**: `kubectl delete pod` on a curator; Bytewax restores from RocksDB checkpoint and resumes without duplicates (Redpanda transactional consumer).

## 10. Risks & Open Questions

- **Iceberg-rust on small data may be overkill**: at sub-million-doc scale, Parquet-on-MinIO without a catalog is enough. needs-measurement: justify the Iceberg overhead in throughput numbers; if measured snapshot-commit cost > 5% of ingest time, fall back to Hive-style partitioned Parquet for bronze and use Iceberg only for silver/gold.
- **License detection is heuristic**: HTML meta + heuristic regex; can mis-classify. Document this honestly in the README - Stream2Pretrain is best-effort, not a legal compliance tool.
- **LSHBloom is research-grade**: reference impl is Globus Labs C++/Python hybrid. Plan: reimplement in ~300 LOC in Rust+PyO3 inside the Bytewax operator; if Week 3 runs over, fall back to datasketch MinHashLSH with Redis backend.
- **FineWeb-Edu classifier is English-biased**: multilingual quality scoring needs a per-language classifier; mark as future work.
- **Cosign signing in K8s requires a key management story**: prototype uses a single in-cluster Ed25519 key in a Secret; a real deployment would use Sigstore Rekor.
- **Throughput numbers are unmeasured**: do not invent docs/sec; benchmark on the actual k3s cluster in Week 5 and report measured values.
- **2 worker nodes is tight for Redpanda 3-broker**: run 1-broker dev mode and document the limitation; 3-broker would need 4 VMs.
- **ONNX classifier inference latency vs streaming throughput**: unmeasured. If ONNX INT8 cannot keep up with peak load, add a Triton inference server with batching.
- **Validity-interval source signals can disagree** (HTTP Last-Modified vs schema.org datePublished vs Wayback first-seen): document the precedence rule in the data passport.
- **GPAI Code of Practice template auto-generation is not in scope** for the prototype; mention as future work to keep the legal-signals story alive without overpromising.

## 11. Open-Source Path After Submission

- **Publishing**: GitHub repo `stream2pretrain-curator/stream2pretrain-curator` under Apache-2.0; tagged release v0.1.0 with the chart pushed to a GitHub Pages Helm repo.
- **README positioning**: "Streaming, K8s-native FineWeb. DataTrove operators on Bytewax, lands in Iceberg. Includes Decon-Gate (signed contamination attestations) and validity intervals (as_of() queries) - features no other OSS curator ships." Do not claim parity on operator surface with NeMo Curator (their classifier zoo is broader); claim parity on the FineWeb baseline plus the three novel primitives.
- **License**: Apache-2.0 for code, ODC-By for any released sample dataset, OpenRAIL for any released classifier.
- **Naming conflicts to avoid**: "Stream2Pretrain" overlaps with FreshLLMs/FreshQA (both from the freshness-evaluation literature). Recommend rebranding to `Curatr`, `Stratify-LM`, or `Cataract` (curated cataract = waterfall of curated text). Verify on PyPI, Helm Hub, GitHub before announcing.
- **Differentiation against incumbents**: explicit comparison table in the README - DataTrove (no streaming, no Iceberg), NeMo Curator (no streaming, GPU-required), Bytewax (no curation operators), Pathway (RAG-only). Cite each project respectfully and link.
- **Community gateway**: post to r/MachineLearning, HN, the Bytewax community Slack, and the EleutherAI Discord. Submit a workshop paper to MLSys / NeurIPS Datasets and Benchmarks.

## 12. Sources

1. https://github.com/huggingface/datatrove
2. https://github.com/NVIDIA-NeMo/Curator
3. https://github.com/allenai/dolma
4. https://github.com/modelscope/data-juicer
5. https://github.com/data-prep-kit/data-prep-kit
6. https://github.com/mlfoundations/dclm
7. https://github.com/Pleias/open_data_toolkit
8. https://github.com/ontocord/mixturevitae
9. https://github.com/r-three/common-pile
10. https://huggingface.co/datasets/PleIAs/common_corpus
11. https://github.com/bytewax/bytewax
12. https://github.com/apache/flink-kubernetes-operator
13. https://github.com/risingwavelabs/risingwave
14. https://github.com/MaterializeInc/materialize
15. https://github.com/quixio/quix-streams
16. https://github.com/pathwaycom/pathway
17. https://github.com/redpanda-data/connect
18. https://github.com/ArroyoSystems/arroyo
19. https://github.com/unclecode/crawl4ai
20. https://github.com/firecrawl/firecrawl
21. https://trafilatura.readthedocs.io
22. https://github.com/chatnoir-eu/chatnoir-resiliparse
23. https://github.com/commoncrawl/cc-pyspark
24. https://github.com/cxcscmu/Craw4LLM
25. https://github.com/apple/ml-tic-lm
26. https://github.com/freshllms/freshqa
27. https://realtimeqa.github.io/
28. https://allenai.org/olmo
29. https://github.com/Spawning-Inc/datadiligence
30. https://github.com/mlcommons/croissant
31. https://github.com/ai-robots-txt/ai.robots.txt
32. https://github.com/apache/polaris
33. https://digital-strategy.ec.europa.eu/en/policies/regulatory-framework-ai
34. https://arxiv.org/abs/2406.17557 (FineWeb)
35. https://arxiv.org/abs/2406.11794 (DCLM)
36. https://arxiv.org/abs/2504.02107 (TiC-LM)
37. https://arxiv.org/abs/2502.13347 (Craw4LLM)
38. https://hotinfra24.github.io/papers/hotinfra24-final5.pdf (Mixtera)
39. https://arxiv.org/abs/2506.01732 (Common Corpus)
40. https://arxiv.org/abs/2509.25531 (MixtureVitae)
41. https://arxiv.org/abs/2412.17847 (Data Provenance Initiative)
42. https://arxiv.org/abs/2411.04257 (LSHBloom)
43. https://arxiv.org/pdf/2005.04740.pdf (sliding-window dedup)
44. https://research.google.com/pubs/archive/33026.pdf (SimHash WWW 2007)
45. https://arxiv.org/abs/2402.00159 (Dolma)
46. https://github.com/facebookresearch/cc_net (CCNet)
47. https://huggingface.co/HuggingFaceFW/fineweb-edu-classifier
48. https://arxiv.org/abs/2106.15110 (Time-Aware LMs)
49. https://lenatriestounderstand.com/notes/llm/008-time-in-corpus/ (Hindsight Corpus)
50. https://arxiv.org/abs/2508.12220 (Unlearning at Scale)
51. https://arxiv.org/abs/2502.19790 (Mixtera SIGMOD 2026)
52. https://arxiv.org/abs/2411.14318 (Velocitune)
53. https://github.com/alon-albalak/online-data-mixing (ODM)
54. https://arxiv.org/abs/2406.14644 (Unveiling Spectrum of Contamination)
55. https://github.com/lm-sys/llm-decontaminator
56. https://peacprotocol.org/spec
57. https://github.com/apache/iceberg/issues/44 (Iceberg signed snapshots)
58. https://github.com/apache/iceberg/issues/6514 (watermark in snapshot properties)
59. https://github.com/apache/amoro/issues/1805 (Amoro event-time watermarks on Iceberg)
60. https://www.cidrdb.org/cidr2021/papers/cidr2021_paper17.pdf (Lakehouse CIDR 2021)
61. https://arxiv.org/html/2602.19548v1 (HTML extraction LLM pretraining)
62. https://arxiv.org/abs/2403.08763 (Ibrahim et al. CPT)
63. https://arxiv.org/abs/2407.07263 (Reuse Don't Retrain)
64. https://argoproj.github.io/rollouts/
65. https://linkerd.io/2-edge/tasks/flagger/
66. https://docs.databricks.com/aws/en/ldp/expectations (Delta Live Tables expectations)
67. https://sloth.dev/usage/kubernetes/
68. https://keptn.sh/stable/docs/guides/slo/
69. https://strimzi.io/blog/2021/03/29/connector-build/
70. https://docs.nvidia.com/nemo/curator/25.09/about/release-notes/migration-faq.html
71. https://anakli.inf.ethz.ch/papers/mixtera_sigmod2026.pdf
72. https://github.com/yidingjiang/ado (ADO)
73. https://research.nvidia.com/labs/lpr/climb/
74. https://openreview.net/forum?id=jnRBe6zatP (FineWeb2)

## 13. v0.2.0 amendment - fulltext, code, and seed mixture

This section is an amendment, not a rewrite. It records the scope change applied on 2026-06-15 between v0.1.0 (Sections 1-12 above) and v0.2.0. Two follow-on research reports drove the change; both live in this repo:

- `docs/research-fulltext-and-code.md` - mid-2026 survey of how to ingest arXiv fulltext and source code at scale without a GPU node. Verified the native arXiv HTML rollout (~97% coverage at `/html/<id>`, ~75% LaTeXML-clean), confirmed OpenReview API v2 + the REVIEWARENA HF dataset are the realistic paths to peer-review prose, and showed that GitHub release tarballs (`/repos/{o}/{r}/tarball/{tag}`) stay well inside the 5000 req/h authed PAT budget for the existing 30-repo allowlist.
- `docs/research-seed-corpus.md` - 5-component HF seed mixture sizing (peS2o cs.* + RedPajama-arxiv + FineWeb-Edu URL-filtered + Stack-Edu Python+ML + custom Wayback backfill) sized at ~275-280 GB Bronze / ~55-65B tokens, all under permissive licenses compatible with this project's Apache-2.0 release.

Resulting v0.2.0 deltas (full source-feed catalogue in `SOURCES.md`):

- Three new ingest modules: `arxiv_html_fetcher`, `openreview_poller`, `github_release_tarball_fetcher`.
- One new processor: `processor/seed_loader.py`, a one-shot Bytewax Job that lands the seed mixture directly on `docs.normalized` so Silver/Gold operators see the seed identically to live polling.
- Schema additions on Bronze, Silver, and Gold: `source_format` (html | pdf | latex | code | web | metadata | review), `extraction_pipeline`, `spdx_license`, `spdx_license_source`. New `CodeFileRecord` model for per-file code records.
- Removal: the v0.1 manual URL submit endpoint (`ingest/submit_api/`). Its demo role is covered by the seed loader plus live pollers, with no abuse surface.

Topic decision: deliberately no fifth `docs.code` topic. Code records ride the existing `raw.fetched` / `docs.normalized` topics with `source_format=='code'`. This keeps the four-topic Redpanda contract stable from v0.1 -> v0.2.

Native publication-date columns populate `valid_from` for every seed component, so the N2 validity-interval novelty has material to query (`gold_as_of('2023-06-01')`) on day 1 of the demo rather than after live polling has accumulated history.

Sections 1-12 above stay as-is; nothing in v0.2.0 retracts a v0.1 design choice.
75. https://aclanthology.org/2025.findings-naacl.291 (NAACL 2025 contamination survey)