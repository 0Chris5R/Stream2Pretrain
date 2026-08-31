# Capacity Benchmark Runbook

PRD-013 cannot be closed from a laptop. It requires measurements from the
target DHBWCloud k3s cluster after the platform dependencies and the
Stream2Pretrain chart are installed. Do not replace missing measurements with
estimates. Keep `needs-measurement` until the command output exists.

## Capacity evidence already available from the lecture repository

The course material provides two concrete deployment references, but only one
has numeric capacity:

- `lecture_slides/04a - Architecture and Core Concepts.md` starts the local
  Minikube environment with 6 CPUs and 7,000 MB RAM.
- The same handout selects the DHBW Cloud flavor `m1.xlarge`, but does not
  publish that flavor's CPU, RAM, ephemeral-disk, or quota values.

The first number is a development recommendation, not evidence for the remote
cluster. The second is only a flavor name. Consequently the chart uses one
strict curator replica and conservative KEDA maxima by default. The larger
`values-prod.yaml` maxima are generic production examples and must not be used
for the course cluster until the probe below records allocatable resources.
FinePDFs v2 plus FineWeb-Edu comparison inference also makes the curator
materially larger than the older FineWeb-only configuration.

## 2026-08-30 live baseline and remediation gates

The cluster diagnosis that triggered this remediation observed, over the same
one-hour interval, 218 fetched records, 210 normalized records, 91 curation
decisions, and 57 durable exports. That interval included catch-up work and is
historical bottleneck evidence, not the clean live capacity denominator. The
2026-08-31 post-scale sample measured arXiv normalization at about 120 records
per hour while the Bytewax frontier advanced at about 89 records per hour and
the 15-minute route rate was about 74.5 records per hour. The immediate clean
rollout gate is therefore 150 durable decisions per hour, 1.25 times that
measured live arrival. Replace this provisional gate with 1.25 times the
weekday p95 normalized arrival after enough uncontaminated evidence exists.

At diagnosis time the single stateful curator used about 1,580 MiB and 1.5 CPU
cores. One combined quality-model Pod used about 2 CPU cores while the other
ready replicas were nearly idle. The remediation leaves Bytewax, its recovery
partition count, every content classifier, OCR, PII scan, deduplication step,
and quality gate intact. It instead separates FinePDFs and FineWeb runtimes,
uses bounded two-segment requests, and retains the exact one-by-one classifier
implementation and pinned revision in each batch result. FinePDFs, FineWeb,
and KenLM have independent bounded executor pools in the curator, so a long
paper cannot fill one shared FIFO with one model family while the other ready
model deployments sit idle. Results remain joined by stable segment ID before
the unchanged policy runs.

The 2026-08-31 post-scale audit found a second, narrower bottleneck. Six
simultaneous FinePDFs connections sent through a ClusterIP reached only about
four distinct Pods at a time, which is the expected random-routing occupancy
`6 * (1 - (5 / 6)^6) = 3.99`. One live sample showed per-Pod demand of
`2, 2, 1, 0, 0, 0`, while model queue p95 reached 46.9 seconds and one worker
node remained at 12 percent CPU. The curator now resolves a readiness-filtered
headless Service and leases each Pod endpoint to at most one request. Requests
wait centrally only when every ready Pod is busy. Endpoint discovery refreshes
during HPA changes, one network or 5xx failure retries the byte-identical
payload once on a different ready Pod, and failure after that still prevents
the Bytewax frontier from advancing. This changes scheduling only. Request
payloads, classifier revisions, score values, result order, gates, the single
state owner, and at-least-once recovery remain unchanged.

The classifier rollout gate runs inside the curator Pod against each quality
Service. It requires at least two ready backends, resolves exactly the Ready
Pod count through the headless Service, sends one concurrent direct inference
to every resolved endpoint, and requires one distinct backend response per
endpoint. It also observes at least 20 fresh ClusterIP inference requests per
ready backend and proves that a two-item batch has byte-for-byte equal JSON
results, order, and revisions to two singleton requests. Prometheus records
server-side request, batch-size, queue-time, inference-time and active-request
metrics, plus curator-side endpoint count, endpoint wait, direct backend and
retry metrics. The course cluster permits FinePDFs to scale from two to six
stateless Pods and gives the curator six independent requests per quality
family. FineWeb remains bounded at three Pods because the live measurements
identify FinePDFs, not the comparison classifier, as the saturated path.

The later live-source sample changed the capacity denominator materially. The
Hugging Face poller emitted about 85 to 93 unique immutable README revisions
per ten-minute pass, or about 510 to 558 per hour. Combined with the earlier
measured arXiv normalization rate of about 120 per hour, simultaneous live
arrival is about 630 to 678 documents per hour. The curator completed about
144 decisions per hour while `docs.normalized` held about 27,000 records.
These are observed rates, not a synthetic benchmark.

The deterministic source-quality, language, minimum-body, card-structure,
C4/Gopher, licence, extraction, PII, and near-duplicate gates cannot safely
skip model inference under the current audit contract. A durable rejected
decision still contains every applicable per-segment FinePDFs, FineWeb-Edu,
and KenLM result and revision. Skipping those calls would change the durable
output even when the final route happened to remain quarantine. Exact-payload
replay already avoids inference through the decision cache.

The next optimization therefore removes a synchronization inefficiency rather
than a classifier. The Kafka connector exposes bounded twelve-document runtime
batches to Bytewax. With the unchanged two-item classifier request size, even
twelve one-segment cards produce six independent requests. PII sanitization
and pinned stateless model calls are prepared across those documents, filling
the six ready FinePDFs endpoints and
the independent FineWeb/KenLM lanes. `curate_one` then finalizes each record in
the original order. Decision-cache lookup and writes, LSH observations,
metrics, Kafka outputs, thresholds, routes, and revisions remain serial and
unchanged. Bytewax uses `flat_map_batch`, not a collecting/window operator, so
the existing flow and recovery identity acquire no new operator state. A
compound wrapper deliberately retains the former recovery-visible
`curate_run.flat_map_batch` core step, and the former stateless
`curate_drop_none` topology also remains present. The source, three sinks,
flow ID, and recovery name therefore keep their exact existing identifiers.

The batch callback returns outputs only after every item in that runtime batch
has finalized. If a transient exception interrupts a later item, Bytewax gets
no returned iterable and cannot advance the source frontier past an un-emitted
earlier item. The already existing decision cache is committed before output;
on at-least-once replay it returns the exact earlier bytes without a second LSH
mutation. Record-local `ValueError` outcomes are still omitted individually,
as they were by the former one-by-one callback.

The former 150-decision/hour provisional gate is now obsolete. With the
already documented 1.25 capacity factor, the current 678/hour observed upper
arrival implies a sustained gate of 848 durable decisions/hour after rounding
up. At exactly 678/hour the worker only keeps pace and never drains the
existing backlog. At 848/hour, a 27,000-record backlog would take about 159
hours to drain while the same live load continues. Replace these rates when a
new clean weekday p95 measurement exists.

Resource expansion remains measurement-driven after the micro-batch rollout.
Let `T` be its sustained durable decisions/hour and `s` the measured mean
applicable segments/document. If `T < 848`, the remaining capacity multiplier
is `848 / T`. Using the observed p95 inference times and the unchanged
two-item request batch, conservative total quality-Pod counts are
`ceil(848 * s * 27.9 / (3600 * 2))` for FinePDFs and
`ceil(848 * s * 12.0 / (3600 * 2))` for FineWeb. The required `s` is currently
`needs-measurement`; do not guess it. Each additional quality Pod currently
requests 1 vCPU and 1 GiB RAM and is limited to 2 vCPU and 3 GiB RAM. Add that
allocatable node capacity before raising KEDA maxima. If model queues are low
but the gate still misses, measure the single state owner's PII/dedup CPU and
RSS, then increase its CPU/RAM rather than removing a stage or adding an unsafe
second state owner.

Do not call the curator fixed after the distribution gate alone. Sustain at
least 848 durable decisions per hour for six hours with shrinking
`docs.normalized` lag, no Bytewax recovery-frontier rollback, no skipped model
signal, and no new processing-failure object. After enough weekday evidence
exists, replace this target with at least 1.25 times the measured weekday p95
normalized arrival rate. If the gate misses while model queue time or CPU is
saturated, increase stateless model replicas or worker CPU/RAM. Do not remove
processing stages.

The same diagnosis found one CoreDNS Pod and an actual 3.1-second DNS timeout
during a MinIO multipart operation. The infrastructure gate therefore requires
two ready CoreDNS replicas protected by a `minAvailable: 1` disruption budget,
100 consecutive in-cluster DNS resolutions for MinIO and Polaris, and ten
successful MinIO put, head, and delete cycles. An ordinary application-only
release performs only an idempotent CoreDNS replica/PDB presence check; the
full probe runs when the DNS infrastructure contract changes.

This does not turn the existing single-Pod, single-volume MinIO deployment
into highly available storage. Client retries and redundant DNS remove the
observed name-resolution failure mode, but a production HA claim still needs
an administrator-provisioned replicated object store or multi-node MinIO
tenant and a measured failure-domain migration. Its additional CPU, RAM, and
storage are `needs-measurement`; do not infer them from the DNS fix.

Iceberg cleanup remains the single scheduled maintenance owner's job. Hot
writer and Foundry commits never delete prior metadata. The existing guarantees
remain unchanged: 20 previous metadata versions, a 168-hour maximum snapshot
age, at least ten snapshots retained, and a 24-hour orphan-object floor. Close
this gate only after 24 hours with no metadata-delete warning, 100 successful
append/read cycles including a CoreDNS restart, and a verified seven-day
`as_of(timestamp)` query.

## Preconditions

- `kubectl` points at the target k3s cluster.
- The `stream2pretrain`, `redpanda`, and `minio` namespaces exist, or their
  namespace overrides are known.
- Metrics Server is installed if you want live `kubectl top` values. The
  capacity report does not require it.
- The historical seed/backfill path is removed. Capacity is measured against
  live source rates and an explicit bounded canary.

## One-Shot Capacity Report

Run:

```sh
uv run python scripts/capacity_probe.py \
  --namespace stream2pretrain \
  --redpanda-namespace redpanda \
  --minio-namespace minio \
  --json-out docs/capacity-report.generated.json \
  --out docs/capacity-report.generated.md
```

The generated JSON is the evidence artifact. The generated Markdown is the
human-readable report to attach to PRD-013. Commit the generated report only
after it was collected from the real target cluster.

The report covers:

- Node allocatable CPU and memory from `kubectl get nodes -o json`.
- Stream2Pretrain pod requests and limits from rendered live Pods.
- PVC requests/capacity, including processor recovery and model caches.
- Redpanda topic metadata for `raw.fetched`, `docs.normalized`, and
  `docs.curated` when `rpk` is reachable inside a broker
  Pod.
- MinIO pod discovery. Throughput remains `needs-measurement` until a MinIO
  benchmark such as `warp` is run against the target tenant.

Immediately after gaining access, also capture the flavor and quota surface:

```sh
kubectl get nodes -o wide
kubectl describe nodes
kubectl get resourcequota,limitrange -A
kubectl get storageclass
kubectl top nodes
```

## Live-frontier smoke

After the approved one-time backlog reset, record an exact newly discovered
document from every active content family. Measure end-to-end duration, peak
worker CPU and memory, and topic lag without generating historical load.

## Redpanda Partition Decision

Keep `schemas/topics.py` production partition counts as `needs-measurement`
until the target report includes:

- Measured producer throughput into `raw.fetched`.
- Measured consumer lag drain rate for fetcher, curate, and Iceberg writer.
- Per-broker CPU and memory headroom during the drain.
- The partition count used for the run.

Only update topic partition defaults after those four values are recorded.

For fetcher, curator, and Iceberg writer, record the `s2p_*_total` Prometheus
counters, durable recovery frontier, partition assignment, processing-failure
objects, and per-Pod CPU/memory. Their broker groups bootstrap a clean live-v3
recovery through `OFFSET_STORED`, but broker commits are not the steady-state
Bytewax frontier. Core worker count, recovery partition count, input topic
partition count, and CPU allocation must be changed and validated together.
The arXiv HTML worker remains fixed at one replica because shared
`raw.fetched` lag is self-amplifying; only a source-specific backlog metric can
justify enabling its scaler.

## MinIO Throughput Decision

The capacity report discovers MinIO pods but does not invent throughput. Run a
MinIO-native benchmark, for example `warp`, against the target tenant and add
the command output to the generated report. The required evidence is:

- Object size and concurrency used by the benchmark.
- Write throughput.
- Read throughput.
- p95/p99 operation latency.
- MinIO Pod CPU/memory during the run.

## Closing PRD-013

PRD-013 can move from `needs-measurement` to `done` only when
`docs/capacity-report.generated.md` contains measured values for:

- Redpanda partitions and drain behavior.
- Worker CPU/RAM headroom for fetcher, curator, Iceberg writer, DuckDB API,
  source pollers, and model services.
- MinIO read/write throughput.
- Recovery PVC and model-cache sizing for the live pipeline.

If any field remains `needs-measurement`, keep PRD-013 open.
