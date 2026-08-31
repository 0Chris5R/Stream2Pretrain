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
decisions, and 57 durable exports. That made curation the limiting stage at
43 percent of the observed normalized arrival rate, or 2.31 times below
break-even. The first rollout target is therefore 315 decisions per hour:
1.5 times the measured 210 normalized records per hour. This is a calculated
gate from that observation, not a claim about long-term source traffic.

At diagnosis time the single stateful curator used about 1,580 MiB and 1.5 CPU
cores. One combined quality-model Pod used about 2 CPU cores while the other
ready replicas were nearly idle. The remediation leaves Bytewax, its recovery
partition count, every content classifier, OCR, PII scan, deduplication step,
and quality gate intact. It instead separates FinePDFs and FineWeb runtimes,
uses fresh service connections plus bounded two-segment requests, and retains
the exact one-by-one classifier implementation and pinned revision in each
batch result. FinePDFs, FineWeb, and KenLM have independent bounded executor
pools in the curator, so a long paper cannot fill one shared FIFO with one
model family while the other ready model deployments sit idle. Results remain
joined by stable segment ID before the unchanged policy runs.

The classifier rollout gate runs inside the curator Pod against each quality
Service. It requires at least two ready backends, observes 60 fresh inference
requests at a bounded concurrency of 12, requires every ready backend to answer
and no backend to receive less than ten percent of requests, and then proves
that a two-item batch has byte-for-byte equal JSON results, order, and revisions
to two singleton requests. Prometheus
also records request, batch-size, queue-time, inference-time, active-request,
model-family, and profile labels for each backend ServiceMonitor.

Do not call the curator fixed after the distribution gate alone. Sustain at
least 315 durable decisions per hour for six hours with no growing
`docs.normalized` lag, no Bytewax recovery-frontier rollback, no skipped model
signal, and no new processing-failure object. After enough weekday evidence
exists, replace the provisional target with at least 1.25 times the measured
weekday p95 normalized arrival rate. If the gate misses while model queue time
or CPU is saturated, increase stateless model replicas or worker CPU/RAM. Do
not remove processing stages.

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
