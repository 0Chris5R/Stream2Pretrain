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

## Preconditions

- `kubectl` points at the target k3s cluster.
- The `stream2pretrain`, `redpanda`, and `minio` namespaces exist, or their
  namespace overrides are known.
- Metrics Server is installed if you want live `kubectl top` values. The
  capacity report does not require it.
- The seed-loader benchmark must run as a small-scale smoke first. Only the
  scale parameter changes for the full run.

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
- PVC requests/capacity, including seed-loader/HF cache claims when present.
- Redpanda topic metadata for `raw.fetched`, `docs.normalized`,
  `docs.curated`, and `decon.attest` when `rpk` is reachable inside a broker
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

## Seed Loader Smoke

Validate the seed-loader path with a sub-minute run first:

```sh
bash scripts/seed_corpus.sh \
  --namespace stream2pretrain \
  --components=pes2o \
  --max-docs=10
```

Record:

- Job wall-clock duration from `kubectl get job` and Pod timestamps.
- Peak Pod CPU/memory from `kubectl top pod` or Prometheus.
- HF cache PVC used bytes from the storage backend or CSI metrics.
- Documents emitted to `docs.normalized`.

After the smoke passes, scale only `--max-docs` or the component list. Do not
change code, images, resource requests, or topic settings between smoke and
the larger run unless the smoke found a defect and the run is restarted from
the beginning.

## Redpanda Partition Decision

Keep `schemas/topics.py` production partition counts as `needs-measurement`
until the target report includes:

- Measured producer throughput into `raw.fetched`.
- Measured consumer lag drain rate for fetcher, curate, and Iceberg writer.
- Per-broker CPU and memory headroom during the drain.
- The partition count used for the run.

Only update topic partition defaults after those four values are recorded.

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
- Worker CPU/RAM headroom for fetcher, curate, Iceberg writer, DuckDB API, and
  seed-loader.
- MinIO read/write throughput.
- Seed-loader PVC sizing for the selected demo seed mixture.

If any field remains `needs-measurement`, keep PRD-013 open.
