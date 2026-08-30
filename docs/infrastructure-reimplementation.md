# DHBW infrastructure reimplementation

Status: configuration rewrite complete, live migration intentionally partial,
2026-08-15.

## Safety boundary

The existing three-node cluster and its PVC data are preserved. The rewrite is
validated locally and with Kubernetes server-side dry-runs before any in-place
change. Terraform destroy, VM replacement, PVC deletion, and catalog recreation
are out of scope unless explicitly approved.

The first deployment target is a verified port-forward demo. Public DNS and TLS
remain a separate opt-in step because the required RFC2136 zone and TSIG
credentials are not available in the repository or local environment.

## Measured baseline

Measurements were taken from the existing DHBW k3s cluster on 2026-08-15.

- Nodes: one control plane and two workers, each with 4 allocatable CPU cores and
  approximately 7.75 GiB allocatable memory.
- Current usage: control plane 209 millicores / 5019 MiB, worker 1 64 millicores /
  2397 MiB, worker 2 22 millicores / 929 MiB.
- Persistent claims: MinIO 10 GiB, Prometheus 10 GiB, Redpanda 50 GiB, curator
  checkpoint 20 GiB.
- Redpanda: one broker, one partition and one replica per application topic.
- Topic high-watermarks: `raw.fetched=113216`, `docs.normalized=113216`, and
  `docs.curated=0`.
- Application images exist only in the control-plane node's containerd store.
- Polaris 1.7.0 is running with in-memory persistence. Catalog recovery after a
  pod replacement is therefore not guaranteed.

These values describe the observed cluster. They are not throughput or capacity
claims. Peak workload resource requirements, retention sizing, partition counts,
and recovery/model-cache storage remain `needs-measurement`.

## Confirmed failures

1. `helmfile.yaml` references a local `charts/polaris-lite` chart that does not
   exist.
2. `scripts/setup_dhbw_demo.sh` installs a different, incomplete stack from the
   Helmfile and does not deploy application images or the application chart.
3. `infra/helmfile-values/stream2pretrain.dev.yaml` uses keys that the chart does
   not consume. The permissive values schema silently accepted them.
4. Processor workloads exported traces to a nonexistent Tempo service.
5. The three processors stopped consuming after an earlier Redpanda outage but
   continued to pass shallow health probes. Redpanda currently accepts IPv6 TCP
   connections, but no processor consumer groups are registered.
6. The Traefik DaemonSet created 1738 failed pods while the control-plane node
   had DiskPressure. Kubernetes retained those terminal pods and cluster-wide
   listing became slow.
7. The application image delivery process is undocumented and non-reproducible.
8. The live release mixes two immutable selector schemes. A full Helm takeover
   requires planned recreation of four stateless workloads; it must not be
   hidden behind `helm upgrade --force`.

## Implemented rewrite

- The application values schema now rejects unknown top-level keys.
- The DHBW override contains only keys consumed by the chart and disables
  unavailable tracing, logging, autoscaling, and unbuilt components.
- Helmfile is the single release graph, split into `platform`, `catalog`, and
  `application` ownership tiers with pinned dependencies.
- The nonexistent `polaris-lite` chart was replaced with the official Apache
  Polaris 1.7.0 chart. Dev persistence is explicitly in-memory.
- The unmeasured production and optional Loki/Tempo/Alloy overlays were removed.
  They can return only after their capacity and storage requirements are
  measured.
- Terraform now requires the project-specific image and keypair, validates the
  worker count, and protects all VMs with `prevent_destroy`.
- The Ansible role is pinned to a commit instead of a moving branch.
- The deployment script validates with Helm 3, never creates credentials, and
  exposes separate plan, cluster, platform, catalog, topics, application, and
  verify commands.
- MinIO is an explicit external prerequisite. The existing stateful deployment
  is preserved until a replacement topology is measured.

## Live deployment result

Only reversible, targeted changes were applied to the existing cluster:

- Deleted 1738 terminal `Failed` Traefik pods. The three live Traefik pods
  remained Running and Ready.
- Disabled the nonexistent OTLP target and trace sampling on the fetcher,
  curator, and Iceberg writer.
- Set the three processors to consume new records from the topic tail and
  restarted only those workloads.
- Verified successful rollouts for the fetcher Deployment, curator StatefulSet,
  and Iceberg writer Deployment.
- Replayed one existing Bronze record that referenced an existing MinIO object.
  During the smoke check, both `raw.fetched` and `docs.normalized` advanced from
  113564 to 113665. No new external document was fetched by the controlled
  replay. At final read-only verification, both high-watermarks had continued
  to 113756.

The fetcher path is working. `docs.curated` remains at zero.
A local evaluation of five captured Silver records measured two rejection
reasons on every record: `c4_nopunc_filter` and `license_excluded`. All five had
missing SPDX metadata. This is a data and curation-policy mismatch, not a cloud
transport failure. The current strict policy records those missing licences in
the admission ledger and quarantines them before content processing for every
format.

## Target ownership model

- Terraform owns only OpenStack compute instances and the generated inventory.
- Ansible owns only k3s configuration and node-level image availability.
- Helmfile owns the in-cluster platform, catalog, and application releases.
- One DHBW values file contains application overrides and must validate against
  the chart schema.
- Secrets are created out of band and are referenced by name. Deployment scripts
  fail when required secrets are absent; they never create demo credentials.
- Optional observability services are disabled until installed. Empty endpoints
  are preferred to retry loops against nonexistent services.

## Remaining migration order

1. Add reproducible AMD64 image publication or distribute identical image
   digests to every worker.
2. Supply externally managed MinIO, Polaris, Grafana, source-token, and signing
   credentials.
3. Recreate the four stateless workloads with legacy immutable selectors during
   an approved maintenance window.
4. Plan the curator StatefulSet and checkpoint PVC migration separately.
5. Replace in-memory Polaris with a relational backend before treating the
   catalog as recoverable.
6. Resolve the measured C4 and license-metadata rejection behavior, then verify
   one controlled record across all four topics.
7. Measure the resulting load before enabling KEDA or increasing replicas,
   partitions, retention, or storage.
