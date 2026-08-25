# Stream2Pretrain infrastructure

This directory contains the measured DHBWCloud deployment path. Terraform owns
the OpenStack VMs, Ansible owns k3s, and Helmfile owns the platform, catalog,
and application releases. MinIO is an explicit external prerequisite because
the existing cluster already has a stateful MinIO deployment and its replacement
topology has not been measured.

Only the `dev` environment is deployable. The former production overlays were
removed because their replica counts, retention periods, and volume sizes were
not based on target-cluster measurements.

## Safety boundary

- Terraform protects every VM with `prevent_destroy`.
- The deployment script copies the existing RFC2136 credential into Kubernetes
  Secrets but never creates, prints, or commits a new credential. It does not
  delete PVCs or run a forced Helm upgrade.
- OpenStack state, plans, credentials, generated inventory, and kubeconfig are
  ignored by Git.
- The live application release cannot be adopted in one Helm upgrade because
  it contains mixed immutable selector schemes. See
  [`../docs/infrastructure-reimplementation.md`](../docs/infrastructure-reimplementation.md).

## Layout

```text
infra/
  terraform/          OpenStack VMs and generated Ansible inventory
  ansible/            pinned k3s role and playbook
  dns/                opt-in RFC2136 certificate configuration
  helmfile-values/    measured DHBW `dev` overrides
  k3s-install/        legacy manual bootstrap, not used by the supported path
```

The release graph and exact chart versions are in `../helmfile.yaml` and
`../helmfile.lock`.

## Prerequisites

- Terraform, Ansible, `kubectl`, Helmfile, Helm 3, and `uv`.
  Helm 4 is not supported
  by the pinned charts. Set `HELM_BINARY` when Helm 3 is not the default.
- DHBWCloud credentials through `OS_CLOUD`, exported `OS_*` variables, or an
  explicit `OPENRC_PATH`.
- Project-specific `image_id` and `key_pair` values in
  `infra/terraform/terraform.tfvars`.
- The OpenStack project `default` security group used by the lecture exercise.
  This is appropriate for the isolated course prototype, but its permissive
  ingress rules are not a production security baseline.
- The existing, non-committed DHBW DNS inventory. By default the script reads
  `../cloud/dns-credentials.yaml`; set `DNS_CREDENTIALS_INVENTORY` when it is
  stored elsewhere.
- A reachable MinIO service named `minio` in namespace `minio`, the five
  application buckets `s2p-bronze`, `s2p-silver`, `s2p-gold`, `s2p-decon`, and
  `s2p-posttrain`,
  and externally managed credentials.
- Application images with identical digests available on every eligible node.
  The current cluster has them only on the control-plane node, so the DHBW
  override temporarily schedules application pods there with `pullPolicy:
  Never`.

Required externally managed objects for enabled components:

| Namespace | Object | Required keys |
| --- | --- | --- |
| `polaris` | Secret `polaris-bootstrap` | `credentials` |
| `polaris` | Secret `polaris-minio` | `accessKey`, `secretKey` |
| `stream2pretrain` | Secret `stream2pretrain-minio` | `accessKey`, `secretKey` |
| `stream2pretrain` | Secret `stream2pretrain-polaris` | `credential`, `scope` |
| `stream2pretrain` | Secret `stream2pretrain-github` | `token` |
| `stream2pretrain` | Secret `stream2pretrain-hf` | `token` |
| `stream2pretrain` | Secret `stream2pretrain-decon-signing` | `ed25519.key`, `ed25519.crt` |
| `stream2pretrain` | Secret `stream2pretrain-foundry-providers` (foundry only) | `HETZNER_INFERENCE_API_KEY`, `controlToken` |
| `stream2pretrain` | ConfigMap `stream2pretrain-decon-benchmarks` | `corpus.json` |

Use Sealed Secrets, External Secrets, or another team-approved mechanism. The
repository intentionally contains no example credential values.

Grafana is the exception: its Helm subchart owns a random administrator
password in a Kubernetes Secret so a malformed external Secret cannot prevent
the monitoring stack from starting. Retrieve it without writing it to disk:

```bash
kubectl -n monitoring get secret kube-prometheus-stack-grafana \
  -o jsonpath='{.data.admin-password}' | base64 --decode
printf '\n'
```

## Commands

Run every command from the repository root. Each stage is explicit so a failed
prerequisite does not turn into a partial full-stack install.

```bash
# Local validation only
HELM_BINARY=/opt/homebrew/opt/helm@3/bin/helm \
  ./scripts/setup_dhbw_demo.sh validate

# Read-only OpenStack plan
OPENRC_PATH=/absolute/path/to/openrc.sh \
  ./scripts/setup_dhbw_demo.sh plan

# Apply the reviewed VM plan and install k3s
OPENRC_PATH=/absolute/path/to/openrc.sh \
  ./scripts/setup_dhbw_demo.sh cluster

# Apply each in-cluster ownership tier after its prerequisites exist
./scripts/setup_dhbw_demo.sh platform
./scripts/setup_dhbw_demo.sh catalog
./scripts/setup_dhbw_demo.sh topics
./scripts/setup_dhbw_demo.sh application

# Safe existing-cluster edge migration. This avoids the full application
# upgrade and applies only cert-manager, Traefik, ExternalDNS, and UI ingress.
./scripts/setup_dhbw_demo.sh edge

# Read-only cluster health summary
./scripts/setup_dhbw_demo.sh verify
```

`platform` installs cert-manager, Traefik, ExternalDNS,
kube-prometheus-stack, KEDA, Gatekeeper, and Redpanda. `catalog` installs the
official Apache Polaris 1.7.0 chart. `topics` idempotently creates the
configured one-partition, one-replica topics
matching the measured live cluster. `application` installs the local
Stream2Pretrain chart. Loki, Tempo, and
Alloy are excluded until their MinIO credentials, retention, storage, and
resource requirements are measured.

The dev Polaris configuration uses in-memory persistence. Pod replacement can
lose catalog state. A production catalog requires a relational JDBC service,
credential secret, recovery test, and measured persistent storage before it can
be added to this deployment path.

The post-training foundry additionally requires its provider Secret, signing
key, and `s2p-posttrain` bucket. Its single-writer worker starts after all
configured models are present in authenticated model discovery. See
[`../docs/POSTTRAIN_FOUNDRY.md`](../docs/POSTTRAIN_FOUNDRY.md) for the runtime
and audit workflow.

Bulk corpus storage must not share a k3s node root filesystem in a production
deployment. The current `local-path` MinIO volume cannot gain physical capacity
by editing its PVC, and it is not a valid target for a PVC autoresizer. See
[`../docs/storage-scaling.md`](../docs/storage-scaling.md) for the data-owner
map, safe maintenance boundary, and the external S3 or expandable-CSI migration
plan.

## Existing cluster migration

Do not run the `application` stage against the current release yet. Four
stateless workloads use an older selector scheme, and the curator StatefulSet
also differs in immutable fields. Kubernetes correctly rejects a full upgrade.
Do not bypass that rejection with `helm upgrade --force` because recreating the
StatefulSet can disturb its checkpoint PVC relationship.

The safe migration is:

1. Distribute the same application image digests to all nodes or publish them
   to a reachable registry.
2. Recreate the four stateless workloads during an approved maintenance window.
3. Plan the curator StatefulSet and PVC migration separately.
4. Run a server-side dry-run of the rendered release.
5. Apply the clean chart and verify one controlled record before enabling KEDA.

Until that migration, only targeted, reversible patches should be applied to
the live application workloads.

## DNS and TLS

The DHBW profile follows Exercise Track 1 and publishes
`stream2pretrain-app.s241221-at-student-dhbw-mannheim-de.users.dhbw.site`. Traefik
uses k3s ServiceLB on ports 80 and 443. ExternalDNS manages only this zone with
the unique owner `stream2pretrain`, the prefix `_stream2pretrain.`, and
`upsert-only` policy, so the existing `registry` record and its ownership TXT
record are not adopted or deleted.

cert-manager uses the DHBW ACME directory at
`https://certificates.dhbw.cloud/directory` and RFC2136 DNS-01. As shown in the
lecture, the DNS credential file is passed as a second Ansible inventory. The
playbook creates namespace-local TSIG Secrets, a DHBW ClusterIssuer, and one
wildcard certificate registered as Traefik's default. Application Ingresses do
not create a separate certificate. The secret value is never present in Helm
values or Git.

Fresh clusters use the lecture role's dual-stack mode with IPv6 as the primary
family. The preserved live cluster was originally installed with IPv6-only pod
CIDRs, so `edge` detects that state and adds Jool NAT64 plus CoreDNS DNS64
without recreating k3s or its local persistent volumes. Application pods can
then reach IPv4-only upstream services while public ingress remains IPv6.
The Jool port-pool split follows the packaged Jool example. Its sustained
connection capacity remains `needs-measurement` on the course workload.

The lecture Terraform attaches the project `default` security group to all
three nodes, so this deployment does the same. The live group was verified to
allow all ingress. That keeps the lab setup simple and ensures k3s overlay
traffic works, but it must be documented as a prototype limitation rather than
presented as production hardening.

## Destructive operations

No teardown command is provided. Removing the cluster or stateful releases can
destroy MinIO, Redpanda, Prometheus, and curator data. Snapshot the relevant
volumes and obtain explicit approval before removing `prevent_destroy` or
deleting any PVC.

## Still needs measurement

- Sustainable document throughput and processor resource requests
- Redpanda partition count and retention capacity
- MinIO production topology and storage throughput
- Polaris relational database sizing and recovery behavior
- Loki and Tempo retention, storage, and CPU/memory requirements
- Seed-loader volume size on the target datasets
- KEDA thresholds and maximum replicas
