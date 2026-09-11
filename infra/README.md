# Stream2Pretrain infrastructure

This directory contains the measured DHBWCloud deployment path. Terraform owns
the OpenStack VMs, Ansible owns k3s, and Helmfile owns the platform, storage,
catalog, and application releases. The repository includes a standalone MinIO
StatefulSet for a fresh course deployment. It is not a distributed or
high-availability object-store topology.

The supported Helmfile environment is `dev`, parameterized for the DHBW
cluster. Other chart overrides require their own measured infrastructure.

CoreDNS availability is bootstrap-owned by `infra/ansible/deploy.yaml`. It
applies `infra/kubernetes/coredns-ha-patch.yaml` and
`infra/kubernetes/coredns-pdb.yaml`, yielding two topology-spread DNS replicas
with a one-Pod minimum availability budget. The application release only
reapplies and fully probes this contract when those manifests change; ordinary
application releases perform a cheap replica/PDB presence check.

## Safety boundary

- Terraform protects every VM with `prevent_destroy`.
- The deployment script copies the existing RFC2136 credential into Kubernetes
  Secrets but never creates, prints, or commits a new credential. It does not
  delete PVCs or run a forced Helm upgrade.
- OpenStack state, plans, credentials, generated inventory, and kubeconfig are
  ignored by Git.
- Normal application upgrades preserve selectors, recovery identities and PVCs.

## Layout

```text
infra/
  terraform/          OpenStack VMs and generated Ansible inventory
  ansible/            pinned k3s role and playbook
  dns/                opt-in RFC2136 certificate configuration
  helmfile-values/    measured DHBW `dev` overrides
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
- Application images published to a registry reachable from every eligible
  node, with digest pins and a pull Secret if the registry is private.

Required Secrets for enabled components are listed below. The deployment owns
the explicitly marked internal identities; all others are externally managed.
Create the operator-supplied Secrets with
[`scripts/configure_dhbw_secrets.sh`](../scripts/configure_dhbw_secrets.sh) as
shown in the root README.
Set `S2P_CORE_ONLY=1` for both Secret creation and application deployment when
the optional Foundry provider is unavailable.

| Namespace | Object | Required keys |
| --- | --- | --- |
| `minio` | Secret `minio-root` | `accessKey`, `secretKey` |
| `polaris` | Secret `polaris-minio` | `accessKey`, `secretKey` |
| `polaris` | Secret `polaris-persistence` | `username`, `password`, `jdbcUrl`; deployment creates it once unless pre-provisioned |
| `stream2pretrain` | Secret `stream2pretrain-minio` | `accessKey`, `secretKey` |
| `stream2pretrain` | Secret `stream2pretrain-polaris` | `credential`, `scope` |
| `stream2pretrain` | Secret `stream2pretrain-hf` | `token` |
| `stream2pretrain` | Secret `stream2pretrain-foundry-signing` | `ed25519.key`, `ed25519.crt`; deployment creates it once unless pre-provisioned |
| `stream2pretrain` | Secret `stream2pretrain-foundry-providers` (foundry only) | `HETZNER_INFERENCE_API_KEY`, `controlToken` |

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
./scripts/setup_dhbw_demo.sh storage
./scripts/setup_dhbw_demo.sh catalog
./scripts/setup_dhbw_demo.sh topics
./scripts/setup_dhbw_demo.sh application

# Apply only cert-manager, Traefik, ExternalDNS and UI ingress.
./scripts/setup_dhbw_demo.sh edge

# Read-only cluster health summary
./scripts/setup_dhbw_demo.sh verify
```

`platform` installs cert-manager, Traefik, ExternalDNS,
kube-prometheus-stack, KEDA, Gatekeeper, and Redpanda. `storage` installs the
repository-owned MinIO StatefulSet and idempotently creates the five application
buckets. `catalog` installs the official Apache Polaris 1.7.0 chart. `topics` idempotently creates the
configured topics. The release reconciles four document-topic partitions
in the DHBW profile, with single-broker replication. `application` installs the local
Stream2Pretrain chart. Loki, Tempo, and
Alloy are excluded until their MinIO credentials, retention, storage, and
resource requirements are measured.

The Polaris release uses its production relational JDBC backend. A dedicated
PostgreSQL StatefulSet retains catalog metadata on a 5 GiB PVC. Deployment
creates the database credential Secret once when it is absent, bootstraps the
schema idempotently, and then reconciles Polaris. Iceberg data remains in MinIO.

The post-training foundry additionally requires its provider Secret, signing
key, and `s2p-posttrain` bucket. Its single-writer worker starts after all
configured models are present in authenticated model discovery. See
[`../docs/POSTTRAIN_FOUNDRY.md`](../docs/POSTTRAIN_FOUNDRY.md) for the runtime
and audit workflow.

Bulk corpus storage must not share a k3s node root filesystem in a production
deployment. The course `local-path` MinIO volume cannot gain physical capacity
by editing its PVC, and it is not a valid target for a PVC autoresizer. See
[`../docs/storage-scaling.md`](../docs/storage-scaling.md) for the data-owner
map, safe maintenance boundary, and the external S3 or expandable-CSI migration
plan.

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

The provisioning script intentionally has no teardown command. The operations
runbook records a separate manual decommission procedure for an explicitly
approved teardown. Removing the cluster or stateful releases can destroy MinIO,
Redpanda, Prometheus, and curator data. Snapshot the relevant volumes before
removing `prevent_destroy` or deleting any PVC.

## Still needs measurement

- Sustainable document throughput and processor resource requests
- Redpanda partition count and retention capacity
- MinIO production topology and storage throughput
- Polaris relational database sizing and recovery behavior
- Loki and Tempo retention, storage, and CPU/memory requirements
- KEDA thresholds and maximum replicas
