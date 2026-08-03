# Stream2Pretrain Infrastructure

This directory bootstraps the cluster the project chart runs on. The control
plane is one VM running `k3s server`; data-plane workloads run on two `k3s agent`
VMs. Terraform creates the VMs on DHBWCloud OpenStack using the same DHBWV6
pattern as the working demo project in `~/DHBW/cloud`; Ansible installs k3s
afterwards.

```
+--------------------+
|  Terraform (here)  |   creates 3 OpenStack VMs on DHBWV6 + inventory
+----------+---------+
           | ansible -i generated-inventory.yml infra/ansible/deploy.yaml
           v
+--------------------+
|  k3s cluster       |   1 control + 2 workers, installed by k3s-dhbw-cloud-role
+----------+---------+
           |
           | helmfile -e dev apply
           v
+--------------------+   cert-manager, Traefik, kube-prometheus-stack, Loki,
|  Platform releases |   Tempo, Alloy, KEDA, Gatekeeper, MinIO, Redpanda,
|  (helmfile.yaml)   |   Polaris-lite, finally ./charts/stream2pretrain.
+--------------------+
```

## Layout

```
infra/
  terraform/
    versions.tf              provider pins (terraform-provider-openstack ~> 3.0)
    variables.tf             all knobs, defaults matching the DHBWV6 demo
    main.tf                  1 master + 2 workers on the existing DHBWV6 network
    outputs.tf               VM IPs + Ansible inventory path
    terraform.tfvars.example copy to terraform.tfvars (gitignored)
  ansible/
    deploy.yaml              installs k3s with the DHBW role, without
                             Gridflex-specific post-tasks
    requirements.yml         role dependency for ansible-galaxy
  k3s-install/
    cloud-init-server.yaml   legacy direct-k3s bootstrap assets, not used by
    cloud-init-agent.yaml    the DHBWV6 Terraform path
    k3s-server.sh            legacy direct-k3s bootstrap script
    k3s-agent.sh             legacy direct-k3s bootstrap script
  dns/
    cert-manager-issuer.yaml letsencrypt + rfc2136 webhook, wildcard cert
  helmfile-values/
    <release>.<env>.yaml     per-release values, dev + prod
```

`/helmfile.yaml` lives at the repo root because `helmfile` resolves chart paths
relative to the file (and we ship two local charts at `./charts/stream2pretrain`
and `./charts/polaris-lite`).

## Prerequisites

1. DHBWCloud OpenStack project with quota for:
   - 3 VMs (default: 3 x `k8s.node`, conservative demo sizing)
   - Access to the existing `DHBWV6` network
   The exact quota still needs to be measured on the target project before
   increasing worker count or resource requests.
2. A wildcard DNS zone (the team needs to confirm; CLAUDE.md open question).
3. Local tools:
   - Terraform >= 1.7
   - Ansible with `kubernetes.core` and the `k3s-dhbw-cloud-role`
   - `helmfile` >= 0.165 + `helm` >= 3.14 (`helm plugin install
     https://github.com/databus23/helm-diff` is recommended)
   - `kubectl` >= 1.30
   - `jq`
   - `openstack` CLI configured via `clouds.yaml` or `source openrc.sh`

## Bootstrap

### Fast path: DHBW demo setup

For the DHBWCloud demo, use the repo script instead of replaying the Terraform,
Ansible and Helm steps by hand:

```bash
./scripts/setup_dhbw_demo.sh
```

The script is idempotent and does the current demo-safe path end to end:

- sources the OpenStack app credential from `/Users/I749974/DHBW/cloud/app-cred-Julian-openrc.sh` when present
- applies the Terraform VM plan
- installs k3s with `infra/ansible/deploy.yaml`
- installs kube-prometheus-stack, cert-manager, Traefik, KEDA, Gatekeeper and Redpanda
- patches Redpanda listener bind addresses for the IPv6-only pod network
- creates the Stream2Pretrain Redpanda topics with replication factor 1
- installs standalone MinIO demo storage and creates the required buckets
- creates local demo Secrets in the `stream2pretrain` namespace

To resume only part of the setup while debugging:

```bash
RUN_TERRAFORM=0 RUN_ANSIBLE=0 ./scripts/setup_dhbw_demo.sh
RUN_TERRAFORM=0 RUN_ANSIBLE=0 RUN_PLATFORM=0 RUN_STORAGE=1 ./scripts/setup_dhbw_demo.sh
```

The script does not destroy OpenStack resources and does not deploy the project
application images yet. The app chart still needs AMD64 images accessible from
the cluster and the missing Polaris deployment path resolved.

### 1. Provision VMs

```bash
cd infra/terraform
cp terraform.tfvars.example terraform.tfvars
$EDITOR terraform.tfvars                    # adjust image_id/key_pair/flavors if needed
source ~/dhbwcloud-openrc.sh                # OS_AUTH_URL etc.
terraform init
terraform plan
terraform apply
```

Terraform writes `infra/terraform/generated-inventory.yml`, shaped like the
working demo inventory.

### 2. Install k3s with Ansible

```bash
cd ../..
ansible-galaxy install -r infra/ansible/requirements.yml --force
ansible-playbook \
  -i infra/terraform/generated-inventory.yml \
  infra/ansible/deploy.yaml
```

The Ansible role writes `infra/kubeconfig-stream2pretrain.yaml`. Treat it as a
secret. The repo-specific playbook deliberately does not reuse the demo
`tasks/k3s-configure.yaml`, because that file contains Gridflex node names and
an overly broad default-ServiceAccount ClusterRoleBinding.

### 3. Install platform releases

```bash
cd ../..               # repo root
helmfile -e dev deps   # download upstream charts
helmfile -e dev apply
```

Order is enforced via `needs:` in `helmfile.yaml`:
1. cert-manager (CRDs + webhook)
2. Traefik (DaemonSet, replaces the disabled k3s built-in)
3. kube-prometheus-stack (Prometheus, Grafana, AlertManager, ServiceMonitor CRDs)
4. Loki (logs, S3 backend on MinIO)
5. Tempo (traces, S3 backend on MinIO)
6. Alloy (DaemonSet: log shipping + OTel OTLP receiver -> Tempo)
7. KEDA (autoscaling)
8. Gatekeeper (admission)
9. MinIO operator + tenant (object store, with the buckets the chart consumes)
10. Redpanda (1-broker dev mode; documented limitation)
11. Polaris-lite (Iceberg REST catalog, in-cluster S3 to MinIO)
12. Stream2Pretrain (project chart at `./charts/stream2pretrain`)

### 4. TLS

Apply the issuer + wildcard cert after editing it with the team's DNS zone +
TSIG credentials:

```bash
$EDITOR infra/dns/cert-manager-issuer.yaml      # replace REPLACE_WITH_* markers
kubectl apply -f infra/dns/cert-manager-issuer.yaml
kubectl -n traefik wait --for=condition=Ready certificate/stream2pretrain-wildcard --timeout=10m
```

In `dev`, `tlsEnabled` is false and the wildcard cert step is optional.

### 5. Verify

```bash
kubectl get pods -A
kubectl -n monitoring port-forward svc/kps-grafana 3000:80
kubectl -n redpanda port-forward svc/redpanda-console 8080:8080
kubectl -n stream2pretrain get scaledobject,servicemonitor,ingressroute
```

All pods Ready, KEDA `ScaledObject`s present for `fetcher`, `curate`,
`iceberg-writer`, Grafana shows the platform dashboards (kube-prometheus-stack
ships them automatically; chart-layer adds Stream2Pretrain dashboards).

## Tear down

```bash
helmfile -e dev destroy
cd infra/terraform && terraform destroy
```

This deletes everything including MinIO data. Snapshot the volumes via
OpenStack first if you want to keep state.

## Decisions you must still make (from CLAUDE.md open questions)

- DHBWCloud project quota (vCPU/RAM/disk per VM): if `k8s.node` is unavailable,
  set `control_flavor` / `worker_flavor` to whatever the team has and revisit the resource
  presets in `helmfile-values/*.dev.yaml` and `*.prod.yaml`.
- DNS zone for the wildcard cert: replace every `stream2pretrain.example.org`
  marker in `infra/dns/cert-manager-issuer.yaml`,
  `infra/helmfile-values/stream2pretrain.prod.yaml`, and the prod
  `environments` block in `helmfile.yaml`.
- TSIG zone + key for rfc2136 DNS-01 challenge.
- Whether to run prod from this same cluster or a second one (the dev/prod
  split here is environment-only, not multi-cluster).
- Polaris upstream chart: availability and version pin are `needs-measurement`.
  The Helmfile currently references `./charts/polaris-lite` (scaffolded by the
  chart-layer agent). Swap to `apache/polaris` once the upstream chart hits
  GA and the version is pinned.

## Why these picks

All locked in CLAUDE.md, 2026-06-15. Short summary:

- k3s on OpenStack, single-server: lecture stack, fits the 1+2 worker constraint.
- Redpanda 1-broker: 3-4x lower RAM than JVM Kafka; replication factor 1 in
  topic creation is documented in the project chart.
- Bytewax: Python-native streaming, no JVM heap tuning.
- MinIO + Iceberg V3 + Polaris: lecture object store; vendor-neutral catalog;
  row lineage via `_row_id` for the validity-interval column.
- KEDA Kafka-lag trigger: lecture default; native consumer-group lag scaling.
- Loki + Tempo + Alloy: lecture stack; Tempo enables the trace-id-in-Iceberg
  novelty (RESEARCH.md N5).
- Traefik + cert-manager: lecture stack; DNS-01 supports wildcards which
  HTTP-01 does not.
- OPA Gatekeeper: more expressive ConstraintTemplates than Kyverno for the
  SourceFeed CRD admission rules.
