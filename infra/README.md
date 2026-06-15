# Stream2Pretrain Infrastructure

This directory bootstraps the cluster the project chart runs on. The control
plane is one VM running `k3s server`; data-plane workloads run on two `k3s agent`
VMs. All VMs are created via Terraform on DHBWCloud OpenStack.

```
+--------------------+
|  Terraform (here)  |   creates 3 OpenStack VMs + volumes + secgroups + FIPs
+----------+---------+
           | cloud-init renders k3s-{server,agent}.sh
           v
+--------------------+
|  k3s cluster       |   1 control + 2 workers, Traefik disabled (Helmfile owns it)
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
    variables.tf             all knobs, defaults sane for DHBWCloud
    main.tf                  network, secgroups, keypair, VMs, volumes, FIPs
    outputs.tf               kubeconfig fetch command, IPs, k3s token
    terraform.tfvars.example copy to terraform.tfvars (gitignored)
  k3s-install/
    cloud-init-server.yaml   base cloud-config, sysctl, package install
    cloud-init-agent.yaml    same, agent variant
    k3s-server.sh            templated by Terraform; installs k3s server
    k3s-agent.sh             templated by Terraform; joins workers
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
   - 3 VMs (1 x m1.large + 2 x m1.xlarge by default; 20 vCPU, 40 GB RAM total)
   - 3 Cinder volumes (50 GB + 2 x 100 GB)
   - 2 floating IPs from `ext-net`
   The exact quota is `needs-measurement` until the team logs in (CLAUDE.md).
2. A wildcard DNS zone (the team needs to confirm; CLAUDE.md open question).
3. Local tools:
   - Terraform >= 1.7
   - `helmfile` >= 0.165 + `helm` >= 3.14 (`helm plugin install
     https://github.com/databus23/helm-diff` is recommended)
   - `kubectl` >= 1.30
   - `openstack` CLI configured via `clouds.yaml` or `source openrc.sh`

## Bootstrap

### 1. Provision VMs

```bash
cd infra/terraform
cp terraform.tfvars.example terraform.tfvars
$EDITOR terraform.tfvars                    # set external_network + ssh_public_key
source ~/dhbwcloud-openrc.sh                # OS_AUTH_URL etc.
terraform init
terraform plan
terraform apply
```

`terraform output kubeconfig_fetch_command` prints the exact one-liner that
copies the `k3s.yaml` from the control node and rewrites the cluster server URL
from `127.0.0.1:6443` to the control floating IP.

### 2. Fetch kubeconfig

```bash
$(terraform output -raw kubeconfig_fetch_command)
kubectl get nodes      # 3 nodes Ready (one control, two workers)
```

The control node pre-stages the kubeconfig in `~ubuntu/.kube/config` for SSH
debugging. Treat the file as a secret.

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

- DHBWCloud project quota (vCPU/RAM/disk per VM): if `m1.xlarge` is unavailable,
  set `worker_flavor` to whatever the team has and revisit the resource
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
