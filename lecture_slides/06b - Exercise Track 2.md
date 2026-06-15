<div class="lecturetitle">Exercise Track 2</div>
<!-- .slide: data-name="Introduction" data-state="hide-menubar" -->

---
<!-- ---------------------------------------------------------------------------------- -->
<!-- ---------------------------------------------------------------------------------- -->
# Observability
<!-- .slide: id="observability" data-name="Observability" -->
<!-- ---------------------------------------------------------------------------------- -->
<!-- ---------------------------------------------------------------------------------- -->

---
## Observability
Three pillars of observability (visible in a single Grafana UI)
- Traces <comment>(optional, e.g., Jaeger, out of scope here)</comment>
- Metrics <comment>(Prometheus scrapes metrics; dashboards in Grafana)</comment>
- Logs <comment>(Loki stores logs, Grafana Alloy collects logs from every pod)</comment>

Single, central dashboard for both metrics and logs
- Correlate metrics and logs for faster troubleshooting

Example: spike in HTTP requests
- Grafana dashboard shows a sudden spike HTTP requests to the API
- Click to filter logs for that time window
- Discover the root cause in the logs

---
## Observability Stack

Deploy metrics/logs tools into the `monitoring` namespace
-  Three Helm charts to deploy [Kube Prometheus Stack](https://github.com/prometheus-operator/kube-prometheus), Loki and Alloy

[`kube-prometheus-stack`](https://github.com/prometheus-community/helm-charts/tree/main/charts/kube-prometheus-stack)
- Deploy Prometheus, Alertmanager, and Grafana
  
[`grafana/loki`](https://github.com/grafana-community/helm-charts)
- Deploy as [single-binary mode](https://grafana.com/docs/loki/latest/get-started/deployment-modes/) <comment>(by setting `deploymentMode: SingleBinary`)</comment>

[`grafana/alloy`](https://github.com/grafana/alloy)
- DaemonSet log collector; auto-discovers pod logs
- No manual sidecar injection or agent configuration required

---
## Observability: Kube Prometheus Stack

Values for Grafana and Prometheus
- Configures Loki as an additional datasource in Grafana

<a data-code='yaml' data-link href="code/gridflex/ansible/templates/kube-prometheus-stack-values.yaml.j2">kube-prometheus-stack-values.yaml</a>
<!-- .element: style="font-size: 0.55em;" -->

---
## Observability: Loki Helm Values

[Uses Loki grafana-community helm chart](https://github.com/grafana-community/helm-charts)

<a data-code='yaml' data-link href="code/gridflex/ansible/files/loki-values.yaml">loki-stack-values.yaml</a>
<!-- .element: style="font-size: 0.7em;" -->

---
## Observability: Alloy Helm Values

Uses `loki.source.kubernetes` to stream logs via the K8s API
- Discovers all pods, extracts namespace/pod/container/app as Loki labels

<a data-code='yaml' data-link href="code/gridflex/ansible/files/alloy-values.yaml">alloy-values.yaml</a>
<!-- .element: style="font-size: 0.36em;" -->

---
## Deploy Observability Stack

<a data-code='bash' data-link data-begin="# === Install" href="code/gridflex/ansible/tasks/observability.yaml">deploy.yaml</a>
<!-- .element: style="font-size: 0.75em;" -->

---
## Instrument gridflex-api

Make the application observable by exposing metrics and logs
- Create a ServiceMonitor for Prometheus to scrape `/metrics`
- See, how the example app is instrumented with `prom-client` to expose metrics

<a data-code='js' data-link href="code/gridflex/api/src/metrics.js">metrics.js</a>
<!-- .element: style="font-size: 0.65em;" -->

---
## Instrument gridflex-api

Add a middleware to count requests and latency

<a data-code='js' data-link data-outdent 
data-begin="Metrics Middleware" data-end="// ───" href="code/gridflex/api/src/index.js">index.js</a>

---
## Add ServiceMonitor to gridflex-api

Update the Helm chart to include a `ServiceMonitor` for gridflex-api
- Add a flag to enable or disable is with default `disabled: true`
- E.g., use `api.serviceMonitor.enabled=true`

<a data-code='yaml' data-link href="code/gridflex/helm-chart/templates/servicemonitor.yaml">deploy.yaml</a>
<!-- .element: style="font-size: 0.8em;" -->

---
## Update the running deployment

Update the running deployment to enable the ServiceMonitor
- Add a [profile](https://skaffold.dev/docs/environment/profiles/) to Skaffold using  a [patch](https://skaffold.dev/docs/environment/profiles/#override-via-patches)
- Set `api.serviceMonitor.enabled=true`

Example code 
- Enable using `skaffold dev -p monitoring`

<a data-code='yaml' data-begin="# Profiles" data-end="# Autoscaling" data-link href="code/gridflex/skaffold.yaml">deploy.yaml</a>
<!-- .element: style="font-size: 1em;" -->

---
## Query in Grafana

Generate some load on the API and query in Grafana
- Metrics: `gridflex_http_requests_total` or `gridflex_http_request_duration_seconds`
- Logs: `{namespace="default", container="gridflex-api"} |= "debug" != "metrics"` in the Loki datasource

<a data-code='bash' data-link href="code/gridflex/load-test.sh">load-test.sh</a>
<!-- .element: style="font-size: 0.65em;" -->

---
## Provision Dashboards Automatically

Ship dashboards as code instead of clicking it together
- Grafana helm chart deploys a sidecar loading Dashboards 
- Loads any ConfigMap labelled with `grafana_dashboard: "1"`
- No restart required; updates are picked up automatically
- Generate in Grafana, export with "Share dashboard" → "Export"

Example dashboard manifest


<a data-code='yaml' data-link data-end="gridflex.json" href="code/gridflex/helm-chart/templates/grafana-dashboard.yaml">grafana-dashboard.yaml</a>

---
## Provision Dashboards Automatically

Example dashboard JSON (excerpt)
- Bundle it in the Helm chart, gated on `monitoring.enabled`
- Includes request rate, latency quantiles, a latency heatmap, replicas vs. load, and logs

<a data-code='yaml' data-link data-begin="gridflex.json" data-end="Request latency (quantiles)" href="code/gridflex/helm-chart/templates/grafana-dashboard.yaml">grafana-dashboard.yaml</a>
<!-- .element: style="font-size: 0.5em;" -->













---
<!-- ---------------------------------------------------------------------------------- -->
<!-- ---------------------------------------------------------------------------------- -->
# Autoscaling
<!-- .slide: id="autoscaling" data-name="Autoscaling" -->
<!-- ---------------------------------------------------------------------------------- -->
<!-- ---------------------------------------------------------------------------------- -->

Application need to handle varying load patterns
- Normal growth: lowly increasing load  <comment>(new devices are onboarded)</comment>
- Grid events: sudden surge <comment>(many devices confirm simultaneously)</comment>

Requires dynamic scaling of the application 
- HPA is too inflexible for the bursty load patterns
- Use KEDA to scale based on custom metrics and CPU

Custom metric: Valkey queue depth
- API pods publish to a Valkey list `event-confirmations`
- KEDA scales the deployment based on the length of this list
- If the list length exceeds a threshold (e.g., 5 per replica)
- KEDA adds more replicas to handle the load

---
## Autoscaling: Install KEDA

Install KEDA in the cluster
- Add this task to the Ansible playbook

<a data-code='yaml' href="code/gridflex/ansible/tasks/keda.yaml">deploy.sh</a>

Verify the installation 
- Run `kubectl get pods -n keda`  

---
## Autoscaling: ScaledObject

Add KEDA autoscaling to the application
- Add a `ScaledObject` to the app's Helm chart targeting gridflex-api
- [CPU trigger](https://keda.sh/docs/latest/scalers/cpu/): scale when average CPU utilization exceeds 70%
- [Redis/Valkey trigger](https://keda.sh/docs/latest/scalers/redis-lists/): scale when `event-confirmations` list length exceeds 5 per replica
- `minReplicaCount: 1`, `maxReplicaCount: 10`

Add it to the helm chart (gated on `autoscaling.enabled`)
- Enable using `skaffold dev -p monitoring -p autoscaling`

<a data-code='yaml' data-begin="# Autoscaling profile" data-end="# Middlewares" data-link href="code/gridflex/skaffold.yaml">deploy.yaml</a>

---
## Example: ScaledObject

<a data-code='yaml' data-link href="code/gridflex/helm-chart/templates/scaledobject.yaml">scaledobject.yaml</a>
<!-- .element: style="font-size: 0.8em;" -->

---
## Observe Scaling Behaviour in Grafana

Generate Load
- `-t` <comment>(seconds)</comment>, `-c` <comment>(concurrency)</comment>, `-p` <comment>(POST body)</comment>, `-T` <comment>(content-type)</comment>

<a data-code='bash' data-link href="code/gridflex/load-test.sh">load-test.sh</a>
<!-- .element: style="font-size: 0.6em;" -->

Watch the ScaledObject and pod count react
- Use the dashboard or query KEDA CR directly <comment>(e.g., using `kubectl get scaledobject gridflex -o yaml`)</comment>











---
<!-- ---------------------------------------------------------------------------------- -->
<!-- ---------------------------------------------------------------------------------- -->
<!-- ---------------------------------------------------------------------------------- -->
# API Gateway
<!-- .slide: id="api-gateway" data-name="API Gateway" -->
<!-- ---------------------------------------------------------------------------------- -->
<!-- ---------------------------------------------------------------------------------- -->
<!-- ---------------------------------------------------------------------------------- -->


Add three policies at the cluster edge with Traefik
- HTTP to HTTPS redirection
- Rate limiting to prevent abuse and protect the API from overload
- OIDC protection of the API using Keycloak as the identity provider


---
## HTTP to HTTPS redirection

Create the middleware
- `kubectl apply -f` <span data-prefix-url data-convert-to-inline-code="bash">code/gridflex/api-gw/middleware-https-redirect.yaml</span>

<a data-code="yaml" href="code/gridflex/api-gw/middleware-https-redirect.yaml" target="_blank">Middleware manifest</a>

---
## Rate Limiting

Create the middleware
- `kubectl apply -f` <span data-prefix-url data-convert-to-inline-code="bash">code/gridflex/api-gw/middleware-rate-limit.yaml</span>

<a data-code="yaml" href="code/gridflex/api-gw/middleware-rate-limit.yaml" target="_blank">Full manifest</a>

---
## OIDC: Register OIDC Plugin

Register the plugin with Traefik via a `HelmChartConfig`
- Changes k3s's Traefik Helm config using a CR

<a data-code="yaml" data-begin="# Traefik: enable traefik-oidc-auth plugin" data-end="# Label nodes and prepare for workloads" href="code/gridflex/ansible/tasks/k3s-configure.yaml">k3s-configure.yaml</a>
<!-- .element: style="font-size: 0.7em;" -->

---
## OIDC: Configure Keycloak IDP

Create a Middleware that uses the OIDC plugin
- Change values to match your environment; in production, the secret should be stored securely
- `kubectl apply -f` <span data-prefix-url data-convert-to-inline-code="bash">code/gridflex/api-gw/middleware-oidc-auth.yaml</span>

<a data-code="yaml" data-link href="code/gridflex/api-gw/middleware-oidc-auth.yaml" target="_blank">Full manifest</a>
<!-- .element: style="font-size: 0.72em;" -->

---
## Apply Middlewares to the Ingress

Annotate the Ingress to apply the middlewares
- Update the applications Helm chart to include middleware values

<a data-code='yaml' data-link data-begin="# Apply middlewares" data-end="spec:" data-outdent href="code/gridflex/helm-chart/templates/ingress.yaml">ingress.yaml</a>

Set the middleware values via a Skaffold profile
- Run `skaffold dev -p monitoring,autoscaling,middlewares`

<a data-code='yaml' data-link data-begin="# Middlewares profile" data-end="# Canary" href="code/gridflex/skaffold.yaml">skaffold.yaml</a>
<!-- .element: style="font-size: 0.8em;" -->

---
## Verify the Policies are in Effect

HTTP to HTTPS
- `curl -I http://gridflex-api.<zone>` should return `301 Moved Permanently` with a `Location: https://...` header

Rate limiting
- `curl -I https://gridflex-api.<zone>` repeatedly should eventually return `429 Too Many Requests`

OIDC
- `curl -I https://gridflex-api.<zone>` should return `401 Unauthorized` with a `WWW-Authenticate: OIDC` header
- Open the URL in a browser to test the full OIDC flow with Keycloak

Remove the middlewares when done
- Run `skaffold run -p monitoring,autoscaling` to disable the middlewares profile




---
<!-- ---------------------------------------------------------------------------------- -->
<!-- ---------------------------------------------------------------------------------- -->
<!-- ---------------------------------------------------------------------------------- -->
# Service Mesh
<!-- .slide: id="service-mesh" data-name="Service Mesh" -->
<!-- ---------------------------------------------------------------------------------- -->
<!-- ---------------------------------------------------------------------------------- -->
<!-- ---------------------------------------------------------------------------------- -->

Optional extension: bring east-west traffic under mesh control
- Install Linkerd and inject the application namespace
- Observe mTLS, golden metrics, and live traffic with no application changes
- Add a request timeout from gridflex-api to gridflex-ai

Linkerd is chosen over Istio for this exercise
- Lower resource footprint on a three-node dev cluster
- Installs and verifies in a few minutes

---
## Deploying Linkerd

[Linkerd](https://github.com/linkerd/linkerd2) is the simplest production-grade service mesh to operate
- Requires [installing the CLI](https://linkerd.io/2.19/getting-started/#step-1-install-the-cli)
- Validate the [cluster for compatibility](https://linkerd.io/2.19/getting-started/#step-3-validate-your-kubernetes-cluster): `linkerd check --pre`

Install the [control plane](https://linkerd.io/2.19/getting-started/#step-4-install-linkerd-onto-your-cluster) in the cluster

<a data-code='bash' data-link href="code/gridflex/mesh/install.sh">install.sh</a>

Verify all components are healthy with `linkerd check`

---
## Inject GridFlex Namespaces

Sidecar injection requires namespace annotation
- Existing Pods need a rollout restart to receive it

<a data-code='bash' data-link href="code/gridflex/mesh/inject.sh">inject.sh</a>

No application code or manifests changed
- The mesh is layered transparently below the existing Deployments

---
## Verify mTLS

Every meshed connection is encrypted
- Uses a certificate issued by the mesh CA
- Check mark in the `SECURED` column means both peers verified each other

Non-mTLS traffic is still allowed by default
- Traffic to and from unmeshed workloads is allowed but not encrypted
- An unmeshed Pod talking to a meshed Pod shows up as not secured

Display edges at the Pod level

<a data-code='bash' data-link href="code/gridflex/mesh/observe-edges.sh">observe-edges.sh</a>

---
## Observe Golden Metrics

Sidecar exports the RED triplet for every meshed Deployment
- Rate <comment>(RPS)</comment>, Errors <comment>(success rate)</comment>, Duration <comment>(p50/p95/p99)</comment> for HTTP
- TCP connections still show up as metrics

<a data-code='bash' data-link href="code/gridflex/mesh/observe-stat.sh">observe-stat.sh</a>
<!-- .element: style="font-size: 0.85em;" -->

Does not require application instrumentation
- Metrics are generated from the network layer

---
## Inspect Live Traffic with `tap`

`tap` streams a structured trace of every request hitting a workload
- HTTP-aware <comment>(method, path, status, latency)</comment>, no root permissions, no application logging

<a data-code='bash' data-link href="code/gridflex/mesh/observe-tap.sh">observe-tap.sh</a>

Useful patterns
- Filter for failures: `grep ':status=5'`
- Filter for policy denials: `grep -i denied`
- Confirm a request actually reached the expected Pod and namespace

---
## Removing Linkerd

Uninstall in reverse order 
- Un-mesh first, then tear down the mesh

<a data-code='bash' data-link data-end="kjhljhjkh" href="code/gridflex/mesh/uninstall.sh">uninstall.sh</a>



---
<!-- ---------------------------------------------------------------------------------- -->
<!-- ---------------------------------------------------------------------------------- -->
<!-- ---------------------------------------------------------------------------------- -->
# Deployment Strategies
<!-- .slide: id="deployment-strategies" data-name="Deployment Strategies" -->
<!-- ---------------------------------------------------------------------------------- -->
<!-- ---------------------------------------------------------------------------------- -->
<!-- ---------------------------------------------------------------------------------- -->

Update with zero downtime and no user impact
- Roll out a new version of gridflex-api without dropping traffic

Rolling Update
- built into the Deployment controller
- One ReplicaSet replaces the other

Canary Release
- A separate Deployment receives a small share of traffic via Traefik
- Observe the canary's health and shift traffic gradually before cutting over

Gridflex reports its version in the `/health` endpoint
- Extracts it from the `SERVER_VERSION` env var
- This is wired to the image tag [in the Helm chart](code/gridflex/helm-chart/templates/deployment.yaml)

---
## Rolling Update

Default strategy for Deployments
- No extra configuration needed

Using helm
- `helm upgrade` triggers a rolling update by default
- No need to change the chart
- Rollback if needed `helm rollback gridflex`

Using Skaffold
- Implicitly performed by `skaffold dev` <comment>(using generated version tags)</comment>
- Can also be triggered by `skaffold run` if the image tag changes
- E.g., `skaffold run --tag v1` and then `skaffold run --tag v2`

---
## Canary Release with TraefikService

We use the same image for both stable and canary
- Just to simplifiy the demonstration
- In a real scenario, the canary would run a newer image version

The helm chart has a `canary` profile 
- Adds a canary Deployment and Service
- Replaces the Ingress with Traefik's IngressRoute
- Uses a TraefikService to split traffic between stable and canary

This profile is activated by `-p canary` in Skaffold
- Flips `canary.enabled=true` in the chart values
- So, no separate v2 image to push for this demo

---
## Canary Release: TraefikService

Splits traffic between the stable and canary Services 
- Based on the `canary.weight` value

<a data-code='yaml' data-begin="# Begin: TraefikService" data-end="---" data-link href="code/gridflex/helm-chart/templates/canary.yaml">canary.yaml</a>

---
## Canary Release: IngressRoute

Replaces the plain Ingress

<a data-code='yaml' data-begin="# Begin: IngressRoute" data-end="---" data-link href="code/gridflex/helm-chart/templates/canary.yaml">canary.yaml</a>

---
## Canary Release: Deploy

Add a canary profile to Skaffold

<a data-code='yaml' data-begin="# Canary profile" href="code/gridflex/skaffold.yaml">skaffold.yaml</a>
<!-- .element: style="font-size: 0.8em;" -->

Deploy both versions with one Skaffold build
- Run `skaffold dev -p canary` 
- Flips `canary.enabled=true` in the helm chart

Verify both versions are running

```bash
for i in {1..50}; do
  curl -s https://gridflex-api.<zone>/health | jq -r .version
done | sort | uniq -c
```

---
## Canary Release: Shift Traffic

Move traffic between stable and canary by patching the weights

<a data-code='bash' data-link href="code/gridflex/api-gw/canary-shift.sh">canary-shift.sh</a>
<!-- .element: style="font-size: 0.9em;" -->

Use the loop from the previous slide
- Observe the shift in traffic distribution








---
<!-- ---------------------------------------------------------------------------------- -->
<!-- ---------------------------------------------------------------------------------- -->
<!-- ---------------------------------------------------------------------------------- -->
# GitOps
<!-- .slide: id="gitops" data-name="GitOps" -->
<!-- ---------------------------------------------------------------------------------- -->
<!-- ---------------------------------------------------------------------------------- -->
<!-- ---------------------------------------------------------------------------------- -->

---
## From `helm upgrade` to GitOps

Until now, changes were applied manually from a developer machine
- The cluster state is what was last applied <comment>(no audit trail or drift detection)</comment>
- Secrets live next to the chart in plain text
- Goal: make Git the single source of truth for the cluster state

Requires Git server and 
- Deploy an in-cluster Git server <comment>(no public GitHub needed)</comment>
- Bootstrap ArgoCD into the cluster <comment>(one-time)</comment>
- Commit the chart and have ArgoCD reconcile it
- Use an ApplicationSet to manage dev and prod from one place

---
## Deploy Forgejo as In-Cluster Git

[Forgejo](https://forgejo.org/) is a lightweight self-hosted Git server <comment>(Gitea fork)</comment>
- Single replica, SQLite, one PVC — fits the dev cluster
- Add a Ansible task to install Forgejo using Helm using this [values.yaml](code/gridflex/ansible/files/forgejo-values.yaml)
- Exposes SSH for git on port 32022

<a data-code='yaml' data-end="# === Forgejo Project ===" href="code/gridflex/ansible/tasks/forgejo.yaml">forgejo.yaml</a>
<!-- .element: style="font-size: 0.75em;" -->

---
## Create Forgejo Project and Repository

Log into the Forgejo UI
- Login with `admin`/`admin`
- Create a new repository
- Add your SSH public key as a deploy key with write access

Upload the gridflex application to the repository
- Use `git remote add` and `git push`

---
## Bootstrap ArgoCD

ArgoCD ships as plain manifests <comment>(no Helm chart needed)</comment>
- Apply manifests into its own namespace, then wait for the server
- Add a Ansible task to bootstrap ArgoCD

Example

<a data-code='yaml' data-end="# === ArgoCD Insecure Mode" href="code/gridflex/ansible/tasks/argocd.yaml">argocd.yaml</a>
<!-- .element: style="font-size: 0.73em;" -->

---
## Bootstrap ArgoCD: Allow Insecure Mode

Allow insecure mode to let Traefik terminate TLS for the UI

<a data-code='yaml' data-begin="# === ArgoCD Insecure Mode" data-end="# ===" href="code/gridflex/ansible/tasks/argocd.yaml">argocd.yaml</a>
<!-- .element: style="font-size: 0.8em;" -->

---
## Bootstrap ArgoCD: Allow Ingress

Create an Ingress for the UI
- To reach it at `https://argocd.<...>.users.dhbw.site`

<a data-code='yaml' data-begin="# === ArgoCD Ingress" data-end="# ===" href="code/gridflex/ansible/tasks/argocd.yaml">argocd.yaml</a>
<!-- .element: style="font-size: 0.67em;" -->

---
## Bootstrap ArgoCD: Admin Password

Create a known admin password (lecture only, not for production)

<a data-code='yaml' data-begin="# === ArgoCD Admin Password" data-end="# ===" href="code/gridflex/ansible/tasks/argocd.yaml">argocd.yaml</a>

Log into the UI
- Use `admin` / `admin`

---
## Application: GridFlex from Git

Point a single Application at the Helm chart and the dev values file
- `repoURL` is the cluster-internal Forgejo Service <comment>(no auth, no public DNS)</comment>

```yaml
apiVersion: argoproj.io/v1alpha1
kind: Application
metadata:
  name: gridflex
  namespace: argocd
spec:
  source:
    repoURL: http://forgejo-http.forgejo.svc:3000/gitops/gridflex.git
    targetRevision: main
    path: code/gridflex/helm-chart
    helm:
      valueFiles: [values-dev.yaml]
  destination:
    server: https://kubernetes.default.svc
    namespace: gridflex
  syncPolicy:
    automated: { prune: true, selfHeal: true }
    syncOptions: [CreateNamespace=true]
```

<a data-code="yaml" href="code/gridflex/gitops/application-gridflex.yaml" target="_blank">Full manifest</a>

---
## Observe Reconciliation and Drift

Make a visible change through Git
- Increase `api.replicas` in `values-dev.yaml`, commit, push
- ArgoCD detects the change within the polling interval and rolls out new pods
- No `kubectl`, no `helm upgrade` — the commit is the deploy

Trigger drift on purpose

```bash
kubectl scale deployment gridflex-api -n gridflex --replicas=10
```

ArgoCD reports `OutOfSync` and reverts the change <comment>(because `selfHeal: true`)</comment>
- The Git state always wins; manual edits are temporary at best

Roll back by reverting the commit
- `git revert` instead of `helm rollback` — the cluster follows automatically

---
## Note: Secrets in GitOps

In this exercise, secrets are deployed before ArgoCD, outside the GitOps loop
- MongoDB password: plain Secret applied by Ansible at cluster setup
- Forgejo admin password: SOPS-encrypted, decrypted with `sops -d | kubectl apply` at bootstrap <comment>(see the SOPS chapter)</comment>
- Both must exist before ArgoCD syncs <comment>(ArgoCD reads its repo from Forgejo)</comment>

Possible extension <comment>(not done here)</comment>: let ArgoCD decrypt SOPS secrets itself
- Add a [helm-secrets](https://github.com/jkroepke/helm-secrets) Config Management Plugin to `argocd-repo-server`
- Store the age private key as a Secret in the `argocd` namespace
- Commit an encrypted `secrets.yaml`; the plugin decrypts it on sync
- Only works for secrets living inside an ArgoCD-managed chart <comment>(not bootstrap secrets like Forgejo's)</comment>

---
## ApplicationSet: Dev and Prod from One Template

A second environment without a second hand-written Application

```yaml
apiVersion: argoproj.io/v1alpha1
kind: ApplicationSet
metadata: { name: gridflex-environments, namespace: argocd }
spec:
  generators:
    - list:
        elements:
          - { env: dev,  namespace: gridflex-dev,  valuesFile: values-dev.yaml }
          - { env: prod, namespace: gridflex-prod, valuesFile: values-prod.yaml }
  template:
    metadata: { name: 'gridflex-{{env}}' }
    spec:
      source:
        repoURL: http://forgejo-http.forgejo.svc:3000/gitops/gridflex.git
        path: code/gridflex/helm-chart
        helm: { valueFiles: ['{{valuesFile}}'] }
      destination:
        server: https://kubernetes.default.svc
        namespace: '{{namespace}}'
      syncPolicy: { automated: { prune: true, selfHeal: true } }
```

<a data-code="yaml" href="code/gridflex/gitops/applicationset-envs.yaml" target="_blank">Full manifest</a>

---
## What Changed for GridFlex

The chart, the secrets, the environments, and the rollout strategy now all live in Git
- A clean cluster + one `kubectl apply` of the root Application brings the full stack up
- Promotion from dev to prod = a pull request from `values-dev.yaml` into `values-prod.yaml`
- Every observability dashboard, every middleware, every canary weight — committed, reviewable, revertible

This closes the arc from the Helm chapter through observability, security, traffic management, and deployment strategies
- The chart written in 04e is the single source of truth for the running cluster
- The platform pieces installed by hand earlier are next candidates to fold into the same App-of-Apps








---
## Assessment: Bachelor Portfolio

Three portfolio parts <comment>(PF)</comment>, submitted and presented progressively during the semester

| Part | Format                                                | Weight | Timing            | Content                                                                                             |
| ---- | ----------------------------------------------------- | ------ | ----------------- | --------------------------------------------------------------------------------------------------- |
| PF1  | Group presentation <comment>(5 min + demo)</comment>  | 30%    | After chapter 04e | Container image, MongoDB StatefulSet, Helm chart — live demo with a running API and replica set     |
| PF2  | Group presentation <comment>(10 min + demo)</comment> | 40%    | After chapter 05c | Observability dashboard, KEDA scaling demo, GitOps workflow, natural language query via gridflex-ai |
| PF3  | Written documentation                                 | 30%    | End of semester   | Architecture diagram, key decisions, lessons learned <comment>(5–8 pages per group)</comment>       |

Groups of 2–4 students

Assessment criteria per presentation
- Deployment runs without errors during the live demo
- At least one student can explain an infrastructure decision and connect it to the business problem
- Written documentation reflects the actual implementation, not a textbook description

---
## Assessment: Master Laborarbeit

Groups of 2–4 students design and implement their own cloud-native application in a domain of their choice
- GridFlex serves as a reference implementation — students may adapt it or build something different
- The domain must genuinely justify the infrastructure choices

Required topics <comment>(all three must be present)</comment>
- Kubernetes deployment with resource management and health probes
- Persistent storage via PVC or CSI-mounted object storage
- Observability: Prometheus metrics, Grafana dashboard, at least one alert rule

---
## Assessment: Master Laborarbeit

Optional topics <comment>(choose at least two)</comment>
- Autoscaling: HPA or KEDA triggered by a domain-relevant custom metric
- Traffic management: API gateway or service mesh with routing rules
- GitOps: ArgoCD or Flux, auto-sync from a Git repository
- Helm chart: full application installable with a single `helm install`, dev and prod values files
- AI workload: function-calling SLM, model serving with environment-specific resource profiles

Deliverable: written report of 15–20 pages per group
- Architecture overview, deployment instructions, and technical decisions with trade-offs
- Repository link <comment>(public GitHub or GitLab)</comment> with a working, reproducible deployment
- One section justifying why the chosen domain benefits from cloud-native infrastructure

---
## Grading Criteria

| Criterion     | Description                                                                       | Weight |
| ------------- | --------------------------------------------------------------------------------- | ------ |
| Correctness   | Deployment runs, services communicate, probes pass                                | 40%    |
| Depth         | Infrastructure decisions are justified and connected to the application domain    | 30%    |
| Coverage      | Required topics are fully implemented, not just declared in manifests             | 20%    |
| Documentation | Architecture is described clearly, instructions are reproducible by a third party | 10%    |

Four criteria by design — the depth criterion rewards students who can explain WHY, not just HOW

