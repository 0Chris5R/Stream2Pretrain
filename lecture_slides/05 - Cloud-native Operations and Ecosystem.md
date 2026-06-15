<div class="lecturetitle">Cloud-native Operations & Ecosystem</div>

---
## The Production Gap

Deploying an application vs. operating it reliably in production
- Deployment is a one-time event; operations is an ongoing process

Day 1 (deployment)
- Write manifests, push an image, apply to the cluster
- Works in a controlled environment <comment>(predictable load, single team)</comment>

Day 2 (operations)
- Traffic grows, spikes, or drops unexpectedly
- Configuration drifts from what was intended
- Services fail in subtle ways that are hard to reproduce
- One cluster, many teams & services <comment>(coordination: bottleneck)</comment>

Requires a different mindset and tooling to bridge this gap

---
## Cloud-native Operations Challenges

Typical operational challenges in production

| Challenge                         | Without a solution                                 | Solution             |
| --------------------------------- | -------------------------------------------------- | -------------------- |
| Unknown system state              | Debugging by guessing                              | Observability        |
| Unpredictable load                | Manual replica adjustment                          | Autoscaling          |
| Configuration drift               | Cluster diverges from Git                          | GitOps               |
| Secrets in plain YAML             | Credentials exposed in repos                       | Secret management    |
| Services trust each other blindly | Compromised services can call/compromise any other | Service Mesh         |
| 50 teams need databases           | Ticket queue to ops team                           | Platform Engineering |

<!-- .element: style="margin-left: 20px;" -->

No single tool solves all of these
- Cloud-native operations is a set of complementary practices
- These are part of a broader organizational shift

---
## DevOps and Site Reliability Engineering

DevOps
- Development and operations teams share responsibility for reliability
- "You build it, you run it" <comment>(developers own lifecycle of their service)</comment>
- Fast feedback loops <comment>(deploy frequently, observe, improve)</comment>

Site Reliability Engineering <comment>(SRE, [originated at Google](https://en.wikipedia.org/wiki/Site_reliability_engineering))</comment>
- Apply software engineering practices to operations problems
- Error budgets: allowed downtime per month <comment>(e.g., 99.9% = ~43 min)</comment>; teams can move fast as long as they stay within budget
- Toil reduction: automate repetitive tasks <comment>(e.g., start service on deploy)</comment>

Cloud-native tooling enables these practices at scale
- Comprises GitOps <comment>(deployments auditable and self-healing)</comment>, Observability <comment>(feedback loop)</comment>, Platform Engineering <comment>(reduces toil)</comment>, ...

---
## The Cloud-Native Ecosystem

[Cloud Native Computing Foundation](https://www.cncf.io/) (CNCF) 
- [CNCF Landscape](https://landscape.cncf.io/) maps over 1,000 tools across 30+ categories
- Kubernetes is the most prominent project, but far from the only one
- Graduation levels: Sandbox → Incubating → Graduated

Choosing tools is a recurring engineering decision
- Maturity, community size, operational complexity, licensing, ...

| Category      | Exemplary tools                            |
| ------------- | ------------------------------------------ |
| Observability | Prometheus, Grafana, Jaeger, OpenTelemetry |
| Autoscaling   | HPA, VPA, KEDA, Cluster Autoscaler         |
| GitOps        | Flux, ArgoCD                               |
| Security      | Falco, Trivy, Sigstore, cert-manager       |
| Service Mesh  | Istio, Linkerd, Cilium                     |
| Platforms     | Crossplane, Backstage                      |
<!-- .element: style="margin-left: 20px;" -->
