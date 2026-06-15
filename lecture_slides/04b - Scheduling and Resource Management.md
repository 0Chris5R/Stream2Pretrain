<div class="lecturetitle">Scheduling &amp; Resource Management</div>
<!-- .slide: data-name="Introduction" data-state="hide-menubar" -->

---
## Shared Cluster &rarr; Competing Workloads

K8s clusters are shared (multiple workloads on the same nodes)
- One Pod could consume all CPU or memory and starve others
- Pods could run on arbitrary nodes regardless of hardware or location

Kubernetes provides scheduling and resource management
- Resource management: assign / limit resources a Pod can consume
- Scheduling control: influence where Pods are placed in the cluster

All features are optional and can be used as needed
- Can be added incrementally as requirements evolve
- Resource and scheduling features are declarative 
- E.g., dev clusters don't need resource limits or scheduling constraints

---
## Resource Management

Resource management
- Guarantees minimum resources a Pod needs to run <comment>(requests)</comment>
- Caps the maximum a Pod may consume <comment>(limits)</comment>
- Handles specialized hardware <comment>(GPUs, TPUs, FPGAs)</comment>
- Detects and recovers broken or overloaded containers <comment>(probes)</comment>

Scheduling control
- Place Pods on nodes with specific hardware or labels <comment>(node affinity)</comment>
- Co-locate <comment>(pod affinity)</comment> or separate <comment>(anti-affinity)</comment> Pods  
- Reserve nodes for specific workloads <comment>(taints & tolerations)</comment>
- Distribute replicas evenly across zones or nodes <comment>(topology spread constraints)</comment>


<!--- ------------------------------------------------------------------- --->
<!--- ------------------------------------------------------------------- --->
<!--- ------------------------------------------------------------------- --->
<!--- ------------------------------------------------------------------- --->
---
# Resource Requests, Limits and Extended Resources
<!-- .slide: data-name="Requests/Limits" -->

---
## Resource Requests and Limits

Using virtual machines, there are implicit resource limits
- E.g., CPU, memory, disk space, etc.
- Processes exceeding these limits can render a VM unresponsive
- But it will not affect other VMs

Kubernetes: Pods are scheduled to nodes
- From a OS perspective, a Pod is one or more processes
- The OS performs scheduling and resource management
- A Pod consuming all resources affects other Pods on the same node

Kubernetes allows to assign and limit resources
- Per Pod limits avoid resource starvation
- Kubernetes' `cadvisor` monitors resource usage

---
## Resource Requests and Limits

Different [resource types](https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/) can be assigned
- CPU, memory, ephemeral storage, etc.

Resource requests
- Minimum resources required for a Pod to run
- Scheduler uses requests to find a node with enough resources
- If a Pod cannot be scheduled, it is in `Pending` / `FailedScheduling` state
- Adding resources to a cluster may allow scheduling

Resource limits
- Maximum resources a Pod can consume
- Using more, it is throttled or terminated

---
## Resource Management: Memory

Assign and limit [memory](https://kubernetes.io/docs/tasks/configure-pod-container/assign-memory-resource/) resources
- Measured in bytes (integer or a fixed-point integer)
- Suffixes: E, P, T, G, M, K, Ei, Pi, Ti, Gi, Mi, Ki

```YAML
apiVersion: v1
kind: Pod
metadata:
  name: memory-demo
spec:
  containers:
  - name: memory-demo-ctr
    image: colinianking/stress-ng
    command: ["stress-ng"]
    args: ["--vm", "1", "--vm-bytes", "99%", "--vm-keep", "--timeout", "60m"]
    resources:
      requests: # requests resources
        memory: "100Mi"
      limits: # resource limit (enforced by the OS)
        memory: "200Mi"
```
<!-- .element: style="font-size: 0.6em;" -->

---
## Resource Management: CPU

Assign and limit [CPU](https://kubernetes.io/docs/tasks/configure-pod-container/assign-cpu-resource/) resources 
- Measured in CPU units (<comment>depending on the cloud provider</comment>)
- 1 CPU Unit &rarr; 1 AWS vCPU, 1 GCP Core, 1 Azure vCore, 1 Hyperthread Bare metal

```YAML
apiVersion: v1
kind: Pod
metadata:
  name: cpu-demo
spec:
  containers:
  - name: cpu-demo-ctr
    image: colinianking/stress-ng
    command: ["stress-ng"]
    args: ["--cpu", "0", "--timeout", "10m", "--metrics-brief"]
    resources:
      requests: # resource request (used for scheduling)
        cpu: "0.5"
      limits: # resource limit (enforced by the OS)
        cpu: "1"
```

<!-- .element: style="font-size: 0.6em;" -->

---
## Resource Management: Ephemeral Storage

Pods use [ephemeral storage](https://kubernetes.io/docs/concepts/storage/ephemeral-volumes/) that can be [limited](https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/#setting-requests-and-limits-for-local-ephemeral-storage)
- Not persistent across Pod restarts
- E.g., writeable layer, emptyDirs, container output, etc.


```YAML
apiVersion: v1
kind: Pod
metadata:
  name: cpu-demo
spec:
  containers:
  - name: cpu-demo-ctr
    image: colinianking/stress-ng
    command: ["stress-ng"]
    args: ["--hdd", "1", "--hdd-bytes", "10G", "--hdd-temp-path", "/tmp", "--timeout", "60m", "--metrics-brief"]
    resources:
      requests:
        ephemeral-storage: "2Gi"
      limits:
        ephemeral-storage: "4Gi"
```
<!-- .element: style="font-size: 0.6em;" -->

---
## Choosing Request and Limit Values

Wrong values cause real problems
- Requests too low → Scheduled on overloaded node, slow
- Limits too low → throttling <comment>(CPU)</comment> or OOM-killed <comment>(memory)</comment>
- Requests too high → resources reserved but idle, cluster fills up

How to find good values
- Start with no limits, observe actual usage under realistic load
- Measure using `kubectl top pods` or a metrics dashboard
- Set requests to typical usage, limits to 2-3× requests
- For memory, use a limit closer to the request
- Monitor and adjust over time as usage changes


---
## Extended Resources and Device Plugins

Kubernetes tracks only CPU, memory, and ephemeral storage
- Specialized hardware <comment>(e.g., GPUs, FPGAs, InfiniBand NICs)</comment> not monitored out-of-the-box

[Device plugins](https://kubernetes.io/docs/concepts/extend-kubernetes/compute-storage-net/device-plugins/) bridge the gap
- DaemonSet <comment>(one Pod/Node)</comment> running on nodes with this hardware
- Detects hardware <comment>(e.g., queries `nvidia-smi` for GPU count)</comment>
- Registers resources and their capacity with the local kubelet
- Kubelet reports capacity to the API server <comment>(for the scheduler to see it)</comment>

Once registered, the extended resource behaves like any other
- Scheduler uses it to find nodes that have capacity available
- Any conformant hardware vendor can publish a device plugin

---
## Resource Management: GPUs

GPUs can be requested and limited like any other resource
- Resource names are vendor-prefixed <comment>
- E.g., `nvidia.com/gpu`, `amd.com/gpu`, `example.com/fpga`

Example: request 1 NVIDIA GPU for a Pod
```yaml
resources:
  limits:
    nvidia.com/gpu: 1       # request exactly 1 NVIDIA GPU
    # amd.com/gpu: 1        # or an AMD GPU
```

Requests and limits must be equal
- GPUs cannot be overcommitted or fractionally shared by default
- A node with 4 GPUs can run at most 4 Pods requesting 1 GPU each

<!--- ------------------------------------------------------------------- --->
<!--- ------------------------------------------------------------------- --->
---
# Detect and recover broken or overloaded containers Using Probes
<!-- .slide: data-name="Probes" -->
<!--- ------------------------------------------------------------------- --->
<!--- ------------------------------------------------------------------- --->

---
## The Problem: Running ≠ Healthy

Docker and Kubernetes both monitor container processes
- A container is considered alive as long as PID 1 is running
- A running process is not necessarily a working process
- Maybe the app is deadlocked, the web server is running but has not sockets available, etc.

Docker already has basic health monitoring
- `HEALTHCHECK` in Dockerfile runs a command to check health
- Can mark container as unhealthy, but Docker does not act on it by default <comment>(no restart, no traffic removal)</comment>

Kubernetes uses Liveness, Readiness and Startup Probes
- Active health checks that drive real actions
- E.g., restart, traffic removal, delayed rollout

---
## Liveness, Readiness and Startup Probes

Startup probes (only during startup)
- Question: has the container started successfully?
- Disables liveness and readiness checks until it succeeds
- Useful for containers that take a long time to start

Liveness probes 
- Question: is the container alive?
- Determine whether to restart a container
- If undefined, uses health of PID 1

Readiness probes (is the container ready to serve traffic)
- Question: is the container ready to serve traffic?
- Not ready &rarr; remove from load balancers

---
## Startup and Liveness Probes

MariaDB initializes its data directory on first start
- Can take seconds to minutes depending on the size of the database

Checking status of database
- Port 3306 only opens after initialization is complete
- Use `mariadb-admin ping` as a command probe
- Checks if it responds to queries <comment>(e.g. using the UNIX socket)</comment>
- The readiness probe can use a TCP probe on port 3306

Liveness and startup probes are both required
- Even if they use the same command
- Liveness probe would kill the container before initialization completes
- Startup probe uses a more generous timeout to finish initialization

---
## Pod: Startup and Liveness Probes

Example using [command probes](https://kubernetes.io/docs/tasks/configure-pod-container/configure-liveness-readiness-startup-probes/#define-a-liveness-command)

<a data-code='yaml' href="code/examples/mysql-startup-init-and-probes.yaml" data-begin="Begin probe" data-end="End probe">Source code</a>

<!-- .element: style="font-size: 0.94em;" -->

---
## Pod: Liveness Probe Example

Example using a [HTTP liveness probe](https://kubernetes.io/docs/tasks/configure-pod-container/configure-liveness-readiness-startup-probes/#define-a-liveness-http-request)
- Checks if the container responds to HTTP requests on `/healthz` endpoint

```YAML
apiVersion: v1
kind: Pod
metadata:
  name: liveness-http
spec:
  containers:
  - name: liveness
    image: registry.k8s.io/liveness
    args:
    - /server
    livenessProbe:
      httpGet:
        path: /healthz
        port: 8080
      initialDelaySeconds: 3
      periodSeconds: 3
```



<!--- ------------------------------------------------------------------- --->
<!--- ------------------------------------------------------------------- --->
---
# Scheduling Control
<!-- .slide: data-name="Scheduling" -->
<!--- ------------------------------------------------------------------- --->
<!--- ------------------------------------------------------------------- --->


---
## Scheduling Control

Scheduler places Pods on any node with enough resources
- Works fine for stateless, homogeneous workloads
- Not sensible when nodes differ or placement matters

Real-world constraints
- A GPU workload must land on a node that has a GPU
- Replicas on the same node means a node failure takes both down
- Regulation may require workloads to stay in a specific region

Several mechanisms to express placement rules exist

| Mechanism                      | Controls                                         |
| ------------------------------ | ------------------------------------------------ |
| `nodeSelector` / Node Affinity | Which nodes a Pod may run on                     |
| Taints & Tolerations           | Reserving nodes for specific workloads           |
| Pod Affinity / Anti-Affinity   | Placement relative to other Pods                 |
| Topology Spread Constraints    | Distributing replica Pods across failure domains |

<!-- .element: style="margin-left: 20px;" -->

---
## Node Selection using nodeSelector
<!-- .slide: data-name="Node Selection" -->

[`nodeSelector`](https://kubernetes.io/docs/concepts/scheduling-eviction/assign-pod-node/#nodeselector) is the simplest way to constrain Pod placement
- Pods are scheduled on nodes whose labels match all key-value pairs
- Supported since Kubernetes 1.0, still valid, but limited
- Use `kubectl label node node-x key=value` to label nodes


```yaml
spec:
  nodeSelector: # nodes must have exactly all these labels
    accelerator: gpu-v100   
    topology.kubernetes.io/zone: eu-central-1a
```

Limitations
- Exact key-value match only <comment>(no advanced expressions)</comment>
- No soft/preferred scheduling  <comment>(always a hard requirement)</comment>
- No way to express OR logic across multiple label sets

Use Node Affinity for advanced placement rules

---
## Node Affinity

More [fine-grained and flexible](https://kubernetes.io/docs/concepts/scheduling-eviction/assign-pod-node/#affinity-and-anti-affinity) than nodeSelector
- Supports [operators](https://kubernetes.io/docs/concepts/scheduling-eviction/assign-pod-node/#operators) (`In`, `NotIn`, `Exists`, `Gt`, `Lt`, ...)
- Hard <comment>(prevent scheduling)</comment> or soft <comment>(prefer matching nodes)</comment> constraints
- Soft constraints use `weight` (1–100) to rank nodes <comment>(sum of weights determines the best node)</comment>

Based on node labels like nodeSelector
- Set by administrators or cloud providers to express node features 
- E.g., GPU type, location, availability zone, etc.

Decision only at scheduling time, not execution time
- If labels change after scheduling, Pods continues running on a node
- No re-scheduling or eviction based on label changes

---
## Node Affinity: Use Case

ML training with V100 or A100 GPUs in a GDPR-compliant region
- E.g., `eu-central-1a` or `eu-central-1b` and `gpu-v100` or `gpu-a100`
- `nodeSelector` could not express this <comment>(no OR logic or alternatives)</comment>

Available nodes

| Node  | Labels                                                                   | Result                  |
| ----- | ------------------------------------------------------------------------ | ----------------------- |
| **A** | `accelerator=gpu-v100` <br> `zone=us-east-1a`                            | ❌ rejected (wrong zone) |
| **B** | `zone=eu-central-1a` <br> `memory-tier=high`                             | ❌ rejected (no GPU)     |
| **C** | `accelerator=gpu-a100` <br> `zone=eu-central-1b` <br> `memory-tier=high` | ✅ scheduled, preferred  |
<!-- .element: style="margin-left: 20px; width: 100%;" -->

Both conditions must hold simultaneously
- Multiple `nodeSelectorTerms` use OR logic
- `matchExpressions` within a `nodeSelectorTerms` use AND logic

---
## Node Affinity: Example

```yaml
spec:
  affinity:
    nodeAffinity:
      # hard: require GPU nodes in specific zones
      requiredDuringSchedulingIgnoredDuringExecution:   
        nodeSelectorTerms: # uses OR logic for multiple terms
        - matchExpressions: # uses AND logic within a term
          - key: accelerator # GPU type
            operator: In
            values: [gpu-v100, gpu-a100]
          - key: topology.kubernetes.io/zone # availability zone
            operator: In
            values: [eu-central-1a, eu-central-1b]
      # soft: prefer high-memory nodes
      preferredDuringSchedulingIgnoredDuringExecution: 
      - weight: 50
        preference:
          matchExpressions:
          - key: memory-tier
            operator: In
            values: [high]
```

---
## Taints and Tolerations
<!-- .slide: data-name="Taints/Tolerations" -->

[Taints](https://kubernetes.io/docs/concepts/scheduling-eviction/taint-and-toleration/) mark a node as unsuitable
- E.g., for Pods that it does not explicitly tolerate
- E.g., reserve GPU nodes for GPU workloads

Example: Taint a GPU node 
- Format is `key=value:effect`
- `key` and `value` are arbitrary labels
```bash
kubectl taint nodes gpu-node-1 accelerator=nvidia:NoSchedule
```

Taint effects

| Effect             | New Pods            | Running Pods             |
| ------------------ | ------------------- | ------------------------ |
| `NoSchedule`       | Not scheduled       | Unaffected               |
| `PreferNoSchedule` | Avoided if possible | Unaffected               |
| `NoExecute`        | Not scheduled       | Evicted if no toleration |
<!-- .element: style="margin-left: 20px;" -->

---
## Taints and Tolerations

A Pod must declare a matching toleration
- Otherwise, it will not be scheduled on the tainted node

```yaml
spec:
  tolerations:
  - key: accelerator   # must match the taint key
    operator: Equal    # Equal: key+value must match; 
                       # Exists: only key must match
    value: nvidia      # must match the taint value
    effect: NoSchedule # must match the taint effect
  affinity:
    nodeAffinity:
      requiredDuringSchedulingIgnoredDuringExecution:
        nodeSelectorTerms:
        - matchExpressions:
          # also require the node to have the label
          - { key: accelerator, operator: Exists }  
```

---
## Pod Affinity and Anti-Affinity
<!-- .slide: data-name="Affinity" -->

[Pod affinity](https://kubernetes.io/docs/concepts/scheduling-eviction/assign-pod-node/#inter-pod-affinity-and-anti-affinity) controls placement relative to other running Pods
- Affinity: schedule Pod near ones with matching labels
- Anti-affinity: schedule Pod away from ones with matching labels

Example: no two replicas of `my-app` land on the same node

```yaml
spec:
  affinity:
    podAntiAffinity:
      requiredDuringSchedulingIgnoredDuringExecution:
      - labelSelector:
          matchLabels:
            app: my-app
        topologyKey: kubernetes.io/hostname
```

`topologyKey` defines the failure domain scope
- `kubernetes.io/hostname` — per node
- `topology.kubernetes.io/zone` — per availability zone

---
## Pod Affinity and Anti-Affinity: Example

Big data pipeline
- Processing Pod <comment>(e.g., Spark)</comment> and DataNode <comment>(e.g., Hadoop)</comment> should run on the same node <comment>(avoid network I/O)</comment>
- But no two processing Pods on the same node <comment>(distribute load)</comment>

```yaml
spec:
  affinity:
    # Co-locate with a DataNode on the same node
    podAffinity:
      requiredDuringSchedulingIgnoredDuringExecution:
      - labelSelector:
          matchLabels:
            app: hdfs-datanode   # must share a node with this Pod
        topologyKey: kubernetes.io/hostname
    # But spread processing Pods across nodes
    podAntiAffinity:
      requiredDuringSchedulingIgnoredDuringExecution:
      - labelSelector:
          matchLabels:
            app: spark-worker    # no two of these on the same node
        topologyKey: kubernetes.io/hostname
```
<!-- .element: style="font-size: 0.6em;" -->

---
## Anti-Affinity: Uneven Distribution

Anti-affinity can separate Pods, but not balance them
- `required...` is binary: at most 1 Pod per domain
- `preferred...` hint for separation but no distribution guarantee
- Unable to express "spread across zones, but allow 2 per node"

Without explicit spreading, the scheduler fills nodes greedily
- 6 replicas of a web API across 3 zones → all 6 may land in zone A
- A single zone outage takes down the entire service instead of 1/3

[Topology Spread Constraints](https://kubernetes.io/docs/concepts/scheduling-eviction/topology-spread-constraints/) fill this gap
- Defines max imbalance between busiest and quietest domain
- Works across any topology: zones, nodes, racks, or custom labels
- Supports hard enforcement and soft best-effort balancing

---
## Topology Spread Constraints
<!-- .slide: data-name="Topology Spread " -->

`maxSkew`: how uneven the distribution may be across domains
- Skew: `max(Pods in a domain) − min(Pods in a domain)`
- `maxSkew: 1` → strict balance: 2–2–2 ✅, 3–2–1 ❌
- `maxSkew: 2` → allows 4–2–2 ✅ (e.g, when co-location is beneficial)

Anti-affinity vs. topology spread constraints

|      | Pod Anti-Affinity   | Topology Spread                                   |
| ---- | ------------------- | ------------------------------------------------- |
| Mode | Hard block only     | Hard (`DoNotSchedule`) or soft (`ScheduleAnyway`) |
| Goal | Prevent co-location | Balance distribution, allow controlled skew       |
| E.g. | At most 1 per node  | At most 2 more than the least-loaded node         |

<!-- .element: style="font-size: 0.75em;" -->

Example: big data pipeline with 3 DataNodes across 3 nodes
- Nodes have local storage: Pods benefit from data locality
- maxSkew: 2 allows 4 workers on a busy node vs. 2 on others <comment>(anti-affinity would block this)</comment>

---
## Topology Spread Constraints: Example

Big data pipeline: spread workers across zones
- Allow up to 2 extra per node for data locality
- `maxSkew: 2`: 4–2–2 ✅ concentrate workers near a data-heavy node; anti-affinity could not express this

```yaml
spec:
  topologySpreadConstraints:
  - maxSkew: 1
    # hard: zone outage loses at most 1/3 of workers
    topologyKey: topology.kubernetes.io/zone
    whenUnsatisfiable: DoNotSchedule
    labelSelector:
      matchLabels:
        app: spark-worker
  - maxSkew: 2
    # soft: allow up to 2 extra workers per node to exploit local CPUs
    topologyKey: kubernetes.io/hostname
    whenUnsatisfiable: ScheduleAnyway
    labelSelector:
      matchLabels:
        app: spark-worker
```
<!-- .element: style="font-size: 0.6em;" -->

---
## Exercise: Availability

<a data-exercise="availability">Exercise: Implement high-availability strategy for gridflex-api</a>


---
## Summary

Resource management controls what a Pod may consume
- Requests: minimum resources guaranteed <comment>(used for scheduling)</comment>
- Limits: maximum to use <comment>(CPU throttled, memory overuse kills)</comment>
- Extended resources <comment>(GPUs)</comment>: device plugins, normal scheduling
- Probes: liveness <comment>(restart)</comment>, readiness <comment>(load balance)</comment>, startup <comment>(delays)</comment>

Scheduling control determines where Pods run

- Place Pods on nodes with specific hardware or labels <comment>(node affinity)</comment>
- Co-locate <comment>(pod affinity)</comment> or separate <comment>(anti-affinity)</comment> Pods
- Reserve nodes for specific workloads <comment>(taints & tolerations)</comment>
- Distribute pods across zones/nodes <comment>(topology spread constraints)</comment>

Declarative, optional, and composable <comment>(use when required)</comment>
