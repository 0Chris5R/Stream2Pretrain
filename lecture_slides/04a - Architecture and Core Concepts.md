<div class="lecturetitle">Kubernetes Architecture &amp; Core Concepts</div>
<!-- .slide: data-name="Introduction" data-state="hide-menubar" -->

---
## Kubernetes Installation Options
<!-- .slide: data-name="Kubernetes Installation" -->

Can be installed in various environments and configurations
- No matter what environment you choose, the core Kubernetes concepts remain the same
- From hyperscaler to single-node local cluster
- Abstracts away from the underlying infrastructure
- Avoids vendor lock-in and allows for flexibility in deployment options

Different types of Kubernetes environments
- Hyperscaler Managed Kubernetes Services
- Open Source Distributions
- Enterprise / Commercial
- Development, local, and testing environments

---
## Hyperscaler, Open Source, Commercial

Hyperscaler / Commercial Kubernetes options

|                     |                                                                                                                                                                                                                                                                                                           |
| ------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Hyperscaler Managed | [Amazon EKS](https://aws.amazon.com/eks/), [Google GKE](https://cloud.google.com/kubernetes-engine), [Microsoft AKS](https://azure.microsoft.com/en-us/products/kubernetes-service)                                                                                                                       |
| Open Source         | [SUSE Rancher](https://github.com/rancher/rancher), [OpenStack Magnum](https://www.openstack.org/software/releases/zed/components/magnum), [OKD](https://www.okd.io/) <comment>(community OpenShift)</comment>, [Talos Linux](https://www.talos.dev/) <comment>(immutable, API-only OS for K8s)</comment> |
| Commercial          | [Red Hat OpenShift](https://www.redhat.com/de/technologies/cloud-computing/openshift), [VMware Tanzu](https://tanzu.vmware.com/), [Mirantis Kubernetes Engine](https://www.mirantis.com/software/mirantis-kubernetes-engine/)                                                                             |

<!-- .element: style="margin-left: 20px; width: 100%;" -->

Local development and testing environments

| Tool                                                                  | Description                                             |
| --------------------------------------------------------------------- | ------------------------------------------------------- |
| **[Minikube](https://github.com/kubernetes/minikube)** (this lecture) | Standard local cluster, supports multiple drivers       |
| [Kind](https://kind.sigs.k8s.io/)                                     | Kubernetes IN Docker — lightweight, CI-friendly         |
| [Docker Desktop](https://docs.docker.com/desktop/kubernetes/)         | Built-in K8s for Mac/Windows developers                 |
| [microk8s](https://microk8s.io/)                                      | Snap-based, Ubuntu-native single-node cluster           |
| [k3s](https://k3s.io/) + [k3sup](https://github.com/alexellis/k3sup)  | Lightweight K8s <comment>(bootstrap over SSH)</comment> |
| [k0s](https://k0sproject.io/)                                         | Zero-dependency single binary, any infrastructure       |
| [k3d](https://github.com/k3d-io/k3d)                                  | Multi-node k3s clusters in Docker on a single machine   |

<!-- .element: style="margin-left: 20px; width: 100%;" -->

---
## Minikube

Mostly used for local development
- Installation on [Linux](https://minikube.sigs.k8s.io/docs/start/linux/) and other [OSes](https://minikube.sigs.k8s.io/docs/start/)
- Do not use Windows containers
- Choose a sensible [driver](https://minikube.sigs.k8s.io/docs/drivers/) for your OS

Starting minikube

```bash
# Start Minikube
minikube start --addons=ingress --memory 7000 --cpus 6

# Stop (or delete) Minikube
minikube stop # or minikube delete
```

Get information about the kubernetes cluster
- Get the current version: `kubectl version`
- Get all nodes of the cluster `kubectl get nodes -o wide`

---
## Minikube and Docker CLI

Sharing the docker daemon
- Useful for faster development cycles (no docker registry required)
- Run `eval $(minikube docker-env)`

<img src="img/docker-on-host-os.svg" style="width: 95%;">

vvv
## Cheat Sheet: MicroK8s on Ubuntu

First step: Prepare a fresh Ubuntu instance
- Log in at <https://stack.dhbw.cloud>

Create new instance
-	Source: Ubuntu Server 22.04 64bit
- Flavor: m1.xlarge
- Network: DHBW
-	Key Pair (upload your public key and add it to this instance)

Log in via SSH
-	Use `ssh ubuntu@IP`
- Become root `sudo bash`

vvv
## Cheat Sheet: MicroK8s on Ubuntu

Second step: Install [MicroK8S](https://microk8s.io/)
-	`snap install microk8s --classic`
-	Check status while Kubernetes starts: `microk8s status --wait-ready`
-	Write config to file: `microk8s.config > /root/.kube/conf ; chmod 600 /root/.kube/conf`
-	Enable addons: `microk8s enable dns ingress helm3`
- Create aliases: 
  - `snap alias microk8s.kubectl kubectl`
  - `snap alias microk8s.helm3 helm`
- Enable shell completion: `kubectl completion bash > /etc/bash_completion.d/kubectl`

<!--- ------------------------------------------------------------------- --->
<!--- ------------------------------------------------------------------- --->
---
# Kubernetes: Pods
<!-- .slide: data-name="Pods" -->

<!--- ------------------------------------------------------------------- --->
<!--- ------------------------------------------------------------------- --->

---
## Pods

[Pods](https://kubernetes.io/docs/concepts/workloads/pods/pod-overview/) are the basic building block
- Pod = application container + storage + unique IP + runtime specs
- Smallest and simplest unit in the Kubernetes object model

Comprised of one (or more) containers
- Represents one (or more) running process(es) on your cluster
- Analogy: Pod = virtual machine, container = process
- Typically 1 Pod = 1 container

Pods are ephemeral, disposable objects
- Pod creation schedules execution on a node in the cluster
- Pod terminates on process(es) termination, Pod deletion, or eviction/node failure

---
## Pod Specification

Pods are specified using YAML files as [Pod Template](https://kubernetes.io/docs/concepts/workloads/pods/pod-overview/#pod-templates)
- These are typically included in other objects (later)
 
Example

<a data-code='yaml' href="code/examples/pod-demo-busybox.yaml">Source code</a>

---
## Pod and kubectl

Start the Pod
- Start from a local file
  - Save the example to a file (e.g., `pod-demo-busybox.yaml`)
  - Run `kubectl apply -f pod-demo-busybox.yaml`
- Start from a URL
  - Execute `kubectl apply -f` <span data-prefix-url data-convert-to-inline-code="bash">code/examples/pod-demo-busybox.yaml</span>

Interact with the Pod

```bash
# Get status
kubectl get pods

# View stdout output
kubectl logs myapp-pod
```

---
## Debugging Inside the Cluster

Execute a command in a (running) pod
- `kubectl exec -ti myapp-pod -- ps aux`
- `kubectl exec -ti myapp-pod -- sh`
- Use `--container='containername'` to select a specific container if more than one exists
- Delete the Pod afterwards: `kubectl delete pod myapp-pod`


Start an extra container
- E.g., to test connectivity to deployed applications

Example: [wbitt/network-multitool](https://hub.docker.com/r/wbitt/network-multitool/)
  ```kubectl
  kubectl run --rm -ti --restart=Never \
    --image=wbitt/network-multitool  debugpod -- sh
  ```
  - After exiting the container (use `exit` or `CTRL-D`)

---
## Example: MariaDB Pod

Run MariaDB as a Pod
- Name `mariadb-demo-pod` and container image `mariadb`
- Set [environment variable](https://kubernetes.io/docs/tasks/inject-data-application/define-environment-variable-container/) `MARIADB_ROOT_PASSWORD` to `mysecretpw`
- [Expose container port](https://kubernetes.io/docs/concepts/services-networking/connect-applications-service/#exposing-pods-to-the-cluster) `3306`
- Create the pod using `kubectl apply -f pod-mariadb.yaml`

Enter the pod to check it is working
  ```kubectl
  kubectl exec -ti mariadb-demo-pod -- \
    mariadb -u root --password=mysecretpw \
    -e "CREATE DATABASE IF NOT EXISTS demo; SHOW databases;"
  ``` 

Delete pod
- `kubectl delete pod mariadb-demo-pod`

<credits>
  Solution: <a href="code/examples/pod-mariadb.yaml">pod-mariadb.yaml</a>
</credits>

---
## Exercise: Pod: Demo

<!-- 
    cd code/k8s-simple
    asciinema rec --overwrite -i 2 ../../img/k8s-pod-demo.cast
-->
<asciinema data-conf='{ "cols": 110, "rows": 25, "theme":"monokai", "autoPlay": true, "idleTimeLimit": 2, "terminalFontSize": "16px"}'
        src="img/k8s-pod-demo.cast" />

vvv
## Exercise: Pod: Demo Script

```bash
kubectl get pod -o wide

kubectl apply -f pod-mariadb.yaml

kubectl get pod -o wide

kubectl exec -ti mariadb-demo-pod -- mariadb -u root --password=mysecretpw -e "CREATE DATABASE IF NOT EXISTS demo; SHOW databases;"

kubectl delete pod mariadb-demo-pod
```

---
## Pod: Command Line Arguments

Override `CMD`/`ENTRYPOINT` from Dockerfile: `args` and `command`
- Often, only `args` is used to supply specific command line arguments to the container

Example

```yaml
apiVersion: v1
kind: Pod
metadata:
  name: args-demo
spec:
  containers:
  - name: data-downloader
    image: curlimages/curl
    command: curl # optional
    args: # save downloaded file as /tmp/data/init.sql
      - "-o"
      - "/tmp/data/init.sql" # directory
      - "https://example.com/mariadb-data.sql"
```

---
## Labels, Selectors, and Annotations

Labels
- Key-value pairs attached to any k8s object
- Example: `app: nginx`, `env: production`, `tier: frontend`
- Selectors use labels to identify and group objects

Annotations
- Also key-value pairs, but not used for selection
- Store non-identifying metadata <comment>(e.g., build version, tool configuration)</comment>

```yaml
metadata:
  labels:
    app: nginx
    env: production
  annotations:
    deployment.kubernetes.io/revision: "1"
    description: "main web frontend"
```

---
<!--- ------------------------------------------------------------------- --->
<!--- ------------------------------------------------------------------- --->
# Kubernetes: Controllers
<!-- .slide: data-name="Controllers" -->
<!--- ------------------------------------------------------------------- --->
<!--- ------------------------------------------------------------------- --->

---
## Controllers

Controllers can create and manage multiple Pods
- Self-healing capabilities at cluster scope, replication, and rollout 

Different types of Controllers are available

| Name                                                                                      | Description                                                                                 |
| ----------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------- |
| **[ReplicaSet](https://kubernetes.io/docs/concepts/workloads/controllers/replicaset/)**   | Maintain a stable set of replica Pods running at any given time                             |
| **[Deployment](https://kubernetes.io/docs/concepts/workloads/controllers/deployment/)**   | Declarative updates for Pods and ReplicaSets                                                |
| **[StatefulSet](https://kubernetes.io/docs/concepts/workloads/controllers/statefulset/)** | For stateful applications (guarantees about the ordering and uniqueness)                    |
| [DaemonSet](https://kubernetes.io/docs/concepts/workloads/controllers/daemonset/)         | Ensures that all nodes run a copy of a Pod                                                  |
| [Job](https://kubernetes.io/docs/concepts/workloads/controllers/jobs-run-to-completion/)  | Creates one or more Pods and ensures that a specified number of them successfully terminate |
| [CronJob](https://kubernetes.io/docs/concepts/workloads/controllers/cron-jobs/)           | Creates Jobs on a time-based schedule                                                       |

<!-- .element: style="margin-left: 20px; font-size: 86%;" -->

---
## ReplicaSet

Maintains a stable set of replica Pods running at any given time
- Replaces failed or deleted Pods automatically
- Selects its Pods using a label selector
- Rarely used directly <comment>(use Deployments instead)</comment>

```yaml
apiVersion: apps/v1
kind: ReplicaSet
metadata:
  name: nginx-rs
spec:
  replicas: 3
  selector:
    matchLabels:
      app: nginx
  template:
    metadata:
      labels:
        app: nginx
    spec:
      containers:
      - image: nginx:1.25
```
<!-- .element: style="font-size: 0.6em;" -->

---
## Deployment

Provides declarative updates for ReplicaSets
- Describe a desired state in a Deployment
- Changes actual state to desired state at controlled rate

```YAML
apiVersion: apps/v1
kind: Deployment
metadata:
  name: nginx-deployment
  labels:
    app: nginx
spec:
  replicas: 3
  selector:
    matchLabels:
      app: nginx
  template:
    metadata:
      labels:
        app: nginx
    spec:
      containers:
      - image: nginx:1.7.9
```

<!-- .element: style="font-size: 0.6em;" -->

---
## StatefulSet

Used for applications that require stable, persistent identities
- Pods get a predictable, stable name <comment>(e.g., `db-0`, `db-1`, `db-2`)</comment>
- Each pod has its own persistent, stable storage
- Pods are created, scaled, and deleted in a defined order

Differences from Deployment
- Pods are not interchangeable <comment>(unique identity and storage)</comment>
- Scaling and updates respect the ordering of replicas
- Termination is done in reverse order

Typical use cases
- Databases <comment>(PostgreSQL, MySQL, Cassandra)</comment>
- Message brokers <comment>(Kafka, RabbitMQ)</comment>

---
## StatefulSet: Example

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: mariadb
spec:
  serviceName: mariadb
  replicas: 3
  selector:
    matchLabels: { app: mariadb }
  template:
    metadata:
      labels: { app: mariadb }
    spec:
      containers:
      - name: mariadb
        image: mariadb:11
        env:
        - name: MARIADB_ROOT_PASSWORD
          value: mysecretpw
```

Pod names: `mariadb-0`, `mariadb-1`, `mariadb-2`

---
## Namespaces
<!-- .slide: data-name="Namespaces" -->

Mechanism to isolate groups of resources within a cluster
- Resources within a namespace must have unique names
- Names only need to be unique within their namespace

Default namespaces
- `default`: resources created without specifying a namespace
- `kube-system`: Kubernetes system components
- `kube-public`: publicly readable resources

```bash
# Create a namespace
kubectl create namespace my-app

# Work in a specific namespace
kubectl get pods -n my-app

# Set a default namespace for the current context
kubectl config set-context --current --namespace=my-app
```

---
## Access Pods for Debugging

Pods are not (yet) reachable from outside the cluster
- Pod network uses a private <comment>(not routed)</comment> IP range
- Cluster IP addresses are only valid inside the cluster

Access ports using [port forwarding](https://kubernetes.io/docs/tasks/access-application-cluster/port-forward-access-application-cluster/)
- Local ports can be forwarded to a the `IP:port` of a pod
- Forwards local port 1234 to remote (Pod) port 80

Examples
- `kubectl port-forward pod/some-pod-name 1234:80` <br> <comment>(or use a deployment's / service's name)</comment>
- Add `--address 0.0.0.0` to allow connecting to the forwarded port from outside your computer

---
## Exercise: Build, Push, Deploy to K8S

<a data-exercise="build-push-deploy">Exercise: Build, Push, Deploy an application in Kubernetes</a>