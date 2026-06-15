<div class="lecturetitle">Application Orchestration</div>
<!-- .slide: data-name="Introduction" data-state="hide-menubar" -->

---
## Application Orchestration

Used to implement microservice architectures
- E.g., using **Docker Compose**, Docker Swarm, **Kubernetes**, ...

<img src='img/monolith-soa-microservices.svg' style='width: 100%'>

---
## Example: Docker Compose
<!-- .slide: data-name="Docker Compose" -->

[Docker Compose](https://docs.docker.com/compose/overview/)
- Defines and runs multi-container applications as a single unit
- All services, networks, and volumes declared in one YAML file
- Single command to build, start, and stop the entire application
- Good for local development and simple single-host deployments

Example (`docker-compose.yml`)

```yaml
services:
  web:
    build: app/
    ports:
      - "8080:8080"
    depends_on:
      - cache
  cache:
    image: memcached
```

---
## Docker Compose: Networks

Networks: isolated communication between services
- Compose creates a default network 
- All services can reach each other by name
- Additional networks can be defined to isolate groups of services

Example: two networks, `frontend` and `backend`

```yaml
networks:
  frontend:
  backend:

services:
  web:
    # Connected to both frontend and backend networks
    networks: [frontend, backend]
  db:
    # Connected to backend network only
    networks: [backend]   
```

---
## Docker Compose: Volumes

Volumes
- Persistent storage that survives container restarts

Two main types
- Named volumes are managed by Docker <comment>(preferred for databases)</comment>
- Bind mounts maps host paths into the container <comment>(e.g., config files)</comment>

```yaml
volumes:
  db-data:

services:
  db:
    volumes:
      # named volume, stored in Docker's volume directory
      - db-data:/var/lib/mysql          
      # bind mount, maps a local file into the container
      - ./init.sql:/docker-entrypoint-initdb.d/init.sql  
```

---
## Docker Compose: Example App

Goal: Deploy a load-balanced, scalable application

<img src="img/docker-compose-scale.svg" style="width: 100%;">

---
## Example App: MariaDB

Uses official MariaDB image ([`mariadb:latest`](https://hub.docker.com/_/mariadb/))
- Service name is `db` and exposes it as `db:3306`
- Configuration mostly using environment variables
- Mounts seed data file at well-known location

<a data-code='Dockerfile' href="code/docker-compose/docker-compose.yml" data-begin="# Begin: MariaDB" data-end="  # End: MariaDB">Source code</a>

---
## Example App: Memcached

Memcached is an in-memory key-value store used for caching
- Speed up dynamic web applications by caching data in memory
- Read and write data at very high speed, but data is not persistent
- Stores key-value pairs <comment>(configurable expiration and maximum memory limit)</comment>

Uses existing [`memcached:alpine`](https://hub.docker.com/_/memcached/) image
  - Alpine Linux: minimal Linux distribution <comment>(~5MB in size)</comment>
  - Uses service replication with 3 [replicas](https://docs.docker.com/compose/compose-file/deploy/#replicas)

<a data-code='Dockerfile' href="code/docker-compose/docker-compose.yml" data-begin="  # Begin: Memcached" data-end="  # End: Memcached">Source code</a>

---
## Example App: Application Code

Simple [Node.js](https://nodejs.org/en) application using [express.js](https://expressjs.com/)
- [index.js](code/docker-compose/app/index.js), [package.json](code/docker-compose/app/package.json), and [Dockerfile](code/docker-compose/app/Dockerfile) in folder `app/`
- Specify folder `app/` as build context for the app service

<a data-code='Dockerfile' href="code/docker-compose/docker-compose.yml" data-begin="  # Begin: App" data-end="  # End: App">Source code</a>

---
## Example App: Nginx Load Balancer

Add a service: Nginx as load balancer ([`nginx:alpine`](https://hub.docker.com/_/nginx/))
- Load balances incoming HTTP requests to the app instances
- Mount [this file](code/docker-compose/nginx-load-balance.conf) read-only as `/etc/nginx/nginx.conf`
- Expose port 8080 to the outside as port 8080

<a data-code='Dockerfile' href="code/docker-compose/docker-compose.yml" data-begin="  # Begin: Nginx" data-end="End: Nginx">Source code</a>

---
## Example App: Putting it all together

Final [docker-compose.yml](code/docker-compose/docker-compose.yml) file 
- Combines all services, networks, and volumes
- Build your docker images using `docker compose -p my-app build`

Verify it's working
- Open [http://localhost:8080](http://localhost:8080) to see the app in action
- Observe the log output to see that loadbalancing and caching works
- After 30s, a cache miss should occur

Stop and delete it again
- Press CTRL-C
- Run `docker compose -p my-app rm -fs`

---
## Docker Compose: Exercise: Demo

<!-- 
    cd docker-compose
    asciinema rec --overwrite -i 2 ../img/docker-compose-demo.cast
-->
<asciinema data-conf='{ "cols": 110, "rows": 25, "theme":"monokai", "autoPlay": true, "idleTimeLimit": 2, "terminalFontSize": "16px"}'
        src="img/docker-compose-demo.cast" />

---
## Limitations of Docker Compose

Designed for local development and simple single-host setups
- No scaling across multiple hosts, no high availability <comment>(single machine)</comment>
- No self-healing <comment>(apart from `restart:always` policy)</comment>
- No automated scaling <comment>(manual setting of replicas)</comment>
- No rolling updates or health-based deployment
- No gradual rollout, no automatic rollback on failure

There is a Swarm mode for Docker
- Allows orchestration across multiple hosts
- Integrates with Compose files
- Not widely adopted and has been deprecated

---
## Scalable Container Orchestration
<!-- .slide: data-name="Scalable Container Orchestration" -->

Production workloads require more than Compose can provide
- Scheduling <comment>(place containers based on available node resources)</comment>
- Self-healing <comment>(restart failed containers, replace unhealthy nodes)</comment>
- Scaling <comment>(distribute across hosts, scale automatically under load)</comment>

Additional capabilities required in production
- Rolling updates <comment>(replace instances gradually, roll back on failure)</comment>
- Secret management <comment>(inject credentials, not storing them in images)</comment>
- Service discovery <comment>(services find each other by name, not by IP)</comment>
- Load balancing <comment>(distribute traffic across replicas automatically)</comment>
- Resource quotas <comment>(limit CPU, memory per workload or namespace)</comment>
- Storage orchestration <comment>(attach persistent volumes across nodes)</comment>

---
## Container Orchestration: Kubernetes
<!-- .slide: data-name="Kubernetes" -->

Definition
> Kubernetes (k8s) is a portable, extensible open-source **platform for managing containerized workloads and services**, that facilitates both **declarative configuration** and automation. It has a large, rapidly growing ecosystem. Kubernetes services, support, and tools are widely available. (Source: [Kubernetes](https://kubernetes.io/docs/concepts/overview/what-is-kubernetes/))

Kubernetes manages containerized workloads
- Was based on Docker, now uses any [CRI-compliant](https://kubernetes.io/docs/setup/production-environment/container-runtimes/) backend
- Sophisticated environment for cloud-native applications
- Features such as container placement, replication, load balancing, auto-scaling, resource monitoring, health checks, self-healing, rolling updates, volume management (persistent storage), service discovery, ...

---
## Kubernetes Architecture

<img src="https://upload.wikimedia.org/wikipedia/commons/b/be/Kubernetes.png" style="width: 93%;">

<credits>
  <a href="https://commons.wikimedia.org/wiki/File:Kubernetes.png">Wikipedia - Khatan66</a>
</credits>

---
## Kubernetes: Controllers / Control Loop

Kubernetes uses a [declarative approach](https://kubernetes.io/docs/concepts/overview/working-with-objects/kubernetes-objects/)
- Stores the desired state of the system in [etcd](https://etcd.io/) (a key-value store)

Controllers continuously monitor the actual state of the system
- Software components that control the state of the cluster
- If required, they act to achieve a state closer to the desired state

Controllers implement the [operator pattern](https://kubernetes.io/docs/concepts/extend-kubernetes/operator/) 
- Non-terminating control loop regulating the state of a system

<div style="text-align: center;">
  <img src='img/kubernetes-control-loop.svg' style='width: 75%'>
</div>

---
## Kubernetes Architecture

Kubernetes clusters are comprised of nodes
- Simple setups: control plane and worker nodes on the same machine

Control plane nodes: management and scheduling

| Component          | Role                                                    |
| ------------------ | ------------------------------------------------------- |
| API server         | Exposes the API, validates and processes requests       |
| Etcd               | Key-value store for all cluster data                    |
| Scheduler          | Assigns workloads to nodes based on resources           |
| Controller manager | Runs controllers that regulate the state of the cluster |

<!-- .element: style="margin-left: 20px; width: 100%;" -->

Worker nodes: run application workloads

| Component         | Role                                                             |
| ----------------- | ---------------------------------------------------------------- |
| Kubelet           | Ensures containers in a Pod are running and healthy              |
| Kube-proxy        | Maintains network rules for Pod-to-Pod and external traffic      |
| cAdvisor          | Collects resource usage and performance metrics per container    |
| Container runtime | Executes containers <comment>(e.g., containerd, CRI-O)</comment> |
<!-- .element: style="margin-left: 20px; width: 100%;" -->



---
## Kubernetes Objects

Persistent entities that represent the state of a cluster
- Mostly expressed in YAML documents
- Describe what containers are running on which nodes

Required contents
- Document schema (required fields `apiVersion` and `kind`)
- Metadata contains `name` (and `labels`, `annotations`, ...)
- Desired state is specified in the `spec` section

```yaml
apiVersion: v1
kind: SomeResourceKind (e.g., Pod)
metadata:
  name: my-resource-name
spec:
  [...]
```

---
## Kubernetes Objects

Set/actual comparison
- Desired state stored in `spec` section
- Kubernetes stores the actual state in the `status` section
- For instance, a container is running or not or starting up

Example

```yaml
apiVersion: v1
kind: SomeResourceKind (e.g., Pod)
metadata:
  name: my-resource-name
spec:
  [...]
status: 
  [...some status depending...]
  [...on a resource's kind...]
```

---
## From Concepts to Practice

So far: motivation and foundations
- Limitations of single-host setups and why orchestration is needed
- Kubernetes as the de-facto standard for container orchestration
- Core architecture: control plane, worker nodes, control loop
- Declarative configuration via Kubernetes objects

Next: from basic to advanced Kubernetes features
- Running a cluster locally and deploying the first workloads
- Controlling resource usage and ensuring application health
- Structuring multi-container Pods using patterns
- Persisting data and connecting services
- Managing updates and complex deployments
