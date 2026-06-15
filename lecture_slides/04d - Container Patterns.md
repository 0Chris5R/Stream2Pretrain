<div class="lecturetitle">Container Patterns</div>
<!-- .slide: data-name="Introduction" data-state="hide-menubar" -->

---
## Motivation

Containers solve packaging and isolation — not architecture
- Applications in containers still face the same cross-cutting concerns
- Logging, metrics, health checks, TLS, and connection management repeat across every service
- Without shared patterns, each team solves the same problems in different ways

Container patterns: recurring, composable solutions
- Described in [Designing Distributed Systems](https://www.oreilly.com/library/view/designing-distributed-systems/9781491983638/) <comment>(Brendan Burns, 2018)</comment>
- Build on Kubernetes primitives: shared namespaces, volumes, and lifecycle hooks
- Give teams a common vocabulary for container-based architectures

---
## Real-World Examples

Example: Web API
- Ship logs to Elasticsearch, expose metrics to Prometheus, and connect to a sharded database
- Couples infrastructure concerns to business logic in app container
- Change requires rebuilding and redeploying the application image

Container patterns address this 
- Separate concerns across containers
- Log shipping, metrics export, and connection routing each become a standalone container
- The application container stays focused on business logic
- Infrastructure containers can be updated, replaced, or configured independently

---
## When to Use and When Not to

Use container patterns when
- Services repeat infrastructure concerns <comment>(logging, proxying, metrics)</comment>
- Main container cannot be modified <comment>(3rd party software, legacy apps)</comment>
- Infrastructure and application logic need independent release cycles

Do not use them when
- Application is simple and added containers create more complexity
- Concern can be solved inside the application without much coupling
- Team is small and overhead of managing multiple containers per Pod is not justified
- Latency between containers matters <comment>(localhost is fast but not free)</comment>

<!--- ------------------------------------------------------------------- --->
<!--- ------------------------------------------------------------------- --->
---
# Sidecar Pattern
<!-- .slide: data-name="Sidecar Pattern" -->
<!--- ------------------------------------------------------------------- --->
<!--- ------------------------------------------------------------------- --->

---
## Sidecar: The Problem

A web application writes logs to a file
- Should be shipped to a central system <comment>(e.g., Elasticsearch, Loki)</comment>
- Adding a log shipper to the application image couples infrastructure concerns to business logic
- Every shipper upgrade or format change requires rebuilding the application image

Sidecar pattern
- Runs a second container alongside the main container
- Both share the same network namespace and can share volumes
- Sidecar handles infrastructure concern without touching the application

---
## Sidecar: Implementation Options

Many options available
- Commercial and open-source

Examples

| Option                                                            | Type        | Notes                                                 |
| ----------------------------------------------------------------- | ----------- | ----------------------------------------------------- |
| [Datadog Agent](https://docs.datadoghq.com/agent/?tab=Host-based) | Commercial  | Metrics, logs, and traces in one sidecar              |
| [AWS Distro for OpenTelemetry](https://aws.amazon.com/de/otel/)   | Commercial  | Managed OpenTelemetry collector sidecar               |
| [Fluentd](https://www.fluentd.org/)                               | Open Source | More features than Fluent Bit, higher resource use    |
| [Envoy](https://www.envoyproxy.io/)                               | Open Source | Proxy sidecar, basis for most service meshes          |
| [Fluent Bit](https://fluentbit.io/)                               | Open Source | Lightweight log processor and forwarder, CNCF project |
<!-- .element: style="margin-left: 20px; width: 100%;" -->

Fluent Bit is a common choice for log shipping
- Small binary, low memory footprint, native Kubernetes support

---
## Sidecar: Example

Fluent Bit ships logs written by the application to a shared volume
- Application unchanged, sidecar updates for destination changes

```yaml
apiVersion: v1
kind: Pod
metadata:
  name: app-with-log-sidecar
spec:
  volumes:
  - name: logs
    emptyDir: {}
  containers:
  - name: app
    image: my-app:1.0
    volumeMounts:
    - name: logs
      mountPath: /var/log/app # app writes logs here
  - name: log-shipper
    image: fluent/fluent-bit:3
    # -o stdout: forward to stdout (e.g., development)
    args: ["-i", "tail", "-p", "path=/var/log/app/*.log", "-o", "stdout"]
    volumeMounts:
    - name: logs
      mountPath: /var/log/app # sidecar reads the same directory
```
<!-- .element: style="font-size: 0.56em;" -->


<!--- ------------------------------------------------------------------- --->
<!--- ------------------------------------------------------------------- --->
---
# Ambassador Pattern
<!-- .slide: data-name="Ambassador Pattern" -->
<!--- ------------------------------------------------------------------- --->
<!--- ------------------------------------------------------------------- --->

---
## Ambassador: The Problem

An application connects to a MariaDB database using `localhost:3306`
- In development: routes to a single local MariaDB instance
- In production: routes to a managed RDS cluster with TLS and connection pooling

The application code is identical in both environments — it always connects to `localhost:3306`

The ambassador pattern places a TCP proxy between the application and the actual database
- Application always connects to localhost
- Ambassador translates the connection to the correct endpoint
- Only the ambassador ConfigMap changes between environments

---
## Ambassador: Implementation Options

| Option                              | Type        | Notes                                                                                            |
| ----------------------------------- | ----------- | ------------------------------------------------------------------------------------------------ |
| Nginx Plus                          | Commercial  | Active health checks, session persistence, and dashboard                                         |
| AWS App Mesh                        | Commercial  | Managed service mesh using Envoy as the data plane                                               |
| [Envoy](https://www.envoyproxy.io/) | Open Source | Highly configurable, used in Istio and AWS App Mesh                                              |
| [Caddy](https://caddyserver.com/)   | Open Source | Simple config, automatic TLS — HTTP only <comment>(no TCP without non-standard plugin)</comment> |
| [HAProxy](https://www.haproxy.org/) | Open Source | Mature, high-performance TCP and HTTP proxy                                                      |
<!-- .element: style="margin-left: 20px; width: 100%;" -->

HAProxy is a typical choice 
- Especially for database ambassador scenarios
- Native TCP proxy mode, minimal overhead, straightforward configuration

---
## Ambassador: Example

Routes `localhost:3306` to the actual database endpoint
- `haproxy.cfg`: `frontend db *:3306` → `backend db` → `server primary db.prod.internal:3306`

```yaml
  containers:
  - name: app
    image: my-app:1.0
    env:
    - name: DB_HOST
      value: "localhost"             # always connects to localhost
    - name: DB_PORT
      value: "3306"
  - name: ambassador
    image: haproxy:3.0
    volumeMounts:
    - name: haproxy-config
      mountPath: /usr/local/etc/haproxy/haproxy.cfg
      subPath: haproxy.cfg          # target host defined here
  volumes:
  - name: haproxy-config
    configMap:
      name: ambassador-config       # swap this ConfigMap per environment
```
<!-- .element: style="font-size: 0.6em;" -->


<!--- ------------------------------------------------------------------- --->
<!--- ------------------------------------------------------------------- --->
---
# Adapter Pattern
<!-- .slide: data-name="Adapter Pattern" -->
<!--- ------------------------------------------------------------------- --->
<!--- ------------------------------------------------------------------- --->

---
## Adapter: The Problem

Legacy service uses proprietary health and metrics formats
- Monitoring stack expects Prometheus metrics
- Application cannot be modified <comment>(third-party, external code)</comment>
- Adding a translation layer inside the application image mixes concerns and risks breaking the app

Adapter pattern runs a second container
- Translates the main container's output to Prometheus metrics
- Sits between the main container and external consumers
- Presents a uniform interface regardless of what the main container produces

---
## Adapter: Implementation Options

Examples of adapters for monitoring and telemetry

| Option                                                              | Type        | Notes                                                    |
| ------------------------------------------------------------------- | ----------- | -------------------------------------------------------- |
| [mysqld_exporter](https://github.com/prometheus/mysqld_exporter)    | Open Source | Translates MariaDB/MySQL internals to Prometheus metrics |
| [redis_exporter](https://github.com/oliver006/redis_exporter)       | Open Source | Same pattern for Redis                                   |
| [OpenTelemetry Collector](https://opentelemetry.io/docs/collector/) | Open Source | Adapts any telemetry format to any backend               |
| Datadog Agent                                                       | Commercial  | Translates metrics, logs, and traces from any source     |
| Dynatrace OneAgent                                                  | Commercial  | Automatic discovery and format translation               |

Prometheus exporters are very common
- Purpose-built per technology, small footprint, well-maintained by the community

---
## Adapter: Example

`mysqld_exporter` as a sidecar adapter
- Translates MariaDB internals to Prometheus metrics

```yaml
  containers:
  - name: mariadb
    image: mariadb:latest
    env:
    - name: MARIADB_ROOT_PASSWORD
      valueFrom:
        secretKeyRef: { name: mariadb-secret, key: password }
  - name: metrics-adapter
    image: prom/mysqld-exporter:latest
    args:
    # connects to main container via localhost
    - --mysqld.address=localhost:3306
    - --mysqld.username=exporter
    env:
    - name: MYSQLD_EXPORTER_PASSWORD
      valueFrom:
        secretKeyRef: { name: mariadb-secret, key: exporter-password }
    ports:
    - containerPort: 9104 # Prometheus scrapes this port
```
<!-- .element: style="font-size: 0.6em;" -->

<!--- ------------------------------------------------------------------- --->
<!--- ------------------------------------------------------------------- --->
---
# Init Container Pattern
<!-- .slide: data-name="Init Container Pattern" -->
<!--- ------------------------------------------------------------------- --->
<!--- ------------------------------------------------------------------- --->

---
## Init Container: The Problem

An app starts and immediately tries to connect to a database
- May not be ready yet <comment>(slow startup, schema not applied)</comment>
- Application crashes, Kubernetes restarts it, and the cycle repeats
- Embedding startup logic in the app adds environment-specific tooling

Init containers solve the startup ordering problem
- Run before the main container; must complete with exit code 0
- Main container only starts if all init containers succeed

Typical use cases
- Wait for dependencies to be ready
- Fetch secrets or configuration from external systems
- Apply database migrations

---
## Init Container: Options

Init containers are a Kubernetes primitive
- Any container image can be used

| Use case                    | Typical image                                                            |
| --------------------------- | ------------------------------------------------------------------------ |
| Wait for a TCP port to open | `busybox`, `alpine` with `nc`                                            |
| Wait for an HTTP endpoint   | `curlimages/curl`                                                        |
| Apply database migrations   | [Flyway](https://flywaydb.org/), [Liquibase](https://www.liquibase.org/) |
| Render config templates     | `hairyhenderson/gomplate`                                                |
| Fetch secrets or config     | `vault` agent, `aws-cli`                                                 |
<!-- .element: style="margin-left: 20px; width: 100%;" -->

Benefits of init containers
- Can include tools that must not be in the production image
- Reduces the attack surface of the main container

---
## Init Container: Example

Wait for MariaDB to be reachable
- Then apply migrations before the application starts

```yaml
spec:
  initContainers:
  - name: wait-for-db
    image: busybox:1.36
    command: ["sh", "-c", "until nc -z mariadb 3306; do sleep 2; done"]
  - name: run-migrations
    image: flyway/flyway:10
    args: ["-url=jdbc:mariadb://mariadb:3306/app", "-user=root", "migrate"]
    env:
    - name: FLYWAY_PASSWORD
      valueFrom:
        secretKeyRef: { name: mariadb-secret, key: password }
  containers:
  - name: app
    image: my-app:1.0
```
<!-- .element: style="font-size: 0.6em;" -->

Init containers run in order
- Migrations only start after the port is open
- App only starts after migrations complete

---
## Summary
<!-- .slide: data-name="Summary" -->

Container Patterns
- Separate infrastructure concerns from business logic

|                |                                                   |                                                 |
| -------------- | ------------------------------------------------- | ----------------------------------------------- |
| Sidecar        | Log shipping, metrics, proxying alongside the app | Shared volume or network namespace              |
| Ambassador     | Environment-specific routing hidden from the app  | App connects to localhost only                  |
| Adapter        | Uniform interface for heterogeneous app output    | Translates format on behalf of consumers        |
| Init Container | Startup ordering, one-time setup before the app   | Runs to completion before main container starts |

<!-- .element: style="margin-left: 20px; width: 100%;" -->

Patterns introduced here appear in more advanced topics
- Sidecar as the foundation of service meshes
- Sidecar and adapter patterns are central to the observability stack
- Init containers are commonly used in GitOps pipelines
