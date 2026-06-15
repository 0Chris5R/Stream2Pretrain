<div class="lecturetitle">Exercise Track 1</div>
<!-- .slide: data-name="Introduction" data-state="hide-menubar" -->

---
# Terraform
<!-- .slide: id="terraform" data-name="Terraform" -->

---
## Terraform for Infrastructure Provisioning

Provision 3 VMs on DHBWCloud using Terraform
- Configure terraform the [OpenStack provider](https://registry.terraform.io/providers/terraform-provider-openstack/openstack/latest/docs) and local providers
- Supply [OpenStack endpoint](https://stack.dhbw.cloud/project/api_access/) and your credentials using env variables <comment>(e.g., OS_AUTH_URL, OS_PASSWORD)</comment> or directly in the provider block in `versions.tf`

<a data-code='hcl' data-link href="code/gridflex/terraform/versions.tf">versions.tf</a>
<!-- .element: style="font-size: 0.8em;" -->

---
## Terraform for Infrastructure Provisioning

Define required infrastructure variables
- Project ID, image ID, flavor name, network name, etc.
- Check [API Access](https://stack.dhbw.cloud/project/api_access/) → View Credentials → Project ID

<a data-code='hcl' data-link href="code/gridflex/terraform/variables.tf">variables.tf</a>
<!-- .element: style="font-size: 0.7em;" -->

---
## Terraform for Infrastructure Provisioning

Provision one master and two worker nodes

<a data-code='hcl' data-link href="code/gridflex/terraform/main.tf">main.tf</a>

---
## Terraform for Infrastructure Provisioning

Save output in a format that Ansible can consume

<a data-code='hcl' data-link href="code/gridflex/terraform/outputs.tf">outputs.tf</a>
<!-- .element: style="font-size: 0.43em;" -->

---
## Terraform for Infrastructure Provisioning

Setup terraform to download plugins

```bash
terraform init
```

Provision the infrastructure and generate the Ansible inventory

```bash
# use -auto-approve for non-interactive execution
terraform apply -auto-approve
```

Writes `generated-inventory.yml`
- Contains details required for software provisioning in the next step
- IP addresses, usernames, host roles (master vs worker)

Verify access to the machines with Ansible ping

```bash
ansible -i generated-inventory.yml all -m ping
```






---
# Ansible
<!-- .slide: id="ansible" data-name="Ansible" -->

---
## Ansible for Software Provisioning

Use Ansible to install a multi-nodes Kubernetes cluster
- Uses a k3s Ansible role with DHBWCloud-specific defaults

The Ansible playbook provides the following features
- Automatic DNS and TLS management
- A private container registry for remote development
- Simulates a multi-region cluster with 3 availability zones
- Ready-to-use kubeconfig for cluster access after playbook completion

---
## Pre-Requisites for DNS and TLS

Create a DNS Zone in DHBWCloud
- Log into the [self-service portal](https://self-service.dhbw.cloud) <comment>(select BWIDM login)</comment>
- Note the zone name, keyname, secret, and algorithm

Create a file `dns-credentials.yaml` with this content
- Replace the placeholders with your actual values

```yaml
all:
  children:
    gridflex_k3s_server:
      vars:
        cert_manager_email: sXXXXXXX@student.dhbw-mannheim.de
        rfc2136_zone: your.zone.here
        rfc2136_tsig_secret_value: XXXXXXX=
        rfc2136_tsig_secret_alg: hmac-sha512
        rfc2136_tsig_secret_keyname: user-key-XXXXXXX
```

---
## Deploy Additional Dependencies

Create a playbook (deploy.yaml)

<a data-code="yaml" data-begin="Play 1" data-end="Play 3" href="code/gridflex/ansible/deploy.yaml">deploy.yaml</a>
<!-- .element: style="font-size: 0.56em;" -->

---
## Kubernetes Cluster Deployment

Install [k3s-dhbw-cloud-role](https://github.com/pfisterer/k3s-dhbw-cloud-role)
- Create `requirements.yaml` as described in the role's README
- Run `ansible-galaxy install -r requirements.yaml --force` to install the role
  
Download [k3s-configure.yaml](code/gridflex/ansible/tasks/k3s-configure.yaml) and [wait.yaml](code/gridflex/ansible/tasks/wait.yaml) 
- Save it to directory `tasks/`

Run the playbook and supply two inventories
- Merges terraform output with DNS credentials from the previous step
- `ansible-playbook -i generated-inventory.yml -i dns-credentials.yaml deploy.yml`
- On success, `kubeconfig-<master-ip>.yaml` is written

---
## Kubernetes Cluster: Check Cluster Status

Test access to the cluster with kubectl
- `export KUBECONFIG=kubeconfig-<master-ip>.yaml`
- `kubectl get nodes` should show three ready nodes

Display nodes and their labels to verify zone distribution

```bash
kubectl get nodes --show-labels
```

Example output
- Each node should have a label `topology.kubernetes.io/zone=region-x`
- `x` should be `a`, `b`, or `c` representing the availability zone

```bash
NAME              STATUS [...] LABELS
gridflex-master   Ready  [...] ...,topology.kubernetes.io/zone=region-a
gridflex-worker-1 Ready  [...] ...,topology.kubernetes.io/zone=region-b
gridflex-worker-2 Ready  [...] ...,topology.kubernetes.io/zone=region-c
```
<!-- .element: style="font-size: 0.65em;" -->

---
## Kubernetes Cluster: Check Registry Status
Check if the container registry is running

```bash
# Check for "registry" deployment
kubectl get deployments.apps registry

# Check for ingress
kubectl get ingress registry

# Certificate should be issued and valid
kubectl get certificate -n kube-system wildcard-tls \
  -o custom-columns='DNS:.spec.dnsNames,READY:.status.conditions[?(@.type=="Ready")].status'
```
<!-- .element: style="font-size: 0.5em;" -->

Example output

```bash
# Deployment
NAME       READY   UP-TO-DATE   AVAILABLE
registry   1/1     1            1

# Ingress
NAME       CLASS     HOSTS          ADDRESS                                    PORTS
registry   traefik   ...dhbw.site   141.72.12.191,141.72.13.42,141.72.176.62   80, 443

# Certificate
DNS                         READY
[*.[...].users.dhbw.site]   True
```
<!-- .element: style="font-size: 0.5em;" -->


---
## Build, Push, Deploy
<!-- .slide: id="build-push-deploy" data-name="App" -->


---
## Folder Structure

<style>
  .dirtree {
    font-size: 20%;
    line-height: 1.1em;
  }
</style>
<!-- 
	generate using 
	find code/gridflex/api  -type f -not -path '*/\.*' | grep -v node_modules | sort
-->
<pre class="dirtree" data-zipname="gridflex-api.zip"> 
code/gridflex/api/Dockerfile
code/gridflex/api/package.json
code/gridflex/api/src/cache/valkey.js
code/gridflex/api/src/index.js
code/gridflex/api/src/logger.js
code/gridflex/api/src/metrics.js
code/gridflex/api/src/openapi.js
code/gridflex/api/src/routes/devices.js
code/gridflex/api/src/routes/events.js
code/gridflex/api/src/routes/query.js
code/gridflex/api/src/routes/telemetry.js
code/gridflex/api/src/store/memory.js
code/gridflex/api/src/store/mongo.js
</pre>
<!-- .element: style="font-size: 0.5em;" -->


---
## Build gridflex-api Container Image

Use this Dockerfile (build steps on next slide)

<a data-code='dockerfile' data-link href="code/gridflex/api/Dockerfile">Dockerfile</a>
<!-- .element: style="font-size: 0.8em;" -->

---
## Build gridflex-api

Build the image and upload to your private registry
- Requires HTTPS or [Docker insecure registry config](https://docs.docker.com/reference/cli/dockerd/#insecure-registries) and [k3s config](https://docs.k3s.io/installation/private-registry#without-tls)
- Tag as `registry.your-domain.tld/gridflex-api:latest`
- Omit `registry.your-domain.tld/` if using Docker Hub

Example multi-platform build using buildx

<a  data-code='bash' data-link href="code/gridflex/api/build-and-push.sh">build-and-push.sh</a>

---
## gridflex-api: Deployment

Run the application in Kubernetes
- Create a Kubernetes Deployment with 3 replicas
- Deploy using kubectl apply

<a data-code="yaml" data-end="# Resources" href="code/gridflex/api/k8s-deployment-and-service.yaml">k8s-deployment-and-service.yaml</a>
<!-- .element: style="font-size: 0.75em;" -->

---
## gridflex-api: Service

Add a Service for gridflex-api with a ClusterIP

<a data-code="yaml" data-begin="---" href="code/gridflex/api/k8s-deployment-and-service.yaml">k8s-deployment-and-service.yaml</a>
<!-- .element: style="font-size: 0.8em;" -->

Access the application
- Currently, the service is cluster-internal
- Use port forwarding to access it from your local machine
- Open <http://localhost:3000/api-docs> in your browser

<a data-code='bash' href="code/gridflex/api/port-forward.sh">port-forward.sh</a>

---
# Resources and Probes
<!-- .slide: id="resources-and-probes" data-name="Resources/Probes" -->

---
## Resources and Probes

Read [Resource Management for Pods and Containers](https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/)
- Add sensible resource requests and limits for CPU and memory
- Prevents resource contention and node instability under load

Read [Configure Liveness and Readiness Probes](https://kubernetes.io/docs/tasks/configure-pod-container/configure-liveness-readiness-startup-probes/)
- Add liveness and readiness probes to the Deployment

Solution: next slides

---
## Resources and Probes

<a data-code='yaml' data-begin="# Resources" data-end="# Probes" data-outdent data-link href="code/gridflex/api/k8s-deployment-and-service.yaml">k8s-deployment-and-service.yaml</a>
<!-- .element: style="font-size: 0.9em;" -->

---
## Probes

<a data-code='yaml' data-begin="# Probes" data-end="# topologySpreadConstraints" data-link data-outdent href="code/gridflex/api/k8s-deployment-and-service.yaml">k8s-deployment-and-service.yaml</a>
<!-- .element: style="font-size: 0.87em;" -->










---
# Availability
<!-- .slide: id="availability" data-name="Availability" -->

---
## Availability

Implement a high-availability strategy for gridflex-api
- Distribute replicas across nodes and zones to tolerate node failures

The nodes in the cluster are labeled with their zone
- E.g., `topology.kubernetes.io/zone=zone-a`
- Check the labels with `kubectl get nodes --show-labels`
- Choose and implement an appropriate strategy 

| Strategy                    | How it works                                                                               | Trade-off                                                   |
| --------------------------- | ------------------------------------------------------------------------------------------ | ----------------------------------------------------------- |
| `podAntiAffinity`           | Prevent two API pods from landing on the same node                                         | Simpler YAML, but no partial balance                        |
| `taints` and `tolerations`  | Taint nodes with `gridflex-api=true:NoSchedule`; add a matching toleration to the API pods | Explicit, but more complex and less flexible                |
| `topologySpreadConstraints` | Spread pods across zones with `maxSkew: 1` and `topologyKey: topology.kubernetes.io/zone`  | Most expressive; tolerates imbalances during scaling events |

<!-- .element: style="font-size: 0.6em;" -->
---
## Availability

<a data-code='yaml' data-begin="# topologySpreadConstraints" data-end="---" data-link data-outdent href="code/gridflex/api/k8s-deployment-and-service.yaml">k8s-deployment-and-service.yaml</a>

---
## Availability

Check pod distribution across nodes and zones

```bash
kubectl get pods -l app=gridflex-api -o wide
```

Example output

```bash
$ kubectl get pods -l app=gridflex-api -o wide
NAME ...          READY [...]  IP           NODE             
gridf...9-f7lrz   1/1   [...]  10.42.0.49   gridflex-master  
gridf...9-lwzmx   1/1   [...]  10.42.4.52   gridflex-worker-1
gridf...9-rdm5v   1/1   [...]  10.42.4.51   gridflex-worker-1
```






---
# External Access
<!-- .slide: id="external-access" data-name="External Access" -->

---
## Production-Readiness: External Access

Make the service reachable from the outside
- Select a hostname, e.g., `gridflex-api.your-domain.tld`
- Use a `/` as prefix
- Add TLS configuration
- Use the [default ingress class](https://kubernetes.io/docs/concepts/services-networking/ingress/#default-ingress-class)

Create and deploy the Ingress
- Verify that one or more IP addresses are assigned to it
- Access the application via the Ingress hostname in your browser

---
## Production-Readiness: External Access

<a data-code='yaml' data-link href="code/gridflex/api/k8s-ingress.yaml">k8s-ingress.yaml</a>
<!-- .element: style="font-size: 0.7em;" -->














---
<!-- ---------------------------------------------------------------------------------- -->
<!-- ---------------------------------------------------------------------------------- -->
# Dependency Installation
<!-- .slide: id="deploy-dependencies" data-name="Dependency Installation" -->
<!-- ---------------------------------------------------------------------------------- -->
<!-- ---------------------------------------------------------------------------------- -->

Gridflex has several external dependenciens
- Valkey for caching and pub/sub
- MongoDB for persistent storage (NoSQL document store)
- Keycloak for authentication and authorization
- PostgreSQL for Keycloak's database (SQL database)

Need to be installed and configured before deploying the application
- Installed via a dedicated Ansible playbook <comment>(see next slides)</comment>

---
## Dependency: Valkey (Redis-Fork)

Valkey (open-source fork of [Redis](https://github.com/redis/redis))
- Widely used in-memory key–value db for caching and messaging
- Client libraries for many languages, e.g., [ioredis](https://github.com/redis/ioredis) for Node.js

Simple API for storing and retrieving key-value pairs
- `SET key value` to store a value
- `GET key` to retrieve it
- Gridflex caches device state <comment>(low-latency access without database hits)</comment>

Can also be used as a pub/sub system for real-time messaging
- `PUBLISH channel message` to send a message
- `SUBSCRIBE channel` to receive messages
- Gridflex publish grid events to all API pods

---
## Dependency: Valkey Helm values.yaml

Uses this [helm chart](https://github.com/valkey-io/valkey-helm) to install Valkey
- The values declare an single-node Valkey instance with persistence disabled

<a data-code='yaml' data-link href="code/gridflex/ansible/files/valkey-values.yaml">valkey-values.yaml</a>
<!-- .element: style="font-size: 0.8em;" -->

---
## Dependency: Valkey Helm Installation

<a data-code='yaml' data-link href="code/gridflex/ansible/tasks/valkey.yaml">valkey.yaml</a>
<!-- .element: style="font-size: 0.6em;" -->

---
## Dependency: MongoDB

[MongoDB](https://www.mongodb.com/)
- Document-oriented NoSQL database <comment>(stores flexible JSON-like docs)</comment>
- Provides persistence and [advanced querying capabilities](https://www.mongodb.com/docs/manual/reference/mql/)

Gridflex and MongoDB
- Primary database for devices, telemetry, and grid events
- 1 primary <comment>(accepts writes)</comment> and 2 secondaries <comment>(handle reads)</comment>

Installation
- Install MongoDB Operator
- Create a secret for admin credentials
- Define a cluster using a MongoDBCommunity CR
- Check for cluster readiness and test connectivity with `mongosh`

---
## Dependency: MongoDB: Operator

Deployment using [MongoDB Controllers for Kubernetes (MCK)](https://github.com/mongodb/mongodb-kubernetes)

<a data-code='yaml' data-link data-begin="# === MongoDB Operator ===" data-end="# === MongoDB Instance ===" href="code/gridflex/ansible/tasks/mongodb.yaml">mongodb.yaml</a>
<!-- .element: style="font-size: 1em;" -->

---
## Dependency: MongoDB: Secret

Create a Secret for MongoDB credentials
- Contains password referenced in the MongoDBCommunity CR
- Used by gridflex-api to authenticate to the database

Applied by the Ansible playbook

<a href="code/gridflex/ansible/files/mongodb-password.yaml" data-link data-code='yaml'>mongodb-password.yaml</a>

---
## Dependency: MongoDB: Cluster

Deploy the [MongoDBCommunity](https://github.com/mongodb/mongodb-kubernetes/blob/master/config/crd/bases/mongodbcommunity.mongodb.com_mongodbcommunity.yaml) (excerpt)
- Assigns admin privileges <comment>(do not use in production)</comment>

<a href="code/gridflex/ansible/files/mongodb-replicaset.yaml" data-link data-code='yaml' data-end="statefulSet:">mongodb-replicaset.yaml</a>
<!-- .element: style="font-size: 0.7em;" -->

---
## Dependency: MongoDB: Readiness

Check for MongoDb cluster readiness

<a data-code='yaml' data-link data-begin="# === MongoDB Wait ===" href="code/gridflex/ansible/tasks/mongodb.yaml">mongodb.yaml</a>

---
## Dependency: MongoDB: Connect

Connect from within the cluster (run `mongosh` inside the cluster)

<a data-code="bash" data-link href="code/gridflex/mongodb-connect-cli.sh">MongoDB CLI</a>

Run some commands
- Test connectivity and replica set configuration

```js
// Reads go to secondaries; writes still go to primary
db.getMongo().setReadPref("secondaryPreferred")

// Check replica set status (one primary, two secondaries)
use("admin")
rs.status().members.map(m => ({ name: m.name, state: m.stateStr }))
```
<!-- .element: style="font-size: 0.67em;" -->

---
## Dependency: MongoDB: CRUD

Run some simple CRUD operations to test the database
- Create a `devices` collection, insert, query, and delete a test document

```js
// Switch to the application database
use("gridflex")

// Write a test document
// mongosh redirects to primary automatically
db.devices.insertOne({ 
  _id: "dev-001", 
  name: "Heat Pump A", 
  status: "online" 
})

// Read it back
db.devices.findOne({ _id: "dev-001" })

// Clean up
db.devices.deleteOne({ _id: "dev-001" })
```

---
## Dependency: MongoDB: Storage

Operator creates a two PVCs per member
- One `data-volume` and one `logs-volume` per pod

```console
NAME                   VOLUME          CAPACITY   ACCESS MODES   STORAGECLASS
data-[...]-mongodb-0   pvc-5cb489...   10G        RWO            local-path  
data-[...]-mongodb-1   pvc-648c3f...   10G        RWO            local-path  
data-[...]-mongodb-2   pvc-9e3351...   10G        RWO            local-path  
logs-[...]-mongodb-0   pvc-828f08...   2G         RWO            local-path  
logs-[...]-mongodb-1   pvc-c4c90b...   2G         RWO            local-path  
logs-[...]-mongodb-2   pvc-88f0a3...   2G         RWO            local-path  
```
<!-- .element: style="font-size: 0.55em;" -->

To start fresh, delete them before applying with
- `kubectl delete pvc -l app=gridflex-mongodb-svc`

---
## Dependency: Keycloak

[Keycloak](https://www.keycloak.org/): open-source identity and access management
- Lets applications offload authentication and authorization logic
- Applications just validate incoming JWTs and enforce scopes/roles

Supports installation using the [Keycloak Operator](https://www.keycloak.org/operator/installation#_installing_by_using_kubectl_without_operator_lifecycle_manager)
- Operator installation: kubectl or Operator Lifecycle Manager ([OLM](https://olm.operatorframework.io/))
- For the exercise, we use plain `kubectl`

Before deploying Keycloak, a SQL database is required
- We will use PostgreSQL; a popular choice for Keycloak in production
- Database management is out-of-scope of the operator
- Must be provisioned separately

---
## Dependency: CloudNativePG: Operator

[CloudNativePG (CNPG)](https://cloudnative-pg.io/): PostgreSQL operator for Kubernetes
- Manages `Cluster` CRs: handles failover, backups, and upgrades

Creates three services per cluster <commment>(different access patterns)</commment>

| Service     | Routes to     | Use for            |
| ----------- | ------------- | ------------------ |
| `<name>-rw` | primary only  | writes             |
| `<name>-r`  | all instances | reads              |
| `<name>-ro` | replicas only | read-only replicas |
<!-- .element: style="font-size: 0.6em; margin-left: 20px;" -->

Install the CNPG operator

<a data-code='yaml' data-link data-begin="# === PostgreSQL Operator ===" data-end="# === PostgreSQL Instance" href="code/gridflex/ansible/tasks/postgres.yaml">postgres.yaml</a>
<!-- .element: style="font-size: 0.65em;" -->

---
## CloudNativePG: Keycloak Database

Describe Password and Database Configuration

<a data-code='yaml' data-link href="code/gridflex/ansible/files/postgres-cluster.yaml">postgres-cluster.yaml</a>
<!-- .element: style="font-size: 0.7em;" -->

---
## CloudNativePG: Keycloak Database

Install and verify the PostgreSQL cluster

<a data-code='yaml' data-link data-begin="# === PostgreSQL Instance ===" href="code/gridflex/ansible/tasks/postgres.yaml">postgres.yaml</a>
<!-- .element: style="font-size: 0.8em;" -->

---
## Dependency: Keycloak Operator (1/2)

Install CRDs and namespace for the Keycloak operator

<a data-code='yaml' data-link data-begin="# === Keycloak CRDs and Namespace" data-end="# === Keycloak Operator" href="code/gridflex/ansible/tasks/keycloak.yaml">keycloak.yaml</a>

---
## Dependency: Keycloak Operator (2/2)

Install the operator

<a data-code='yaml' data-link data-begin="# === Keycloak Operator ===" data-end="# === Keycloak Instance ===" href="code/gridflex/ansible/tasks/keycloak.yaml">keycloak.yaml</a>
<!-- .element: style="font-size: 0.7em;" -->

---
## Dependency: Keycloak Instance

Describe the Keycloak instance configuration and Secret

<a data-code='yaml' data-link href="code/gridflex/ansible/templates/keycloak.yaml.j2">keycloak.yaml.j2</a>
<!-- .element: style="font-size: 0.47em;" -->

---
## Dependency: Keycloak Installation

Install the Keycloak instance

<a data-code='yaml' data-link data-begin="# === Keycloak Instance" data-end="# === Keycloak Realm" href="code/gridflex/ansible/tasks/keycloak.yaml">keycloak.yaml</a>
<!-- .element: style="font-size: 0.94em;" -->

---
## Dependency: Keycloak: Gridflex Realm

Keycloak uses realms to isolate applications
- Each realm has its own users, clients, and roles

Create a `gridflex` realm for our application
- We use a role `operator` for users who can trigger grid events
- A user `grid-operator` is created with the `operator` role
- In production, integrate with an existing identity provider <comment>(LDAP, SAML, or OIDC federation)</comment> instead of creating users directly
- For devices, we create a client `gridflex-device` that uses the client credentials flow

Keycloak operator provides a [KeycloakRealmImport](https://www.keycloak.org/operator/realm-import) CRD
- Used to create realm configurations from YAML

---
## Dependency: Keycloak: Gridflex Realm

<a data-code='yaml' data-link href="code/gridflex/ansible/templates/keycloak-realm.yaml.j2">keycloak-realm.yaml.j2</a>
<!-- .element: style="font-size: 0.5em;" -->

---
## Dependency: Keycloak: Install Realm

<a data-code='yaml' data-link data-begin="# === Keycloak Realm" href="code/gridflex/ansible/tasks/keycloak.yaml">keycloak.yaml</a>
<!-- .element: style="font-size: 0.97em;" -->

---
## Dependency: Keycloak: Login

Login to the Keycloak admin console
- Username `temp-admin`, password `admin`
- Change the activerealm to `gridflex`

Test token issuance for a device client
- Should return a valid JWT
- Can be decoded and verified at <https://jwt.io>

<a data-code='bash' data-link href="code/gridflex/keycloak-test-token-issuance.sh">test-token-issuance.sh</a>
<!-- .element: style="font-size: 0.7em;" -->







---
<!-- ---------------------------------------------------------------------------------- -->
<!-- ---------------------------------------------------------------------------------- -->
# Application Packaging and Deployment
<!-- .slide: id="app-packaging-and-deployment" data-name="Deployment" -->
<!-- ---------------------------------------------------------------------------------- -->
<!-- ---------------------------------------------------------------------------------- -->

---
## Create a Helm Chart

Create the chart skeleton

```console
code/gridflex/
├── helm-chart/
│   ├── Chart.yaml            # name, version, appVersion
│   ├── values.yaml           # defaults (all features off)
│   ├── values-dev.yaml       # in-memory, ai+ollama enabled, no ingress
│   ├── values-prod.yaml      # mongo, valkey, ai+ollama, TLS ingress
│   └── templates/
│       ├── _helpers.tpl
│       ├── deployment.yaml   # gridflex-api
│       ├── service.yaml
│       └── ingress.yaml
└── api/
    ├── Dockerfile
    └── src/
```
<!-- .element: style="font-size: 0.6em;" -->

Example Chart.yaml

<a data-code='yaml' data-link href="code/gridflex/helm-chart/Chart.yaml">Chart.yaml</a>
<!-- .element: style="font-size: 0.8em;" -->

---
## Parameterize the Deployment

Replace every hardcoded value with a Helm expression
- Excerpt from the Deployment manifest

<a data-code='yaml' data-outdent data-begin="# Container for gridflex-api" data-end="env:" data-link href="code/gridflex/helm-chart/templates/deployment.yaml">deployment.yaml</a>

---
## Helm Default Values

<a data-code='yaml' data-link data-end="# ── Autoscaling" href="code/gridflex/helm-chart/values.yaml">values.yaml</a>
<!-- .element: style="font-size: 0.6em;" -->

---
## Helm Dev/Prod Values

Development

<a data-code='yaml' data-link data-end="# gridflex-ai" href="code/gridflex/helm-chart/values-dev.yaml">values-dev.yaml</a>
<!-- .element: style="font-size: 0.5em;" -->

Production

<a data-code='yaml' data-link data-end="# gridflex-ai" href="code/gridflex/helm-chart/values-prod.yaml">values-prod.yaml</a>
<!-- .element: style="font-size: 0.5em;" -->

---
## Install and Upgrade with Helm

Delete the current application from the cluster

```bash
kubectl delete deployment gridflex-api
kubectl delete service gridflex-api
kubectl delete ingress gridflex-api
```

First install

```bash
helm upgrade --install gridflex \
  ./helm-chart -f helm-chart/values-dev.yaml
```

Upgrade (e.g. switch to prod values)

```bash
helm upgrade --install gridflex \
  ./helm-chart -f helm-chart/values-prod.yaml
```

Inspect the release

```bash
helm list
helm get values gridflex-api
```

---
## Skaffold for Development

Create `skaffold.yaml` in the project root
- Used for local development with Skaffold

Build-phase of Skaffold

<a data-code='yaml' data-link data-end="# Deploy" href="code/gridflex/skaffold.yaml">skaffold.yaml</a>
<!-- .element: style="font-size: 0.7em;" -->

---
## Skaffold for Development

Deployment phase
- Includes port-forwarding for API access without any Ingress

<a data-code='yaml' data-link data-begin="# Deploy" data-end="# Profiles" href="code/gridflex/skaffold.yaml">skaffold.yaml</a>
<!-- .element: style="font-size: 0.7em;" -->

---
## Skaffold for Development

Start the development loop with Skaffold
- Port-forwarding is automatically setup <comment>(no need for manual `kubectl port-forward` commands)</comment>
- Run `skaffold dev` and access the API at <http://localhost:3000>
- Stop with `Ctrl-C` for cleanup of all deployed resources

Skaffold watches for file changes
- Rebuilds the image and redeploys the application automatically

Some files do not trigger a rebuild 
- Per our configuration, this affects all JavaScript files (`src/**/*.js`) 
- These files are synced into the running container
- Triggers hot-reload <comment>(due to `node --watch`)</comment>
