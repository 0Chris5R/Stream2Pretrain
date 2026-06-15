<div class="lecturetitle">Deployment Strategies & Package Management</div>
<!-- .slide: data-name="Introduction" data-state="hide-menubar" -->

---
## Chapter Overview

How do we get applications into a Kubernetes cluster
- And keep and update them reliably

Package management
- Kustomize: overlay-based, template-free manifest customization
- Helm: templated, versioned application bundles

Development workflow
- Skaffold: inner dev loop tying build, render, and deploy together

Extending Kubernetes
- Operators: packaging operational knowledge as code

---
<!--- ------------------------------------------------------------------- --->
<!--- ------------------------------------------------------------------- --->
# Package Management
<!-- .slide: data-name="Package Management" -->
<!--- ------------------------------------------------------------------- --->
<!--- ------------------------------------------------------------------- --->

---
## Deploying Complex Applications

Applications are comprised of many building blocks
- Database, Proxy, Load Balancer, Services, Certificates, Secrets, Configuration, ... 
- Challenge of orchestrating many different building blocks
- Requires deep understanding of the interplay of components
- Deploying, cleaning up, and updating can be complex

Kubernetes: Different solutions exist
- [**Kustomize**](https://kustomize.io/): overlay-based patching, built into kubectl
- [**Helm**](https://helm.sh/): templated YAML charts with release management
- [Jsonnet](https://jsonnet.org/): data templating language for generating manifests

---
<!--- ------------------------------------------------------------------- --->
# Kustomize
<!-- .slide: data-name="Kustomize" -->
<!--- ------------------------------------------------------------------- --->

[Kustomize](https://kustomize.io/): template-free way to customize Kubernetes manifests
- Built into `kubectl` <comment>(kubectl apply -k ./my-overlay)</comment>
- No templating language <comment>(uses overlays to patch base manifests)</comment>

Core concepts
- Base: common resources shared across all environments
- Overlay: environment-specific patches applied on top of the base
- Patches can modify any field <comment>(replicas, image tag, resource limits, ...)</comment>

When to use Kustomize vs. Helm
- Kustomize: simpler, no templating language, good for small projects
- Helm: complex values, publishable packages, large ecosystem

---
## Kustomize: Directory Structure

```bash
k8s/
  # Shared resources, applied to all environments
  base/                    
    # lists base resources (deployment.yaml, service.yaml, ...)
    kustomization.yaml     
    # the actual k8s manifests
    deployment.yaml
    service.yaml
  
  # Environment-specific patches
  overlays/
    # Patch: 1 replica, latest image tag, no resource limits
    dev/                   
      kustomization.yaml
    # Patch: 3 replicas, pinned tag, resource limits
    production/            
      kustomization.yaml
```

---
## Kustomize: Base Resources

`base/kustomization.yaml`
- Lists all shared resources <comment>(e.g., Deployment, Service, ConfigMap, ...)</comment>
- No environment-specific configuration <comment>(e.g., image tags, replica counts, resource limits)</comment>
- No wildcard support for resources <comment>(explicitly list all resources)</comment>
- No templating language <comment>(use overlays to customize fields)</comment>

Example: `base/kustomization.yaml`

```yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization

resources:
  - deployment.yaml
  - service.yaml
```

---
## Kustomize: Base Resources

Shared Deployment definition: `base/deployment.yaml`
- Standard Kubernetes manifest with some missing or placeholder values <comment>(e.g., image tag is left empty or could be set to a default)</comment>

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: web-app
spec:
  replicas: 1
  selector:
    matchLabels: { app: web-app }
  template:
    metadata:
      labels: { app: web-app }
    spec:
      containers:
        - name: web-app
          image: my-web-app # image tag is set per overlay
          ports:
            - containerPort: 8080
```
<!-- .element: style="font-size: 0.65em;" -->

---
## Kustomize: How Overlays Work

Overlay's references the base and declares what to change
- Defined in `/k8s/overlay-name/kustomization.yaml`

Referencing one or more bases (and/or other overlays)
- Order matters: resources are processed in the order they are listed

```yaml
resources:
  - ../../base # pull in all base resources as a starting point
  - ../other-overlay # optionally pull in another overlay
```

Customization options to modify fields in the base resources

| Field                       | What it does                                                       | Example                     |
| --------------------------- | ------------------------------------------------------------------ | --------------------------- |
| `namePrefix` / `nameSuffix` | Prepend/append a string to all resource names                      | `dev-web-app`               |
| `images`                    | Replace an image tag by image name — no patch needed               | `newTag: "1.4.2"`           |
| `patches`                   | Deep-merge a partial resource spec onto the matching base resource | change replicas, add limits |

---
## Kustomize: Patches

Patch: normal resource but only contains the fields to change
- Resources to update are identified by comparing apiVersion, kind, and metadata.name with resources in the base

```yaml
patches:
  - patch: |-
      apiVersion: apps/v1
      kind: Deployment
      metadata:
        name: web-app
      spec:
        replicas: 3
        template:
          spec:
            containers:
              - name: web-app
                resources:
                  requests: { cpu: "100m", memory: "128Mi" }
                  limits:   { cpu: "500m", memory: "256Mi" }
```

---
## Kustomize: Dev Overlay

Dev-Environment: `overlays/dev/kustomization.yaml`

```yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization

resources:
  - ../../base

namePrefix: dev- # all resource names get a "dev-" prefix

images:
  - name: my-web-app
    newTag: latest # always pull the latest image in dev
```

Yields a Deployment named `dev-web-app` 
- With `image: my-web-app:latest`, 1 replica, no resource limits
- Activate by running `kubectl apply -k k8s/overlays/dev`
- Delete with `kubectl delete -k k8s/overlays/dev`

---
## Kustomize: Production Overlay

```yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization

resources: [ "../../base" ]
namePrefix: prod-
images:
  - name: my-web-app
    newTag: "1.4.2"
patches:
  - patch: |-
      apiVersion: apps/v1
      kind: Deployment
      metadata:
        name: web-app
      spec:
        replicas: 3
        template:
          spec:
            containers:
              - name: web-app
                resources:
                  requests: { cpu: "100m", memory: "128Mi" }
                  limits:   { cpu: "500m", memory: "256Mi" }
```
<!-- .element: style="font-size: 0.65em;" -->


---
<!--- ------------------------------------------------------------------- --->
# Helm
<!-- .slide: data-name="Helm" -->
<!--- ------------------------------------------------------------------- --->

[Helm](https://helm.sh/) is a package manager for Kubernetes
- Supports developers in deploying complex application bundles
- Client-side command line tool

Applications are packaged as so-called Charts
- Helm charts are configurable templates of YAML files
- Allows configuring different aspects for each deployment
- Helm generates client-side Kubernetes resources from templates and deploys/deletes them

Charts are published in repositories
- Many applications are available in different repositories
- Charts can be searched using [Artifact Hub](https://artifacthub.io/)

---
## Helm CLI 

Adding repositories: [`helm repo add`](https://helm.sh/docs/helm/helm_repo_add/)
- Example: `helm repo add bitnami https://charts.bitnami.com/bitnami`
- Update repositories: `helm repo update`

Searching for charts: [`helm search`](https://helm.sh/docs/helm/helm_search/)
- Search hub: `helm search hub mariadb`
- Search local repos: `helm search repo mariadb`

Installing installs a chart: [`helm install`](https://helm.sh/docs/helm/helm_install/)
- Example: `helm install my-mariadb-instance bitnami/mariadb`

Deleting a release
- Example: `helm uninstall my-mariadb-instance`

---
## Helm Templating 

Template are [Go templates](https://golang.org/pkg/text/template/) parameterized with data

```yaml
docker:
  image: mariadb
  tag: latest
```

Examples of templates (cf. [tutorial](https://blog.gopheracademy.com/advent-2017/using-go-templates/) and [online tester](https://camlittle.com/go-template-validation))

| Template                                                                                                    | Output             |
| ----------------------------------------------------------------------------------------------------------- | ------------------ |
| `Hallo {{ .Values.docker.image }}`                                                                          | `Hallo mariadb`    |
| <code>{{ .Values.docker.image }} :  <br> {{ .Values.docker.tag }}</code>                                    | `mariadb : latest` |
| <code>{{ .Values.docker.image -}} : <br> {{- .Values.docker.tag }}</code>                                   | `mariadb:latest`   |
| <code>{{- if .Values.bla -}}<br>&nbsp;bla vorhanden<br>{{- else -}}<br>&nbsp;Kein bla<br>{{- end -}}</code> | `Kein bla`         |

<!-- .element: style="margin-left: 20px; width: 93%; font-size: .75em;" -->

---
## Templating Kubernetes Resources

File: `values.yaml`

```yaml
docker:
  image: mariadb
  tag: latest
mariadb:
  password: "mysecretpw"
```

File: `templates/pod.yaml`

```yaml
apiVersion: v1
kind: Pod
metadata:
  name: mariadb-demo-pod
spec:
  containers:
    - name: "{{ .Chart.Name }}-{{ .Release.Name }}"
      image: {{ .Values.docker.image }}:{{ .Values.docker.tag }}
      env:
        - name: MARIADB_ROOT_PASSWORD
          value: "{{ .Values.mariadb.password }}"
```

---
## Overriding Default Values

A chart provides default values in `values.yaml`
- These can be overridden with [custom values](https://helm.sh/docs/chart_template_guide/values_files/)

Overriding using YAML files
- Pass one or more files using `-f`
- Example: `-f values1.yaml -f values2.yaml`

Overriding using command-line parameters
- Using (one or more) `--set` flags
- Example: `--set docker.tag=8.0.22`

Both options can be combined

---
## Validating Values with JSON Schema

Values are the core mechanism to configure Helm charts
- Users override default values to customize the chart for their needs
- Requires understanding of the chart and its configuration options
- Chart updates can lead to misconfigurations and errors

Typical issue: invalid values
- E.g., unexpected types, missing or unused required values, ...
- Lead to rendering errors or runtime errors in Kubernetes

Helm supports [JSON Schema validation](https://helm.sh/docs/topics/charts/#schema-files) for `values.yaml`
- Define a schema in `values.schema.json` at the root of the chart
- Helm validates supplied values before rendering any templates
- Errors are reported before any resources are deployed

---
## Validating Values with JSON Schema

Stronger guarantees about the correctness of supplied values
- Catches errors early in the deployment process

Makes `docker.repository` and `docker.tag` required strings
- Requires `replicaCount` to be an integer greater than or equal to 1

```json
{
  "$schema": "https://json-schema.org/draft-07/schema",
  "properties": {
    "docker": {
      "type": "object",
      "properties": {
        "repository": { "type": "string" },
        "tag":        { "type": "string" }
      },
      "required": ["repository", "tag"]
    },
    "replicaCount": { "type": "integer", "minimum": 1 }
  }
}
```

---
## Example: Deploy Grafana using Helm

Install Grafana using the [Grafana Helm Chart](https://artifacthub.io/packages/helm/grafana/grafana)
- Add repo `helm repo add grafana https://grafana.github.io/helm-charts` 
- Update local repository content `helm repo update`
- Use `helm install -f `<span data-prefix-url data-convert-to-inline-code="bash">code/helm-grafana/values.yaml</span>` my-grafana grafana/grafana`
- Run `minikube tunnel` in a separate terminal to expose the service

Access Grafana at <http://localhost:3000>
- Credentials: user `admin`, password: `admin`

Uninstall Grafana 
- Run `helm uninstall my-grafana` to remove all resources


---
<!--- ------------------------------------------------------------------- --->
# Skaffold
<!-- .slide: data-name="Skaffold" -->
<!--- ------------------------------------------------------------------- --->

Problem: development loop on Kubernetes is slow
- Code change → build image → push → update → wait rollout
- [Skaffold](https://skaffold.dev) automates this loop by watching source files, rebuilding, pushing, and redeploying on every change

No cluster-side components <comment>(runs on developer's machine)</comment>
- Works with local clusters <comment>(minikube, kind)</comment> and remote clusters
- Remote clusters require pushing to a registry <comment>(more overhead)</comment>

Pipeline stages configured in `skaffold.yaml`
- Build: Dockerfile, Buildpacks, Jib, Bazel, ...
- Render: raw YAML, Helm, Kustomize, ...
- Deploy: kubectl, Helm, kpt, ...

---
## Skaffold: Commands

| Command           | What it does                                                                     |
| ----------------- | -------------------------------------------------------------------------------- |
| `skaffold dev`    | Watch, rebuild, and redeploy on every file change; stream logs; clean up on exit |
| `skaffold run`    | Build and deploy once (equivalent to a CI pipeline step)                         |
| `skaffold build`  | Build and push images only, no deployment                                        |
| `skaffold delete` | Delete all resources deployed by Skaffold                                        |
| `skaffold debug`  | Like `dev`, but configures debugger ports for the running container              |

`skaffold dev` is the primary inner-loop command
- Streams logs from all deployed pods to the terminal
- Cleans up all resources when the process is interrupted <comment>(Ctrl+C)</comment>
- Use `--port-forward` to forward service ports to localhost automatically

---
## Skaffold with Raw YAML

Simplest setup: point Skaffold at plain Kubernetes manifests

```yaml
apiVersion: skaffold/v4beta11
kind: Config

build:
  artifacts:
    - image: my-web-app # image name (tag injected by Skaffold)
      context: app/ # directory with Dockerfile
      docker: # build using Dockerfile
        dockerfile: Dockerfile # optional if named "Dockerfile"
  local:
    push: false # skip registry push for local clusters

manifests:
  rawYaml:
    - k8s/deployment.yaml
    - k8s/service.yaml

deploy:
  kubectl: {} # deploy using kubectl apply
```


---
## Skaffold with Kustomize

Use Skaffold to apply a specific overlay during development
- Builds image, patches image tag into the overlay before applying

```yaml
apiVersion: skaffold/v4beta11
kind: Config

build:
  artifacts:
    - image: my-web-app
      context: app/
  local:
    push: false

manifests:
  kustomize:
    paths:
      # use the dev overlay
      - k8s/overlays/dev        

deploy:
  kubectl: {}
```

---
## Skaffold with Helm

Skaffold can use Helm instead of raw YAML manifests
- Skaffold builds image and passes tag to the chart via `setValues`

```yaml
apiVersion: skaffold/v4beta11
kind: Config
build:
  artifacts:
    - image: my-web-app
      context: app/
manifests:
  helm:
    releases:
      - name: my-web-app
        # local Helm chart directory
        chartPath: charts/my-web-app   
        valuesFiles:
        # values to use for rendering
          - charts/my-web-app/values.yaml 
deploy:
  helm: {}
```

---
<!--- ------------------------------------------------------------------- --->
<!--- ------------------------------------------------------------------- --->
# Kubernetes Operators
<!-- .slide: data-name="Operators" -->
<!--- ------------------------------------------------------------------- --->
<!--- ------------------------------------------------------------------- --->

---
## Operators: Motivation

Traditional approach: system administrators manage applications
- They have deep knowledge of certain applications <comment>(deploy, manage, update, troubleshoot, delete, ...)</comment>

Operators implement this knowledge in software
- Users define the desired state of an instance of an application
- Operators deploy and manage instances of an application

The desired state is defined using YAML files
- Using so-called [Custom Resources](https://kubernetes.io/docs/concepts/extend-kubernetes/api-extension/custom-resources/) (CRs)
- Their schema is defined using [Custom Resource Definitions](https://kubernetes.io/docs/tasks/extend-kubernetes/custom-resources/custom-resource-definitions/) (CRDs)
- For known CRDs, API server validates and stores CRs
- Operators watch the API server for changes to CRs and react accordingly

---
## Operators

Operators are software extensions to Kubernetes
- These implement the [Operator Pattern](https://kubernetes.io/docs/concepts/extend-kubernetes/operator/) and run inside the cluster and watch the API server for changes to CRs
- Operators have access to the Kubernetes API and can create, delete, or update resources <comment>(e.g., Pods, Deployments, Services, ...)</comment>

Many different operators already exist
- List of existing operators: <https://operatorhub.io>
- They vary in the degree of automation they provide
- Some provide installation only, others provide full lifecycle management

Additional reading
- [Introducing Operators: Putting Operational Knowledge into Software](https://cloud.redhat.com/blog/introducing-operators-putting-operational-knowledge-into-software)

---
## Example: Strimzi Operator for Kafka

Provides custom resources to manage [Kafka](https://kafka.apache.org/) clusters
- E.g., Kafka clusters, topics, users, etc.
- Watches these resources and deploys the required components
- Example: KafkaNodePool and Kafka custom resources

KafkaNodePool: defines a pool of Kafka nodes
- Defines the number of nodes, resources, and other properties

Kafka: defines a Kafka cluster
- Defines the Kafka cluster name, version, and other properties
- The operator creates the required resources (e.g., StatefulSets, Services, etc.)

---
## Example: Kafka Node Pool

<a data-code='yaml' data-link href="https://raw.githubusercontent.com/strimzi/strimzi-kafka-operator/refs/heads/main/examples/kafka/kafka-single-node.yaml" data-end="---">Source code</a>
<!-- .element: style="font-size: 1em">

---
## Example: Kafka Cluster Definition

<a data-code='yaml' data-link href="https://raw.githubusercontent.com/strimzi/strimzi-kafka-operator/refs/heads/main/examples/kafka/kafka-single-node.yaml"  data-begin="---">Source code</a>
<!-- .element: style="font-size: 0.77em">

---
## Example: Deploying a Kafka Cluster
<!-- .slide: id="strimzi-operator" -->

Install the [strimzi operator](http://strimzi.io) in Kubernetes

```bash
# Add the Strimzi Helm repository
helm repo add strimzi http://strimzi.io/charts/
# Install the Strimzi operator using Helm
helm install --wait my-kafka-operator strimzi/strimzi-kafka-operator
```

Create a Kafka cluster using the Strimzi operator
- [Documentation](https://strimzi.io/docs/operators/latest/configuring) for `KafkaNodePool` and `Kafka`

```bash
# Create a Kafka cluster using the operator
kubectl apply -f https://raw.githubusercontent.com/strimzi/strimzi-kafka-operator/refs/heads/main/examples/kafka/kafka-ephemeral.yaml
```

Get cluster status and then delete it
- Cluster status: `kubectl describe kafkas my-kafka-cluster`
- Delete it: `kubectl delete -f kafka-cluster-def.yaml`

---
## Example: Operators: Demo

<!-- 
    cd code/helm-kafka-operator
    asciinema rec --overwrite -i 2 ../../img/operator-strimzi.cast
-->
<asciinema data-conf='{ "cols": 120, "rows": 30, "theme":"monokai", "autoPlay": true, "idleTimeLimit": 2, "terminalFontSize": "16px"}' src="img/operator-strimzi.cast" />

---
## Exercises

Deploy Software
- <a data-exercise="deploy-dependencies">Deploy Software with Helm and Operators</a>

Packaging and Deployment
- <a data-exercise="app-packaging-and-deployment">App Packaging and Deployment</a>


---
<!--- ------------------------------------------------------------------- --->
<!--- ------------------------------------------------------------------- --->
# Custom Operators
<!-- .slide: data-name="Custom Operators" -->
<!--- ------------------------------------------------------------------- --->
<!--- ------------------------------------------------------------------- --->

Implementing your own operator
- [Kubernetes Operators: what are they? Some examples](https://www.cncf.io/blog/2022/06/15/kubernetes-operators-what-are-they-some-examples/)
- [Understanding Kubernetes Operators](https://earthly.dev/blog/kubernetes-operators/)
- [Writing your own Operator](https://kubernetes.io/docs/concepts/extend-kubernetes/operator/#writing-operator)
- [Writing a Kubernetes Operator: From Zero to Hero](https://anupamgogoi.medium.com/writing-a-kubernetes-operator-from-zero-to-hero-8ca5dc2462b7)

Choose an operator framework
- Golang: [kubernetes/client-go](https://github.com/kubernetes/client-go)
- [kubernetes-client](https://github.com/kubernetes-client) (different programming languages) 
- [Operator SDK](https://sdk.operatorframework.io/docs/)

Implement a custom controller
- Develop a simple idea and implement, deploy, and test the operator

---
## Example: Web App Operator

Idea: quickly deploy a web page using custom resources
- Users define a web page using a custom resource
- Operator creates a configmap, deployment, service, and ingress
- Configmap holds the web page's content
- Deployment runs nginx serving the web page from the configmap

Example for a custom resource

<a data-code='yaml' data-link href="code/operator-webapp-go/example-cr1.yaml" data-link>Source code</a>

---
## Custom Resource Definition

<a data-code='yaml' data-link href="code/operator-webapp-go/webappcrd.yaml" data-link>Source code</a>
<!-- .element: style="font-size: 0.7em">

---
## Operator Implementation

Simple operator implementation
- Watches for changes to custom resources (add, delete, update)
- Handles upserting and deleting resources

Download project
<!-- 
	generate using 
	find code/operator-webapp-go -type f -not -path '*/\.*' | sort | grep -v \.log
-->
<pre 
  class="dirtree" 
  data-zipname="webapp-operator.zip" 
  data-strip-prefix="code/operator-webapp-go"> 
code/operator-webapp-go/Dockerfile
code/operator-webapp-go/example-cr1.yaml
code/operator-webapp-go/example-cr2.yaml
code/operator-webapp-go/go.mod
code/operator-webapp-go/go.sum
code/operator-webapp-go/k8s-helpers.go
code/operator-webapp-go/operator.go
code/operator-webapp-go/skaffold.yaml
code/operator-webapp-go/webapp-operator-deployment.yaml
code/operator-webapp-go/webappcrd.yaml
</pre>

---
## Operator Implementation

Can be run inside or outside the cluster
- For development, outside the cluster is easier

Outside the cluster
- For development, use [`air`](https://github.com/air-verse/air) 
- Alternatively, run `go build -o operator . && ./operator`

Inside the cluster
- Run `skaffold dev` to build and deploy the operator
- Requires a service account with cluster-admin permissions

```bash
kubectl create clusterrolebinding rds-admin-binding \
  --clusterrole=cluster-admin \
  --serviceaccount=default:default
```

---
## Operator Implementation: Demo

<!-- 
    ctrl + a + S // split horizontally
    ctrl + a + Tab // switch panes
    ctrl + a + c // start new shell in new plane

    asciinema rec -c "screen" --overwrite -i 2 ../../img/webapp-operator-go.cast
-->
<asciinema data-conf='{ "cols": 120, "rows": 30, "theme":"monokai", "autoPlay": true, "idleTimeLimit": 2, "terminalFontSize": "10px"}' src="img/webapp-operator-go.cast" />
