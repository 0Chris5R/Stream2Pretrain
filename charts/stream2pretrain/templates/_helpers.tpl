{{/*
Expand the name of the chart.
*/}}
{{- define "stream2pretrain.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars (k8s DNS naming limit).
*/}}
{{- define "stream2pretrain.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
Component fullname: <fullname>-<component>
*/}}
{{- define "stream2pretrain.componentFullname" -}}
{{- $top := index . 0 -}}
{{- $component := index . 1 -}}
{{- printf "%s-%s" (include "stream2pretrain.fullname" $top) $component | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Chart label.
*/}}
{{- define "stream2pretrain.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Common labels applied to every resource.
*/}}
{{- define "stream2pretrain.labels" -}}
helm.sh/chart: {{ include "stream2pretrain.chart" . }}
{{ include "stream2pretrain.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: stream2pretrain
{{- with .Values.commonLabels }}
{{ toYaml . }}
{{- end }}
{{- end -}}

{{/*
Selector labels.
*/}}
{{- define "stream2pretrain.selectorLabels" -}}
app.kubernetes.io/name: {{ include "stream2pretrain.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{/*
Per-component selector labels. Caller passes (dict "ctx" . "component" "<name>").
*/}}
{{- define "stream2pretrain.componentSelectorLabels" -}}
{{- $ctx := .ctx -}}
app.kubernetes.io/name: {{ include "stream2pretrain.name" $ctx }}
app.kubernetes.io/instance: {{ $ctx.Release.Name }}
app.kubernetes.io/component: {{ .component }}
app.kubernetes.io/part-of: stream2pretrain
{{- end -}}

{{/*
Per-component service selector labels. Keep this intentionally smaller than
the workload selector so Services keep matching pods after non-Helm smoke
deployments that only stamp the stable name + component labels.
*/}}
{{- define "stream2pretrain.componentServiceSelectorLabels" -}}
{{- $ctx := .ctx -}}
app.kubernetes.io/name: {{ include "stream2pretrain.name" $ctx }}
app.kubernetes.io/component: {{ .component }}
{{- end -}}

{{/*
Per-component common labels. Caller passes (dict "ctx" . "component" "<name>").
*/}}
{{- define "stream2pretrain.componentLabels" -}}
{{- $ctx := .ctx -}}
helm.sh/chart: {{ include "stream2pretrain.chart" $ctx }}
{{ include "stream2pretrain.componentSelectorLabels" . }}
app.kubernetes.io/version: {{ $ctx.Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ $ctx.Release.Service }}
app.kubernetes.io/part-of: stream2pretrain
{{- with $ctx.Values.commonLabels }}
{{ toYaml . }}
{{- end }}
{{- end -}}

{{/*
Render the image reference for a component.
Caller passes (dict "ctx" . "image" "<repo>" "tag" "<override-or-empty>").
Falls back to .Chart.AppVersion when tag is empty.
*/}}
{{- define "stream2pretrain.image" -}}
{{- $ctx := .ctx -}}
{{- $tag := default $ctx.Chart.AppVersion $ctx.Values.image.tag -}}
{{- if .tag -}}{{- $tag = .tag -}}{{- end -}}
{{- if eq $tag "" -}}{{- $tag = $ctx.Chart.AppVersion -}}{{- end -}}
{{- printf "%s/%s:%s" $ctx.Values.image.registry .image $tag -}}
{{- end -}}

{{/*
Service account name.
*/}}
{{- define "stream2pretrain.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "stream2pretrain.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{/*
Image pull secrets block.
*/}}
{{- define "stream2pretrain.imagePullSecrets" -}}
{{- with .Values.image.pullSecrets }}
imagePullSecrets:
  {{- range . }}
  - name: {{ . }}
  {{- end }}
{{- end }}
{{- end -}}

{{/*
Render the env block shared by every Stream2Pretrain pod (Redpanda + MinIO +
Iceberg + observability).
*/}}
{{- define "stream2pretrain.commonEnv" -}}
- name: S2P_ENV
  value: {{ .Values.profile | quote }}
- name: LOG_LEVEL
  value: "INFO"
- name: REDPANDA_BROKERS
  value: {{ .Values.redpanda.bootstrapServers | quote }}
- name: REDPANDA_SCHEMA_REGISTRY
  value: {{ .Values.redpanda.schemaRegistry | quote }}
- name: S2P_KAFKA_START_OFFSET
  value: {{ .Values.redpanda.startingOffset | quote }}
- name: S2P_KAFKA_MESSAGE_MAX_BYTES
  value: {{ .Values.redpanda.maxMessageBytes | int64 | quote }}
- name: S2P_RAW_TOPIC
  value: {{ .Values.redpanda.topics.rawFetched | quote }}
- name: S2P_NORMALIZED_TOPIC
  value: {{ .Values.redpanda.topics.docsNormalized | quote }}
- name: S2P_CURATED_TOPIC
  value: {{ .Values.redpanda.topics.docsCurated | quote }}
- name: S2P_DECISIONS_TOPIC
  value: {{ .Values.redpanda.topics.curationDecisions | quote }}
- name: S2P_LICENSE_ADMISSIONS_TOPIC
  value: {{ .Values.redpanda.topics.licenseAdmissions | quote }}
- name: S2P_DECON_TOPIC
  value: {{ .Values.redpanda.topics.deconAttest | quote }}
- name: S2P_FOUNDRY_JOBS_TOPIC
  value: {{ .Values.redpanda.topics.foundryJobs | quote }}
- name: S2P_FOUNDRY_EVENTS_TOPIC
  value: {{ .Values.redpanda.topics.foundryEvents | quote }}
- name: S2P_FOUNDRY_ARTIFACTS_TOPIC
  value: {{ .Values.redpanda.topics.foundryArtifacts | quote }}
- name: MINIO_ENDPOINT
  value: {{ .Values.minio.endpoint | quote }}
- name: AWS_DEFAULT_REGION
  value: {{ .Values.minio.region | quote }}
- name: MINIO_BRONZE_BUCKET
  value: {{ .Values.minio.buckets.bronze | quote }}
- name: MINIO_SILVER_BUCKET
  value: {{ .Values.minio.buckets.silver | quote }}
- name: MINIO_GOLD_BUCKET
  value: {{ .Values.minio.buckets.gold | quote }}
- name: MINIO_DECON_BUCKET
  value: {{ .Values.minio.buckets.decon | quote }}
- name: MINIO_POSTTRAIN_BUCKET
  value: {{ .Values.minio.buckets.posttrain | quote }}
- name: ICEBERG_CATALOG
  value: {{ .Values.iceberg.catalog | quote }}
- name: ICEBERG_NAMESPACE
  value: {{ .Values.iceberg.namespace | quote }}
- name: S2P_ICEBERG_GOLD_TABLE
  value: {{ .Values.iceberg.goldTable | quote }}
- name: S2P_ICEBERG_DECISIONS_TABLE
  value: {{ .Values.iceberg.decisionsTable | quote }}
- name: S2P_ICEBERG_LICENSE_ADMISSIONS_TABLE
  value: {{ .Values.iceberg.licenseAdmissionsTable | quote }}
- name: S2P_ICEBERG_SNAPSHOT_RETENTION_HOURS
  value: {{ .Values.processor.iceberg.maintenance.snapshotRetentionHours | quote }}
- name: S2P_ICEBERG_MIN_SNAPSHOTS_TO_KEEP
  value: {{ .Values.processor.iceberg.maintenance.minSnapshotsToKeep | quote }}
- name: S2P_ICEBERG_METADATA_VERSIONS
  value: {{ .Values.processor.iceberg.maintenance.metadataVersions | quote }}
- name: S2P_ICEBERG_ORPHAN_MINIMUM_AGE_HOURS
  value: {{ .Values.processor.iceberg.maintenance.orphanMinimumAgeHours | quote }}
- name: S2P_ICEBERG_ACCESS_DELEGATION
  value: {{ .Values.iceberg.accessDelegation | quote }}
- name: POLARIS_URI
  value: {{ .Values.iceberg.polarisUrl | quote }}
- name: POLARIS_WAREHOUSE
  value: {{ .Values.iceberg.warehouse | quote }}
- name: POLARIS_CREDENTIAL
  valueFrom:
    secretKeyRef:
      name: {{ .Values.iceberg.credentialSecret | quote }}
      key: {{ .Values.iceberg.credentialKey | quote }}
- name: POLARIS_SCOPE
  valueFrom:
    secretKeyRef:
      name: {{ .Values.iceberg.credentialSecret | quote }}
      key: {{ .Values.iceberg.scopeKey | quote }}
- name: OTEL_EXPORTER_OTLP_ENDPOINT
  value: {{ .Values.observability.tracing.otlpEndpoint | quote }}
- name: OTEL_TRACES_SAMPLER
  value: parentbased_traceidratio
- name: OTEL_TRACES_SAMPLER_ARG
  value: {{ .Values.observability.tracing.sampleRatio | quote }}
- name: AWS_ACCESS_KEY_ID
  valueFrom:
    secretKeyRef:
      name: {{ .Values.minio.credentialsSecret | quote }}
      key: accessKey
- name: AWS_SECRET_ACCESS_KEY
  valueFrom:
    secretKeyRef:
      name: {{ .Values.minio.credentialsSecret | quote }}
      key: secretKey
- name: MINIO_ACCESS_KEY
  valueFrom:
    secretKeyRef:
      name: {{ .Values.minio.credentialsSecret | quote }}
      key: accessKey
- name: MINIO_SECRET_KEY
  valueFrom:
    secretKeyRef:
      name: {{ .Values.minio.credentialsSecret | quote }}
      key: secretKey
{{- end -}}

{{/*
Pod-level security context block.
*/}}
{{- define "stream2pretrain.podSecurityContext" -}}
{{- with .Values.podSecurityContext }}
securityContext:
  {{- toYaml . | nindent 2 }}
{{- end }}
{{- end -}}

{{/*
Container-level security context block.
*/}}
{{- define "stream2pretrain.containerSecurityContext" -}}
{{- with .Values.containerSecurityContext }}
securityContext:
  {{- toYaml . | nindent 2 }}
{{- end }}
{{- end -}}

{{/*
Default scheduling block (nodeSelector / tolerations / affinity / spread).
*/}}
{{- define "stream2pretrain.scheduling" -}}
{{- with .Values.nodeSelector }}
nodeSelector:
  {{- toYaml . | nindent 2 }}
{{- end }}
{{- with .Values.tolerations }}
tolerations:
  {{- toYaml . | nindent 2 }}
{{- end }}
{{- with .Values.affinity }}
affinity:
  {{- toYaml . | nindent 2 }}
{{- end }}
{{- with .Values.topologySpreadConstraints }}
topologySpreadConstraints:
  {{- toYaml . | nindent 2 }}
{{- end }}
{{- with .Values.priorityClassName }}
priorityClassName: {{ . | quote }}
{{- end }}
{{- end -}}
