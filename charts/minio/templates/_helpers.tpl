{{- define "stream2pretrain-minio.name" -}}
{{- default .Chart.Name .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "stream2pretrain-minio.labels" -}}
app.kubernetes.io/name: {{ include "stream2pretrain-minio.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: stream2pretrain
{{- end -}}
