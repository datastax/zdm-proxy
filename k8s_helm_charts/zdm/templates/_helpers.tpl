{{/*
Expand the name of the chart.
*/}}
{{- define "zdm.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "zdm.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "zdm.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "zdm.labels" -}}
{{ include "zdm.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "zdm.selectorLabels" -}}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "zdm.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "zdm.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Create name of the secret from which container environment variables will be populated
*/}}
{{- define "zdm.secretName" -}}
{{- .Values.secretNameOverride | default "zdmproxy" }}
{{- end }}

{{/*
Create name of the secret from which containers will be configured with SCB values
*/}}
{{- define "zdm.secretScbName" -}}
{{- .Values.secretScbNameOverride | default "zdmproxy-scb" }}
{{- end }}

{{/*
Determine whether CDM should be created
*/}}
{{- define "cdm.enabled" -}}
{{- .Values.cdm.enabled | default "true" | toString }}
{{- end }}

{{/*
Determine whether SCB volume & mounts should be created from expected secret
*/}}
{{- define "scb.enabled" -}}
{{- .Values.scb.enabled | default "true" | toString }}
{{- end }}