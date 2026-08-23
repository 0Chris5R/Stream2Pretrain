# Stream2Pretrain - Threat Model (STRIDE)

This is the threat model for the curator and its data plane. Format follows
STRIDE: each row maps a class of threat to the affected component, the
existing mitigation, and any residual risk that needs follow-up.

The model assumes a deployment on a small k3s cluster on DHBWCloud with
TLS-terminated ingress and the OPA Gatekeeper policy bundle active. It does
**not** treat Stream2Pretrain as a legal-compliance product (license
detection is heuristic; see the README caveat).

## Trust boundaries

```
Internet                      kube-apiserver           cluster-internal
  ----[Traefik+cert-mgr]--------->  [UI / source pollers]  --> [Redpanda]
                                      |                          |
                                      v                          v
                                  [MinIO]                [Bytewax curator]
                                                              |
                                                              v
                                                       [Iceberg + Polaris]
```

Six trust boundaries:
1. Internet -> ingress
2. Ingress -> UI
3. Pod -> Redpanda (Kafka API)
4. Pod -> MinIO (S3 API)
5. Pod -> Polaris (REST)
6. Cluster -> kube-apiserver (CRD admission)

## STRIDE matrix

### Spoofing identity

| Asset | Threat | Mitigation | Residual |
|---|---|---|---|
| Decon-Gate signing key | Attacker convinces a verifier that a forged attestation is genuine | Ed25519 signature + x509 cert chain + canonical JSON; verifier replays the rule | Single in-cluster key in prototype. Rotation runbook in `operations.md`. Prod path is Sigstore Rekor (`needs-measurement` to integrate). |
| Iceberg snapshot author | Attacker writes a snapshot impersonating the curator | Polaris RBAC restricts `gold` writes to the curator service account | If the SA token leaks, attacker can write. Use short-lived tokens (`needs-measurement`). |
| SourceFeed CRD | Attacker creates a SourceFeed pointing at an internal URL | OPA Gatekeeper constraint allowlists protocols + denies RFC1918 hosts | Constraint coverage is `needs-measurement`. |

### Tampering with data

| Asset | Threat | Mitigation | Residual |
|---|---|---|---|
| Bronze HTML in MinIO | Attacker rewrites stored bytes to bypass Decon-Gate | sha256 `doc_id` is content-addressed; MinIO bucket policy forbids overwrites except by curator; periodic integrity scan | Integrity scan cadence is `needs-measurement`. |
| Iceberg metadata | Attacker tampers with snapshot manifests | Iceberg V2 metadata + Polaris commit auth | Polaris commit log is not signed; Decon attestations bind the decision batch to the accepted Gold snapshot id. |
| Decon attestation in transit | Attacker swaps the attestation between writer and topic | Signature is over canonical JSON of the body, computed pre-publish | None (signature-bound). |
| Mixture recipe | Attacker patches a `MixtureRecipe` to elevate a contaminated source | Gatekeeper constraint on `weight` sums + RBAC on the CRD | The constraint cannot detect semantic intent; mixture controller logs every patch. |

### Repudiation

| Asset | Threat | Mitigation | Residual |
|---|---|---|---|
| Curator output | Operator denies that a contaminated row was ever written | Iceberg snapshots + Decon-Gate attestations are append-only and signed | Attestations only cover the gold table; silver-only contamination is not signed (`needs-measurement` if we add silver attestations). |
| Source poll | A poller denies fetching a URL | Bronze object on MinIO + bronze record on Redpanda + OTel trace | Trace retention is short (default Tempo retention). Loki retains the structured log line for the configured period. |

### Information disclosure

| Asset | Threat | Mitigation | Residual |
|---|---|---|---|
| Fetched URL bodies (PII) | A polled page contains private data that lands in MinIO bronze | PII regex on the silver path strips emails, phones, SSNs, credit cards, IPs, passport numbers; gold rows omit text segments containing PII flags | PII regex is heuristic; a determined classifier would do better (`needs-measurement`). Bronze still contains the raw payload for 30 days. |
| Bearer tokens | HF / GitHub PAT exposure via logs or env | Tokens in K8s Secrets, never logged (the structured logger filters known token-shaped fields) | If a developer adds a `print(token)` it bypasses the filter. Add a CI secret-scan (`needs-measurement`). |
| Polaris RBAC tokens | Token leak grants table read | Short-lived tokens, namespace-scoped roles | Polaris token TTLs are `needs-measurement`. |
| Inter-pod traffic | An attacker on one Pod reads another's traffic | NetworkPolicy default-deny + per-component allowlists | mTLS within the cluster is not enabled by default (`needs-measurement`; would require a service mesh). |

### Denial of service

| Asset | Threat | Mitigation | Residual |
|---|---|---|---|
| Curator | Adversarial document that triggers deterministic parsing or validation failure | The record is written idempotently to `s2p-gold/processing-failures/` before Bytewax may advance; transient or unknown failures stop and replay from recovery | Pathological but valid model inputs can still raise tail latency; core executions remain fixed at one coordinated replica. |
| Redpanda | Topic flood | Pollers are bounded by their schedules and cursors; KEDA is used only where a dedicated input backlog is a valid signal | Single-broker dev mode has no replica failover. |
| MinIO | Storage exhaustion | Bucket retention + monthly rotation job | Retention defaults are conservative; `needs-measurement` after Week 5 benchmark. |

### Elevation of privilege

| Asset | Threat | Mitigation | Residual |
|---|---|---|---|
| Pod | Compromised pod escalates to node | Pod Security Standards `restricted` profile, no `hostPath` mounts, read-only root FS where possible | A few containers (Bytewax with RocksDB) need writable scratch; documented in chart values. |
| ServiceAccount | SA token used outside its intended scope | Audience-restricted SA tokens; Polaris and MinIO accept only the curator's audience | Audience configuration is `needs-measurement`. |
| Gatekeeper | Bypass of admission constraints by a cluster-admin | Cluster-admin is a trusted role; audited via Loki | Out of scope: a malicious cluster-admin is not in the threat model. |

## Out-of-scope explicit assumptions

- The k3s control plane and its etcd are trusted.
- The container registry serving the chart's images is trusted.
- The DNS zone used for ingress is under the team's control (rfc2136 +
  tsig). If not, cert-manager cannot issue.
- Third-party APIs (arXiv, GitHub, HF) honour their published rate limits.

## Periodic review

This file is reviewed at every release cut (see `CHANGELOG.md`) and
refreshed when:
- A new component is added to the data plane.
- The signing path changes (Sigstore Rekor migration).
- A real CVE in a dependency triggers a re-evaluation.
