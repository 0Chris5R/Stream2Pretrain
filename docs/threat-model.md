# Threat model

## Trust boundaries

- External sources are untrusted input.
- Redpanda records are authenticated cluster traffic but their payloads still
  require schema validation.
- MinIO content is immutable by object key and verified by recorded hashes.
- Polaris controls Iceberg namespace and commit authority.
- The Next.js backend is the only browser-facing service and never exposes
  provider or storage credentials.
- Foundry model output is untrusted until schema, scientific, verifier,
  package, replay, and security validation pass.

## Main threats and controls

| Asset or boundary | Threat | Current control | Residual limitation |
|---|---|---|---|
| Source licence | A hosting-platform or wrapper licence is mistaken for item rights | Item-level evidence is resolved and durably routed before retained body processing | The policy is conservative provenance, not legal advice |
| Source body | Malformed HTML, PDF, Markdown, or oversized content exhausts a worker | Bounded downloads, schema limits, resource requests/limits, deterministic failure ledger | Adversarial parser coverage is incomplete |
| Bronze object | Stored bytes are swapped | Content hash, immutable object key, least-privilege MinIO policy | Integrity-scan cadence is `needs-measurement` |
| Kafka progress | A poison record is silently skipped | Transient errors fail and replay; deterministic record errors must be durably recorded before progress | Multi-sink exactly-once semantics are not claimed |
| Dedup state | Stateful index is lost or forked | Retained Bytewax recovery and curator PVC, single coordinated curator execution | Backup and restore must preserve checkpoint and index together |
| Iceberg metadata | Unauthorized table commit | Polaris credentials and namespace isolation | Dev Polaris is not a production HA catalog |
| Dashboard | Mutation of runtime configuration through the user UI | Normal pages are read-only; source and pipeline configuration have no browser mutation routes | Cluster administrators can still change Helm/CRDs intentionally |
| Provider prompt | Paper content injects instructions into Foundry calls | Fixed role prompts, typed output schemas, bounded tool loops, independent critics, deterministic validation | Model judges remain probabilistic |
| RL package | Generated code accesses network, credentials, files, or processes | Static security gate, signed package, isolated execution contract | Full sandbox proof remains a live deployment gate |
| Human audit | Reviewer identity is forged or decision is overwritten | Reviewer entered per artifact and audits are append-only | Identity is asserted, not federated in the student deployment |

## Privacy handling

Presidio and explicit regular expressions inspect retained segments. Segments
with redactable contact metadata can be removed while preserving the rest of a
document. Remaining high-confidence secrets or identity-bearing values
quarantine the document. Raw content follows bounded retention, while route
decisions retain only the evidence required for audit.

## Operational principles

- Never log credential values or raw provider authorization headers.
- Do not expose MinIO, Redpanda, Polaris, or Foundry control endpoints directly
  to the public Internet.
- Treat recovery PVC deletion, cursor resets, and replay as destructive
  operations requiring an explicit snapshot and approval.
- Keep production and smoke topics, object prefixes, recovery state, and
  document ids isolated.
