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
| Kafka progress | A poison record is silently skipped | Transient errors fail and replay. Deterministic record errors must be durably recorded before progress | Multi-sink exactly-once semantics are not claimed |
| Dedup state | Stateful index is lost or forked | One coordinated curator execution, retained Bytewax recovery, and atomic exact, near-duplicate, and decision state in PostgreSQL | Checkpoint and database backup, restore, and replica-loss recovery have not been demonstrated live together |
| Iceberg metadata | Unauthorized or conflicting table commit | Polaris credentials, namespace isolation, deterministic row identities, optimistic conflict retry, and a three-instance CloudNativePG manifest | Database failover and concurrent writers are covered by configuration and tests, not live cluster evidence |
| Foundry coordination | Two workers generate the same candidate, exceed a shared quota, or let a stale owner publish | PostgreSQL candidate and quota reservations use expiring lease tokens, fenced transitions, idempotent calls, and a durable outbox | Live multi-worker failure injection and provider-capacity behavior remain `needs-measurement` |
| Dashboard | Mutation of runtime configuration through the user UI | Normal pages are read-only. Source and pipeline configuration have no browser mutation routes | Cluster administrators can still change Helm/CRDs intentionally |
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
- Treat an RWO-to-RWX checkpoint move as a data migration. Stop the coordinated
  execution, copy and verify the retained state, then change the claim.
- Keep production and smoke topics, object prefixes, recovery state, and
  document ids isolated.
