# Auto-derived JSON Schemas

These files are generated from the Pydantic v2 models in `schemas/`. They
exist so the Next.js UI (TanStack Query hooks) and the OPA Gatekeeper
constraint templates can validate payloads without taking a Python
dependency.

## Regenerate

```
uv run python -m schemas.json_schema.generate
```

The generator writes one JSON Schema per model into this directory. Files are
checked in so reviewers can see schema diffs in PRs without running the
generator.

## Files

| File | Source model |
|---|---|
| `bronze_record.schema.json` | `schemas.bronze.BronzeRecord` |
| `silver_record.schema.json` | `schemas.silver.SilverRecord` |
| `gold_record.schema.json` | `schemas.gold.GoldRecord` |
| `decon_attestation.schema.json` | `schemas.decon.DeconAttestation` |
| `source_feed_spec.schema.json` | `schemas.sourcefeed.SourceFeedSpec` |
| `mixture_recipe_spec.schema.json` | `schemas.sourcefeed.MixtureRecipeSpec` |
| `license_admission_decision.schema.json` | `schemas.license_admission.LicenseAdmissionDecision` |
| `paper_bundle.schema.json` | `schemas.foundry.PaperBundle` |
| `paper_evidence_graph.schema.json` | `schemas.foundry.PaperEvidenceGraph` |
| `task_spec.schema.json` | `schemas.foundry.TaskSpec` |
| `verifier_spec.schema.json` | `schemas.foundry.VerifierSpec` |
| `provider_trace.schema.json` | `schemas.foundry.ProviderTrace` |
| `artifact_audit_record.schema.json` | `schemas.foundry.ArtifactAuditRecord` |
| `foundry_event.schema.json` | `schemas.foundry.FoundryEvent` |
| `foundry_artifact_record.schema.json` | `schemas.foundry.FoundryArtifactRecord` |
