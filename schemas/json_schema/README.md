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
