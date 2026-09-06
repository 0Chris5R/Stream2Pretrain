# Novelty claims

## N1: Validity-aware streaming corpus

Every document carries `[valid_from, valid_to)` from source evidence through
Gold and export. DuckDB exposes deterministic point-in-time selection. This
makes freshness and later invalidation first-class corpus properties instead of
properties reconstructed from filenames after a batch run.

Implementation: `schemas/gold.py`, `processor/iceberg_writer.py`,
`processor/duckdb_api.py`, and the Datasets page.

## N2: Scientific-paper SFT and RL Foundry

The same immutable scientific artifact can become inspectable SFT trajectories
or a packaged verifier environment. Generation retains exact paper-element
provenance; acceptance requires deterministic validation and named human audit.

Implementation: `processor/foundry/`, `schemas/foundry.py`, and
`ui/app/post-training/`.
