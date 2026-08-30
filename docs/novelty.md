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

## N3: Shadow mixture delivery

Two `MixtureRecipe` CRDs subscribe to the same stream and materialize separate
Iceberg branches. Future small proxy-LM workers train on rolling windows and use
held-out per-domain loss deltas to recommend promotion. This transplants
progressive-delivery mechanics onto streaming data curation.

Implementation scaffold: `processor/mixture_controller/`,
`schemas/sourcefeed.py`, and `ui/app/mixture/`. Continuous GPU-backed proxy
training is deferred.
