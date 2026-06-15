"""Seed-corpus loaders for Stream2Pretrain v0.2.0.

The seed_loader Bytewax Job composes the five components in priority order:

1. :mod:`processor.seed.pes2o`              - allenai/peS2o cs.* slice
2. :mod:`processor.seed.redpajama_arxiv`    - togethercomputer arxiv config
3. :mod:`processor.seed.fineweb_edu_filter` - URL-allowlisted FineWeb-Edu
4. :mod:`processor.seed.stack_edu_filter`   - HuggingFaceTB Python+ML
5. :mod:`processor.seed.wayback_backfill`   - 24-month RSS/Atom Wayback walk

Each component module exports an iterator of :class:`SeedDocument` records
that the dataflow maps onto SilverRecord rows. Cursor bookkeeping lives in
:mod:`processor.seed.cursor`.

All modules are streaming-only: HuggingFace loaders are constructed with
``streaming=True`` and downstream consumers must not list-materialize the
iterators.

Honest scope notes:

- The HF dataset cache directory (``HF_HOME`` / ``HF_DATASETS_CACHE``) can
  exceed 500 GB across the full 5-component ingest because parquet shards
  are downloaded eagerly even in streaming mode (the metadata index is
  materialized; payload is streamed). Use the ``hf_cache_pvc`` PVC and
  scope ``--components`` on the demo cluster.
- The validity-interval source-precedence rule from
  :mod:`processor.operators.validity` applies to seed records too. For
  seed records the precedence is dataset-metadata > nothing; we never fall
  back to ``fetched_at`` because that value is meaningless for replays.
"""

from __future__ import annotations

from processor.seed.cursor import CursorStore, SeedCursor
from processor.seed.types import SeedDocument

__all__ = ["CursorStore", "SeedCursor", "SeedDocument"]
