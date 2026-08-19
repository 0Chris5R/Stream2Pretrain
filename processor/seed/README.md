# processor/seed - Seed corpus loaders

Stream2Pretrain v0.2.0 ships a one-shot Bytewax Job (``processor.seed_loader``)
that streams the five-component seed mixture from
``docs/research-seed-corpus.md`` directly into ``docs.normalized`` as Silver
records. This directory holds the per-component loaders the dataflow
composes.

## Components, in priority order

| # | Module                        | HuggingFace id (or source)                          | License      |
|---|-------------------------------|-----------------------------------------------------|--------------|
| 1 | ``pes2o``                     | ``allenai/peS2o`` (data/v3/, falls back to default) | Per-paper only; wrapper is not inherited |
| 2 | ``redpajama_arxiv``           | ``togethercomputer/RedPajama-Data-1T`` (``arxiv``)  | Apache-2.0   |
| 3 | ``fineweb_edu_filter``        | ``HuggingFaceFW/fineweb-edu`` + URL allowlist       | Per-page only; wrapper is not inherited |
| 4 | ``stack_edu_filter``          | ``HuggingFaceTB/stack-edu`` + Python+ML filter      | Per-file SPDX only; wrapper is not inherited |
| 5 | ``wayback_backfill``          | Wayback Machine timemap of the Phase-1 RSS/Atom set | per-feed     |

All five honor the ``valid_from`` precedence rule from
``processor/operators/validity.py``: dataset-native publication metadata
takes priority over any other signal. We never fall back to ``fetched_at``
for seed records.

## Cursor + idempotency

Each loader reads a ``SeedCursor`` from
``s3://<state_bucket>/seed-loader/<repo_id>.cursor.json`` via
``processor.seed.cursor.CursorStore`` and skips rows whose ``native_id``
sorts <= the cursor's ``last_native_id``. The cursor is flushed every
``CURSOR_FLUSH_INTERVAL`` rows (default 200) and once at end-of-stream so
a kill -9 mid-Job never replays more than that many rows.

The ``native_id`` shape per component:

- peS2o: zero-padded 16-digit ``id`` column (S2ORC paper id).
- RedPajama-arxiv: arXiv id from ``meta.url`` (``2402.01234``).
- FineWeb-Edu: ``id`` column or original URL.
- Stack-Edu: ``blob_id`` (content sha) when present.
- Wayback: ``<feed_name>:<14-char timestamp>``.

## Honest scope notes

- **HF cache directory** can exceed 500 GB across the full 5-component
  ingest because parquet shard indices are downloaded eagerly even with
  ``streaming=True``. The PVC default in ``charts/stream2pretrain/values.yaml``
  is 50 GB; full backfill assumes 1-2 TB. The demo cluster runs only a
  selected subset via ``--components=...``.
- **Token counts** for peS2o v3 and FineWeb-Edu domain-filter yield are
  ``needs-measurement`` per ``docs/research-seed-corpus.md``. Treat the
  ranges in that doc as upper bounds for capacity planning, not contracts.
- **Wayback** is best-effort: the timemap endpoint is not rate-limited
  per IP but archive.org may serve degraded snapshots. The loader catches
  every HTTP exception and continues; it never fails the Job because of a
  single bad capture.
- **No wrapper-licence inheritance**: per-document licences are surfaced via
  ``SilverRecord.spdx_license``. Rows without an explicit content licence are
  written to `license.admissions` and quarantined before `docs.normalized`,
  even if the hosting dataset is ODC-By. Curator and export checks provide
  additional defence for legacy rows.

## Running

The canonical entry point is the Bytewax Job rendered by the Helm template
``charts/stream2pretrain/templates/job-seed-loader.yaml``. For local dev
the bundled script is::

    bash scripts/seed_corpus.sh --components=pes2o,stack-edu --dry-run

In-process execution (no Bytewax runtime) is supported via
``S2P_SEED_INPROCESS=1``; this is what the unit tests exercise.
