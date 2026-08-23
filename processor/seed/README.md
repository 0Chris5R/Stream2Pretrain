# processor/seed - Seed corpus loaders

Stream2Pretrain v0.2.0 ships a one-shot Kubernetes Job (``processor.seed_loader``)
that streams the five-component seed mixture from
``docs/research-seed-corpus.md`` directly into ``docs.normalized`` as Silver
records. This directory holds the per-component loaders the dataflow
composes.

## Components, in priority order

| # | Module                        | HuggingFace id (or source)                          | License      |
|---|-------------------------------|-----------------------------------------------------|--------------|
| 1 | ``pes2o``                     | ``allenai/peS2o`` (data/v3/, falls back to default) | Per-paper only; wrapper is not inherited |
| 2 | ``redpajama_arxiv``           | ``togethercomputer/RedPajama-Data-1T`` (``arxiv``)  | Per-paper only; wrapper is not inherited |
| 3 | ``fineweb_edu_filter``        | ``HuggingFaceFW/fineweb-edu`` + URL allowlist       | Per-page only; wrapper is not inherited |
| 4 | ``stack_edu_filter``          | ``HuggingFaceTB/stack-edu`` + Python+ML filter      | Per-file SPDX only; wrapper is not inherited |
| 5 | ``wayback_backfill``          | Wayback Machine timemap of the Phase-1 RSS/Atom set | Per-item captured evidence |

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
- Wayback: ``<feed_name>:<14-char timestamp>:<item-url-hash>``.

## Honest scope notes

- **HF cache directory** can exceed 500 GB across the full 5-component
  ingest because parquet shard indices are downloaded eagerly even with
  ``streaming=True``. The PVC default in ``charts/stream2pretrain/values.yaml``
  is 50 GB; full backfill assumes 1-2 TB. The demo cluster runs only a
  selected subset via ``--components=...``.
- **Token counts** for peS2o v3 and FineWeb-Edu domain-filter yield are
  ``needs-measurement`` per ``docs/research-seed-corpus.md``. Treat the
  ranges in that doc as upper bounds for capacity planning, not contracts.
- **Wayback** treats feed captures as discovery envelopes only. Item rights
  come from the archived entry or a bounded captured-page probe. After the
  admission event is durably acknowledged, the retained item is fetched and
  extracted with the scientific arXiv or Resiliparse web profile. Feed XML and
  summaries never enter Silver. Archive errors skip only the affected capture.
- **No wrapper-licence inheritance**: per-document licences are surfaced via
  ``SilverRecord.spdx_license``. Permissive rows enter both routes, reviewed
  NC or arXiv non-exclusive rows enter transform-only post-training, and
  unresolved or ND rows become quarantine events in the corpus routes ledger
  before `docs.normalized`, even if the hosting dataset is ODC-By. Curator and
  export checks provide additional defence for legacy rows.

## Running

The canonical entry point is the Kubernetes Job rendered by the Helm template
``charts/stream2pretrain/templates/job-seed-loader.yaml``. For local dev
the bundled script is::

    bash scripts/seed_corpus.sh --components=pes2o,stack-edu --dry-run

In-process execution is the deployment default. It is required when the
Wayback component is selected because the synchronous admission sink waits for
the durable acknowledgement before invoking the deferred retained-body fetch.
The deployed Job uses the ordered in-process runner for all five components.
It acknowledges the admission decision before an admitted Silver row and the
Silver row before cursor advancement. The older split-sink Bytewax graph is
not deployable until it has a transactional cross-topic outbox.
