# UI Design Provenance

Status: implemented design review
Reviewed: 2026-08-15

Stream2Pretrain remains one Next.js application. The projects below were
inspected for mature data-curation interaction patterns. Their applications
were not embedded, and no source file or component was copied into this
repository. Patterns were reimplemented with the existing React, Tailwind, and
shadcn primitives, so the project does not inherit six UI frameworks or their
runtime dependencies.

## DataFlow WebUI

- Source: https://github.com/OpenDCAI/DataFlow-WebUI
- License at review: Apache-2.0
- Inspected pattern: a left-to-right data-flow graph with explicit processing
  stages and state.
- Stream2Pretrain adaptation: Sources shows the product path from source to raw
  object, extraction, scoring, routing, and dataset output as a compact topology
  strip. It gives the pipeline a stable visual model without adding a general
  graph editor that this product does not need.

## Data-Juicer

- Source: https://github.com/datajuicer/data-juicer
- License at review: Apache-2.0
- Inspected areas: demos, operator catalogue, filtering configuration, dataset
  processing, and bibliography-removal operators.
- Stream2Pretrain adaptation: Documents exposes retained versus removed
  scientific sections, section-level classifier values, exclusion reasons, and
  the final projection side by side. This carries the useful operator-effect
  model into a scientific-document workflow instead of exposing a raw operator
  configuration editor.

## Hugging Face Dataset Viewer

- Source: https://github.com/huggingface/dataset-viewer
- License at review: Apache-2.0
- Inspected pattern: server-side rows, bounded pages, typed facets, search,
  stable slices, assets, and export-oriented dataset inspection.
- Stream2Pretrain adaptation: Documents uses server-side filtering and
  pagination rather than loading the corpus into the browser. Datasets builds a
  reproducible selection and exports bounded JSONL or Parquet. Scientific
  assets have a dedicated inspector rather than being flattened into table
  strings.

## Argilla

- Source: https://github.com/argilla-io/argilla
- License at review: Apache-2.0
- Inspected pattern: compact record table, metadata filters, status queues,
  selected-record detail, and durable human decisions.
- Stream2Pretrain adaptation: routes act as curation queues, the selected
  document remains visible beside the collection, and decisions and reasons are
  kept in a compact summary. Advanced machine provenance is collapsed by
  default.

## Lilac

- Source: https://github.com/lilacai/lilac
- License at review: the current repository did not expose a license that
  permits source reuse, so no Lilac code was copied.
- Inspected pattern: multi-signal filtering, tags, reversible inspection, and
  dataset export.
- Stream2Pretrain adaptation: content tags and independent score filters remain
  multi-selectable, raw artifacts are preserved even when projection text is
  removed, and export selections are captured in a manifest.

## Renumics Spotlight

- Source: https://github.com/Renumics/spotlight
- License at review: MIT
- Inspected pattern: table plus inspector layouts, multimodal asset views,
  predictions, confidence, filters, and linked selections.
- Stream2Pretrain adaptation: the Assets tab joins images, source captions,
  figure type/confidence, structured tables, and audit-only OCR in one selected
  document view. OCR remains visually distinct from source-authored text.

## Resulting product rules

1. Collection browsing is table-first, paginated, and filterable.
2. One selected object drives the detail inspector.
3. Processing effects are shown as retained/removed content, not explanatory
   prose.
4. Scores are named by meaning and show exact evidence without conflation.
5. Multimodal artifacts stay attached to the source document.
6. Dataset output is an explicit selection plus immutable manifest.
7. Audit details are available but never dominate the main workflow.

Any future direct code adaptation must add the exact upstream file, commit,
license, and local destination to this document before merge.
