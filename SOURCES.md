# Stream2Pretrain source catalogue

This is the complete current source catalogue. Discovery mechanisms are
internal schedulers, not corpus sources: they never appear as accepted,
posttrain-only, or quarantined documents.

## Active content sources

| Source | Live input | Corpus artifact | Intended use |
|---|---|---|---|
| arXiv full text | Four RSS categories (`cs.CL`, `cs.LG`, `cs.AI`, `cs.CV`) plus OAI-PMH `set=cs` discover current arXiv ids | Native HTML, ar5iv fallback, or bounded CPU PDF extraction with sections, math, tables, figures, and OCR | Pretraining and paper-based SFT/RL, subject to the per-paper licence tier |
| Hugging Face model cards | `/api/models` sorted by `lastModified` | Public root `README.md` prose versioned by immutable README blob, with the exact repository commit retained as provenance | Pretraining technical documentation |
| Hugging Face dataset cards | `/api/datasets` sorted by `lastModified` | Public root `README.md` prose versioned by immutable README blob, with the exact repository commit retained as provenance | Pretraining technical documentation |

## Internal discovery paths

- `https://rss.arxiv.org/rss/cs.CL`
- `https://rss.arxiv.org/rss/cs.LG`
- `https://rss.arxiv.org/rss/cs.AI`
- `https://rss.arxiv.org/rss/cs.CV`
- `https://oaipmh.arxiv.org/oai`, `set=cs`
- Hugging Face Hub list API responses for model and dataset cards

These envelopes carry ids, revisions, and scheduling state. The full-content
worker makes the licence decision and emits the only corpus document.

## Licence policy

- Permissive item: pretraining and post-training eligible.
- Grey-area or missing item licence: derived post-training only, never verbatim
  pretraining.
- Explicit incompatible or prohibitive item licence: quarantine.

Hugging Face cards are a special exact-projection case. Public immutable-blob
README content is admitted under the versioned Hugging Face public-repository
terms; model and dataset artefact licences do not control this prose-only
projection. This does not admit model weights, dataset rows, Space
repositories, or linked artifacts.

## Operational budgets

- arXiv requests follow the published polite client guidance and are handled
  by one live full-text worker today.
- Hugging Face cards follow every `Link: rel=next` page back to the previous
  completed `lastModified` watermark. Same-timestamp rows use repository id and
  commit SHA as deterministic ties. Page progress and per-repository README
  blob identity are durable; the watermark advances only after a complete
  traversal. Repository commits that leave README bytes unchanged emit no
  corpus item.
- Daily item counts and sustained throughput are `needs-measurement` until the
  controlled current-frontier deployment run completes.

## Official references

- [arXiv RSS](https://info.arxiv.org/help/rss.html)
- [arXiv OAI-PMH](https://info.arxiv.org/help/oa/index.html)
- [Hugging Face repository licences](https://huggingface.co/docs/hub/repositories-licenses)
- [Hugging Face Terms of Service](https://huggingface.co/terms-of-service)
