# Source licence admission matrix

Status: binding source contract, revised 2026-08-25.

Licence admission has exactly three outcomes for every content item:

1. A permissive grant allows `pretrain_and_posttrain`.
2. A reviewed grey-area grant or no stated licence allows
   `posttrain_transform_only`. The source may ground derived SFT or RL data,
   but its text cannot appear in a verbatim pretraining export.
3. An explicit incompatible, no-derivatives, contradictory, or otherwise
   prohibitive grant is `quarantined`.

Discovery envelopes are not content items. They create no licence admission,
curation decision, document row, accepted count, or quarantine count. They are
internal scheduling messages only.

## Active source paths

| Source | Content used | Evidence and decision | Training use |
|---|---|---|---|
| arXiv RSS `cs.CL` | No content. The arXiv id schedules the canonical paper fetch. | No discovery licence decision. | None directly. |
| arXiv RSS `cs.LG` | No content. The arXiv id schedules the canonical paper fetch. | No discovery licence decision. | None directly. |
| arXiv RSS `cs.AI` | No content. The arXiv id schedules the canonical paper fetch. | No discovery licence decision. | None directly. |
| arXiv RSS `cs.CV` | No content. The arXiv id schedules the canonical paper fetch. | No discovery licence decision. | None directly. |
| arXiv OAI-PMH `set=cs` | No content. Current records supply a no-gap discovery path for arXiv ids. | No discovery licence decision. The cursor starts at the current day, not at a historical date. | None directly. |
| `arxiv-html-fetcher` | Full paper sections plus table, equation, figure-caption, and OCR projections. References and author metadata are excluded from training text. | Resolve the exact paper licence from arXiv before HTML, ar5iv, or PDF body retrieval. CC BY/SA/CC0 is permissive. arXiv non-exclusive, NC, or missing rights are posttrain-only. ND or conflicting rights quarantine. | Permissive papers may enter pretraining and the paper Foundry. Grey or missing rights may only enter the paper Foundry. |
| `hf-models` | Root `README.md` model-card prose only, identified by immutable README blob and fetched at an exact repository commit. Weights, code, and linked datasets are never fetched by this path. | The versioned public README is admitted under the Hugging Face public-repository terms. A model-weight licence in Hub metadata does not control this prose-only projection. A missing immutable commit or private repository is only unresolved discovery metadata and creates no document decision. | Pretraining prose only. Model cards are not SFT/RL candidates. |
| `hf-datasets` | Root `README.md` dataset-card prose only, identified by immutable README blob and fetched at an exact repository commit. Dataset rows and binaries are never fetched by this path. | The versioned public README is admitted under the Hugging Face public-repository terms. Dataset and wrapper licences do not control this prose-only projection. | Pretraining prose only. Dataset cards are not SFT/RL candidates. |

## Audit fields

Every content decision retains the normalized licence, raw licence string,
resolver, evidence URL, immutable revision where available, evidence scope,
policy revision, timestamp, source URL, source name, document id, and selected
training usage. The product exposes these fields inside the document's advanced
audit view. It does not create a separate licence product ledger.

This is a conservative engineering policy and provenance record, not legal
advice.
