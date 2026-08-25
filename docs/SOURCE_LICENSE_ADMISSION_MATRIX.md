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
| GitHub Releases Atom, one feed for each curated repository | No content. An exact repository and release ref schedules the tarball worker. | No discovery licence decision. | None directly. |
| `github-release-tarballs` source files | Retained UTF-8, non-generated, non-vendored source files. | Resolve the repository licence at the exact tag and preserve its blob SHA. A file SPDX header may supply more specific evidence. Permissive software licences allow both routes. Missing rights are posttrain-only. Explicit incompatible rights quarantine. | Permissive files may enter pretraining. The current post-training Foundry is paper-specific, so code is not submitted to it yet. |
| `github-release-tarballs` README and documentation files | Narrative Markdown, reStructuredText, and text after front matter and fenced-code removal. | Same exact-ref and per-file evidence as source files. | Permissive prose may enter pretraining. It is not a paper Foundry candidate. |
| `hf-models` | Exact-commit `README.md` model-card prose only. Weights, code, and linked datasets are never fetched by this path. | The versioned public README is admitted under the Hugging Face public-repository terms. A model-weight licence in Hub metadata does not control this prose-only projection. A missing immutable commit or private repository is only unresolved discovery metadata and creates no document decision. | Pretraining prose only. Model cards are not SFT/RL candidates. |
| `hf-datasets` | Exact-commit `README.md` dataset-card prose only. Dataset rows and binaries are never fetched by this path. | The versioned public README is admitted under the Hugging Face public-repository terms. Dataset and wrapper licences do not control this prose-only projection. | Pretraining prose only. Dataset cards are not SFT/RL candidates. |
| OpenReview live paper | Submission PDF and structured scientific projection. | Use the submission Note's explicit `license` or `license_url`. If absent, the paper is posttrain-only. Explicit incompatible rights quarantine. | Permissive papers may enter pretraining and the paper Foundry; missing or grey rights may only enter the Foundry. |
| OpenReview official review | Substantive public review, meta-review, rebuttal, or author-response fields. Generic comments are not training items. | Explicit Note rights win. Otherwise OpenReview's versioned public-comment terms provide CC-BY-4.0 evidence for public comments and reviews. | Permissive review prose may enter pretraining. The paper Foundry does not consume reviews. |

OpenReview is licence-suitable. Each deployment performs a live public listing
probe and creates the current-frontier workload only when it succeeds; a failed
probe leaves no workload or active Sources entry.

## Removed sources

GitHub Events and Hugging Face Daily Papers duplicated the release and arXiv
discovery paths and were removed. Hugging Face Spaces had no clear corpus role
and was removed. The OpenAI, DeepMind, Hugging Face, BAIR, and EleutherAI blog
feeds exposed no audited site-wide grant suitable for verbatim corpus reuse;
they were removed instead of producing a permanent wall of missing-licence
rows. All historical seed and backfill-only workloads were removed.

## Audit fields

Every content decision retains the normalized licence, raw licence string,
resolver, evidence URL, immutable revision where available, evidence scope,
policy revision, timestamp, source URL, source name, document id, and selected
training usage. The product exposes these fields inside the document's advanced
audit view. It does not create a separate licence product ledger.

This is a conservative engineering policy and provenance record, not legal
advice.
