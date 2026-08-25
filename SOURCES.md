# Stream2Pretrain source catalogue

This is the complete current source catalogue. Discovery mechanisms are
internal schedulers, not corpus sources: they never appear as accepted,
posttrain-only, or quarantined documents.

## Active content sources

| Source | Live input | Corpus artifact | Intended use |
|---|---|---|---|
| arXiv full text | Four RSS categories (`cs.CL`, `cs.LG`, `cs.AI`, `cs.CV`) plus OAI-PMH `set=cs` discover current arXiv ids | Native HTML, ar5iv fallback, or bounded CPU PDF extraction with sections, math, tables, figures, and OCR | Pretraining and paper-based SFT/RL, subject to the per-paper licence tier |
| Curated GitHub releases | Release Atom feeds for the 30 repositories in `charts/stream2pretrain/values.yaml` | Per-file code plus README/documentation projections from the exact release tag | Pretraining code and technical prose; no current paper-Foundry route |
| Hugging Face model cards | `/api/models` sorted by `lastModified` | Exact-commit public `README.md` prose only | Pretraining technical documentation |
| Hugging Face dataset cards | `/api/datasets` sorted by `lastModified` | Exact-commit public `README.md` prose only | Pretraining technical documentation |

## Pending live source

OpenReview live ingestion is implemented for ICLR, NeurIPS, ICML, and COLM
2026 papers plus official reviews, meta-reviews, rebuttals, and responses.
OpenReview article PDFs use each Note's licence. A missing paper licence is
posttrain-only; public comments and reviews are covered by OpenReview's
CC-BY-4.0 public-comment terms. Each deployment probes the public listing API;
the CronJob and Sources entry exist only when that probe succeeds.

## Internal discovery paths

- `https://rss.arxiv.org/rss/cs.CL`
- `https://rss.arxiv.org/rss/cs.LG`
- `https://rss.arxiv.org/rss/cs.AI`
- `https://rss.arxiv.org/rss/cs.CV`
- `https://oaipmh.arxiv.org/oai`, `set=cs`
- `https://github.com/<owner>/<repo>/releases.atom` for each curated repository
- Hugging Face Hub list API responses for model and dataset cards

These envelopes carry ids, revisions, and scheduling state. The full-content
worker makes the licence decision and emits the only corpus document.

## Licence policy

- Permissive item: pretraining and post-training eligible.
- Grey-area or missing item licence: derived post-training only, never verbatim
  pretraining.
- Explicit incompatible or prohibitive item licence: quarantine.

Hugging Face cards are a special exact-projection case. Public exact-revision
README content is admitted under the versioned Hugging Face public-repository
terms; model and dataset artefact licences do not control this prose-only
projection. This does not admit model weights, dataset rows, Space
repositories, or linked artifacts.

## Removed sources

HF Daily Papers duplicated arXiv discovery; GitHub Events duplicated curated
release discovery; HF Spaces lacked a clear corpus role. All three were
removed. The OpenAI, DeepMind, Hugging Face, BAIR, and EleutherAI blogs were
removed after their public pages failed to provide a defensible site-wide
training-content grant. Historical seed mixtures, ReviewArena, Wayback, and
all other backfill-only workloads were also removed.

## Operational budgets

- arXiv requests follow the published polite client guidance and are handled
  by one live full-text worker today.
- Hugging Face cards poll in bounded `lastModified` pages and retain exact
  commit SHAs.
- GitHub Atom discovery is conditional with ETags. The tarball worker uses the
  authenticated REST budget and bounded per-release file selection.
- Daily item counts and sustained throughput are `needs-measurement` until the
  controlled current-frontier deployment run completes.

## Official references

- [arXiv RSS](https://info.arxiv.org/help/rss.html)
- [arXiv OAI-PMH](https://info.arxiv.org/help/oa/index.html)
- [Hugging Face repository licences](https://huggingface.co/docs/hub/repositories-licenses)
- [Hugging Face Terms of Service](https://huggingface.co/terms-of-service)
- [GitHub REST rate limits](https://docs.github.com/en/rest/using-the-rest-api/rate-limits-for-the-rest-api)
- [OpenReview Terms](https://openreview.net/legal/terms)
- [OpenReview API v2](https://docs.openreview.net/reference/api-v2)
