# Source licence admission matrix

Status: implementation and cloud-validation companion to
`docs/PIPELINE_REMEDIATION_CONTRACT.md`.

This document inventories every active or preload-capable content source in
`SOURCES.md`, Helm values/templates, `scripts/load_seed_feeds.sh`, and
`ingest/`. It is deliberately item-scoped. A hosting platform, SourceFeed
default, repository topic, venue, domain allowlist, or dataset wrapper does
not grant rights in every contained paper, page, review, or source file.

## Shared outcomes

Every content item is assigned exactly one outcome by
`ingest/common/license_admission.py`:

| Outcome | Meaning |
|---|---|
| `pretrain_and_posttrain` | The exact item has an allowlisted permissive licence. Its retained projection may enter pretraining and it may later ground post-training artifacts. |
| `posttrain_transform_only` | The exact item has a reviewed grey-area licence. Its body may ground derived SFT/RL artifacts but may never enter a verbatim pretraining export. |
| `quarantined` | Rights are missing, contradictory, wrapper-only, no-derivatives, or otherwise outside policy. No body is stored, extracted, OCR-processed, classified, curated, or exported. |

Current permissive identifiers are CC BY, CC BY-SA, CC0, Apache-2.0, MIT,
BSD-2-Clause, BSD-3-Clause, MPL-2.0, ISC, and Unlicense at the versions listed
in the shared policy. Current transform-only identifiers are arXiv's
non-exclusive distribution licence and the listed CC BY-NC / CC BY-NC-SA
versions. CC ND variants, unknown values, `NOASSERTION`, and ODC-By dataset
wrappers quarantine.

Every immutable admission row records the raw and normalized licence,
resolver, evidence URL, evidence revision, evidence scope, policy revision,
resolution time, source URL, and source feed. Dataset export separately
requires `training_usage = pretrain_and_posttrain`.

## Live and backfill source matrix

In the Gold column, "eligible" means a licence-admitted body has a complete
code path through Bronze, `processor/fetcher.py`, Silver,
`processor/curate.py`, the durable curation-decisions table, and the accepted
Gold subset. Quality, safety, deduplication, language, and contamination can
still quarantine a licence-admitted body. Discovery records intentionally have
no Gold path.

| Configured source | Role | Resolver and immutable evidence | Outcomes and fail-closed boundary | Gold path | Implementation |
|---|---|---|---|---|---|
| `rss-arxiv-cs-cl`, `rss-arxiv-cs-lg`, `rss-arxiv-cs-ai`, `rss-arxiv-cs-cv` | Discovery | RSS/DC item rights or `arxiv:{html_meta,arxiv_api}`. Evidence is feed ETag/Last-Modified plus entry URL, or canonical abs URL plus arXiv id | Shared three outcomes. The linked abstract is stored only as `metadata`; no extraction or curation. Full text performs a new canonical item resolution before any HTML/PDF request | Discovery: none. Its canonical id schedules `arxiv-html-fetcher` | `ingest/rss_poller/poller.py`, `ingest/common/arxiv_license.py` |
| `oai-arxiv-cs` | Discovery | OAI record rights with OAI identifier/datestamp, otherwise canonical arXiv resolver with abs URL/arXiv id | Shared outcomes. Non-arXiv records with missing rights quarantine and never inherit a feed default. OAI XML is `metadata` and skipped before MinIO read in the processor | Discovery: none. Its canonical id schedules `arxiv-html-fetcher` | `ingest/oaipmh_poller/client.py`, `ingest/oaipmh_poller/poller.py` |
| `arxiv-html-fetcher` and optional ids-file backfill | Content: native HTML, ar5iv HTML, bounded CPU PDF fallback | Canonical arXiv abs-page/Atom resolution for every full-body doc id. Evidence is abs URL and versioned arXiv id. Any fetched HTML licence must agree | CC BY/SA/CC0 -> both; arXiv non-exclusive or CC BY-NC/SA -> posttrain only; unresolved, ND, or conflict -> quarantine before HTML/PDF fetch | Eligible. Exact licence id/source and `training_usage` reach Bronze, Silver, curation decisions, and Gold | `ingest/arxiv_html_fetcher/fetcher.py` |
| `hf-daily-papers` | Discovery and ranking | Per-paper API field when present, otherwise canonical arXiv resolution. Evidence is HF paper or arXiv abs URL plus arXiv id | Same outcomes as arXiv. Emitted JSON is `metadata`, so no classifier treats it as paper prose | Discovery: none. It schedules the canonical arXiv full-text body | `ingest/hf_poller/poller.py` |
| `github-events` | Discovery | No event/platform licence is inferred. Only a ReleaseEvent with exact repository and tag becomes a release job | No licence route at discovery. Event JSON is `metadata` and never reaches processors as corpus text | Discovery: none | `ingest/github_events/poller.py` |
| `github-releases` over the 30 Helm-pinned repositories | Discovery | No Atom/feed licence is inferred. Repository and exact tag are carried to the release job | No licence route at discovery. Atom XML is `metadata` | Discovery: none | `ingest/github_releases/poller.py` |
| `github-release-tarballs` | Content: release source files | `GET /repos/{owner}/{repo}/license?ref={tag}` with exact top-level licence blob SHA; a file SPDX header instead uses SHA-256 of the exact extracted file bytes | Permissive software SPDX -> both; unresolved/non-allowlisted or missing immutable SHA -> quarantine. Tarball fetch waits for repository-ref admission, then each retained file receives its own decision before storage/emission | Eligible per file. `source_format=code` selects code extraction and quality policy before Gold | `ingest/github_release_tarball_fetcher/fetcher.py` |
| `hf-models` | Content: versioned model card | Model-card licence metadata and exact API commit SHA; evidence URL is `blob/{sha}/README.md` | Shared outcomes. Missing licence or SHA quarantines before the README request. Model binaries are never fetched | Eligible card prose only. Referenced model artifacts get no inherited rights | `ingest/hf_poller/poller.py` |
| `hf-datasets` | Content: versioned dataset card | Dataset-card licence metadata and exact API commit SHA; evidence URL is `datasets/.../blob/{sha}/README.md` | Shared outcomes. ODC-By is treated as wrapper-only and quarantines. Missing licence/SHA blocks README fetch. The card never licenses referenced dataset rows | Eligible card prose only | `ingest/hf_poller/poller.py` |
| `hf-spaces` | Content: versioned Space card | Space-card licence metadata and exact API commit SHA; evidence URL is `spaces/.../blob/{sha}/README.md` | Shared outcomes. Missing/custom licence or SHA blocks README fetch. Repository files are not fetched through this path | Eligible card prose only | `ingest/hf_poller/poller.py` |
| `rss-openai-news` | Content: linked page | RSS item rights, RFC 8288 `Link: rel=license`, or bounded page-head licence metadata with item URL and ETag/Last-Modified | Shared outcomes; unresolved currently quarantines before the separate full GET | Eligible only when the page exposes item-scoped rights | `ingest/rss_poller/poller.py`, `ingest/common/page_license.py` |
| `rss-deepmind-blog` | Content: linked page | Same item resolver as OpenAI News | Same fail-closed boundary; no domain or channel default | Eligible only with item evidence | Same |
| `rss-hf-blog` | Content: linked page | Same item resolver as OpenAI News | Same fail-closed boundary; HF platform ownership is not item rights | Eligible only with item evidence | Same |
| `rss-bair-blog` | Content: linked page | Same item resolver as OpenAI News | Same fail-closed boundary | Eligible only with item evidence | Same |
| `rss-eleuther-blog` | Content: linked page | Same item resolver as OpenAI News | Same fail-closed boundary | Eligible only with item evidence | Same |
| Configurable sitemap adapter, currently disabled with an empty Helm feed list | Discovery plus linked page content | Each item uses HTTP Link/bounded page-head evidence. Sitemap `lastmod` is revision context, not licence evidence | Shared outcomes. Full body waits for decision; no sitemap/domain default is used | Eligible for each separately admitted linked page | `ingest/sitemap_poller/poller.py`, `ingest/common/page_license.py` |
| OpenReview live: `ICLR.cc/2026/Conference`, `NeurIPS.cc/2025/Conference`, `ICML.cc/2025/Conference`, `COLM/2025/Conference`; live workload currently disabled | Content: paper PDF | Explicit `license`/`license_url` on the submission note, with note id and note revision | Shared outcomes. Missing article rights block the PDF request. Venue/platform identity is never a paper licence | Eligible paper body when live is enabled and it passes downstream policy | `ingest/openreview_poller/live.py` |
| Same OpenReview venues: public reviews, comments, rebuttals, decisions | Content: review prose | Explicit note licence; otherwise versioned OpenReview public Comment/Configuration terms URL. This source-wide evidence applies only to public comments, not papers | CC-BY-4.0 terms -> both; explicit restrictive note follows shared policy. Listing API necessarily transfers note prose with metadata, but no Bronze/model processing precedes admission | Eligible review body | `ingest/openreview_poller/live.py` |
| ReviewArena pinned backfill, splits `neurips`, `iclr`, `icml`, `tmlr`, `emnlp`, `corl`, `colm`; Job currently disabled | Content in pinned rows | Paper-level row field for paper Markdown. Review-level row field or OpenReview public-comment terms for review prose. Dataset revision and native row id are retained; wrapper ignored | Shared outcomes. HF streaming transfers a row body with its metadata, but no Bronze storage/model processing occurs before admission | Eligible paper/review artifacts independently | `ingest/openreview_poller/backfill.py` |

OpenReview terms are intentionally split by artifact. The official terms say
authors retain article copyright and an article's explicit `license` field
establishes its public licence, while public Comments and Configuration
Records use CC-BY-4.0. The platform distribution grant is not inherited by a
paper. See <https://openreview.net/legal/terms>.

GitHub's official licence endpoint accepts a `ref` parameter. The release
worker must therefore query the exact tag rather than the repository default
branch. See <https://docs.github.com/en/rest/licenses/licenses>.

The shared GitHub release resolver covers every repository currently pinned in
Helm: `huggingface/transformers`, `vllm-project/vllm`, `pytorch/pytorch`,
`ggml-org/llama.cpp`, `karpathy/llm.c`, `unslothai/unsloth`,
`meta-llama/llama`, `openai/whisper`, `anthropics/courses`,
`apple/ml-tic-lm`, `mlfoundations/dclm`, `huggingface/datatrove`,
`NVIDIA-NeMo/Curator`, `allenai/dolma`, `bytewax/bytewax`,
`redpanda-data/redpanda`, `apache/iceberg`,
`MaterializeInc/materialize`, `risingwavelabs/risingwave`,
`pathwaycom/pathway`, `unclecode/crawl4ai`, `firecrawl/firecrawl`,
`microsoft/onnxruntime`, `ogx-ai/ogx`, `triton-lang/triton`,
`google-deepmind/gemma`, `mistralai/mistral-inference`, `tinygrad/tinygrad`,
`huggingface/pytorch-image-models`, and `pytorch/torchtitan`. GitHub Events
also filters the ten configured organizations, but that filter is relevance
only and cannot establish a licence.

Hugging Face declares repository licences in card metadata. Model, dataset,
and Space cards are stored only at the exact revision observed by the API.
These declarations do not license externally referenced content. See
<https://huggingface.co/docs/hub/model-cards> and
<https://huggingface.co/docs/hub/datasets-cards>.

## Preloaded seed matrix

The one-shot seed Job is disabled by default. Dataset rows already contain the
body when `datasets` streaming yields them, so these sources can guarantee
admission before `docs.normalized`, downstream models, and storage, but cannot
claim that no body byte crossed the network before row metadata was inspected.
This distinction must remain visible in deployment evidence.

| Seed component | Resolver and immutable evidence | Route behavior and body boundary | Gold path |
|---|---|---|---|
| `pes2o`: `allenai/peS2o` v3, cs.* | `pes2o-paper-item-field`; paper field and native S2 id at pinned dataset revision `636a503e44a3ca1b58e01fb61eab0825cd574de0` | Shared outcomes. ODC-By wrapper ignored. Row body necessarily arrives with metadata, then admission precedes Silver and every classifier | Eligible after direct Silver emission and normal curator; missing paper rights have no Silver/Gold row |
| `redpajama-arxiv`: `togethercomputer/RedPajama-Data-1T`, config `arxiv` | `redpajama-arxiv-paper-item-field`; paper field and arXiv/native row id at pinned revision `398f92572e94f4793e41c22ab7ea2a788d9e7de4` | Shared outcomes. Dataset wrapper ignored. Hash-only row without paper rights quarantines after row transfer but before Silver | Eligible through the same Silver/curator/Gold path |
| `fineweb-edu`: `HuggingFaceFW/fineweb-edu` | `fineweb-page-item-field`; per-page row rights, row id/crawl revision, and pinned dataset revision `87f09149ef4734204d70ed1d046ddc9ca3f2b8f9` | Shared outcomes. ODC-By and domain allowlist do not grant page rights. Current live rights are not applied retroactively. Row transfer precedes the decision, but Silver/models do not | Eligible through the same path |
| `stack-edu`: `HuggingFaceTB/stack-edu`, `Python` config plus ML relevance | `stack-edu-file-item-field`; unambiguous `detected_licenses`, exact SWH blob id, and pinned dataset revision `eeec5caac5cc3758a18f1d3ba4416837a9ba814c` | Metadata row arrives first. Missing or multiple ambiguous licences quarantine. After durable admission, the exact body is fetched lazily from `s3://softwareheritage/content/{blob_id}`. The dataset wrapper and `license_type` category do not qualify | Eligible as `source_format=code` through code-specific curation |
| `wayback`: nine archived Phase-1 feed snapshots | Archived item rights, an archived RFC 8288 licence link, or bounded archived item/arXiv abs-page metadata. Evidence is the immutable Wayback capture URL and 14-digit capture timestamp | Archived feed XML and entry summaries are discovery only and never become Silver text. The discovery envelope necessarily transfers before item resolution. If item rights are absent, at most 65,536 bytes of the archived item evidence page are probed and discarded. The in-process runner durably publishes admission before the separate retained page/arXiv HTML fetch; unknown and ND items never perform it | Eligible. Admitted retained pages are extracted with Resiliparse; admitted arXiv captures use the math-preserving scientific extractor. Both enter the normal Silver/curator/Gold path |

The seed cursor advances after each admission outcome so repeated missing
licences do not loop forever. Admitted and posttrain-only rows retain exact
`training_usage`; quarantined rows go only to `license.admissions`.

Seed revision pins were read from the official Hub repository state on
2026-08-23. The Stack-Edu card also documents that its rows contain SWH blob
ids rather than code bodies and identifies `s3://softwareheritage/content/` as
the content channel:

- <https://huggingface.co/api/datasets/allenai/peS2o>
- <https://huggingface.co/datasets/togethercomputer/RedPajama-Data-1T/tree/main>
- <https://huggingface.co/api/datasets/HuggingFaceFW/fineweb-edu>
- <https://huggingface.co/api/datasets/HuggingFaceTB/stack-edu>

## Catalogue-only sources

The remaining Phase-2 entries in `SOURCES.md` are not active workloads:
remaining arXiv categories, Semantic Scholar, Papers With Code, GitHub
READMEs, long-tail blogs, and Alignment Forum. HF Dataset and Space card prose
is now active and appears in the live matrix above. Catalogue-only entries
must not appear healthy merely because documentation lists them. Before
activation each needs a concrete adapter and the same evidence contract:

- remaining arXiv categories reuse the canonical arXiv resolver;
- GitHub README/code uses the exact repository ref and file-level exceptions;
- Semantic Scholar and Papers With Code are discovery only unless they expose
  independently licensed content;
- every long-tail blog or forum item uses RSS item rights or the bounded page
  resolver, never a domain label.

## Test contract

The non-runtime test suite covers the following source invariants:

- source defaults cannot substitute for item evidence;
- arXiv RSS, OAI, and HF Daily Papers schedule one canonical full-text item and
  do not create a metadata/abstract corpus duplicate;
- missing arXiv rights resolve through the canonical item resolver;
- generic RSS and sitemap items accept only item HTTP/HTML evidence and never
  perform the separate full-body GET after an unresolved probe;
- exact HF model, dataset, and Space revisions are mandatory before card fetch;
- GitHub events/releases emit discovery jobs only, the licence API receives
  the exact tag, a rejected repository licence blocks tarball fetch, and every
  retained file receives its own admission decision;
- OpenReview paper and review rights are independent; missing paper rights
  block PDF fetch while public review terms do not leak onto the paper;
- every seed adapter ignores wrapper licences and stamps source-specific
  evidence identity and scope;
- audit views retain quarantined and posttrain-only decisions, while only
  licence-permitted content reaches Gold and exports enforce the strict
  pretraining filter.

The source-adapter tests use mocked upstream responses only. They prove call
ordering and record shape without running a local service, model, container,
or pipeline. Live admission and Gold reachability still require the cloud
validation below.

## Source UI inventory contract

The Sources page merges two inventories rather than pretending every workload
is a user-created SourceFeed CRD:

- CRD-managed arXiv RSS/OAI, blog RSS, and operator-added sitemap feeds;
- Helm-managed arXiv full text, GitHub discovery/tarballs, HF card catalogues,
  HF Daily Papers, and OpenReview live/backfill workloads.

The Kubernetes controller reports the real Deployment, CronJob, or Job state
for every Helm-managed source, including one row for each configured seed
component. Per-source document counts and licence
distributions come from the durable admission/curation tables, not a hardcoded
success state. A disabled workload remains visible as disabled. Seed components
are one-shot preload jobs and are presented as disabled, running, failed, or
completed preload history rather than healthy streaming feeds.

Current completeness limitation: the Helm sitemap list is empty and disabled.
If operators populate that list directly rather than creating SourceFeed CRDs,
the UI has no per-feed CRD inventory row. Configure sitemap inputs as
SourceFeeds, or extend the controller with rendered Helm descriptors, before
claiming that path is visible in Sources.

## Required live cloud validation

Code and contract tests do not prove upstream metadata availability. The
deployed validation must capture, for every enabled logical source:

1. one admitted item, one posttrain-only item when the source naturally has
   one, and one quarantined item;
2. the immutable admission row preceding the first full-body request/storage
   timestamp;
3. the exact evidence URL/revision and final licence distribution;
4. discovery-to-content lineage without a second corpus document;
5. a restart/replay showing no lost item and no duplicate Gold identity;
6. the Sources and Documents UI counts matching the durable ledger.

Items not naturally observed during the validation window use committed
synthetic metadata fixtures against isolated canary topics. They must not be
inserted into production tables.
