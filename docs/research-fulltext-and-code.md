# Full-Text Paper and Source-Code Ingestion at Scale for Stream2Pretrain

Verified 2026-06-15. All numerical figures sourced; values not yet measured on the Stream2Pretrain k3s cluster are explicitly marked `needs-measurement`.

## TL;DR

1. **Use arXiv HTML (`https://arxiv.org/html/<id>`) as the primary full-text channel for new papers**, with `ar5iv.labs.arxiv.org` as fallback for older papers and the AWS requester-pays bucket `s3://arxiv/src` for a one-shot LaTeX-source backfill. arXiv reports ~97% of new submissions have HTML (~75% with no LaTeXML errors) [arxiv.org/pdf/2605.16562]. This avoids the cost and latency of PDF parsing for the freshest tier.
2. **Skip live PDF parsing in Phase 1.** If long-tail PDFs are required, `marker` is the open-weights leader for science PDFs in 2026 (~95.7% structure / 91.2% inline-equation accuracy on its own benchmark; H100-first; CPU runs around 2-4 minutes per arXiv paper, so unworkable for 5-20k docs/day on 2 worker nodes) [datalab-to/marker, huridocs.org]. Treat marker as a Phase-2 GPU sidecar.
3. **For code, do not crawl GitHub repositories yourself.** Pull `bigcode/the-stack-v2` (32.1 TB dedup, ~900B tokens, 658 languages, Software Heritage-backed, SPDX-permissive only) once via HuggingFace, then use the GitHub Releases Atom + Events feeds (already in Phase 1) to track delta tarballs of ~30 curated AI repos through the GitHub `/repos/{o}/{r}/tarball/{ref}` endpoint. This stays inside the 5000 req/h authed budget.
4. **Add three full-text sources to the curator with low integration cost**: ACL-OCL `acl-anthology-corpus` (~45 GB, all ACL PDFs + GROBID parses) [github.com/shauryr/ACL-anthology-corpus]; the S2ORC bulk dataset via the Semantic Scholar `datasets` API (8.1M open-access full-text papers) [github.com/allenai/s2orc]; and the OpenReview API v2 metadata + per-PDF HTTPS fetch path (no bulk PDF endpoint).
5. **Avoid the X/Twitter and Reddit traps**: X's mid-2025 ToS now explicitly forbids using X content to train foundation models [techcrunch.com 2025-06-05]. Reddit API pricing makes large-scale harvest non-viable [mashable.com]. Stream2Pretrain should keep both off the source list, which the project already does.

---

## 1. arXiv full text

### 1.1. Channels available, ranked by mid-2026 freshness

| Channel | URL pattern | Format | Coverage | Latency | License | Use in Stream2Pretrain |
|---|---|---|---|---|---|---|
| Native arXiv HTML | `https://arxiv.org/html/<arxiv_id>` | HTML5 + MathML | ~97% of new submissions (~75% error-free) [arxiv.org/pdf/2605.16562] | Same-day on arxiv pipeline | arXiv terms | **Primary fresh-tier source** |
| ar5iv lab | `https://ar5iv.labs.arxiv.org/html/<arxiv_id>` | HTML5 + MathML, single-version only | Through ~end May 2026 [ar5iv.labs.arxiv.org] | Stale; "not a live preview service" | arXiv terms | **Fallback for older papers** |
| ar5iv-04.2024 dataset | SIGMathLing release | HTML5 + MathML, 2.1M docs (366k clean / 1.3M warning / 500k error) [sigmathling.kwarc.info] | Through April 2024 | arXiv terms | One-shot historical backfill |
| RSS / OAI-PMH abstracts | already in Phase 1 SOURCES.md | XML metadata only | All papers | Same-day | arXiv terms | Already wired |
| LaTeX source bulk | `s3://arxiv/src` (requester-pays) | Tarballs of `.tex` (~500 MB chunks) [info.arxiv.org/help/bulk_data_s3.html] | Full corpus (~2.9 TB, March 2023) [info.arxiv.org] | Lag of weeks | arXiv terms | Backfill only; egress cost ≈ $0.09/GB above first 100 GB free [aws.amazon.com/s3/pricing] |
| PDF bulk | `s3://arxiv/pdf` (requester-pays) | Tarballs of `.pdf` (~500 MB chunks) [info.arxiv.org] | Full corpus | Lag of weeks | arXiv terms | Avoid (PDF parse cost) |
| Combined bulk size | both buckets | "complete set ... about 9.2 TB as of April 2025 ... estimated growth ~100 GB/month" [info.arxiv.org/help/bulk_data_s3.html] | n/a | n/a | n/a | Budget ~$830 one-shot egress for the full set in US-region [aws.amazon.com/s3/pricing] |

### 1.2. LaTeX vs PDF for LLM training

What public curators do:

- Common Crawl curation pipelines do not handle arXiv structure natively [arxiv.org/html/2406.17557v1].
- **NeMo Curator** ships an arXiv download/extract helper (output: JSONL) but the public docs do not specify whether it goes through LaTeX source or PDF [developer.nvidia.com/blog/scale-and-curate-high-quality-datasets-for-llm-training-with-nemo-curator].
- **Dolma / RedPajama / The Pile** historically built the arXiv subset from `s3://arxiv/src` LaTeX with custom strip-comment and inline-macro expansion. Public confirmation in the cited results is partial.

LaTeX is structurally cleaner than PDF for math, citations, and section headings, but introduces macro-expansion and `\input` resolution. The 2026 native arXiv HTML pipeline (LaTeXML-rendered server-side) is functionally equivalent to LaTeX-source ingestion without that work. **Recommendation: pull the rendered HTML; fall back to LaTeX only when HTML conversion errored** (~25% of submissions per arxiv.org/pdf/2605.16562).

### 1.3. PDF -> Markdown extractors as of mid-2026

| Tool | Open weights | CPU-only viable | Math quality | Throughput on H100 | Throughput on CPU | License |
|---|---|---|---|---|---|---|
| **Marker** [github.com/datalab-to/marker] | Yes (OpenRAIL-class, revenue cap) | Slow; ~2:14 - 4:30 min per arXiv paper on Hetzner CPX31 [huridocs.org/2026/06/markdown-conversion-tool] | ~95.7% block / 91.2% inline equations [blog.csdn.net/gitblog_00775] | 0.18-0.23 s/page batched, ~25-122 pages/sec [datalab-to/marker] | needs-measurement on Stream2Pretrain workers | OpenRAIL-class, GPL-3 code |
| **Docling** (IBM) [danilchenko.dev/posts/markitdown-vs-docling-vs-marker] | Yes | Yes; ~0.61 s/page CPU [huridocs.org] | ~86.7% (good structure, math behind Marker) | ~3.7 s/page serial [datalab-to/marker] | ~0.61 s/page modern i7 [huridocs.org] | MIT |
| **MinerU 2** | Mostly yes | Yes | Competitive but no head-to-head Marker win in cited results | needs-measurement | needs-measurement | Apache-2.0 |
| **Nougat** (Meta) | Yes | Yes (very slow) | Below Marker | needs-measurement | ~400 s/page (~4x Marker) [youtube.com/watch?v=mdLBr9IMmgI] | CC-BY-NC-4.0 |
| **Llama-Parse** [llamaindex.ai] | No (cloud only) | n/a | ~84.2% benchmark, 23.35 s/page [datalab-to/marker] | n/a | n/a | Commercial |
| **pix2tex** | Yes | Yes | Per-equation only | n/a | n/a | MIT |

For Stream2Pretrain on a 2-worker k3s cluster without GPUs, **CPU PDF parsing is not throughput-feasible** for 5-20k docs/day. The realistic path is: arXiv HTML for the fresh tier; queue PDF parsing as a low-priority KEDA-scaled batch with a bounded GPU node added in Phase 2 if measurements demand it.

### 1.4. LaTeX -> plaintext

- **LaTeXML** powers arXiv's HTML pipeline; arXiv reports ~75% of submissions convert with no LaTeXML errors [arxiv.org/pdf/2605.16562]. A Rust port is in progress for faster previews [arxiv.org/pdf/2605.16562].
- **pandoc** is a reasonable fallback for cleanly authored sources; degrades on heavy macro/tikz papers (no head-to-head numbers in cited sources, mark `needs-measurement`).
- **ar5iv** is essentially the LaTeXML pipeline pre-rendered for the historical corpus (2.1M docs, 17% clean, 62% with warnings, 24% errored as of April 2024) [sigmathling.kwarc.info].

Recommendation: do not run LaTeXML inside the curator. Consume the already-rendered HTML.

---

## 2. Code at scale

### 2.1. The right entry point: pull a snapshot, do not crawl

**Pull `bigcode/the-stack-v2` from HuggingFace** rather than crawling GitHub:

- 67.5 TB raw / 32.1 TB deduplicated / ~900B tokens / 658 languages [huggingface.co/datasets/bigcode/the-stack-v2].
- Built on Software Heritage; permissive SPDX licenses only [bigcode-project.org/docs/about/the-stack].
- Repository metadata collection extends through 2023-09-14 [github.com/bigcode-project/the-stack-v2/issues/7]; **this is the latest publicly documented BigCode Stack release as of mid-2026**, so it is best treated as a ~2.5-year-stale baseline.

For freshness on top of that snapshot:

- Use the **GitHub Releases Atom feeds** that Phase 1 already polls. For each release, fetch the tarball at `https://api.github.com/repos/{o}/{r}/tarball/{tag}` (5000 req/h authed budget; tarball downloads count as 1 request each) [docs.github.com/rest].
- Consume the curated ~30-repo list from `SOURCES.md` for the demo; the entire fan-out fits inside ~720 requests/day even with hourly polling.
- For broader code repos, layer **GitHub Public Events (already in Phase 1)** to detect new repositories matching topic filters (`machine-learning`, `pytorch`, `jax`), then queue a tarball fetch.

### 2.2. Bulk options for the long tail

| Option | Endpoint | Bulk-suitability | License attestation | Use in Stream2Pretrain |
|---|---|---|---|---|
| **The Stack v2** | `huggingface.co/datasets/bigcode/the-stack-v2` | Best; pre-curated | SPDX-permissive only [bigcode-project.org] | Phase-2 backfill |
| **Software Heritage Vault** | `POST /api/1/vault/{bundle}/{swhid}` then `GET .../raw` [docs.softwareheritage.org] | Per-snapshot only; their ToU forbids "massive data extraction" through the public API [softwareheritage.org/legal/api-terms-of-use] | License via GraphQL/swh-graph | Targeted only |
| **swh-graph dataset** | ORC + compressed CSV exports [docs.softwareheritage.org] | Yes for graph analytics; raw blobs via `s3://softwareheritage/content/<sha1>` [docs.softwareheritage.org] | Per-blob | Out of scope for Phase 1; consider for license forensics |
| **GHArchive** | `gharchive.org`; mirrored to BigQuery | Event metadata only, not file contents | n/a | Already covered indirectly via `/events` |
| **BigQuery `bigquery-public-data.github_repos`** | BigQuery | Reduced/deprecated coverage in 2024-2026 (verify before relying on it) | Per-repo `license` field | Out of scope; The Stack v2 supersedes |
| **Sourcegraph / Cody** | search/index layer | Search-and-analysis only, not bulk fetch | n/a | Out of scope |
| **`gh repo list ... | xargs gh repo clone`** [github.com/git-guides/git-clone] | direct clone | Works up to thousands; respect informal throttling [github.com/orgs/community/discussions/44515] | Per-repo | Tertiary fallback |

### 2.3. Existing code-pretraining datasets to mirror

| Dataset | Volume | Provenance | License signals | Notes |
|---|---|---|---|---|
| **The Stack v2** | 67.5 TB raw / 32.1 TB dedup / ~900B tokens / 658 languages | Software Heritage, ~2023-09 cutoff | SPDX permissive only | Keystone code corpus [huggingface.co/datasets/bigcode/the-stack-v2] |
| **OpenCoder pretraining (`opc-annealing-corpus`, `opc-sft-stage1/2`)** | Pretraining ~2.5 trillion tokens (90% raw code, 10% code-related web) [huggingface.co/infly/OpenCoder-8B-Instruct]; cleaned corpus ~960B tokens [arxiv.org/html/2411.04905v1] | RefineCode + algorithmic corpus + synthetic | Per-license filtering inherited from RefineCode | Public on HF |
| **CodeParrot, CommitPack, CommitPackFT** | not verified in cited sources; mark `needs-measurement` | GitHub | Heuristic | Public on HF; second-tier |
| **Qwen2.5-Coder data composition** | not publicly itemized | Mixed | n/a | Closed mixture |
| **StarCoder2 training data filtering** | Built on The Stack v2; OpenCoder paper notes rules extend StarCoder's [arxiv.org/html/2411.04905v1] | Permissive only | n/a | Reference recipe |

### 2.4. Filtering strategy for "AI research code"

The filter is multi-layer (NeMo Curator pattern, code-adapted) [docs.nvidia.com/nemo/curator]:

1. **Repo selection** before file ingestion: GitHub topics `machine-learning`, `deep-learning`, `pytorch`, `jax`, `cuda`, `transformers`, `llm`, plus the curated 30-repo list from `SOURCES.md`. Optional: stargazer count, last-commit recency.
2. **License**: SPDX in the OSI-approved permissive subset (MIT, BSD-2/3-Clause, Apache-2.0, MPL-2.0). Source the SPDX ID from the GitHub License API (`/repos/{o}/{r}/license`) or, for The Stack v2 documents, the per-blob attestation already in the dataset.
3. **Programming-language detection** (Pygments, `enry`, or GitHub linguist) to keep `.py`, `.ipynb`, `.cu`, `.cuh`, `.cc`, `.cpp`, `.h`, `.hpp`, `.mlir`. Drop assets, configs, lockfiles.
4. **Path heuristics**: drop `vendor/`, `third_party/`, `node_modules/`, `dist/`, `build/`, files containing `Generated by ...` or `DO NOT EDIT`.
5. **Syntax-error filter** per language (e.g., `ast.parse` for Python, `clang -fsyntax-only` for C/C++/CUDA).
6. **Exact dedup** (SHA-256 over normalized text), then **MinHash near-dedup** (Rensa + LSHBloom, 128 perms, 0.9 Jaccard, n=5 token n-grams) - reuses Stream2Pretrain's existing dedup operators.

NeMo Curator does not ship a dedicated SPDX filter; the standard pattern is a `LambdaFilter(lambda d: d.license in ALLOWED_SPDX)` over a pre-tagged `license` column [docs.nvidia.com/nemo/curator/curate-text].

### 2.5. Volume estimate

`needs-measurement` for Stream2Pretrain. Order-of-magnitude reference: 30 curated repos at ~5-50 release tarballs/day yields tens of MB/day post-filter; The Stack v2 is a one-shot multi-TB backfill, not a steady stream.

---

## 3. Conference proceedings and OpenReview

### 3.1. OpenReview API v2 (ICLR / NeurIPS / ICML / COLM)

- Endpoint: `https://api2.openreview.net` (v2 for 2023+; some pre-2023 venues still on v1) [stackoverflow.com/questions/77708720].
- Pagination: `limit` (typical max 1000) + `offset`; the `openreview.api.OpenReviewClient.get_all_notes` helper paginates internally [parse.bot ICLR Conference Papers API].
- **Total record counts** are returned as `total` in paginated responses.
- **PDFs are not delivered through the JSON API.** The note's content carries the PDF URL (HTTPS under `openreview.net/pdf/...`); you must fetch each binary over HTTPS [github.com/pranftw/openreview_scraper, openreview.net/pdf/a0a676530e3922b80db5929dcbda1af9340522e8].
- Pinned fallback: [ReviewArena on Hugging Face](https://huggingface.co/datasets/anonymousNeurIPS2026submission4281/reviewarena) provides OCR Markdown, structured reviews, rebuttals, and decisions across NeurIPS, ICLR, ICML, TMLR, EMNLP, CoRL, and COLM. The adapter pins revision `c2978add17c2099219eaddbc2599974d69d4d09b`, streams its real splits, and admits the paper and review artifacts independently. A paper without per-item rights quarantines rather than inheriting the dataset wrapper. A substantive public review may use its item licence or the versioned OpenReview public-comment terms.

### 3.2. ACL Anthology

- **Bulk BibTeX**: `anthology.bib.gz`, `anthology+abstracts.bib.gz`, sharded `anthology-1.bib`...  [aclanthology.org/faq].
- **Per-paper PDF**: `https://aclanthology.org/<id>.pdf`. There is **no official "all PDFs" tarball**.
- Pre-built mirror: **ACL-OCL `acl-anthology-corpus`** distributes "All PDFs in ACL anthology: size 45G" plus BibTeX-with-abstracts (172 MB) and GROBID parses [github.com/shauryr/ACL-anthology-corpus]. **Use this as the bulk channel.**
- **License**: CC-BY-NC-SA up to 2015, CC-BY 4.0 from 2016 onward for ACL-owned content; third-party venues (COLING, LREC) keep their original copyright [github.com/luanyi/acl-anthology, people.cs.georgetown.edu/nschneid/p/aclanth.pdf].
- Programmatic metadata: `acl-anthology` Python library [acl-anthology.readthedocs.io] over the official GitHub metadata repo.

### 3.3. PMLR (proceedings.mlr.press)

- No documented bulk-download API or all-PDF tarball in the cited sources.
- Each volume page lists per-paper PDF URLs and a volume BibTeX. Bulk acquisition requires polite crawling (respect `robots.txt`, rate-limit). Confirm policy before scaling.
- License: per-volume; most are open-access but verify per paper.

### 3.4. Semantic Scholar S2ORC

- Available via the Semantic Scholar Public API datasets endpoint [github.com/allenai/s2orc, semanticscholar.org/faq].
- **Current corpus: 8.1M open-access papers with structured full text** plus 1.5M LaTeX parses; the original 2020 paper described 81.1M total English papers with metadata [arxiv.org/pdf/1911.02782v2]. The frequently quoted "40M full text" is not supported by current cited sources; use 8.1M as the verified figure.
- Workflow: API key -> `datasets` endpoint -> shard download. Free; fair-use for research.

### 3.5. Daily volume from these sources

`needs-measurement`. Order-of-magnitude during conference seasons: OpenReview can yield thousands of PDFs in a few-day window; ACL/PMLR are bursty around publication; S2ORC is one-shot.

---

## 4. What changed in 2025-2026

| Change | Impact on Stream2Pretrain |
|---|---|
| **arXiv native HTML rollout** at `/html/<id>`; ~97% coverage, ~75% LaTeXML-clean [arxiv.org/pdf/2605.16562] | Replaces PDF parsing as the fresh-tier path; major cost saving |
| **X/Twitter ToS** now bans LLM training on X content [techcrunch.com/2025/06/05] | Already excluded in `SOURCES.md`; keep excluded |
| **Reddit API pricing** makes large-scale harvest economically infeasible [mashable.com] | Already excluded; keep excluded |
| **Marker, Docling, MinerU upgrades** [github.com/datalab-to/marker, danilchenko.dev] | Marker is the open-weights leader; defer to GPU sidecar |
| **The Stack v2** is still the last public BigCode release [github.com/bigcode-project/the-stack-v2] | Use as code baseline; supplement with curated GitHub release tarballs |
| **Software Heritage public API hardened** against bulk extraction [softwareheritage.org/legal/api-terms-of-use] | Do not target SWH for bulk; use The Stack v2 instead |
| **OpenCoder dataset family** released on HuggingFace (`opc-annealing-corpus`, `opc-sft-stage1/2`) [huggingface.co/OpenCoder-LLM] | Optional layer on top of The Stack v2 |

---

## 5. Recommended additions to `ingest/`

The minimum changes to bring real full text and code into Stream2Pretrain:

### 5.1. New ingest modules (Phase 1.5, no GPU required)

- `ingest/arxiv_html_fetcher/` - CronJob; for every arXiv ID surfaced by the existing OAI-PMH and RSS pollers, fetch `https://arxiv.org/html/<id>` (and fall back to `https://ar5iv.labs.arxiv.org/html/<id>` on 404). Land in `raw.fetched` with `content_type=text/html`. Resiliparse already in the stack handles extraction.
- `ingest/openreview_poller/` - CronJob; poll `api2.openreview.net` for new submissions in `ICLR.cc/<year>/Conference`, `NeurIPS.cc/<year>/Conference`, `ICML.cc/<year>/Conference`, `COLM/<year>/Conference`. For each note, emit metadata immediately; queue an HTTPS PDF fetch with a low-priority KEDA scaler.
- `ingest/github_release_tarball_fetcher/` - companion to the existing Releases Atom poller; for each new release, call `GET /repos/{o}/{r}/tarball/{tag}` once, stream into MinIO bronze, emit one record per included file (after path-extension and license filtering).

### 5.2. New batch backfill jobs (one-shot, run from `scripts/`)

- `scripts/backfill_the_stack_v2.py` - HuggingFace `datasets` streaming download of `bigcode/the-stack-v2` filtered by language and SPDX license, written into the silver Iceberg table with `source_feed=the-stack-v2`.
- `scripts/backfill_acl_ocl.py` - download `shauryr/ACL-anthology-corpus` (~45 GB), extract per-paper text from GROBID parses, land in silver with `source_feed=acl-ocl`.
- `scripts/backfill_s2orc.py` - Semantic Scholar `datasets` API; pull S2ORC shards, extract structured-full-text JSON, land in silver with `source_feed=s2orc`.

### 5.3. Phase 2 (requires GPU node)

- `processor/pdf_marker_sidecar/` - marker on a GPU pod, KEDA-scaled by `pdf.queue` topic lag. Used only for OpenReview, PMLR, and arXiv-HTML-failed PDFs. Throughput target on a single H100: ~25 pages/sec; on the 2-worker CPU-only cluster: do not run inline.

### 5.4. Schema additions

Extend the silver schema (`schemas/silver_doc.json`) with:
- `source_format` enum: `html`, `latex`, `pdf`, `code`, `metadata-only`
- `extraction_pipeline` string: e.g., `arxiv-html`, `marker-1.6.0+gpu`, `the-stack-v2`, `acl-ocl-grobid`
- `repo_url`, `repo_commit`, `file_path`, `programming_language`, `spdx_license` for code documents

### 5.5. Decisions to capture in `CLAUDE.md`

- **Primary arXiv full-text channel**: native arXiv HTML; LaTeX source bulk only as a one-shot backfill; PDF as last resort.
- **Code primary path**: The Stack v2 backfill + curated 30-repo Releases Atom delta. Do not crawl GitHub at scale.
- **Marker is Phase 2 only** until the cluster has a GPU node; mark all PDF-throughput numbers `needs-measurement` until then.
- **SPDX whitelist for code**: MIT, BSD-2-Clause, BSD-3-Clause, Apache-2.0, MPL-2.0. Document this in the Risks section of the README alongside the existing "license detection is heuristic" caveat.

---

## Sources

All claims above link inline. Primary references:

- arXiv bulk: https://info.arxiv.org/help/bulk_data_s3.html
- arXiv HTML: https://arxiv.org/pdf/2605.16562 ; https://arxiv.org/html/2402.08954v1
- ar5iv: https://ar5iv.labs.arxiv.org ; https://sigmathling.kwarc.info/resources/ar5iv-dataset-2024/
- AWS S3 pricing: https://aws.amazon.com/s3/pricing/ ; https://www.cloudzero.com/blog/s3-pricing/
- Marker: https://github.com/datalab-to/marker ; https://huridocs.org/2026/06/markdown-conversion-tool/ ; https://blog.csdn.net/gitblog_00775/article/details/151429144
- Docling vs Marker: https://www.danilchenko.dev/posts/markitdown-vs-docling-vs-marker/
- FineWeb / NeMo / Dolma: https://arxiv.org/html/2406.17557v1 ; https://developer.nvidia.com/blog/scale-and-curate-high-quality-datasets-for-llm-training-with-nemo-curator/
- The Stack v2: https://huggingface.co/datasets/bigcode/the-stack-v2 ; https://www.bigcode-project.org/docs/about/the-stack/ ; https://arxiv.org/abs/2402.19173
- Software Heritage: https://docs.softwareheritage.org/devel/getting-started/api.html ; https://www.softwareheritage.org/legal/api-terms-of-use/
- OpenReview API v2: https://docs.openreview.net/reference/api-v2 ; https://github.com/pranftw/openreview_scraper ; https://openreview.net/pdf/a0a676530e3922b80db5929dcbda1af9340522e8
- ACL Anthology: https://aclanthology.org/faq/ ; https://github.com/shauryr/ACL-anthology-corpus ; https://acl-anthology.readthedocs.io/latest/guide/getting-started/
- S2ORC: https://github.com/allenai/s2orc ; https://arxiv.org/pdf/1911.02782v2.pdf ; https://www.semanticscholar.org/faq
- NeMo Curator code recipe: https://docs.nvidia.com/nemo/curator/curate-text ; https://github.com/NVIDIA-NeMo/Curator
- OpenCoder: https://huggingface.co/papers/2411.04905 ; https://arxiv.org/html/2411.04905v1 ; https://huggingface.co/infly/OpenCoder-8B-Instruct
- 2025-2026 access changes: https://techcrunch.com/2025/06/05/x-changes-its-terms-to-bar-training-of-ai-models-using-its-content/ ; https://mashable.com/article/social-media-paid-api-internet-future
- GitHub bulk: https://github.com/git-guides/git-clone ; https://docs.github.com/rest/repos ; https://github.com/orgs/community/discussions/44515
