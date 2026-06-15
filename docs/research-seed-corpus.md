# Stream2Pretrain v0.1 - Seed Corpus Research

Research date: 2026-06-15. Sources cited inline. Every numerical figure not directly verified against an upstream dataset card or paper is marked `needs-measurement`.

## TL;DR

For a frontier-LLM-research-focused seed corpus that fits a DHBWCloud 2-worker MinIO budget (target 500 GB - 2 TB usable), the recommended Stream2Pretrain v0.1 seed mixture is: **(1) `allenai/peS2o` v3** (academic papers from S2ORC, the Dolma/OLMo academic backbone, ~120 GB on Hub, ~42B+ tokens at v2, freshness cutoff 2023-01-03 for v2; v3 is on Hub but un-carded), **(2) `togethercomputer/RedPajama-Data-1T` arxiv subset** (~92 GB, ~28B tokens, LaTeX-derived arXiv, 2023 cutoff) as a complementary LaTeX-derived view, **(3) `HuggingFaceFW/fineweb-edu`** sampled to a ~50-100 GB AI/ML-domain slice via URL filter (CC-derived web with FineWeb-Edu classifier scores >=3, ODC-By 1.0), **(4) `HuggingFaceTB/stack-edu`** filtered for Python ML repos (~100 GB target, ODC-By, the educational-quality code subset that Dolma 3 Mix uses for its 0.41T code component), and **(5) a custom historical RSS/Atom backfill** of the same Phase-1 source list defined in `SOURCES.md` going back 24 months (arXiv OAI-PMH `from=2024-06-01`, GitHub Releases Atom history, lab blogs via Wayback). Total target: 400-700 GB on disk, 50-90B tokens, all under ODC-By or CC-BY family licenses compatible with our Apache-2.0 release. Wire it in as a dedicated **Bronze backfill mode**: a one-shot Bytewax dataflow that reads HF parquet/zst shards, tags them with `valid_from = original_publication_date`, `valid_to = null`, `source_feed = "seed:<dataset_id>"`, and emits to the same `docs.normalized` topic the live pipeline already consumes - so Silver/Gold curation operators run identically over historical and live documents.

## 1. Generic science / AI corpora

### 1.1 `allenai/peS2o` (the AI2 academic-paper backbone)

- HF id: `allenai/peS2o` (https://huggingface.co/datasets/allenai/peS2o).
- Versions: v1 (deprecated), **v2** is what the dataset card documents - 38.97M documents, 42.01B whitespace-separated tokens, knowledge cutoff **2023-01-03** (https://huggingface.co/datasets/allenai/peS2o). **v3** exists on Hub under `data/v3/` (136 zst shards, ~120 GB total on Hub) but the dataset card has not been updated to v3 stats as of 2026-06; treat v3 token count as `needs-measurement` until we run a count locally.
- License: ODC-By 1.0 (inherited from the Dolma collection it was built for) (https://allenai.org/blog/dolma-3-trillion-tokens-open-llm-corpus-9a0ff4b8da64).
- Primary use case: filtered S2ORC-derived academic prose, the canonical AI2 "papers" component for OLMo-1/2 pretraining. Scope is broader than AI/ML (covers all of S2ORC), so to focus on frontier LLM research we should post-filter on `field_of_study contains {"Computer Science"}` if metadata is preserved - the metadata is in the source S2ORC; v3 shards may strip it (`needs-measurement`).
- Freshness: v2 cutoff 2023-01-03; v3 likely later (Hub commit history goes through 2024-10) but exact cutoff is `needs-measurement`.

### 1.2 `togethercomputer/RedPajama-Data-V2` (and v1 arxiv subset)

- HF id: `togethercomputer/RedPajama-Data-V2` (https://huggingface.co/datasets/togethercomputer/RedPajama-Data-V2). Multi-trillion-token CC-derived corpus, multilingual; **arxiv is NOT in V2** - V2 is CC-only. The arXiv shard lives in V1.
- arXiv subset: **`togethercomputer/RedPajama-Data-1T`** with config name `"arxiv"`. Per the original RedPajama release this slice is ~92 GB / ~28B tokens of LaTeX-extracted arXiv papers (https://www.together.ai/blog/redpajama). Cutoff 2023-04 (`needs-measurement` for exact date).
- License: Apache-2.0 wrapper, content under arXiv's per-paper licenses (mostly CC-BY or arXiv non-exclusive; permissive for research, must respect upstream).
- Primary use case: LaTeX-source-derived arXiv papers, complementary to peS2o's PDF-derived view. Useful for the Stream2Pretrain shadow-A/B demo (compare PDF vs LaTeX extraction quality).

### 1.3 `allenai/dolma` (v1.7 and v3) - aggregate corpora that EMBED peS2o + arxiv

- HF id v1.7: `allenai/dolma` with v1.7 tag - 3T tokens total, 4.5 TB gzipped, includes peS2o + RedPajama arxiv + Stack code + CC web (https://allenai.org/blog/dolma-3-trillion-tokens-open-llm-corpus-9a0ff4b8da64). License ODC-By 1.0.
- HF id v3 (OLMo-3): the **Dolma 3 Mix** is ~5.93T tokens (https://www.emergentmind.com/topics/olmo-3-think-32b, https://allenai.org/blog/olmo3). Documented breakdown: 4.51T CommonCrawl, 0.81T olmOCR academic PDFs, 0.41T Stack-Edu code, **50B arXiv LaTeX**, 152B FineMath3+, 2.5B Wikipedia/Wikibooks. Released 2025-11. License: per-component, packaged under ODC-By 1.0.
- Implication for us: Dolma is too big to download whole, but its **subset components** are exactly what we need; we can pick the arXiv-LaTeX 50B slice and the academic PDF 0.81T slice as standalone HF datasets when AI2 publishes them under `allenai/dolma3_*` (existing as of 2025-12 per OLMo-3 report; specific HF IDs `needs-measurement`).

### 1.4 `mlfoundations/dclm-baseline-1.0`

- HF id: `mlfoundations/dclm-baseline-1.0`. ~3.8T tokens, CC-BY-4.0 (https://presenc.ai/research/open-pretraining-datasets-2026, https://arxiv.org/abs/2406.11794).
- Primarily CC-derived web; not academic-paper-focused. Useful only if we filter URL domains for arxiv.org / huggingface.co / github.io / lab blogs - that filter token yield is `needs-measurement`.

### 1.5 `nvidia/Nemotron-CC` and `nvidia/Nemotron-CC-Math`

- HF id (root): `nvidia/Nemotron-CC` (~6.3T tokens; multi-license per-document, https://presenc.ai/research/open-pretraining-datasets-2026). Math subset documented in the Nemotron-CC-Math paper (NVIDIA 2025) but exact HF id and token count `needs-measurement`.
- License: mixed, must respect per-document tags. **Risk for Apache-2.0 release**: not safe to redistribute wholesale. Use only for derived metrics or with strict per-document license filtering.

### 1.6 `common-pile/*` (Common Pile v0.1)

- HF org: `common-pile`. Aggregate ~8 TB raw / ~1.8 TB filtered, ~233M documents, 30 sources, all under permissive/PD licenses passing Open Definition 2.1 (https://blog.eleuther.ai/common-pile/, https://arxiv.org/abs/2506.05209).
- License: per-component (CC0/CC-BY/PD), curated for permissive use - **safest license posture** for a downstream Apache-2.0 release.
- Components on-domain for AI research: arXiv subset (`common-pile/arxiv_*`), USPTO patents (some AI patents), GitHub-Issues-with-permissive-license, public-domain books (low signal for frontier LLM research). Need the per-component HF ids; `needs-measurement` for exact subset sizes.

### 1.7 `HuggingFaceFW/fineweb` and `fineweb-edu`

- HF id: `HuggingFaceFW/fineweb` (~15T tokens English CC, ODC-By 1.0). `HuggingFaceFW/fineweb-edu` (~1.3T tokens, FineWeb-Edu classifier score >=3, ODC-By 1.0) (https://huggingface.co/datasets/HuggingFaceFW/fineweb-edu, https://arxiv.org/abs/2406.17557).
- Primary use for us: domain-filter to AI/ML by URL allowlist (arxiv.org, huggingface.co/blog, openai.com, deepmind.google, eleuther.ai, bair.berkeley.edu, distill.pub, lilianweng.github.io, sebastianraschka.com, etc.). Yield is `needs-measurement` but should land in the 5-50 GB range.
- FineWeb-2 (multilingual, ~10T tokens, ODC-By) is out of scope for English-first frontier LLM research.

### 1.8 OpenAlex

- Bulk dump on AWS S3 + HF mirror snapshots (https://docs.openalex.org/download-all-data/openalex-snapshot). Metadata + abstracts for ~250M scholarly works; full text only where the paper itself is OA. License: CC0.
- Use case: high-quality metadata layer (DOI, publication date, citation graph) to **enrich validity intervals** on peS2o/RedPajama-arxiv documents. Probably not a primary text seed.

## 2. Frontier-LLM-research-specific datasets

There is **no dedicated "frontier LLM research only" pretraining corpus** as of 2026-06. The closest approximations are:

### 2.1 arXiv subset filtered by category

- Build by filtering peS2o or RedPajama-arxiv on `categories LIKE 'cs.CL' OR 'cs.LG' OR 'cs.AI' OR 'stat.ML'`. Token yield `needs-measurement` but historically (per arXiv stats https://arxiv.org/stats) cs.CL+cs.LG+cs.AI submissions are ~30% of cs total; applied to peS2o v2's 42B tokens that is ~12B tokens of on-domain frontier-LLM content.

### 2.2 HuggingFace Daily Papers historical archive

- Endpoint `https://huggingface.co/api/daily_papers` (already in `SOURCES.md`). Backfill via paginating `before=<date>` going back to 2023-05 launch. Estimate: ~20-50 papers/day x ~750 days = 15-37k papers. License: HF API ToS allows research use (https://huggingface.co/docs/hub/en/rate-limits). Each paper points to an arXiv id, so the actual paper text comes from peS2o/arxiv anyway; the value here is the **community signal** (upvotes, comments) used to weight quality.

### 2.3 AI-lab blog historical archives

- OpenAI News archive (https://openai.com/news/), DeepMind Blog (https://deepmind.google/blog), HF Blog (https://huggingface.co/blog), BAIR (https://bair.berkeley.edu/blog/), EleutherAI (https://blog.eleuther.ai/), Anthropic engineering posts (Anthropic does not publish full-archive RSS; community mirrors exist). Total volume is small - mid-thousands of posts over 5 years. Practical approach: write a one-shot Wayback-Machine backfill scraper, license under fair-use research, store with original `valid_from`.

### 2.4 Distill.pub archive

- Archived 2021. `https://distill.pub/`. ~50 long-form articles, CC-BY-4.0. Tiny volume, very high signal for visualization-of-training-dynamics writeups. Worth a one-shot ingest.

### 2.5 Lecture corpora (CS224N, CS25, CS336, Karpathy's nanoGPT/llm.c)

- Stanford CS224N transcripts: published on YouTube, transcripts via youtube-transcript-api - **out of scope** per `SOURCES.md` (ToS).
- Stanford CS25 / CS336 lecture notes: PDFs on course websites, public, no clear license - fair-use ingest only.
- Karpathy walkthroughs: nanoGPT and llm.c READMEs and code comments are MIT-licensed (https://github.com/karpathy/nanoGPT, https://github.com/karpathy/llm.c). Already covered by Stack-Edu/StackV2.

### 2.6 LessWrong / Alignment Forum

- GreaterWrong RSS (https://www.greaterwrong.com/index.rss?view=alignment-forum). Already in Phase-2 source list. Archive is downloadable in bulk via the LessWrong GraphQL API. License: CC-BY-NC-SA per LW ToS - **incompatible with Apache-2.0 release**, exclude or document under research-fair-use only.

### 2.7 Verdict on frontier-LLM-research-specific corpora

There is no shrink-wrapped "frontier LLM research" pretraining dataset. The realistic seed strategy is:
1. Take the broad arXiv coverage of peS2o + RedPajama-arxiv;
2. **Post-filter to AI-research categories** as a Bytewax operator;
3. Backfill the same RSS/Atom/HF-API sources that the live pipeline polls, but with `from_date = 2024-06-01` to walk back 24 months.

This is structurally identical to what AllenAI does inside Dolma's academic component but scoped to our domain.

## 3. Code corpora on-domain for frontier LLM research

### 3.1 `bigcode/the-stack-v2`

- HF id: `bigcode/the-stack-v2` (https://huggingface.co/datasets/bigcode/the-stack-v2). Size: tens of TB raw across all languages (~50-70 TB `needs-measurement` against the dataset card). License: per-repo upstream licenses + BigCode dataset wrapper terms requiring respect for upstream. Variants `the-stack-v2-dedup` and `the-stack-v2-train-smol-ids` are filtered; the smol-ids variant (~775B tokens) is what Starcoder2 trained on.
- For Stream2Pretrain v0.1: too large and too broad. We do not need every JS/PHP repo. Use the next dataset.

### 3.2 `HuggingFaceTB/stack-edu`

- HF id: `HuggingFaceTB/stack-edu`. The educational-quality code subset that **Dolma 3 Mix uses** at 0.41T tokens for OLMo-3 code training (https://www.emergentmind.com/topics/olmo-3-think-32b). License: ODC-By family (per HF org policy; per-file underlying license preserved).
- For us: filter to Python only, then on-topic AI repos by URL or by educational-classifier score. Target ~50-100 GB.

### 3.3 Hand-curated AI repo bulk clone (Phase-2 expansion)

- The 30-repo allowlist already in `SOURCES.md` (`huggingface/transformers`, `vllm-project/vllm`, `pytorch/pytorch`, `karpathy/llm.c`, `mlfoundations/dclm`, `huggingface/datatrove`, `NVIDIA-NeMo/Curator`, `allenai/dolma`, `bytewax/bytewax`, etc.). Bulk-clone via `gh repo clone`, run through the same Bytewax curator. Volume: a few GB. License: each repo's OSI license; honor the Apache-2.0 / MIT / BSD-3 they're under.
- This is essentially **already covered by Stack-Edu**, but a fresh clone ensures the most recent commits are in.

### 3.4 Codeforces / DeepMind code-contest corpora

- Out of domain for *frontier LLM research* - these are competitive programming, not LLM training systems code. Skip.

## 4. Sizing + license matrix

| Dataset | HF id | Total size (compressed) | Tokens | Cutoff | License | Apache-2.0 release safe? | On-domain for frontier LLM? |
|---|---|---|---|---|---|---|---|
| `allenai/peS2o` v2 | `allenai/peS2o` | `needs-measurement` (HF Hub ~100GB v2) | 42.01B | 2023-01-03 | ODC-By 1.0 | yes | broad academic, filter to cs.* |
| `allenai/peS2o` v3 | `allenai/peS2o` (`data/v3/`) | ~120 GB on Hub (Hub UI) | `needs-measurement` | `needs-measurement` (~2024-10) | ODC-By 1.0 | yes | broad academic, filter to cs.* |
| RedPajama v1 arxiv | `togethercomputer/RedPajama-Data-1T` config `arxiv` | ~92 GB | ~28B | 2023-04 | Apache-2.0 wrapper + per-paper | yes (with attribution) | yes (LaTeX arXiv) |
| Dolma v1.7 | `allenai/dolma` | 4.5 TB gz | 3T | 2024-04 | ODC-By 1.0 | yes | aggregate, too big |
| Dolma 3 Mix | `allenai/dolma3_*` (per-component) | `needs-measurement` | 5.93T | 2025-11 | ODC-By 1.0 | yes | aggregate, slice the arxiv-50B and academic-810B subsets |
| FineWeb | `HuggingFaceFW/fineweb` | tens of TB | ~15T | 2024-04 | ODC-By 1.0 | yes | only after URL filter |
| FineWeb-Edu | `HuggingFaceFW/fineweb-edu` | a few TB | ~1.3T | 2024-04 | ODC-By 1.0 | yes | sample after URL filter |
| DCLM-Baseline | `mlfoundations/dclm-baseline-1.0` | `needs-measurement` | ~3.8T | 2024 | CC-BY-4.0 | yes | only after URL filter |
| Nemotron-CC | `nvidia/Nemotron-CC` | `needs-measurement` | ~6.3T | 2024 | mixed per-doc | **NO**, redistribute only with per-doc license tracking | yes after filter |
| Common Pile v0.1 | `common-pile/*` | ~8 TB raw / 1.8 TB filtered | ~8T (counted) | 2025-06 | per-component PD/CC-BY | yes (most permissive) | mixed; arxiv subset useful |
| `bigcode/the-stack-v2` | `bigcode/the-stack-v2` | 50-70 TB `needs-measurement` | hundreds of B | 2024 | upstream + BigCode terms | conditional, must filter to permissive-only | broad code, filter |
| `HuggingFaceTB/stack-edu` | `HuggingFaceTB/stack-edu` | `needs-measurement` | ~410B (OLMo-3 used) | 2024-2025 | ODC-By family | yes | yes after Python+ML filter |
| OpenAlex bulk | external S3, not HF dataset | ~330 GB metadata (https://docs.openalex.org/download-all-data/openalex-snapshot) | metadata only | rolling | CC0 | yes | metadata enrichment only |
| HF Daily Papers archive | `huggingface.co/api/daily_papers` (custom backfill) | small | small | live | HF API ToS | yes (research) | very on-domain |
| AI-lab blog backfill | custom Wayback | small | small | live | per-blog ToS | research-fair-use only | very on-domain |
| LessWrong / Alignment Forum | GreaterWrong RSS / GraphQL | small | small | live | CC-BY-NC-SA | **NO** for Apache-2.0 release | on-domain but excluded |

License-posture summary for an Apache-2.0 release:
- **Safe to redistribute curated outputs**: peS2o, RedPajama (v1 arxiv), Dolma, FineWeb/FineWeb-Edu, DCLM-Baseline, Common Pile, Stack-Edu (under ODC-By/CC-BY-4.0).
- **Do not redistribute, only metadata + URL pointers**: Nemotron-CC (mixed per-doc), Stack v2 (upstream-respect), LessWrong/Alignment Forum (NC).
- For our project the model is: ingest content under any license, **carry the per-document `license` and `license_source` columns into Gold** (already in the data passport in `RESEARCH.md` section 6), and at distribution time filter to documents whose license is in the Apache-2.0-compatible allowlist.

## 5. Recommended Stream2Pretrain v0.1 seed mixture

Constraint: 500 GB - 2 TB usable on MinIO across the 2-worker cluster. Reserve ~30% for downstream Silver/Gold so ingest budget is ~350 GB - 1.4 TB compressed Bronze.

Recommended 5-component seed:

| # | Component | HF id / source | Bronze GB target | Tokens (B) target | Why |
|---|---|---|---|---|---|
| 1 | `peS2o` v3 filtered to cs.* | `allenai/peS2o` (data/v3/) | 50 GB | ~12-15B `needs-measurement` | The AI2 academic backbone; filter to cs.CL/cs.LG/cs.AI/stat.ML if metadata available, else accept full v3 and let downstream classifier score. |
| 2 | RedPajama v1 arxiv | `togethercomputer/RedPajama-Data-1T` config `arxiv` | 90 GB | ~28B | LaTeX-derived arxiv view, complementary to peS2o's PDF-derived view. Lets us demo shadow-A/B between extraction methods. |
| 3 | FineWeb-Edu domain-filtered | `HuggingFaceFW/fineweb-edu` | 50 GB | 5-10B `needs-measurement` | URL allowlist (arxiv.org, openai.com, deepmind.google, anthropic.com, ai.meta.com, huggingface.co/blog, distill.pub, eleuther.ai, bair.berkeley.edu, lilianweng.github.io, sebastianraschka.com, magazine.sebastianraschka.com, jalammar.github.io, karpathy.ai, davidsuter.github.io). |
| 4 | Stack-Edu Python+ML filter | `HuggingFaceTB/stack-edu` | 80 GB | 8-12B `needs-measurement` | Code, especially Python ML repos. Same dataset OLMo-3 used for code. |
| 5 | Historical RSS/Atom backfill | custom (arXiv OAI, GH Releases, HF Daily Papers, lab blogs via Wayback) | 5-10 GB | ~1B | Validates the streaming pipeline can operate in "backfill mode" before live polling kicks in; this is also the demo for `valid_from` populated from per-document publication dates. |

Total: ~275-280 GB Bronze, ~55-65B tokens, all under permissive licenses compatible with Apache-2.0.

This stays well under the 500 GB lower bound, leaving room for live ingestion to grow the corpus organically over the demo period without re-tuning storage.

If the cluster has the full 2 TB available, expand component 4 to 200 GB, add a sixth: **Common Pile arxiv subset** (`common-pile/arxiv_*`) at ~50 GB for a CC0/PD license-clean baseline. Final total then ~530 GB, ~85-95B tokens.

## 6. How to wire seeding into the existing pipeline

The streaming pipeline's seam is the Redpanda topic `docs.normalized` (per `RESEARCH.md` section 4 architecture). Anything that lands there with the Silver schema is treated identically by downstream Gopher/C4/MinHash/LSHBloom/FineWeb-Edu/PII/Decon-Gate operators. This means seeding is implementable without touching the live operators.

### 6.1 Add a Bronze backfill mode

New file `processor/seed_loader.py`:

- One-shot Bytewax dataflow (not a long-running KEDA-scaled deployment - a `Job` or `Argo Workflow`).
- Input source: the HF datasets library (`datasets.load_dataset(repo_id, split="train", streaming=True)`) for HF-hosted datasets, and a thin Wayback fetcher for blog backfill.
- For each document: build a Silver-schema record with:
  - `doc_id = sha256(repo_id + ":" + original_id_or_url)`
  - `text = <document text>`
  - `lang = "en"` or detect with fastText
  - `valid_from = original_publication_date` from the dataset's metadata column (peS2o has `created`, RedPajama-arxiv has `meta.timestamp`, FineWeb-Edu has `date`, Stack-Edu has commit date) - **this is exactly the validity-interval signal Stream2Pretrain's N2 novelty needs, and seeding gives us a pre-populated history rather than empty intervals**.
  - `valid_to = null`
  - `valid_from_source = "dataset:<repo_id>"`
  - `source_feed = "seed:<repo_id>"`
  - `trace_id = <new uuid>` (so the seed run is one trace tree).
- Output sink: Redpanda producer to `docs.normalized`. Same topic the live fetcher writes to.

### 6.2 Why not write directly to Gold

We could shortcut and write Gold rows directly, but that defeats the demo:
- Decon-Gate would not see the seed documents and emit no attestation for them.
- MinHash/LSHBloom would not have signatures for them, so live duplicates would not be caught.
- Quality-classifier scores would be missing.

By writing to `docs.normalized`, the seed is treated as **just more documents**, which is exactly the point of a streaming-first design.

### 6.3 Per-document validity interval population

| Dataset | `valid_from` source field | Notes |
|---|---|---|
| peS2o v3 | `metadata.year` + `metadata.month` if present, else `metadata.created`, else `2023-01-03` (v2 cutoff) | Coarse-grained but sufficient. |
| RedPajama-arxiv | `meta.timestamp` (arXiv submission date) | High quality, day-precision. |
| FineWeb-Edu | `date` column (CC-derived `Last-Modified` or `crawl_date`) | Already populated. |
| Stack-Edu | last commit date column | Per-file commit; coarse but better than nothing. |
| RSS/Atom backfill | `entry.published` or `entry.updated` | Same logic as the live RSS poller. |

`valid_to` is left null on ingest. The retraction-handling Bytewax operator (out of scope for v0.1) will set it later for arXiv withdrawals.

### 6.4 Suggested seeding order and operational notes

1. Day 1 (cluster ready): kick off a one-time `seed-pes2o` Job, ~50 GB. Verify Bronze landing, MinHash signatures populating, Decon-Gate emitting an attestation per snapshot.
2. Day 2: `seed-redpajama-arxiv`. This will trigger LSHBloom near-dup hits against peS2o (same papers via different extraction paths) - exactly the dedup demo.
3. Day 3: `seed-fineweb-edu-domain`. Domain-filtering pass first (cheap), then ingest the filtered slice.
4. Day 4: `seed-stack-edu-python`. Code goes through the same operators; demonstrate that the curator does not corrupt code (no Gopher rejections on indented blocks).
5. Day 5+: live RSS/sitemap pollers come online and grow the corpus by 5-20k docs/day per `SOURCES.md`.

### 6.5 Storage layout

- Each seed dataset gets a Hive partition under `s3://bronze/seed/dataset=<repo_id>/year=YYYY/month=MM/`. This way `kubectl logs` output and Iceberg snapshot-property breadcrumbs make the seed origin obvious.
- Silver/Gold tables stay schema-identical between seed and live; the only marker is `source_feed` starting with `seed:` vs `rss:`/`hf:`/`gh:`.

### 6.6 What this delivers for the exam

- `as_of('2023-06-01')` queries return only seed material - immediate temporal-view demo with material to query, on day 1 of the demo.
- Decon-Gate attestation viewer has snapshots to display from day 1 instead of waiting for live polling to accumulate.
- Quality-score histograms have meaningful distributions immediately.
- The "5 V's" mapping in the README has volume (50-90B tokens) and variety (academic PDF, academic LaTeX, web blog, code) materialized.

## Sources cited

- peS2o dataset card: https://huggingface.co/datasets/allenai/peS2o
- peS2o v3 directory: https://huggingface.co/datasets/allenai/peS2o/tree/main/data/v3
- AllenAI Dolma blog: https://allenai.org/blog/dolma-3-trillion-tokens-open-llm-corpus-9a0ff4b8da64
- AllenAI OLMo-3 blog: https://allenai.org/blog/olmo3
- OLMo-3 Technical Report PDF: https://kyleclo.com/assets/pdf/olmo-3.pdf
- OLMo-3 Think 32B summary: https://www.emergentmind.com/topics/olmo-3-think-32b
- Common Pile arxiv paper: https://arxiv.org/abs/2506.05209
- Common Pile EleutherAI blog: https://blog.eleuther.ai/common-pile/
- FineWeb paper: https://arxiv.org/abs/2406.17557
- DCLM paper: https://arxiv.org/abs/2406.11794
- Open pretraining datasets 2026 overview: https://presenc.ai/research/open-pretraining-datasets-2026
- HF rate limits: https://huggingface.co/docs/hub/en/rate-limits
- OpenAlex snapshot: https://docs.openalex.org/download-all-data/openalex-snapshot
- arXiv stats: https://arxiv.org/stats
- nanoGPT: https://github.com/karpathy/nanoGPT
- llm.c: https://github.com/karpathy/llm.c
- Together AI RedPajama blog: https://www.together.ai/blog/redpajama
- BigCode Stack v2: https://huggingface.co/datasets/bigcode/the-stack-v2
- Together RedPajama-Data-V2: https://huggingface.co/datasets/togethercomputer/RedPajama-Data-V2
- HuggingFaceFW FineWeb-Edu: https://huggingface.co/datasets/HuggingFaceFW/fineweb-edu
- mlfoundations DCLM: https://huggingface.co/datasets/mlfoundations/dclm-baseline-1.0
