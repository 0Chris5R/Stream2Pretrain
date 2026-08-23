# Stream2Pretrain - Source Feed Catalog

Streaming curation pipeline for fresh AI-research pretraining data. This document defines every source the system polls, with endpoints, rate limits, and the demo / expansion split.

All endpoints verified against official documentation 2026-06-15. Volumes marked `needs-measurement` will be benchmarked once the cluster is live.

## Phase-1 Demo Set (5-7 feeds, target 5-20k docs/day)

These are the locked-in sources for the first deliverable. Picked for: free, no-auth-or-free-token, four distinct protocols, mix of metadata + short-form + long-form content.

| # | Source | Endpoint | Protocol | Auth | Poll interval | Est. docs/day | Why |
|---|---|---|---|---|---|---|---|
| 1 | arXiv OAI-PMH (set=cs) | `https://oaipmh.arxiv.org/oai` | OAI-PMH 2.0 XML | none | every 2h via `from=<last>`+resumption tokens | ~400-500 | Workhorse: structured metadata + abstracts for all cs.* papers |
| 2 | arXiv RSS cs.CL | `https://rss.arxiv.org/rss/cs.CL` | RSS 2.0 | none | every 2h | ~65 | NLP/LLM papers, RSS-shape coverage, dedup test fodder vs OAI |
| 3 | arXiv RSS cs.LG | `https://rss.arxiv.org/rss/cs.LG` | RSS 2.0 | none | every 2h | ~126 | Core ML methods |
| 4 | arXiv RSS cs.AI | `https://rss.arxiv.org/rss/cs.AI` | RSS 2.0 | none | every 2h | ~125 | General AI/agents |
| 5 | arXiv RSS cs.CV | `https://rss.arxiv.org/rss/cs.CV` | RSS 2.0 | none | every 2h | needs-measurement (~150) | Vision research |
| 6 | GitHub Public Events (AI-filtered) | `https://api.github.com/events` | REST/JSON | personal token (5000 req/h) | obey `X-Poll-Interval` (~60s); 304s do not count | needs-measurement (hundreds-low-thousands after filter) | Live signal of new code/PRs/releases on AI repos |
| 7 | GitHub Releases Atom (~30 curated repos) | `https://github.com/<org>/<repo>/releases.atom` | Atom | none | every 2h with ETag/`If-None-Match` | 10-50 | High-signal release-note text |
| 8 | Hugging Face Hub models | `https://huggingface.co/api/models?sort=lastModified&direction=-1&limit=100` | REST/JSON | optional bearer (token raises 5-min quota from 500 -> 1000-2500) | every 10-15 min, dedup on `id`+`lastModified` | needs-measurement (hundreds-thousands) | Model cards = engineering artifacts + metadata |
| 9 | HF Daily Papers | `https://huggingface.co/api/daily_papers?sort=publishedAt&limit=100` | REST/JSON | bearer required | every 6h | ~20-50 | Curated frontier papers w/ community signal |
| 10 | AI-lab blog RSS bundle | OpenAI News, DeepMind, HF Blog, BAIR, EleutherAI | RSS | none | every 6h | ~5-10 total | Long-form lab writeups |
| ~~11~~ | ~~FastAPI submit~~ | ~~`POST /submit`~~ | ~~HTTP/JSON~~ | ~~none~~ | ~~on-demand~~ | ~~ad-hoc~~ | **DELETED in v0.2.0** - manual URL submit endpoint removed; the live pollers and seed loader cover demo needs without the abuse surface |

### Curated GitHub repos for Releases Atom (Phase 1)
The canonical 30-repository allowlist lives in `charts/stream2pretrain/values.yaml`. Deployment validation polls every configured repository and fails on removed or renamed upstreams instead of silently treating a 404 as an empty release feed.

### AI-lab blog RSS list (Phase 1)
- `https://openai.com/news/rss.xml`
- `https://deepmind.google/blog/rss.xml`
- `https://huggingface.co/blog/feed.xml`
- `https://bair.berkeley.edu/blog/feed.xml`
- `https://blog.eleuther.ai/index.xml`

## Phase-1.5 Fulltext + Code (v0.2.0)

These four sources land in v0.2.0 alongside the existing Phase-1 pollers. They turn the curator from a metadata-and-blog feed into a real fulltext pipeline (arXiv HTML for the freshest tier, OpenReview for peer-review prose) and a real code pipeline (release tarballs from the curated AI-repo allowlist). All endpoints + rate-limit math from `docs/research-fulltext-and-code.md` (verified 2026-06-15).

| # | Source | Endpoint | Protocol | Auth | Poll interval | Est. docs/day | Why |
|---|---|---|---|---|---|---|---|
| 12 | arXiv native HTML | `https://arxiv.org/html/<arxiv_id>` (fallback `https://ar5iv.labs.arxiv.org/html/<id>` on 404) | HTTPS HTML | none | fan-out from #1 / #2-5; fetch each new id once, conditional-GET via ETag thereafter | ~700-900 (paper bodies, equals Phase-1 abstract count) | Replaces PDF parsing for the fresh tier; ~97% of new submissions covered, ~75% LaTeXML-clean per arxiv.org/pdf/2605.16562 |
| 13 | OpenReview API v2 | `https://api2.openreview.net/notes?invitation=<venue>/-/Submission` + per-note PDF over HTTPS | REST/JSON + HTTPS PDF | none for reads | every 6h between deadlines, every 30 min during ICLR/NeurIPS/ICML/COLM submission windows | needs-measurement (bursty: thousands during conference deadlines) | Peer-review prose + rebuttals. Full review texts unavailable elsewhere |
| 14 | ReviewArena HF dataset | `anonymousNeurIPS2026submission4281/reviewarena` at revision `c2978add17c2099219eaddbc2599974d69d4d09b` | HF datasets streaming | public; optional HF token | one-shot backfill | bounded by `maxRows` | 51,529 papers across seven conference splits; OCR Markdown + structured reviews + rebuttals + decisions, admitted only for derived post-training unless a per-record content license says otherwise |
| 15 | GitHub release tarballs | `https://api.github.com/repos/{o}/{r}/tarball/{tag}` | REST/binary | personal token (5000 req/h shared with #6) | fan-out from #7 (Releases Atom) on every new release | needs-measurement (~30 repos x mean ~5 releases/day x ~50-200 files/release after path+license filter) | Per-file source records under SPDX-permissive license; lands as `source_format=code` on the existing topics |

Notes on rate-limit budget:
- Tarballs share the 5000 req/h GitHub PAT budget with sources #6 and #7. With 30 curated repos and conservative ~5 releases/repo/day the daily fetch count is well under 200 requests, leaving > 4800 req/h headroom.
- arXiv HTML fan-out sits inside the same 4 req/s, 1-second-sleep budget the OAI / RSS pollers use; the queue is bounded by Phase-1's daily new-id count (~700-900).
- OpenReview pagination uses `limit=1000` + `offset`; total page count is bounded by the venue size (typically 5-50k notes). PDF fetch is rate-limited by `respect_x_poll_interval`-style 1 req/s politeness because OpenReview does not publish a hard cap.

## Seed corpus (v0.2.0)

One-shot Bytewax `seed-loader` Job that evaluates a 5-component HF mixture before `docs.normalized`. Dataset wrapper licences do not qualify. Only rows with an explicit allowlisted paper, page, or file licence proceed; every other row is written to the licence quarantine ledger. Per-document `valid_from` is populated from each dataset's native publication-date column so the validity-interval N2 novelty has data to query on day 1.

| # | Component | HF id / source | Bronze GB target | Tokens (B) target | License | Role |
|---|---|---|---|---|---|---|
| S1 | peS2o v3 filtered to cs.* | `allenai/peS2o` (`data/v3/`) | ~50 | needs-measurement (~12-15B; v2 baseline 42.01B at cutoff 2023-01-03) | ODC-By wrapper; per-paper required | AI2 academic backbone; rows without paper rights are quarantined |
| S2 | RedPajama v1 arxiv | `togethercomputer/RedPajama-Data-1T` config `arxiv` | ~92 | ~28 | Apache-2.0 wrapper; per-paper required | LaTeX view; rows without an allowlisted paper licence are quarantined |
| S3 | FineWeb-Edu URL-filtered | `HuggingFaceFW/fineweb-edu` (URL allowlist for arxiv.org / openai.com / deepmind.google / huggingface.co/blog / eleuther.ai / bair.berkeley.edu / lilianweng.github.io / sebastianraschka.com / etc.) | ~50 | needs-measurement (~5-10B post-filter) | ODC-By wrapper; per-page required | Rows without explicit page rights are quarantined |
| S4 | Stack-Edu Python+ML | `HuggingFaceTB/stack-edu` filtered to Python + AI/ML repos | ~80 | needs-measurement (~8-12B; OLMo-3 used ~410B from this dataset) | ODC-By wrapper; per-file SPDX required | Only allowlisted per-file code licences proceed |
| S5 | Custom Wayback backfill | arXiv OAI `from=2024-06-01`, GitHub Releases Atom history, HF Daily Papers `before=...`, AI-lab blogs via Wayback Machine | ~5-10 | ~1 | explicit per-source or per-record licence required | Validates streaming backfill time semantics; unlicensed archive rows are quarantined |

The original discovery target was ~275-280 GB and ~55-65B tokens. Retained
volume after strict per-record licence admission is `needs-measurement`. The
seed loader otherwise rides the same `docs.normalized` topic and Silver/Gold
operators as live content, so seeding remains a backfill mode rather than a
parallel policy path.

## Phase-2 Expansion Set (breadth, plug-in after Week 6)

| Source | Endpoint | Notes |
|---|---|---|
| Remaining arXiv categories | `rss.arxiv.org/rss/<cat>` | stat.ML, cs.IR, cs.CR, cs.DC, cs.CY, cs.MA, cs.RO |
| HF Datasets | `huggingface.co/api/datasets?sort=lastModified` | Same envelope as models |
| HF Spaces | `huggingface.co/api/spaces?sort=lastModified` | Mostly UI code, lower training-token value |
| OpenReview API v2 | `https://api2.openreview.net/notes?invitation=<venue>/-/Submission` | Bursty during ICLR/NeurIPS/ICML/COLM windows; thousands during deadlines |
| Semantic Scholar Graph | `https://api.semanticscholar.org/graph/v1/paper/search/bulk` | API key recommended; baseline 1 RPS authed |
| Papers With Code | `https://paperswithcode.com/api/v1/papers/` | ~50-200 new entries/day; verify uptime |
| GitHub READMEs | `https://api.github.com/repos/<o>/<r>/readme` | Fan-out from events/releases queue, license-filtered (`license.spdx_id` in OSI-approved list) |
| Long-tail blogs | RSS each | Lil'Log, Sebastian Raschka, The Gradient, AI2, Anthropic news (community feed), Meta AI (community feed) |
| Alignment Forum | `https://greaterwrong.com/index.rss?view=alignment-forum` | Deliberate noisy-input case for the quality classifier |

## Out of Scope

| Source | Reason |
|---|---|
| GitHub Trending (`github.com/trending`) | No official API; site policy permits research scraping only at low volume; safer to skip |
| Distill.pub | Archived 2021, not "fresh" |
| Connected Papers / Litmaps | No documented free API |
| YouTube transcripts (youtube-transcript-api) | Violates YouTube ToS at scale |
| Twitter/X | Free API tier removed |
| Reddit r/MachineLearning | Reddit API paywalled for high volume |
| Substack scraping past paywall | Stick to first-party RSS only |
| Full arXiv PDF fetching | Bandwidth + parsing latency exceeds 2-worker demo; in Phase 2 do it on-demand for a quality-filtered subset only |
| Closed-access proceedings (IEEE, Springer) | Paid APIs |
| Anthropic / Meta AI first-party RSS | Confirmed not to exist; use community feeds in Phase 2 with caveat |

## Big-Data V's Mapping

**Volume**: Phase-1 mix targets 5-20k docs/day. arXiv OAI ~500, RSS ~470, HF Hub hundreds-thousands, GitHub events hundreds-low-thousands after filter, blog bundle ~10. Phase-2 with Semantic Scholar + OpenReview burst-fills past 50k/day during conference seasons - well within the 10k-100k assignment target.

**Velocity**: cadences span four orders of magnitude. GitHub events at the 60s `X-Poll-Interval` floor; HF Hub at 10 min; arXiv OAI/RSS at 2h (bound by arXiv's once-daily rebuild); curated blog feeds at 6-24h. Single Redpanda topology must handle bursty + slow polling simultaneously.

**Variety**: protocol mix covers OAI-PMH XML with resumption tokens, RSS 2.0/Atom, REST/JSON, and HTML. Content shapes cover paper abstracts, model cards, code release notes, blog long-form, and event metadata - exactly the heterogeneity a curation pipeline must demonstrate.

**Veracity**: arXiv, OpenReview, Semantic Scholar, and major-lab blogs are high-trust. GitHub events and HF Hub are noisy (typo-fixes, auto-bumps, low-effort spaces) and need quality scoring + dedup. LessWrong/Alignment Forum (Phase 2) is the deliberate noisy-input case to demonstrate the quality classifier earning its keep.

**Value**: highest training-token value comes from arXiv abstracts/PDFs (peer-adjacent dense technical prose), curated lab blog posts, GitHub READMEs/release-notes for OSI-licensed AI repos, and OpenReview rebuttals (unique critical discourse not available elsewhere).

## Rate-Limit + Politeness Cheatsheet

| Source | Rate cap | Cluster strategy |
|---|---|---|
| arXiv (OAI + RSS + abstracts/PDFs) | 4 req/s burst, 1 sec sleep recommended | Single fetcher pod per arXiv source; use `export.arxiv.org` host; resumption tokens expire daily |
| HF Hub (anon) | 500 req / 5-min window | Stay under by polling `lastModified` once per 10-15 min |
| HF Hub (free token) | 1000 req / 5-min | Token in Secret; bump poll to 5 min if needed |
| HF Hub (PRO) | 2500 req / 5-min | Not needed for demo |
| GitHub REST anon | 60 req/h | Unusable - always use a token |
| GitHub REST authed | 5000 req/h | Single PAT in Secret; 304 responses don't count |
| GitHub Events | use `X-Poll-Interval` header verbatim | Typically 60s; honour it strictly |
| OpenReview v2 | page size cap ~1000 | Paginate with `offset`; venues split between v1 and v2 |
| Semantic Scholar | shared 1 RPS pool unauth, 1 RPS dedicated authed | API key in Secret; request larger limit on review |

## Citations

- arXiv RSS: https://info.arxiv.org/help/rss.html
- arXiv OAI-PMH: https://info.arxiv.org/help/oa/index.html
- arXiv bulk-data + 4 req/s guidance: https://info.arxiv.org/help/bulk_data.html
- HF Hub rate limits: https://huggingface.co/docs/hub/en/rate-limits
- HF API reference: https://huggingface.co/docs/huggingface_hub/en/package_reference/hf_api
- GitHub REST rate limits: https://docs.github.com/en/rest/using-the-rest-api/rate-limits-for-the-rest-api
- GitHub Events: https://docs.github.com/en/rest/activity/events
- GitHub site policy on scraping: https://github.com/github/site-policy/issues/56
- OpenReview API v2: https://docs.openreview.net/reference/api-v2
- Semantic Scholar API: https://www.semanticscholar.org/product/api
- arXiv volume figures: https://blog.arxiv.org/2024/11/04
