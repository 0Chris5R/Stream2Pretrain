# Source processing and classifier policy

Status: implementation contract  
Resolver: `processor/source_policy.py`  
Extraction: `processor/fetcher.py` and `processor/scientific.py`  
Curation: `processor/curate.py`

This is the exhaustive source-to-processing matrix for every source configured
in `SOURCES.md`, Helm values, ingest workloads, and the seed loader. Discovery
metadata and trainable content are different artifacts. A list response, RSS
entry, OAI record, GitHub event, or release envelope can schedule content work,
but it is never itself converted into training text.

All trainable profiles retain the shared item-level licence gate,
Presidio/regex privacy scan, exact and MinHash near deduplication, E5
decontamination, validity interval, and immutable provenance. English language
identification gates natural-language profiles. Source code uses its file
language and does not treat natural-language ID as a programming-language
classifier.
Operations marked not applicable are intentionally absent rather than reported
as artificial zero-valued classifier outputs.

## Live and backfill source matrix

| Configured source | Discovery artifact | Trainable artifact and extraction | Grounded quality policy | Not applicable | Route and Gold reachability | Contract tests |
|---|---|---|---|---|---|---|
| `rss-arxiv-cs-cl`, `rss-arxiv-cs-lg`, `rss-arxiv-cs-ai`, `rss-arxiv-cs-cv` | RSS entry carrying arXiv id and per-item licence evidence | Native arXiv HTML through the `arxiv-html-fetcher`; ar5iv HTML fallback; bounded Docling CPU PDF fallback; scientific sections, tables, equations, captions and figures | FinePDFs Edu v2 at `90ddef285f67230389057c14b2f6bbfeb70d40ea`; FineWeb-Edu comparison is audit-only | Web C4/Gopher gates and KenLM rejection | RSS entry never reaches Gold. Its full paper can reach pretraining under an allowlisted verbatim licence or post-training only under an allowed transform-only licence. | `ingest/arxiv_html_fetcher/tests/test_fetcher.py`, `processor/tests/test_source_policy.py`, scientific tests |
| `arxiv-oai-cs` / `oai-arxiv-cs` | OAI-PMH XML metadata and arXiv id | Same full-paper fan-out as arXiv RSS | Same scientific policy | Metadata educational score, direct Gold row | OAI metadata never reaches Gold. The scheduled full paper can reach Gold under the same item-level licence rules. | OAI poller tests, arXiv source-filter test, source-policy test |
| `arxiv-html-fetcher` and optional id backfill | Exact arXiv id | Native HTML, ar5iv fallback, then bounded PDF fallback | FinePDFs Edu v2 | FineWeb web threshold, C4/Gopher hard gate, KenLM hard gate | Can reach Gold. Extraction failure routes to retry; licence, privacy, duplicate or contamination failures quarantine. | arXiv fetcher/extractor tests, processor scientific tests |
| `rss-openai-news`, `rss-deepmind`, `rss-deepmind-blog`, `rss-hf-blog`, `rss-bair`, `rss-bair-blog`, `rss-eleuther`, `rss-eleuther-blog` | RSS entry | Fetched page body through Resiliparse main-content extraction | FineWeb-Edu at `284663cbb2dabf9bda30d8f8cc49601251ee1631`; official score 3 boundary for ordinary web prose; DataTrove FineWeb/Gopher/C4 recipe | FinePDFs, scientific structure, code syntax | Can reach Gold only when the RSS item or bounded page probe supplies allowlisted item-level rights. `licenseDefault: per-record` is a resolver instruction, not a licence grant; unresolved pages quarantine before full fetch. | RSS/common Bronze and page-licence tests, source-policy test, web curate tests |
| `sitemap-poller` and any configured sitemap feed | Sitemap URL/last-modified discovery | Each discovered licensed page through Resiliparse | Same web policy | Scientific and code metrics | The workload is disabled with an empty feed list by default. A configured page reaches Gold only with qualifying content licence evidence. | sitemap poller tests, source-policy test |
| `github-events` | Filtered `ReleaseEvent` with repository and exact tag | None directly. It emits only a durable `github.release.jobs` message. | None | Every text-quality, privacy, dedup and decon operation | Never reaches Gold by design. It schedules the release tarball worker. | GitHub Events tests |
| `github-releases` | Curated repository Atom entry with exact release ref | None directly. It emits only a durable `github.release.jobs` message. | None | Every text-quality, privacy, dedup and decon operation | Never reaches Gold by design. Per-ref and per-file licence admission occurs in the tarball worker. | GitHub Releases tests |
| `github-release-tarballs` source files | Release ref and repository licence lookup, followed by per-file SPDX header override | UTF-8, non-binary, non-generated, non-vendored, allowlisted source files; one Bronze row per file | Stack v2 and Dolma-grounded rules: generated/vendor path, average line length 100, maximum line 1,000, alphanumeric fraction 0.25, alphabetic characters per token 1.5, syntax signal, and credential patterns | FinePDFs, FineWeb-Edu, Gopher, C4, KenLM, and natural-language ID gating | Permissively licensed files can reach pretraining after code-quality, secret, privacy, duplicate and decon gates. The current Foundry is paper-specific, so code is not labelled as a runnable post-training artifact. | tarball extractor/fetcher tests, code-quality tests, source-policy test |
| `github-release-tarballs` README/docs | `.md`, `.rst`, and `.txt` files from an admitted release | YAML/front matter and fenced code removed; narrative Markdown/text retained | FineWeb-Edu as an audit signal on documentation prose; Common-Crawl page-shape threshold is not a hard gate | FinePDFs, code syntax, web C4/Gopher/KenLM gates | Can reach Gold under the same per-file/ref licence. This is a separate repository-documentation profile, not code. | tarball mixed-format test, Markdown projection test, source-policy test |
| `hf-models` | `/api/models` list item | Exact-revision `README.md` model card after card licence admission; front matter and fenced code excluded | FineWeb-Edu audit signal on card prose | FinePDFs, metadata score, web page-shape and KenLM gates | Can reach Gold only when the exact revision and card licence are present and allowlisted. Mutable/unresolved cards fail closed. | HF poller tests, Markdown projection test, source-policy test |
| `hf-datasets` | `/api/datasets` list item | Exact-revision dataset card README, processed as card prose | Same card policy | Same non-applicable metrics as model cards | Can reach Gold only with exact revision and allowlisted card/content licence. Dataset card rights do not establish rights in dataset rows. | HF dataset-card poller test, source-policy test |
| `hf-spaces` | `/api/spaces` list item | Exact-revision Space card README only; Space repository code is not fetched | Same card policy | Same non-applicable metrics; Space code classifier is not claimed | Can reach Gold only with exact revision and allowlisted card licence. | HF card poller test scaffolding, source-policy test |
| `hf-daily-papers` | Daily Papers JSON and arXiv id | No direct text. It fans out to the same native arXiv full-paper path. | Scientific policy on the resulting paper | Metadata educational score and direct Gold row | Daily Papers metadata never reaches Gold. The resulting arXiv paper can reach Gold under its per-paper licence. | HF Daily Papers tests, arXiv source-filter test, source-policy test |
| `openreview` (`openreview-live` legacy alias) submission paper | OpenReview Note and invitation schema | Public PDF through bounded Docling CPU extraction | FinePDFs Edu v2 | Web page gates and KenLM rejection | Can reach Gold when the exact note/paper licence permits. Transform-only papers can only become post-training candidates. | OpenReview live tests, scientific and source-policy tests |
| `openreview` (`openreview-live` legacy alias) official reviews, meta-reviews, rebuttals and responses | Public Note with invitation, forum, rating/confidence metadata | Only substantive public form fields. Rating, confidence, recommendation and decision remain audit metadata and are not training labels. Generic comments with only a `comment` field are rejected as `insubstantial_review`; recognized official review/response Invitations remain eligible without an invented word-count threshold. | `openreview-schema-completeness-v1`, a count of represented form-field families; no sentiment/acceptance inference | FinePDFs, FineWeb-Edu, Gopher, C4 and KenLM | A permissively licensed substantive review may enter pretraining. Reviews are not labelled as paper Foundry candidates; a future review-specific Foundry requires its own artifact contract. Transform-only reviews remain quarantined until that contract exists. | OpenReview live tests, review projection, source-quality and curate tests |
| `openreview-backfill` / ReviewArena paper | Dataset row used to locate OCR Markdown and item rights | Scientific Markdown through the structured scientific extractor | FinePDFs Edu v2 | Web page gates and KenLM rejection | Per-item permissive content can reach pretraining. Missing/grey item rights can only use the explicitly allowed transform-only path; excluded rights quarantine. | OpenReview backfill tests, scientific text tests, source-policy test |
| `openreview-backfill` / ReviewArena review | Dataset row with review and paper linkage | Review text through the same public-field projection where fields exist | OpenReview schema completeness | PDF/web/code metrics | The same substantive-form and licence rules apply: permissive content may reach pretraining. Transform-only review content remains quarantined until a review-specific Foundry contract exists. | OpenReview backfill, source-policy and curate tests |

## Seed and historical source matrix

| Seed component | Trainable projection | Policy | Gold reachability |
|---|---|---|---|
| `seed:allenai/peS2o` | Scientific paper body from the peS2o row | Structured scientific extraction plus FinePDFs Edu v2 | Only rows with qualifying per-paper rights reach Gold. The ODC-By wrapper is not sufficient. Direct-to-Silver seed rows do not manufacture a paper Foundry artifact. |
| `seed:togethercomputer/RedPajama-Data-1T` `arxiv` | LaTeX-derived scientific body | Scientific extraction plus FinePDFs Edu v2 | Only rows with qualifying per-paper rights reach Gold. The dataset wrapper is not sufficient. Direct-to-Silver seed rows do not manufacture a paper Foundry artifact. |
| `seed:HuggingFaceFW/fineweb-edu` | URL-allowlisted page text | FineWeb/DataTrove web profile | Only rows with qualifying per-page rights reach Gold. The ODC-By wrapper is not sufficient. |
| `seed:HuggingFaceTB/stack-edu` | Python/ML source file | Stack v2/Dolma code profile | Only rows with per-file or exact repository-ref permissive SPDX evidence reach Gold. The wrapper is not sufficient. |
| `seed:wayback` | Historical page or scientific artifact selected by original source identity | Web, scientific, code, or discovery profile through the same resolver as live data | Only records with original item-level or audited source-wide content rights reach Gold. Archive presence is not licence evidence. |

Seed loaders must preserve the original source format and extraction pipeline.
The `seed:` prefix is provenance, not a classifier override.

## Exact models, CPU runtime, and licences

| Component | Exact artifact | CPU status | Licence and integration decision |
|---|---|---|---|
| Scientific quality | `HuggingFaceFW/finepdfs_edu_classifier_v2_eng_Latn`, revision `90ddef285f67230389057c14b2f6bbfeb70d40ea` | Transformers CPU is implemented; latency/throughput on the target cluster is `needs-measurement`. ONNX/OpenVINO INT8 is an optional optimization. | Model Apache-2.0. The upstream FinePDFs code repository is AGPL-3.0, so Stream2Pretrain consumes the model and independently implements the contract rather than copying AGPL code. |
| Web/card quality | `HuggingFaceFW/fineweb-edu-classifier`, revision `284663cbb2dabf9bda30d8f8cc49601251ee1631` | Transformers CPU is implemented; cluster throughput is `needs-measurement`. | Model Apache-2.0. Ordinary web uses the model-card score 3 boundary. Cards/docs retain it as a signal because that threshold was not validated for structured Markdown. |
| Scientific extraction | Docling `2.114.0`, Tesseract English, Docling figure classifier revision `f859dfbff5c9916cd996942d4b0db7fa25808220` | CPU paths are implemented. Native arXiv HTML preserves source LaTeX. The bounded PDF fallback retains layout, text, tables, figures, and OCR, uses TableFormer FAST with cell matching after ACCURATE exceeded the 2 GiB worker limit, and disables the optional CodeFormulaV2 image-to-formula VLM because it exceeds that envelope. FAST peak RSS is `needs-measurement`. | Docling MIT, Tesseract Apache-2.0. OCR text is audit-only unless the structured extraction policy admits the source-authored surrogate. |
| Web extraction/filter recipe | Resiliparse plus pinned local FineWeb-style rules | CPU-native. | Resiliparse Apache-2.0. DataTrove is Apache-2.0. The repository uses the official FineWeb `0.12` punctuation signal and does not restore C4 terminal-punctuation as a universal gate. |
| Code quality | Local `stack-v2-dolma-code-rules-v2` implementation | CPU-native. | Grounded in Apache-2.0 BigCode and Dolma repositories. Python Edu Scorer is optional future work because it covers Python only and requires separate model packaging/calibration. |
| Peer reviews | `openreview-schema-completeness-v1` | CPU-native deterministic parsing. | OpenReview public Note/Invitation schemas are the authority. No credible general public review-quality classifier was found, so the project does not invent one. |
| Privacy | Presidio plus deterministic PII/secret patterns | CPU-native. | Presidio MIT. Code-specific `detect-secrets`/BigCode `pii-lib` remains an optional dependency until packaged and cloud-validated; the current hard secret patterns cover common provider keys and private-key blocks. |
| Decontamination | `intfloat/e5-small-v2`, revision `ffb93f3bd4047442299a41ebb6fa998a38507c52`, plus exact n-grams | ONNX Runtime CPU is implemented. | MIT model. Runs on every trainable text projection, including code/reviews/cards, against the versioned benchmark reserve. |
| KenLM | `edugp/kenlm`, revision `3fbe35c83b1a39f420a345b7c96a186c8030d834` | mmap CPU is implemented. | Used as a gate only for ordinary web prose. It is off for scientific, code, review, metadata and structured card/documentation profiles. |

## Required versus optional components

The required cloud path is intentionally small and source-grounded:

- FinePDFs Edu v2 for structured scientific bodies;
- FineWeb-Edu plus the official DataTrove web recipe for ordinary web prose;
- the local Stack v2/Dolma code rules and credential patterns for source code;
- OpenReview form and Invitation rules for public review artifacts;
- Resiliparse/Markdown/scientific extraction, language ID, privacy, exact and
  near deduplication, E5 benchmark decontamination, and item-level provenance.

Optional additions do not block this remediation:

- BigCode `pii-lib` or Yelp `detect-secrets` can broaden code-secret recall
  after packaging and canary calibration. Both are CPU-capable and
  Apache-2.0, but adding either changes dependency and false-positive behavior.
- `allenai/specter2_base` is an Apache-2.0 CPU-capable scientific embedding
  backbone for future research-topic clustering. It is not a quality gate and
  needs a separately reviewed taxonomy and pinned adapter before integration.
- An ONNX/OpenVINO INT8 export can reduce classifier CPU cost only after a
  same-sample parity test against the pinned Transformers checkpoints.
- A learned Python educational scorer would cover only one code language and
  therefore cannot replace the language-agnostic Stack/Dolma baseline.

## Sources that do not reach Gold directly

- arXiv RSS/OAI, Hugging Face Daily Papers, GitHub Events, and GitHub Releases
  are discovery-only by design. Their scheduled paper, card, or release-file
  artifact may reach Gold independently.
- Generic sitemap ingest is disabled until a feed is configured.
- Lab-blog pages without recognized item-level page rights quarantine before
  the full-body fetch.
- Hub cards without an immutable revision or qualifying card licence
  quarantine. Dataset-card rights never authorize the dataset rows.
- Seed rows without original item-level rights quarantine even when the
  dataset wrapper itself has a licence.

Every other configured content adapter has a path to Gold under its item-level
licence and source-specific quality policy. Cloud validation must measure the
actual acceptance yield.

## Primary sources

- FinePDFs pipeline and model: <https://github.com/huggingface/finepdfs>, <https://huggingface.co/HuggingFaceFW/finepdfs_edu_classifier_v2_eng_Latn>
- FineWeb and official DataTrove recipe: <https://arxiv.org/abs/2406.17557>, <https://github.com/huggingface/datatrove/blob/main/examples/fineweb.py>, <https://huggingface.co/HuggingFaceFW/fineweb-edu-classifier>
- Stack v2 and BigCode filters: <https://huggingface.co/datasets/bigcode/the-stack-v2-train-smol-ids/blob/main/README.md>, <https://github.com/bigcode-project/bigcode-dataset>
- Dolma taggers: <https://github.com/allenai/dolma/blob/main/docs/taggers.md>
- Hugging Face card schemas and API: <https://huggingface.co/docs/hub/model-cards>, <https://huggingface.co/docs/hub/datasets-cards>, <https://huggingface.co/docs/huggingface_hub/package_reference/cards>
- OpenReview Notes and retrieval: <https://docs.openreview.net/getting-started/using-the-api/objects-in-openreview/introduction-to-notes>, <https://docs.openreview.net/how-to-guides/data-retrieval-and-modification/how-to-get-all-notes-for-submissions-reviews-rebuttals-etc>
- Presidio: <https://microsoft.github.io/presidio/analyzer/>
- BigCode code-PII pipeline and Yelp secret scanner: <https://github.com/bigcode-project/pii-lib>, <https://github.com/Yelp/detect-secrets>
- SPECTER2 model card: <https://huggingface.co/allenai/specter2_base>

## Cloud validation still required

No local service, model, container, test suite, Kubernetes command, or pipeline
was run for this remediation. The deployment must validate:

1. Helm schema/template rendering with the new Hub dataset and Space entries.
2. Deployment rollout and ServiceMonitor discovery under the renamed
   `ingest-hf-cards` component.
3. Hub list API response shape, exact SHA availability, card licence fields,
   and exact-revision README fetches for models, datasets, and Spaces.
4. Full arXiv fan-out from each RSS category, OAI, Daily Papers, and the
   explicit backfill path without metadata Gold rows or duplicate paper rows.
5. One admitted and one quarantined lab-blog/sitemap page through the bounded
   page-licence probe and the ordinary-web classifier path.
6. One mixed GitHub release containing source code, generated/vendor paths,
   undecodable/binary content, credentials, and Markdown/RST/TXT documentation.
7. OpenReview invitation-specific extraction for a paper, official review,
   meta-review, rebuttal, generic public comment, and the ReviewArena paper and
   review projections across the configured venues/splits.
8. One row from each peS2o, RedPajama arXiv, FineWeb-Edu, Stack-Edu, and
   Wayback seed component, verifying original per-item rights and the same live
   source policy.
9. Gold/decision records for each trainable profile, checking the exact
   classifier revision, `not-applicable` provenance, route, reason, and Sources
   cockpit label.
10. Per-source acceptance distributions and false-positive review on a
    labelled sample. Any unmeasured throughput or yield remains
    `needs-measurement`.
11. Code secret recall against an approved canary set before adding the
    optional BigCode `pii-lib` or `detect-secrets` dependency.
