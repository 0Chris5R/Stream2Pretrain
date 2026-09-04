# Source processing policy

[The licence matrix](SOURCE_LICENSE_ADMISSION_MATRIX.md) defines admission.
`processor/source_policy.py` dispatches extraction and curation; discovery
metadata never becomes a corpus row.

| Source | Projection | Processing | Output |
|---|---|---|---|
| arXiv | Native HTML, ar5iv fallback, CPU Docling PDF fallback | Scientific cleanup; language, privacy, template and duplicate checks; arXiv quality mean >=3.0; both auxiliary heads after quality passes | Permissive pretraining text and eligible paper Foundry candidates |
| HF model cards | Public root README at an immutable blob/commit | YAML, code and asset removal; model-card content gate, language, privacy and duplicate checks; HF quality mean >=3.5 | Technical pretraining prose only |
| HF dataset cards | Public root README at an immutable blob/commit | Dataset-card projection and content gate, language, privacy and duplicate checks; HF quality mean >=3.5 | Dataset documentation prose only |

arXiv RSS categories and OAI-PMH records schedule the canonical full-paper
worker. Hub list pages schedule exact README revisions. They have no corpus
acceptance, rejection or training route of their own.

## Applicability

All three content sources retain validity intervals, exact item provenance,
language checks, privacy scanning, exact hashes and MinHash deduplication.
Gopher, C4 and KenLM are not applied to paper or card prose. The generic web
profile exists for controlled integration fixtures and requires its own source
policy before a new source is enabled.

Scientific projections preserve retained headings, prose, equations, structured
tables, captions and policy-approved OCR. Authors, references, acknowledgements,
navigation, duplicate tables and publication templates are excluded. Card
projections exclude hosted weights, dataset rows, linked assets and binaries.

## Decisions and ranking

Every retained section is scored completely under [the classifier input
contract](CLASSIFIERS.md). Source quality gates whole documents, not individual
sections. The math and post-training heads only run on quality-passing arXiv
papers. Mean suitability ranks the daily cohort; section maxima only guide
optional task-designer hints. The paper input remains unchanged.

A permissive item can reach pretraining. A paper restricted to derived use can
reach only post-training, if it satisfies the same quality and evidence checks.
HF cards are not Foundry candidates. Explicit incompatible rights or blocking
privacy/quality findings quarantine. Recoverable scientific extraction failures
use the bounded retry route.

Policy and classifier revisions are retained in each record. Deduplication
anchors are namespaced by content generation. The UI and corpus exports use
the latest decision per document across all generations, with the current
licence/export eligibility checks, not a hidden policy-version filter.
