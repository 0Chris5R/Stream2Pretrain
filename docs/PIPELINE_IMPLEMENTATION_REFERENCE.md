# Exact pipeline implementation reference

This document describes the current implementation, not an intended future
design. File and function names are included so every statement can be checked
against code. Prompt templates are reproduced verbatim. In the templates,
capitalized blocks such as `PAPER_BUNDLE` are the exact canonical JSON values
inserted at runtime. `REQUIRED_JSON_SCHEMA` is the exact output of
`canonical_json(Model.model_json_schema()).decode()`.

## 1. Active source and licence boundary

The active content products are full arXiv papers, exact-revision Hugging Face
model-card README prose, and exact-revision Hugging Face dataset-card README
prose. arXiv RSS and OAI records are internal scheduling envelopes. They do not
become documents, decisions, or corpus rows.

`ingest/common/license_admission.py` normalizes Creative Commons URLs and common
aliases, then applies three item-level outcomes:

- `admitted`: `CC-BY-4.0`, `CC-BY-SA-4.0`, `CC-BY-3.0`, `CC-BY-SA-3.0`,
  `CC0-1.0`, `Apache-2.0`, `MIT`, `BSD-2-Clause`, `BSD-3-Clause`, `MPL-2.0`,
  `ISC`, `Unlicense`, or `HF-Public-Repository-Terms-2022-09-15`;
- `posttrain_transform_only`: arXiv non-exclusive distribution,
  `CC-BY-NC[-SA]-3.0/4.0`, missing item rights, or wrapper-only `ODC-By-1.0`;
- `quarantined`: every other explicit licence, including no-derivatives terms.

The admission decision hashes the canonical URL/doc id, source, format,
normalized licence, evidence resolver/URL/revision/scope, status, and
`license-policy-2026-08-25`. Explicitly incompatible items stop before retained
body fetch. Missing or grey-area rights may be fetched only to ground derived
post-training output and can never enter verbatim pretraining export.

For Hugging Face, `HF-Public-Repository-Terms-2022-09-15` applies only to the
exact-revision public README card. It does not admit weights, dataset rows,
binaries, or other repository files.

## 2. Source projection

### 2.1 arXiv

The full-text worker tries native `https://arxiv.org/html/<id>`, then ar5iv, then
the bounded PDF path. It validates arXiv ids with:

```regex
^(?:[a-z\-]+/)?\d{4}\.\d{4,6}(?:v\d+)?$|^[a-z\-]+/\d{7}(?:v\d+)?$
```

Native HTML and LaTeX extraction preserve sections, display equations, semantic
tables, figures, captions, citations, stable ids, and provenance. Heading roles
are assigned by these exact patterns:

- references: `\b(references|bibliography)\b`;
- acknowledgements: `acknowledg|author contribution|funding|conflict of interest|declaration`;
- background: `background|related work|preliminar`;
- methods: `method|approach|experimental setup|materials|implementation`;
- results: `result|evaluation|experiment|analysis|finding`;
- limitations: `limitation|threats to validity`;
- direct string tests cover abstract, introduction, discussion, appendix,
  conclusion, summary, and future work.

Author/front matter, references, acknowledgements, ethics statements, author
contributions, conflicts of interest, and declarations stay in the structured
artifact for provenance but are excluded from training text. The projection is
title, included heading-delimited sections, then bounded structured surrogates:
at most 64 tables, 40 rows per table, 20 cells per row, 400 characters per cell,
and 128 display equations of at most 2,000 characters each. Figure captions,
alt text, and type are included. OCR is not included unless the figure has
`ocr_training_eligible=true`; its default is false.

PDF fallback uses Docling 2.114.0 on CPU, Tesseract English OCR, TableFormer FAST
with cell matching, picture images at scale 1.5, no page images, and no formula
vision enrichment. Defaults are two CPU threads, 180-second document timeout,
50 MiB PDF cap, no normal page limit, and no normal figure-count limit. Tesseract
uses `lang="eng"`, `--psm 6`, a 20-second timeout, and keeps at most 8,000 cleaned
characters per figure. The figure classifier is
`docling-project/DocumentFigureClassifier-v2.5` revision
`f859dfbff5c9916cd996942d4b0db7fa25808220`. Its ONNX CPU wrapper resizes RGB
images to 224x224, divides by 255, normalizes with mean
`[0.485,0.456,0.406]` and standard deviation
`[0.47853944,0.4732864,0.47434163]`, then records the argmax softmax label and
confidence. Label order is exactly: `logo`, `photograph`, `icon`,
`engineering_drawing`, `line_chart`, `bar_chart`, `other`, `table`,
`flow_chart`, `screenshot_from_computer`, `signature`,
`screenshot_from_manual`, `geographical_map`, `pie_chart`, `page_thumbnail`,
`stamp`, `music`, `calendar`, `qr_code`, `bar_code`, `full_page_image`,
`scatter_plot`, `chemistry_structure`, `topographical_map`,
`crossword_puzzle`, `box_plot`. Figure payloads are capped at 10 MiB and Pillow
decompression-bomb warnings are errors. Any figure/page limit or figure
enrichment failure marks the scientific extraction incomplete and routes it to
bounded retry rather than accepting a partial paper.

### 2.2 Hugging Face model and dataset cards

Only the root README at an exact API commit SHA is fetched. The list API is
followed through `Link: rel=next` until the previous durable `lastModified`
watermark is crossed; repository id and commit SHA make timestamp ties
deterministic. The watermark advances only after the traversal succeeds, and
mid-scan progress resumes after failure. README Git-blob identity, retained
per repository without count truncation, defines corpus revisions: a weights
or data commit with unchanged README bytes emits nothing. The exact repository
commit and README content SHA-256 remain provenance. The Markdown projection
removes front matter, fenced code, multi-line HTML comments, HTML
tags, link destinations, image destinations, and list markers. Link text is
kept. Headings define sections, the first heading supplies a schema-bounded
title, a duplicate first title is not emitted twice, and unfilled sections are
removed independently. Dataset rows and hosted files never enter the corpus.
The exact inline Markdown substitutions are
`!?\[([^\]]*)\]\([^)]*\) -> captured link text`, `<[^>]+> -> space`, and
`^[-*+]\s+ -> empty`; whitespace is then collapsed with `" ".join(split())`.
Fences start and end only on stripped lines beginning with three backticks or
three tildes. YAML front matter is recognized only when line one is `---` and
ends at `---` or `...`. HTML comments are removed across line boundaries.

The deterministic card gate accepts a card only when all negative conditions
are false and either `(overview AND expected detail section)` or at least two
technical dimensions are present. The technical dimensions and exact markers
are in `processor/operators/hf_card_quality.py`: architecture, training,
evaluation, data, usage, limitations, artifact format, and runtime. The exact
negative marker groups are:

```text
PLACEHOLDERS:
more information needed
provide a longer summary
developers should write
content goes here
fill in this section
insert description
todo:
tbd

UPLOAD_STUBS:
uploaded model
uploaded with
checkpoint converted
automatic model card
this is a model card
this should be a paper title
static quants of
weighted/imatrix quants of

MARKETING:
revolutionary
best model ever
state-of-the-art solution for everyone
download now
join our discord
contact us for pricing

GENERIC_BENCHMARK_MARKERS:
model1-v2
other leading models
benchmark evaluations, including mathematics, programming, and general logic

TRAINER_TEMPLATE_MARKERS:
this model is a fine-tuned version of
it has been trained using trl
framework versions
cite trl as
```

A generic unidentified evaluation is rejected when at least two generic
markers occur. A trainer template is rejected when at least two trainer markers
occur and no architecture/evaluation/usage/limitations dimension supplies
independent technical content. Marketing rejects only when the card is not
otherwise technically grounded. Model cards that only have dataset headings,
and dataset cards that only have model headings, reject as wrong repository
type.

## 3. Curation and deterministic checks

All checks run on retained, section-level text, then on the rebuilt export
projection where appropriate.

### 3.1 Quality models

Four independent ModernBERT-base heads are checksum-pinned in
`processor/source-classifiers.json`. CPU FP32 inference uses Transformers
4.57.6, Safetensors and SDPA. Strict profiles require the real artifacts.

Input is `[SOURCE=arxiv|hf] [SECTION_TYPE=...] [SECTION_TITLE=...]`,
newline, full sanitized section text. All 8,192-token windows with 512-token
overlap are scored. Window logits are averaged before softmax. With six bins,
score is `sum(i*p[i])`, class is the rounded score, and confidence is
`1-entropy(p)/log(6)`.

Document quality is the token-weighted section mean, with encoded lengths minus
overlap as weights. arXiv gates at 3.0 and HF at 3.5. Only quality-passing arXiv
papers run `arxiv-math-reasoning` and `arxiv-posttrain-suitability` on every
retained section. Mean suitability ranks candidates. Maxima and high sections
only supply optional task-designer hints, never section-only generator input.
See [Classifiers](CLASSIFIERS.md) for the full contract.

Corpus token counts use tiktoken `cl100k_base`. Strict mode rejects a missing
or failed tokenizer.

### 3.2 Language and web-only heuristics

Production language identification is `fastlangid.LID().predict(text,
prob=True)`, recorded as `fastlangid-1`. The worker sends the complete training
projection; inputs shorter than 64 characters use the development heuristic
only when fallbacks are permitted. The gate requires `lang == "en"` and
confidence at least 0.5. Strict production startup rejects the fallback. The
non-strict fallback tests English, German, French, and Spanish stopword-set
overlap, multiplies the best overlap by five, caps confidence at 0.9, and emits
`und,0.0` when no language has evidence.

Gopher and C4 are hard gates only for ordinary web prose. Their diagnostics are
still recorded for other source families. Gopher requires:

- 50 to 100,000 words;
- mean word length 3.0 to 10.0;
- symbol-word ratio at most 0.10, where symbols are `#@&*<>{}[]\`;
- alpha-word ratio at least 0.80;
- at least two hits from the embedded 50-word English stopword set;
- bullet-line ratio at most 0.90;
- ellipsis-ending-line ratio at most 0.30.

C4-style diagnostics use sentence terminators `. ! ? " ” ’ )`. The line
punctuation fraction threshold is 0.12. Any `{` or `}` fails the curly-brace
signal. Placeholder detection counts `lorem ipsum`, `dolor sit amet`, and
`consectetur adipiscing elit`; one hit fails a section of at most 40 words,
while longer text requires at least three total hits.

KenLM is off for scientific papers and HF cards. Ordinary web prose uses
`edugp/kenlm` revision `3fbe35c83b1a39f420a345b7c96a186c8030d834`, the
English Wikipedia `en.arpa.bin`, and `en.sp.model` after digit-to-zero,
Unicode-punctuation, and control-character normalization. Buckets are head at
perplexity <= 200, middle <= 1,000, and tail above 1,000. It rejects only when
at least 75 percent of measured retained sections are tail and weighted-median
perplexity exceeds 2,000.

### 3.3 PII and secrets

Presidio is configured with English spaCy and only credit-card, email, IP,
phone, US passport, and US SSN recognizers; it runs in whitespace-aligned
32,768-character chunks. The deterministic regexes are:

```regex
email:      \b[\w.+-]+@[\w-]+\.[\w.-]+\b
phone:      (?<![\w.])(?:\+\d{1,3}[\s.-]?(?:\(?\d{2,4}\)?[\s.-]?){1,3}\d{3,4}|\(\d{2,4}\)[\s.-]?\d{3,4}[\s.-]\d{3,4}|\d{3}[.-]\d{3}[.-]\d{4})(?![\w.])
IBAN:       \b[A-Z]{2}\d{2}[A-Z0-9]{11,30}\b
card:       \b(?:\d[ -]?){13,19}\b
SSN:        \b\d{3}-\d{2}-\d{4}\b
IPv4:       \b(?:\d{1,3}\.){3}\d{1,3}\b
IPv6:       \b(?:[A-Fa-f0-9]{1,4}:){2,7}[A-Fa-f0-9]{1,4}\b
passport:   \b[A-Z]{1,2}\d{6,9}\b
secret:     (?ix)(?:-----BEGIN\s+(?:RSA |EC |OPENSSH )?PRIVATE KEY-----|\b(?:api[_ -]?key|access[_ -]?token|client[_ -]?secret|password)\b\s*[:=]\s*['"]?[A-Za-z0-9_+/.=-]{16,}|\b(?:gh[pousr]_[A-Za-z0-9]{20,}|AKIA[0-9A-Z]{16})\b|\bBearer\s+[A-Za-z0-9._~+/=-]{20,})
```

Phone matches require at least nine digits. Cards require 13 to 19 digits and a
valid Luhn checksum. IPv4 octets must be 0 to 255. Passport-shaped strings only
count when `passport` occurs within 48 characters. Email, phone, IPv4, and IPv6
become `[EMAIL]`, `[PHONE]`, or `[IP_ADDRESS]`. Credit card, IBAN, SSN, passport,
and secret are blocking and quarantine the artifact; they are also replaced in
the stored safe projection. Matched values are never written to Gold metadata.

### 3.4 Exact and near duplicates

MinHash uses lower-cased `\w+` tokens, word 5-grams, 112 permutations, seed
`0xC0FFEE`, and 28 bands of four rows. The production backend is Rensa, with
datasketch fallback; strict mode refuses the pure-Python fallback. LSH requires
all 28 band keys to exist for a near-duplicate decision. A replayed identical
`doc_id` is not treated as a duplicate of itself. Durable LevelDB is preferred,
then SQLiteDict; strict mode refuses an in-memory index.

### 3.5 Scores and routes

Scientific completeness is exactly:

```text
+0.12 title
+0.18 abstract
+0.20 at least 3 included sections, else +0.10 for any section
+0.20 at least 500 training words, else +0.10 at least 100
+0.12 any equations/tables/figures
+0.08 any citations
+0.10 no extraction warnings, else +0.03
clamped to 1.0
```

Structural score is `5 * (0.55*completeness + 0.25*role_coverage +
0.20*evidence_coverage)`. Role coverage is presence of abstract, methods, and
results/discussion/conclusion divided by three. Evidence coverage is presence
of equations, tables, and figures divided by three.

Reasoning score is exactly
`0.22*methods + 0.24*results + 0.14*equations + 0.12*tables + 0.08*figures +
0.10*(structural/5) + 0.10*(edu/5)`, clamped to `[0,1]`.

Composite quality is a renormalized weighted mean of applicable signals:
educational 0.35, structure 0.25, language 0.15, web heuristics 0.15, and KenLM
typicality 0.10. Non-applicable signals are removed with their weight. KenLM
typicality maps head/middle/tail to 1.0/0.72/0.25. The result is multiplied by
five.

Blocking reasons route to quarantine. Only incomplete scientific body or
incomplete scientific extraction can route to retry, with at most two alternate
attempts and only when an admitted arXiv Bronze pointer exists. A clean
permissive record is pretraining-eligible. A structured paper with a persisted
`ScientificDocument`, at least one retained section, and reasoning score >= 0.55
also becomes `posttrain_candidate`. Grey/missing-rights scientific input has
only the post-training route. HF cards never become Foundry candidates.

Gold content tags are deterministic: `mathematical_reasoning` for at least
three equations; `empirical_evidence` for results/discussion or any table or
figure; `methods_and_procedures` for methods; `benchmark_or_dataset` for a
matching heading/title term; `survey_synthesis`; `systems_implementation`;
`visual_evidence`; otherwise `general_scientific`. HF adds
`hf_model_documentation` or `hf_dataset_documentation` plus its card-assessment
category.

Every outcome goes to the decision table. Only risk tier 1, no reject reason,
no blocking PII, and a trainable route enters `docs.curated` and the accepted
Iceberg table.

## 4. Post-training cohort and outputs

The Foundry consumes only scientific `posttrain_candidate` rows and snapshots
the exact structured JSON with the queue entry. At the configured 08:30 UTC
boundary it removes queued entries older than 24 hours, freezes every queued
candidate in the preceding 24-hour interval, and sorts by:

1. ranking score descending;
2. reasoning score descending;
3. quality score descending;
4. `valid_from` descending;
5. `doc_id` ascending.

There is currently no candidate-count cap and no API-cost/context-size term.
Ranking score is the learned token-weighted mean post-training suitability
normalized by five. Records without active learned diagnostics use the
structural fallback in `processor/foundry/worker.py`. Arrivals
after the cutoff wait for the next boundary. Each accepted SFT or RL paper
family receives an idempotent pool ordinal; ordinal multiples of five are the
held-out post-training benchmark split, independently within SFT and RL.

Every model-authored role uses Hetzner Experiments Inference and exact model
`Qwen3.8-27B`, whose recorded licence is Apache-2.0. Provider requests are
OpenAI-compatible chat completions with exactly two messages, system and user;
temperature 0, seed 7342, JSON-object response format, streaming enabled, usage
included, and `chat_template_kwargs.enable_thinking=false`. The context limit
is 262,144 tokens. Completed structured calls are cached by request hash and
prompt version.

The processing order is: six evidence-graph passes, independent graph critic,
optional graph repair/recheck, task design, deterministic task normalization,
independent answerability audit, diversity selection, two independent solver
plans with bounded frozen tools, grounding critic, SFT deterministic verifier
or RL compiled verifier, full executable suite, signed package, and human audit.

Task-family priority is derivation, figure/table, assumption/consequence,
corruption diagnosis, claim/evidence, single-paper research, method DAG,
grounded explanation, experiment configuration, and result reproduction.
Up to six tasks are proposed and up to three diverse tasks are retained per
paper. If no corruption task exists, a deterministic reversed relation task is
added when a suitable relation is available.

## 5. Verbatim model prompt templates

### 5.1 Provider transport

```json
{
  "model": "Qwen3.8-27B",
  "messages": [
    {"role": "system", "content": SYSTEM_TEMPLATE_RESULT},
    {"role": "user", "content": USER_TEMPLATE_RESULT}
  ],
  "temperature": 0.0,
  "max_tokens": ROLE_SPECIFIC_LIMIT,
  "seed": 7342,
  "response_format": {"type": "json_object"},
  "stream": true,
  "stream_options": {"include_usage": true},
  "chat_template_kwargs": {"enable_thinking": false}
}
```

### 5.2 Evidence-graph pass system prompt

```text
You compile a hidden scientific evidence graph. Use only the supplied PaperBundle.
Return one prioritized incremental JSON patch that validates exactly against REQUIRED_JSON_SCHEMA
below. Add at most 24 nodes and 40 edges in this pass, do not restate existing nodes, and prefer
the evidence most useful for difficult grounded reasoning tasks over exhaustive transcription.
Do not rename fields. In particular, EvidenceNode uses canonical_text and supporting_spans, never
text or spans. Use only the node-type and edge-relation enum values in the schema. uncertainties
and conflicts are arrays of strings, not objects. Never use outside knowledge, invent missing
experimental details, or cite a span ID absent from the bundle. Separate explicit statements from
inference and leave ambiguous regions uncertain.
REQUIRED_JSON_SCHEMA:
{canonical_json(BoundedGraphPatch.model_json_schema()).decode()}
```

The exact user template, once per pass, is:

```text
PASS: {pass_name}
INSTRUCTION: {instruction}
PAPER_BUNDLE:
{bundle_prompt_json(bundle, section_roles=role_focus).decode()}
PRIVATE_OFFICIAL_ORACLE_RESULTS:
{canonical_json(oracle_results).decode()}
CURRENT_GRAPH:
{canonical_json(graph).decode()}
```

The six exact instructions are:

```text
Recover only explicit entities, equations, figures, tables, method steps, findings, limitations, inputs, outputs, and resources.
Add atomic independently checkable claims and assumptions. Split compound statements and mark inference explicitly in metadata.
Attach exact supporting and qualifying stable span IDs to every claim, finding, limitation, and method step. Remove unsupported nodes.
Add derivation, prerequisite, causal, comparison, assumption, method-order, input, and output edges using only existing node IDs.
Canonicalize equations, units, identifiers, method names, table values, and accepted equivalence classes.
Identify caveats, changing definitions, contradictory results, negative evidence, and unresolved ambiguity without forcing agreement.
```

### 5.3 Graph critic and repair

```text
You are a fresh scientific grounding critic with no access to compiler reasoning.
Return one JSON object that validates exactly against REQUIRED_JSON_SCHEMA. Check source-span
grounding, atomicity, overclaims, missing qualifiers, equation dependencies, method order,
conflicts, and suitability for deterministic verification.
REQUIRED_JSON_SCHEMA:
{canonical_json(GraphCritique.model_json_schema()).decode()}
```

```text
PAPER_BUNDLE:
{bundle_prompt_json(bundle, span_ids=supporting_spans).decode()}
PRIVATE_OFFICIAL_ORACLE_RESULTS:
{canonical_json(oracle_results).decode()}
CANDIDATE_GRAPH:
{canonical_json(graph).decode()}
```

Repair system prompt:

```text
You repair a hidden scientific evidence graph. Use only the supplied PaperBundle.
Return one prioritized incremental JSON patch that validates exactly against REQUIRED_JSON_SCHEMA
below. Correct only the critic findings: omit unchanged nodes and edges, replace a node by emitting
its corrected version with the same ID, and use remove_node_ids or remove_edges for deletions. Add
at most 24 nodes and 40 edges. Do not rename fields. In particular, EvidenceNode uses
canonical_text and supporting_spans, never text or spans. Use only the node-type and edge-relation
enum values in the schema. uncertainties and conflicts are arrays of strings, not objects. Never
use outside knowledge, invent missing experimental details, or cite a span ID absent from the
bundle. Separate explicit statements from inference and leave ambiguous regions uncertain.
REQUIRED_JSON_SCHEMA:
{canonical_json(BoundedGraphPatch.model_json_schema()).decode()}
```

```text
Return only a bounded delta against GRAPH that resolves the CRITIQUE; do not restate unchanged graph content.
PAPER_BUNDLE:
{bundle_prompt_json(bundle, span_ids=supporting_spans).decode()}
PRIVATE_OFFICIAL_ORACLE_RESULTS:
{canonical_json(oracle_results).decode()}
GRAPH:
{canonical_json(graph).decode()}
CRITIQUE:
{canonical_json(critique).decode()}
```

### 5.4 Task designer

```text
Design high-value scientific post-training TaskSpecs from one hidden evidence graph.
Return strict JSON {"tasks": [...]}. Use only supplied stable spans. Propose the requested mixture
across claim/evidence, derivation, method DAG, figure/table, corruption diagnosis,
assumption/consequence, long single-paper research, and grounded SFT reasoning where evidence
permits. Prefer answerable formula derivations, scaling-law calculations, numeric transformations,
factorizations, approximations, and figure/table synthesis over another routine method DAG when the
paper supports them. When audited official artifacts are present, also consider experiment configuration and
result reproduction. Separate public context from hidden targets, add same-paper distractors only,
avoid answer leakage, and reject underspecified families rather than inventing. A derivation task must
provide a canonical, parseable LaTeX expected expression or equality for deterministic checking, but its
public instruction must ask for a normal mathematical derivation rather than span-ID citations. The response must
validate exactly against REQUIRED_JSON_SCHEMA.
REQUIRED_JSON_SCHEMA:
{canonical_json(TaskBatch.model_json_schema()).decode()}
```

```text
Propose exactly {count} materially different TaskSpecs. Cover the strongest supported families, including the five deterministic RL templates and a long paper-local tool task. Configuration or result-reproduction tasks require an audited official artifact. Route valuable but non-finite work to SFT.
AVAILABLE_PRIVATE_ORACLE_RESULT_IDS:
{canonical_json(sorted(oracle_result_ids)).decode()}
PAPER_BUNDLE:
{bundle_prompt_json(bundle, span_ids=supporting_spans).decode()}
EVIDENCE_GRAPH:
{canonical_json(graph).decode()}
```

The task-designer user message includes `classifier_section_hints` from
PaperBundle metadata. The exact optional sentences are:

```text
Sections {titles} seem especially relevant.
Sections {titles} seem mathematically suited to potentially creating a derivation or reasoning task.
```

Each selects up to three section titles with score >=4 from the appropriate
head. No hint changes the PaperBundle or appears in other model-role inputs.

### 5.5 Answerability critic

```text
Independently audit scientific tasks using only the supplied paper. Return strict JSON
with decisions. Reject tasks that require external knowledge, expose their answer, admit multiple
incompatible valid interpretations, cite unavailable evidence, or cannot support a finite verifier.
The response must validate exactly against REQUIRED_JSON_SCHEMA.
REQUIRED_JSON_SCHEMA:
{canonical_json(AnswerabilityBatch.model_json_schema()).decode()}
```

```text
PAPER_BUNDLE:
{bundle_prompt_json(bundle, span_ids=task_spans).decode()}
EVIDENCE_GRAPH:
{canonical_json(graph).decode()}
TASKS:
{canonical_json(tasks).decode()}
```

### 5.6 Generic schema repair

```text
Repair one model-authored structured response so it validates exactly against the
required JSON schema. Preserve supported scientific content and identifiers. Do not introduce new
claims, evidence, calculations, tool observations, or external knowledge. Remove fields that cannot
be represented truthfully, including unknown or symbolic values placed in numeric-only fields.
Return only the repaired JSON object.
REQUIRED_JSON_SCHEMA:
{canonical_json(model.model_json_schema()).decode()}
```

```text
TARGET_TYPE: {model.__name__}
REPAIR_CONSTRAINT: {context}
VALIDATION_ERROR:
{validation_error}
INVALID_RESPONSE:
{canonical_json(data).decode()}
```

### 5.7 Solver A and solver B

```text
Solve one scientific task using only its supplied same-paper context and the allowed
frozen tools. Return strict JSON with status, report, answer_manifest, and tool_calls. Use status
tool_request with non-empty tool_calls when evidence must be searched or recomputed; use status final
with report and answer_manifest only after reviewing the returned observations. Every conclusion must
commit to allowed graph node IDs. Include evidence span IDs only when the public answer policy requires
citations; derivation tasks instead require a natural step-by-step derivation and final expression. Do not
quote long passages, use outside knowledge, claim unexecuted
tool results, or expose hidden construction instructions. The response must validate exactly against
REQUIRED_JSON_SCHEMA.
REQUIRED_JSON_SCHEMA:
{canonical_json(SolverTurn.model_json_schema()).decode()}
```

The first user prompt is exactly:

```text
PLAN_VARIATION: {plan}
PUBLIC_TASK:
{canonical_json(public_task).decode()}
PUBLIC_CONTEXT:
{canonical_json(context).decode()}
```

`plan` is exactly one of:

```text
Use a direct constructive plan and verify every structured commitment.
Use a structurally different plan and independently recompute the answer.
```

For every tool turn, the user prompt becomes:

```text
PLAN_VARIATION: {plan}
PUBLIC_TASK:
{canonical_json(public_task).decode()}
PUBLIC_CONTEXT:
{canonical_json(context).decode()}
ALLOWED_TOOLS:
{canonical_json(task.public_context_policy.tool_access).decode()}
PRIOR_TOOL_TURNS:
{canonical_json(transcript).decode()}
```

`public_task` contains only task id, paper id, family, public instruction,
answer contract, allowed tools, graph node id/type pairs supported by the public
spans, output target ids, and either `optional_internal_provenance` for
derivation or `cite_public_span_ids` otherwise. It does not contain hidden
expected values, required relations, accepted evidence sets, or construction
instructions.

The solution-contract repair user prompt is exactly:

```text
Repair this final reference solution's structured manifest without changing its scientific conclusion. Claims and method nodes use graph node IDs. Numeric expected values use numeric_results entries with the exact target key. For a derivation task, string expected values use equations entries with the exact target key; for all other task families, string expected values use configuration entries. Preserve the readable report, evidence, and required relations.
CONTRACT_VIOLATIONS:
{canonical_json(violations).decode()}
TASK:
{canonical_json(task).decode()}
CURRENT_FINAL_TURN:
{canonical_json(turn).decode()}
```

### 5.8 Grounding critic

```text
Audit the available independently generated reference solutions against one paper
and task. Return strict
JSON with accepted, findings, unsupported_claims, contradictory_claims. Check
manifest/prose consistency, exact evidence support, calculations, completeness, and scientific value.
Your vote cannot override deterministic checks. The response must validate exactly against
REQUIRED_JSON_SCHEMA.
REQUIRED_JSON_SCHEMA:
{canonical_json(GroundingCritique.model_json_schema()).decode()}
```

```text
PAPER_BUNDLE:
{bundle_prompt_json(bundle, span_ids=task_spans).decode()}
GRAPH:
{canonical_json(graph).decode()}
TASK:
{canonical_json(task).decode()}
SOLUTIONS:
{canonical_json(trajectories).decode()}
```

### 5.9 Verifier compiler, critic, and repair

```text
Compile an English scientific rubric into a strict VerifierSpec using only these
predicate types: nonempty_report, manifest_required, required_nodes, forbidden_nodes,
required_dependency_nodes, evidence_membership, evidence_coverage, symbolic_equivalence,
numeric_tolerance, method_partial_order, fault_identification, required_relations,
derivation_partial_order,
required_qualifications, configuration_constraints, report_manifest_consistency.
Return one JSON VerifierSpec. Use finite hidden targets, hard gates,
weighted outcome checks, no prose judgement, no network, and no executable model-generated code.
The response must validate exactly against REQUIRED_JSON_SCHEMA.
REQUIRED_JSON_SCHEMA:
{canonical_json(VerifierSpec.model_json_schema()).decode()}
```

```text
TASK:
{canonical_json(task).decode()}
GRAPH:
{canonical_json(graph).decode()}
PAPER_SPAN_IDS:
{canonical_json([span.span_id for span in bundle.stable_spans]).decode()}
```

```text
Independently inspect a deterministic scientific VerifierSpec for false positives,
false negatives, equivalent correct answers, missing hard gates, reward hacks, circular target use,
and brittle ordering or tolerance checks. Return strict JSON with accepted, findings,
false_positive_risks, false_negative_risks, repair_instructions. Set accepted=false only when a
listed risk is release-blocking and requires a repair; accepted=true may retain explicitly
documented residual risks that do not invalidate the deterministic verifier. The response must
validate exactly against REQUIRED_JSON_SCHEMA.
REQUIRED_JSON_SCHEMA:
{canonical_json(VerifierCritique.model_json_schema()).decode()}
```

```text
TASK:
{canonical_json(task).decode()}
GRAPH:
{canonical_json(graph).decode()}
VERIFIER:
{canonical_json(spec).decode()}
SPAN_IDS:
{canonical_json([span.span_id for span in bundle.stable_spans]).decode()}
```

```text
Return a complete replacement VerifierSpec using only the allowlisted predicates.
TASK:
{canonical_json(task).decode()}
GRAPH:
{canonical_json(graph).decode()}
CURRENT_VERIFIER:
{canonical_json(spec).decode()}
CRITIQUE:
{canonical_json(critique).decode()}
SPAN_IDS:
{canonical_json([span.span_id for span in bundle.stable_spans]).decode()}
```

### 5.10 Exported RL task prompt

The packaged Prime-compatible environment constructs the runtime prompt exactly
as follows:

```python
instruction = (
    prompt["instruction"]
    + "\n\nReturn one JSON object matching answer_schema: "
    + json.dumps(prompt["answer_schema"], sort_keys=True)
)
```

## 6. Frozen tools and deterministic acceptance

Solvers may receive `search`, `open`, `find`, `calculator`, and `symbolic` only
when the task allowlist names them. Search tokenizes `[a-z0-9]+`, scores overlap
density plus a logarithmic length penalty, sorts deterministically, limits to 20,
and returns 500-character snippets. Find is literal, case-insensitive, and
returns at most 100 matches. Calculator accepts numeric constants, `pi`, `e`,
`+ - * / // % **`, unary signs, and `abs/sqrt/log/exp/sin/cos/tan/min/max`; no
keyword arguments and no exponent magnitude above 100. Symbolic expressions are
at most 2,000 characters, use a safe AST conversion, and allow only simplify,
expand, factor, or equivalence. A repeated invalid tool request or eight tool
turns rejects that solver trajectory, not the whole queue.

Task normalization checks paper id, public and distractor span ids, graph node
ids, graph relations, qualifications, oracle ids, accepted evidence sets, and
required/forbidden-fault consistency. RL requires a finite machine-verifiable
contract. Derivations specifically require an equation target plus numeric or
parseable LaTeX expected values. LaTeX is parsed with SymPy's Lark backend;
prose conditionals, empty/over-2,000-character values, tuples, or unparseable
expressions are not checkable. Equality reversal is accepted.

The verifier is normalized against hidden task truth. It always injects
required hard predicates for non-empty report, manifest presence, report/
manifest consistency, required/forbidden nodes, evidence membership/coverage
except for derivations, required relations, method/derivation order,
qualifications, faults, and configuration constraints when applicable. Every
numeric target receives tolerance `max(1e-9, abs(expected)*1e-6)`. Every
checkable derivation target receives symbolic equivalence. Network is forced
off, seed is deterministic from task id, and `sympy==1.13.3` is pinned.

Reward is the weighted predicate mean, but any required-predicate failure sets
reward to zero. Passing requires every hard predicate and reward >= 0.999.

### 6.1 Exact predicate semantics

- `nonempty_report`: `answer.report.strip()` must be non-empty.
- `manifest_required`: the number of committed claim, method, fault, equation,
  qualification, numeric-result, and relation endpoint IDs plus evidence IDs
  and numeric-result entries must be greater than zero.
- `required_nodes` and `required_dependency_nodes`: every configured target
  must occur in the committed manifest. For a derivation, an equation target
  counts only when it occurs in `answer_manifest.equations`, not merely in a
  claim or relation.
- `forbidden_nodes`: no configured target may occur in the committed manifest.
- `evidence_membership`: the submitted evidence list must be non-empty and a
  subset of the task's public included spans. This predicate is not installed
  for derivation tasks.
- `evidence_coverage`: at least one complete accepted evidence set must be a
  subset of submitted evidence. Its score is the maximum fraction covered
  among accepted sets and it passes only at exactly 1.0. It is not installed
  for derivation tasks.
- `symbolic_equivalence`: the equation entry with the target ID is compared to
  the hidden expected string, or the target graph equation's canonical form or
  LaTeX. SymPy parses LaTeX through the Lark backend, simplifies the difference
  between sides, and also tries equality reversal. At least one comparison must
  be equivalent.
- `numeric_tolerance`: the numeric-result entry with the target ID, and the
  configured unit when one exists, must have absolute error no greater than
  `max(1e-9, abs(expected)*1e-6)`. The diagnostic score outside tolerance is
  `max(0, 1 - error/max(abs(expected), tolerance, 1e-12))`, but a required miss
  still zeros the final reward.
- `method_partial_order`: for every configured `[left,right]` pair, both IDs
  must occur in `method_nodes` and the left index must be smaller.
- `derivation_partial_order`: the same rule is applied to the ordered equation
  entries. Required graph relations `precedes`, `derives`, `enables`, and
  `produces` map source before target; `depends_on` and `uses` map target before
  source.
- `fault_identification`: submitted faults must contain every required fault,
  contain no forbidden fault, and contain no extra fault outside the required
  set. The score is Jaccard overlap with the required set.
- `required_relations`: every hidden `(source, relation, target)` triple must
  occur in the submitted relation list.
- `required_qualifications`: every hidden qualification ID must occur in the
  submitted qualification list.
- `configuration_constraints`: every `required_values` key must equal its
  configured value; every `ranges` value must be a non-boolean number within
  the inclusive two-element bound; every `forbidden_keys` key must be absent.
  At least one supported check must exist and all checks must pass.
- `report_manifest_consistency`: the stripped report and at least one committed
  manifest or evidence ID must both be present.

Provider-authored predicates are canonicalized against the hidden task before
execution. Unknown graph/span/value targets reject normalization. Model choices
cannot weaken hidden requirements: missing hard predicates are injected, the
network flag is forced false, runtime dependencies gain `sympy==1.13.3`, and a
verifier with no positive-weight outcome receives weight 1.0 on the first
available outcome in this order: numeric, symbolic, required relations, method
order, derivation order, required nodes, evidence coverage, faults,
configuration, report/manifest consistency, manifest presence.

### 6.2 Exact generated validation cases

Both SFT and RL execute the same acceptance suite:

- positive: every retained solver trajectory passes;
- equivalent: reordered/deduplicated commitments still pass;
- adversarial: empty answer, report-only answer, invented evidence when relevant,
  missing commitments, and sign-flipped targeted numeric values all fail;
- mutation: each required node/fault/relation/qualification, targeted evidence,
  method/derivation order, numeric target, symbolic target, and configuration
  constraint is mutated when applicable, and every non-no-op mutation must fail;
- metamorphic: harmless order changes and an extra allowed evidence span preserve
  both pass/fail result and reward exactly;
- replay: two evaluations serialize to identical reward/predicate output;
- static security: `network_required` is false and the package payload contains
  none of `HETZNER_INFERENCE_API_KEY`, `ZAI_API_KEY`, or `GLM_API_KEY`;
- false positives and false negatives must both be zero.

The equivalent variant reverses and de-duplicates claims and evidence, reverses
numeric results, relations, and qualifications, and must still pass. The
adversarial set contains: a fully empty answer; the valid report with an empty
manifest; invented `external:invented-span` evidence when evidence predicates
exist; a manifest with claims, method nodes, faults, equations, relations,
qualifications, and configuration emptied; and a sign flip of every targeted
numeric result when one exists.

Mutation candidates remove each required node simultaneously from claims,
method nodes, faults, equations, numeric results, relation endpoints, and
qualifications; remove each required fault; replace one evidence ID by
`mutated:span`; reverse method order; reverse derivation-equation order; add
`max(abs(value),1.0)` to each targeted numeric result; replace each targeted
equation by `-(original)`; remove one required relation; remove one required
qualification; and remove the first constrained configuration key or add the
first forbidden key. Byte-identical no-op mutations are discarded. If there is
neither a required node nor evidence, an answer containing only report
`mutated` and an empty manifest is added. Every remaining mutation must fail.

Metamorphic cases reverse claims, evidence, relations, and qualifications, and
when possible add one other allowed evidence span. Each must preserve both the
boolean result and reward within `1e-12`. Replay serializes two independent
evaluations to canonical JSON and requires byte equality. Static security
serializes the verifier, paper bundle, graph, and task, requires
`network_required=false`, and rejects any occurrence of the three provider
credential variable names listed above.

SFT uses a task-derived deterministic verifier and stores each accepted solver
trajectory. RL first lets the model compile a verifier, normalizes it against
hidden truth, uses deterministic fallback if construction or critic repair is
invalid, then packages one environment only after the suite passes. Packages
contain frozen public context, hidden truth, verifier, validation cases, traces,
hash manifest, Ed25519 signature/certificate, and no provider credentials or
runtime network requirement. Human approval/rejection is an additional named,
append-only audit decision and does not replace automatic validation.
