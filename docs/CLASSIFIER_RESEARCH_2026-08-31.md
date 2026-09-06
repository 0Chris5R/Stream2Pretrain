# Source-aware classifier research, 2026-08-31

## Research basis

The active pipeline uses four independently trained ModernBERT-base heads.
[The classifier guide](CLASSIFIERS.md) defines their scope and exact artifacts.
The references below informed extraction, cheap filtering, mathematical
reasoning and topic annotation. They are not additional active classifiers.

## Reference implementations

| Research group | Component | Potential role | Runtime and licence |
|---|---|---|---|
| P0 | Official Hugging Face [`ModelCard` and `DatasetCard`](https://huggingface.co/docs/huggingface_hub/package_reference/cards) parsing | Parse YAML and Markdown structure before rejecting templates, mirrors, inventories, access-only pages, wrong-type cards, and empty prose | Lightweight CPU, Apache-2.0 library |
| P0 | [Dolma](https://github.com/allenai/dolma) and [DataTrove](https://github.com/huggingface/datatrove) patterns | Exact and near deduplication plus repeated-template removal before expensive inference | CPU-native building blocks |
| P0 | [CSO Classifier v4.0.1](https://github.com/angelosalatino/cso-classifier), commit `03c27f02e1501e1191d3b2bccfafe5bada43b2d5` | Grounded fine-grained computer-science topics with weights, parents, and evidence | Apache-2.0, CPU; full Word2Vec footprint is about 2 GB |
| P1 | [Meta-rater Reasoning](https://huggingface.co/opendatalab/meta-rater-reasoning-rating), revision `0072a9a` | Post-training candidate ranking or audit of logical depth and multi-step reasoning, never a pretraining gate | ModernBERT-base, 149M parameters, 598 MB, 4,096 tokens, MIT |
| P1 | [FineMath classifier](https://huggingface.co/HuggingFaceTB/finemath-classifier), revision `bd0b0e330750ccaafb16c47066f875b3fcb707c3` | Auxiliary score for equation and derivation-rich sections only | E5-small based, 471 MB, MIT |
| P1 | [SPECTER2](https://huggingface.co/allenai/specter2_base), revision `3447645e1def9117997203454fa4495937bfbd83` | Backbone for our own arXiv contribution-type and post-training-suitability head | 440 MB, Apache-2.0; requires project labels |
| P1 | [OLMo Bonepick](https://github.com/allenai/olmo-bonepick) with [Potion-32M](https://huggingface.co/minishlab/potion-base-32M) or fastText | Compare smaller card-quality backbones with annotation, agreement, calibration, and learning-curve evaluation | CPU-native; Apache-2.0 and MIT |
| P1 | [Presidio](https://github.com/microsoft/presidio) | Span-level PII detection and redaction after structural removal of paper contact metadata | CPU, MIT |
| P1 | [GROBID 0.9.1](https://github.com/grobidOrg/grobid) | Possible CPU PDF fallback for document boundaries; native arXiv HTML and MathML remain primary | CPU, Apache-2.0 |
| P2 | [Propella-1 0.6B](https://huggingface.co/ellamind/propella-1-0.6b), revision `5f03c30` | Sampled shadow audit for information density, technical depth, reasoning, audience, and marketing bias | 1.5 GB, Apache-2.0; CPU throughput is `needs-measurement` |
| P2 | [NVIDIA prompt task and complexity classifier](https://huggingface.co/nvidia/prompt-task-and-complexity-classifier), revision `fea1121511eafabaf7dd6fc66863dcb04f74defb` | Audit generated SFT/RL prompt diversity and triviality, not source-document quality | DeBERTa, 735 MB |

## Optional categorical annotations

The deployed HF and arXiv quality heads return usefulness, not a long taxonomy.
A separate future HF tagger could use the content classes recorded in
[the card audit rubric](HF_CARD_QUALITY_AUDIT.md).

A future paper-topic tagger should keep these concepts separate:

- Domain: native arXiv categories.
- Topic: CSO or OpenAlex topics.
- Contribution type: architecture/model, optimization, training dynamics,
  scaling, data/curation, systems, evaluation/benchmark, theory,
  interpretability/safety, application, survey, or position.
- Reasoning affordance: formula derivation, theorem/proof, numerical-table
  reasoning, ablation comparison, algorithm tracing, experimental-design
  critique, complexity analysis, or methodological reconstruction.

Provider errors, context-limit errors and verifier defects must be excluded
from negative labels when using Foundry outcomes as future training data.

This research follows the useful patterns in
[DCLM](https://github.com/mlfoundations/dclm/blob/main/baselines/README.md)
and [Dolma](https://github.com/allenai/dolma): cheap cleanup before expensive
inference, explicit source strata, and evaluation against downstream outcomes
rather than equating one reference distribution with universal quality.
