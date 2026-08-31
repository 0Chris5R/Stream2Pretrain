# Source processing and classifier policy

Status: binding implementation contract, revised 2026-08-25. The dispatcher is
`processor/source_policy.py`; extraction is implemented in
`processor/fetcher.py` and `processor/scientific.py`; curation is implemented
in `processor/curate.py`.

Discovery metadata never enters body extraction, classifiers, curation,
Documents, or source acceptance statistics. Every content profile retains
applicable privacy scanning, exact and MinHash deduplication, validity
intervals, and immutable provenance. A metric is
absent when it is not meaningful for that source rather than reported as a
fabricated zero.

## Source-by-source policy

| Source | Projection | Quality and safety processing | Result |
|---|---|---|---|
| arXiv RSS `cs.CL` | ArXiv id only | None. Internal scheduling only. | Schedules one canonical paper. |
| arXiv RSS `cs.LG` | ArXiv id only | None. Internal scheduling only. | Schedules one canonical paper. |
| arXiv RSS `cs.AI` | ArXiv id only | None. Internal scheduling only. | Schedules one canonical paper. |
| arXiv RSS `cs.CV` | ArXiv id only | None. Internal scheduling only. | Schedules one canonical paper. |
| arXiv OAI-PMH `set=cs` | ArXiv id only | None. Internal current-frontier scheduling only. | Schedules one canonical paper; canonical ids deduplicate overlap with RSS. |
| arXiv full paper | Native arXiv HTML, ar5iv fallback, then bounded CPU Docling PDF fallback. Retain main sections, equations, tables, figure captions, selected OCR, and provenance. Remove authors, references, navigation, and excluded sections from the training projection. | Deterministic extraction, language, privacy, licence, and publication-template rejection first; then FinePDFs Edu v2 at revision `90ddef285f67230389057c14b2f6bbfeb70d40ea`; structured scientific completeness and reasoning signals; exact/MinHash dedup. KenLM/C4 web gates are off for scientific text. | Permissive clean papers can enter pretraining and, when methods/results evidence is sufficient, the paper Foundry. Posttrain-only clean papers can enter only the Foundry. |
| Hugging Face model card | Exact-revision README prose split by Markdown section after YAML, fenced-code, HTML, and asset removal. | Grounded deterministic card-content, language, privacy, and licence rejection first; then FinePDFs Edu v2; exact/MinHash dedup. C4/Gopher and KenLM remain off. | Substantive technical documentation enters pretraining only. Synthetic scripts, templates, upload/quantization shells, minimal inventories, ungrounded marketing, wrong-type, and insufficient cards quarantine. |
| Hugging Face dataset card | Exact-revision README prose split by Markdown section after YAML, fenced-code, HTML, and asset removal. Dataset rows and hosted binaries are excluded. | Dataset-specific grounded deterministic card-content gate, privacy, licence, and language checks first; then FinePDFs Edu v2 and deduplication. | Substantive dataset documentation enters pretraining only. It never licenses or imports dataset rows. |

## Routing rules

- Quality signals are calculated over retained segments, not the whole raw
  paper or page.
- A permissive item can be eligible for pretraining. Only a structured
  scientific paper can additionally become a current Foundry candidate.
- Grey-area or missing rights remove the pretraining route. A structured
  scientific paper may still enter the Foundry. Hugging Face cards are admitted
  only as pretraining prose under their exact public-repository terms.
- Explicit incompatible rights, blocking privacy findings, or failed
  source-specific quality gates quarantine the item.
- Post-training benchmark allocation occurs after SFT/RL generation, never in
  pretraining curation.

## Model references

- FinePDFs Edu v2 model revision:
  `90ddef285f67230389057c14b2f6bbfeb70d40ea`
- CPU inference runs through the pinned ONNX/transformers runtime in the model
  service. Throughput on the target cluster remains `needs-measurement` until
  the fresh-frontier measurement is complete.

## Current-output boundary

`pretrain-content-v3` is the active content generation. It versions the HF card
gate, publication-template rejection, and corrected near-duplicate policy.
Normal UI queries and dataset exports include only this generation. Historical
decisions remain in Iceberg for audit but require replay through the current
policy before they can appear in a current export. Near-duplicate anchors are
durable and namespaced by generation, so restarts retain current deduplication
state without allowing superseded anchors to reject the first clean record.
