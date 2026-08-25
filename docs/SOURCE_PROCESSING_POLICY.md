# Source processing and classifier policy

Status: binding implementation contract, revised 2026-08-25. The dispatcher is
`processor/source_policy.py`; extraction is implemented in
`processor/fetcher.py` and `processor/scientific.py`; curation is implemented
in `processor/curate.py`.

Discovery metadata never enters body extraction, classifiers, curation,
Documents, or source acceptance statistics. Every content profile retains
applicable privacy scanning, exact and MinHash deduplication, E5 benchmark
decontamination, validity intervals, and immutable provenance. A metric is
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
| arXiv full paper | Native arXiv HTML, ar5iv fallback, then bounded CPU Docling PDF fallback. Retain main sections, equations, tables, figure captions, selected OCR, and provenance. Remove authors, references, navigation, and excluded sections from the training projection. | FinePDFs Edu v2 at revision `90ddef285f67230389057c14b2f6bbfeb70d40ea`; structured scientific completeness and reasoning signals; CPU language ID; segment-level privacy removal; exact/MinHash dedup; E5 benchmark decontamination. FineWeb-Edu is comparison-only and KenLM/C4 web gates are off for scientific text. | Permissive clean papers can enter pretraining and, when methods/results evidence is sufficient, the paper Foundry. Posttrain-only clean papers can enter only the Foundry. |
| Hugging Face model card | Exact-revision README prose after card metadata and fenced-code removal. | FineWeb-Edu audit signal, CPU language ID, privacy, dedup, and E5 decontamination. FinePDFs, code, C4/Gopher, and KenLM gates are off. | Pretraining only. |
| Hugging Face dataset card | Exact-revision README prose after card metadata and fenced-code removal. | Same dedicated card policy as model cards. Dataset rows are out of scope. | Pretraining only. |

## Routing rules

- Quality signals are calculated over retained segments, not the whole raw
  paper or page.
- A permissive item can be eligible for pretraining. Only a structured
  scientific paper can additionally become a current Foundry candidate.
- Grey-area or missing rights remove the pretraining route. A structured
  scientific paper may still enter the Foundry. Hugging Face cards are admitted
  only as pretraining prose under their exact public-repository terms.
- Explicit incompatible rights, blocking privacy findings, benchmark
  contamination, or failed source-specific quality gates quarantine the item.
- Post-training benchmark allocation occurs after SFT/RL generation, never in
  pretraining curation.

## Model references

- FinePDFs Edu v2 model revision:
  `90ddef285f67230389057c14b2f6bbfeb70d40ea`
- FineWeb-Edu comparison model revision:
  `284663cbb2dabf9bda30d8f8cc49601251ee1631`
- CPU inference runs through the pinned ONNX/transformers runtime in the model
  service. Throughput on the target cluster remains `needs-measurement` until
  the fresh-frontier measurement is complete.
