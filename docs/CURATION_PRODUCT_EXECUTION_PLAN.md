# Curation product execution contract

This file records the product decisions that implementation work must preserve.

## Processing

- Scientific text is a structured artifact, not one monolithic string. Remove
  author blocks, references, acknowledgements, navigation, and other excluded
  sections before scoring or export. Preserve headings, prose, equations,
  tables, figure captions, selected OCR, and stable source-element provenance.
- FinePDFs Edu v2 is the primary scientific quality model. FineWeb-Edu is a
  comparison signal and KenLM is an audit signal for papers, not a hard
  scientific-paper rejection gate.
- Hugging Face cards use exact-revision README prose and the dedicated card
  structure policy. Hosted weights, dataset rows, binaries, generated upload
  mirrors, blank templates, and marketing-only cards are excluded.
- PII is checked at segment level. Redactable contact fields are removed from
  an otherwise useful projection. Secrets or unsafe identity-bearing material
  block the record.
- Exact-hash and stateful MinHash/LSH duplicate handling are required.
- Every score must remain separately inspectable; the composite score is only a
  convenience summary.

## Routing

- Permissive rights: pretraining and, for qualified scientific papers,
  post-training.
- Missing or reviewed grey-area rights: derived post-training only.
- Explicit incompatible rights: quarantine before retained body fetch.
- Scientific extraction failures that may improve through another extractor go
  to retry, not permanent quarantine.
- Pretraining never allocates the SFT/RL evaluation split. That split is made
  only from validated post-training artifacts.

## UI

- Normal pages are concise monitoring surfaces and contain no source or pipeline
  configuration actions.
- Sources shows configured workloads and observed health, volume, and licence
  outcomes.
- Documents and Post-training keep the collection table stable and open record
  details in dialogs.
- Datasets exposes only date range, route, source, one content tag, structured
  content inclusion, and output format.
- Only per-artifact SFT/RL approval or rejection is interactive in the normal
  cockpit. The reviewer name is entered at decision time and retained.

## Capacity

Do not remove quality stages to work around resource pressure. First identify
whether the constraint is CPU, memory, or storage, then scale the responsible
workload or request more cluster capacity. Optimize only demonstrated
inefficiencies. Raw source documents and extracted assets use bounded retention;
Iceberg training data, route decisions, provenance, and accepted post-training
packages are durable.

## Acceptance

The pipeline is ready when every enabled source has a live item through its
source-specific licence, extraction, classification, routing, and Iceberg path;
the stream keeps up with measured intake; dashboard totals match durable tables;
and at least one inspectable SFT and one executable RL artifact pass unchanged
validation and human audit.
