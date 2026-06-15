"""Bytewax dataflow: ``docs.normalized`` -> ``docs.curated``.

End-to-end FineWeb-style curation. The dataflow:

1. Consumes :class:`SilverRecord` payloads from ``docs.normalized``.
2. Runs the Gopher heuristic gate.
3. Runs the C4 nopunc / curly-brace / lorem-ipsum gate.
4. Re-scores perplexity (KenLM) and re-buckets - the fetcher emitted
   stub values; curation owns the real signals.
5. Recomputes the MinHash signature (cheap, ~us/doc) and tests the
   :class:`LSHBloomIndex` near-dup index.
6. Runs the FineWeb-Edu ONNX classifier (or proxy heuristic).
7. Runs the PII regex pack.
8. Runs the Decon-Gate (n-gram Bloom + optional embedding sketch).
9. Emits a :class:`GoldRecord` on ``docs.curated``.

Records that fail any rule are still emitted, with ``reject_reasons`` and
``risk_tier`` populated. The Iceberg writer downstream is the only
component that decides whether a row enters the gold table.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import timezone
from typing import Any

from processor import common
from processor.decon_gate import DeconGate, _EmbeddingSketch  # type: ignore[attr-defined]
from processor.operators.c4 import C4Filter
from processor.operators.gopher import GopherFilter
from processor.operators.kenlm_score import KenLMScorer
from processor.operators.lshbloom import LSHBloomIndex
from processor.operators.minhash import MinHasher, MinHashSignature
from processor.operators.pii import PiiScanner
from processor.operators.quality import QualityClassifier
from processor.tokenize import Tokenizer
from schemas.gold import GoldRecord, RejectReason, RiskTier
from schemas.silver import SilverRecord

POLICY_REVISION_ENV = "S2P_POLICY_REVISION"
SCORING_VERSION_ENV = "S2P_SCORING_VERSION"


@dataclass(slots=True)
class CurateState:
    """Per-worker state for the curation dataflow."""

    gopher: GopherFilter
    c4: C4Filter
    kenlm: KenLMScorer
    minhasher: MinHasher
    lsh: LSHBloomIndex
    quality: QualityClassifier
    pii: PiiScanner
    decon: DeconGate
    tokenizer: Tokenizer
    policy_revision: str
    scoring_version: str

    def close(self) -> None:
        self.lsh.close()


def build_state(cfg: common.ProcessorConfig) -> CurateState:
    """Construct a :class:`CurateState` from the runtime config."""
    models = cfg.models_dir
    kenlm_path = os.path.join(models, "kenlm", "en.bin")
    quality_dir = os.path.join(models, "fineweb-edu")
    e5_dir = os.path.join(models, "e5-small")
    embedding = _EmbeddingSketch(e5_dir if os.path.isdir(e5_dir) else None)
    decon = DeconGate(
        benchmark_set_version=cfg.benchmark_set_version,
        benchmark_corpus=_load_benchmark_corpus(cfg.benchmark_corpus_path),
        embedding=embedding,
    )
    return CurateState(
        gopher=GopherFilter(),
        c4=C4Filter(),
        kenlm=KenLMScorer(kenlm_path if os.path.isfile(kenlm_path) else None),
        minhasher=MinHasher(),
        lsh=LSHBloomIndex(state_dir=os.path.join(cfg.state_dir, "lshbloom")),
        quality=QualityClassifier(quality_dir if os.path.isdir(quality_dir) else None),
        pii=PiiScanner(),
        decon=decon,
        tokenizer=Tokenizer(),
        policy_revision=os.environ.get(POLICY_REVISION_ENV, "git:dev"),
        scoring_version=os.environ.get(SCORING_VERSION_ENV, "v0.1.0"),
    )


def _load_benchmark_corpus(path: str | None) -> dict[str, list[str]] | None:
    """Read benchmark prompts from a JSON file, or return ``None``."""
    if not path or not os.path.isfile(path):
        return None
    import orjson

    with open(path, "rb") as fh:
        data = orjson.loads(fh.read())
    if not isinstance(data, dict):
        return None
    return {str(k): list(v) for k, v in data.items()}


def curate_one(state: CurateState, silver: SilverRecord) -> GoldRecord:
    """Run the full curation pipeline on one silver record.

    Always returns a GoldRecord - rejected docs carry ``reject_reasons``
    and an elevated ``risk_tier``.
    """
    text = silver.text
    reject: list[RejectReason] = []
    # Gopher
    gstats = state.gopher.stats(text)
    gopher_pass = state.gopher.passes(text)
    if not gopher_pass:
        reject.append("gopher_filter")
    # C4 - the gold schema only carries the ``c4_nopunc_filter`` reject
    # reason, but a failure on any C4 sub-rule (nopunc, curly-brace,
    # lorem-ipsum) is enough for the gate to reject the document. Surface
    # all of them under the canonical reason so downstream consumers see
    # the C4 stage as a single reject signal.
    cstats = state.c4.stats(text)
    if not (cstats.nopunc_pass and cstats.curly_brace_pass and cstats.lorem_ipsum_pass):
        reject.append("c4_nopunc_filter")
    # Perplexity
    ppl = state.kenlm.score(text)
    if ppl.bucket == "tail":
        reject.append("high_perplexity")
    # Near-dup. We refuse to LSH-band a signature whose backend differs
    # from the curator's local MinHasher: the byte layouts (uint32 LE vs
    # rensa-native vs datasketch) are not interchangeable, so banding
    # across backends would silently produce wrong cluster assignments.
    if (
        silver.minhash_backend != state.minhasher.backend
        or silver.minhash_num_perms != state.minhasher.num_perms
    ):
        reject.append("minhash_backend_mismatch")
    sig = MinHashSignature(
        digest=silver.minhash_sig,
        num_perms=silver.minhash_num_perms,
        backend=silver.minhash_backend,
    )
    near = state.lsh.observe(silver.doc_id, sig)
    if near.is_near_duplicate:
        reject.append("near_duplicate")
    # Quality
    quality = state.quality.score(text)
    if quality.quality_score < 1.0:
        reject.append("low_quality_score")
    # PII
    pii_flags = state.pii.flags(text)
    if pii_flags:
        reject.append("pii_detected")
    # Decon-Gate
    risk = _risk_from_reject(reject, pii_flags)
    pre_record = GoldRecord(
        doc_id=silver.doc_id,
        text=text,
        lang=silver.lang,
        tokens=state.tokenizer.count(text).tokens,
        quality_score=quality.quality_score,
        edu_score=quality.edu_score,
        license="unknown",
        license_source="unknown",
        risk_tier=risk,
        pii_flags=pii_flags,
        contaminated_with=[],
        valid_from=silver.valid_from,
        valid_to=silver.valid_to,
        reject_reasons=reject,
        scoring_version=state.scoring_version,
        classifier_revision=state.quality.revision,
        policy_revision=state.policy_revision,
        snapshot_id=None,
        trace_id=silver.trace_id,
    )
    post_record, hits = state.decon.scan(pre_record)
    if hits:
        new_reasons = list(post_record.reject_reasons)
        if "decontamination_hit" not in new_reasons:
            new_reasons.append("decontamination_hit")
        return post_record.model_copy(update={"reject_reasons": new_reasons, "risk_tier": 3})
    return post_record


def _risk_from_reject(reject: list[RejectReason], pii_flags: list[str]) -> RiskTier:
    """Map current reject signals onto the 1/2/3 risk-tier ladder."""
    if "decontamination_hit" in reject or pii_flags:
        return 3
    if reject:
        return 2
    return 1


def process_silver_payload(state: CurateState, payload: bytes) -> bytes | None:
    """Deserialize a SilverRecord payload, curate, return GoldRecord JSON."""
    silver = common.silver_loads(payload)
    gold = curate_one(state, silver)
    return common.gold_dumps(gold)


def build_dataflow(cfg: common.ProcessorConfig) -> object:
    """Build the Bytewax dataflow object."""
    from bytewax.connectors.kafka import KafkaSink, KafkaSinkMessage, KafkaSource
    from bytewax.dataflow import Dataflow
    from bytewax import operators as op

    tracer = common.init_tracer("s2p-curate", cfg)
    state = build_state(cfg)
    flow = Dataflow("s2p-curate")
    # Default to ``beginning`` so a restart with no committed group offset
    # replays the topic instead of dropping in-flight bytes (at-least-once
    # semantics; matches the Kappa/streaming-first contract). Operators can
    # override via ``S2P_KAFKA_START_OFFSET=end`` for short-lived debug runs.
    start_offset = os.environ.get("S2P_KAFKA_START_OFFSET", "beginning")
    source = KafkaSource(
        brokers=cfg.redpanda_brokers.split(","),
        topics=[cfg.normalized_topic],
        consumer_group=cfg.consumer_group + "-curate",
        starting_offset=start_offset,
    )
    inp = op.input("docs_normalized", flow, source)

    def _step(msg: object) -> KafkaSinkMessage | None:
        with tracer.start_as_current_span("curate.process") as span:
            payload = getattr(msg, "value", None)
            if payload is None:
                return None
            try:
                out = process_silver_payload(state, payload)
            except Exception as exc:  # noqa: BLE001
                span.record_exception(exc)
                return None
            if out is None:
                return None
            return KafkaSinkMessage(key=getattr(msg, "key", None) or b"", value=out)

    mapped = op.map("curate.run", inp, _step)
    filtered = op.filter("curate.drop_none", mapped, lambda m: m is not None)
    sink = KafkaSink(brokers=cfg.redpanda_brokers.split(","), topic=cfg.curated_topic)
    op.output("curate.sink", filtered, sink)
    return flow


def main() -> None:
    """Entrypoint for the ``s2p-curate`` console script."""
    cfg = common.load_config()
    common.configure_logging(cfg.log_level, json_output=not cfg.is_dev)
    log = common.get_logger("s2p.curate")
    log.info(
        "starting curate dataflow",
        brokers=cfg.redpanda_brokers,
        topic=cfg.normalized_topic,
    )
    flow = build_dataflow(cfg)
    from bytewax.run import cli_main

    cli_main(flow)


def now_utc() -> Any:
    """Re-exported for tests; returns a tz-aware UTC datetime."""
    from datetime import datetime

    return datetime.now(timezone.utc)
