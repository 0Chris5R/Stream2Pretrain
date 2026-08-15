"""Streaming Decon-Gate: 13-gram Bloom + E5 embedding sketch.

This is novelty pillar N1 from RESEARCH.md. The gate has two layers:

1. **13-gram Bloom**: every benchmark prompt/answer is shingled into 13-token
   sequences and added to a per-benchmark Bloom filter. Each curated
   document is shingled in the same way; if a shingle hits, the document
   is rejected as contaminated.
2. **E5 embedding sketch**: an optional ONNX ``intfloat/e5-small`` index of
   benchmark prompts. We compute a max-cosine-similarity gate; ``>= 0.92``
   is treated as a hit. The sketch catches semantically-paraphrased
   benchmark variants that the n-gram filter misses. Falls back to a
   deterministic hash sketch if onnxruntime / e5 weights are unavailable.

Per ingested ``GoldRecord``, the gate appends contamination flags to the
``contaminated_with`` list and increments the per-snapshot scan counters.
On every Iceberg snapshot commit, :meth:`DeconGate.flush_attestation`
serializes a :class:`schemas.decon.DeconAttestation`, signs it via
:mod:`processor.sign`, and returns it for publication on the
``decon.attest`` topic.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from collections.abc import Iterable, Iterator
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

from processor.sign import AttestationSigner
from schemas.decon import BenchmarkName, DeconAttestation
from schemas.gold import GoldRecord

DEFAULT_NGRAM: int = 13
DEFAULT_BLOOM_BITS: int = 1 << 24  # 16 MiB
DEFAULT_BLOOM_HASHES: int = 5
DEFAULT_EMBED_THRESHOLD: float = 0.92
BENCHMARKS: tuple[BenchmarkName, ...] = ("MMLU", "GSM8K", "HumanEval", "MATH", "GPQA")

_WORD = re.compile(r"\w+", re.UNICODE)


def shingle_ngrams(text: str, n: int = DEFAULT_NGRAM) -> Iterator[str]:
    """Yield space-joined n-grams of lowercase word tokens.

    For documents shorter than ``n`` tokens we emit the whole token sequence
    as a single "short" shingle so benchmark prompts shorter than 13 words
    still register in the n-gram Bloom. This is the same trick CCNet uses
    in its short-prompt fallback path.
    """
    tokens = [t.lower() for t in _WORD.findall(text)]
    if len(tokens) < n:
        if tokens:
            yield " ".join(tokens)
        return
    for i in range(len(tokens) - n + 1):
        yield " ".join(tokens[i : i + n])


class _Bloom:
    """Tiny Bloom filter used by the per-benchmark n-gram index."""

    __slots__ = ("_bits", "_count", "_data", "_hashes")

    def __init__(self, bits: int = DEFAULT_BLOOM_BITS, hashes: int = DEFAULT_BLOOM_HASHES) -> None:
        self._bits = bits
        self._hashes = hashes
        self._data = bytearray((bits + 7) // 8)
        self._count = 0

    def add(self, item: bytes) -> None:
        for h in self._iter_hashes(item):
            self._data[h >> 3] |= 1 << (h & 7)
        self._count += 1

    def __contains__(self, item: bytes) -> bool:
        return all((self._data[h >> 3] & (1 << (h & 7))) for h in self._iter_hashes(item))

    def _iter_hashes(self, item: bytes) -> Iterable[int]:
        digest = hashlib.blake2b(item, digest_size=16, person=b"s2pdcgt").digest()
        a = int.from_bytes(digest[:8], "little", signed=False)
        b = int.from_bytes(digest[8:], "little", signed=False)
        for k in range(self._hashes):
            yield (a + k * b) % self._bits

    @property
    def cardinality(self) -> int:
        """Approx insertions (exact when no removals)."""
        return self._count


@dataclass(slots=True)
class _ScanState:
    """Mutable counters reset on each :meth:`flush_attestation`."""

    tokens_scanned: int = 0
    tokens_flagged: int = 0
    rejected_doc_hashes: list[str] = field(default_factory=list)
    per_benchmark_hits: dict[BenchmarkName, int] = field(
        default_factory=lambda: {b: 0 for b in BENCHMARKS}
    )


class DeconGate:
    """Streaming contamination guard.

    The gate is intentionally side-effect-free other than its internal
    accumulators - calling code decides whether to reject the document
    or just tag it.
    """

    def __init__(
        self,
        *,
        benchmark_set_version: str,
        benchmark_corpus: dict[BenchmarkName, list[str]] | None = None,
        signer: AttestationSigner | None = None,
        ngram: int = DEFAULT_NGRAM,
        embedding: _EmbeddingSketch | None = None,
    ) -> None:
        self._set_version = benchmark_set_version
        self._ngram = ngram
        self._signer = signer or AttestationSigner()
        self._blooms: dict[BenchmarkName, _Bloom] = {
            benchmark: _Bloom() for benchmark in BENCHMARKS
        }
        # Track every shingle width we have indexed so ``scan`` knows which
        # window sizes to try. The default n is always present; short
        # benchmark prompts add their own sub-13 token window.
        self._indexed_widths: set[int] = {ngram}
        self._embedding = embedding
        self._state = _ScanState()
        if benchmark_corpus:
            for bench, prompts in benchmark_corpus.items():
                for prompt in prompts:
                    self.index_benchmark_prompt(bench, prompt)

    @property
    def benchmark_set_version(self) -> str:
        return self._set_version

    def index_benchmark_prompt(self, benchmark: BenchmarkName, prompt: str) -> None:
        """Add a single benchmark prompt to the n-gram (and embedding) index.

        Short prompts (fewer than ``self._ngram`` tokens) cannot fit into a
        single 13-gram shingle. We register them under their natural width
        too, and remember that width so :meth:`scan` knows to slide a
        window of that size over candidate documents.
        """
        bf = self._blooms[benchmark]
        for shingle in shingle_ngrams(prompt, n=self._ngram):
            bf.add(shingle.encode("utf-8"))
        prompt_tokens = _WORD.findall(prompt)
        if 0 < len(prompt_tokens) < self._ngram:
            short_n = len(prompt_tokens)
            self._indexed_widths.add(short_n)
            for shingle in shingle_ngrams(prompt, n=short_n):
                bf.add(shingle.encode("utf-8"))
        if self._embedding is not None:
            self._embedding.add(benchmark, prompt)

    def scan(self, record: GoldRecord) -> tuple[GoldRecord, list[BenchmarkName]]:
        """Inspect one curated document; return (possibly-tagged record, hits).

        The returned record has ``contaminated_with`` extended with the
        benchmarks that fired on this document. The caller decides whether
        to also append ``"decontamination_hit"`` to ``reject_reasons`` and
        drop the row from the gold table.
        """
        text = record.text or ""
        # ``tokens_scanned`` is defined as the number of canonical n-gram
        # shingles examined per document. We count once at the canonical
        # width regardless of how many short-prompt widths the gate also
        # tested - the canonical width is the one the attestation
        # documents to consumers, and counting per-width would inflate
        # the denominator without adding signal.
        tokens_scanned = max(1, sum(1 for _ in shingle_ngrams(text, self._ngram)))
        hits: list[BenchmarkName] = []
        exact_hits: list[BenchmarkName] = []
        semantic_hits: list[BenchmarkName] = []
        max_similarity = 0.0
        flagged_shingles: set[str] = set()
        for bench, bf in self._blooms.items():
            matched = False
            for width in self._indexed_widths:
                for shingle in shingle_ngrams(text, n=width):
                    if shingle.encode("utf-8") in bf:
                        hits.append(bench)
                        exact_hits.append(bench)
                        flagged_shingles.add(shingle)
                        matched = True
                        break
                if matched:
                    break
        if self._embedding is not None:
            for chunk in _semantic_chunks(text):
                for bench, sim in self._embedding.query(chunk):
                    max_similarity = max(max_similarity, float(sim))
                    if sim >= DEFAULT_EMBED_THRESHOLD and bench not in hits:
                        hits.append(bench)
                        semantic_hits.append(bench)
        # Update accumulators. ``tokens_flagged`` is the count of unique
        # shingles that actually fired - bounded above by tokens_scanned so
        # the ratio remains a meaningful "fraction of corpus contaminated".
        self._state.tokens_scanned += tokens_scanned
        if hits:
            flagged_count = max(1, min(tokens_scanned, len(flagged_shingles)))
            self._state.tokens_flagged += flagged_count
            self._state.rejected_doc_hashes.append(record.doc_id)
            for bench in hits:
                self._state.per_benchmark_hits[bench] += 1
        diagnostic_record = record.model_copy(
            update={
                "decon_exact_matches": sorted(set(exact_hits)),
                "decon_semantic_matches": sorted(set(semantic_hits)),
                "decon_max_similarity": max_similarity,
                "decon_ngram_size": self._ngram,
                "decon_embedding_revision": (
                    f"{self._embedding.revision}/{self._embedding.backend}"
                    if self._embedding is not None
                    else "disabled"
                ),
                "benchmark_set_version": self._set_version,
            }
        )
        # Make a tagged copy with the hits appended.
        if not hits:
            return diagnostic_record, []
        new_marks = sorted(set(diagnostic_record.contaminated_with) | set(hits))
        tagged = diagnostic_record.model_copy(update={"contaminated_with": new_marks})
        return tagged, hits

    def flush_attestation(
        self,
        *,
        snapshot_id: int,
        committed_at: datetime | None = None,
        extra_per_benchmark_hits: dict[BenchmarkName, int] | None = None,
        extra_rejected_doc_hashes: list[str] | None = None,
        extra_tokens_scanned: int = 0,
        extra_tokens_flagged: int = 0,
    ) -> DeconAttestation:
        """Mint a signed attestation for the just-committed snapshot.

        After this call the internal counters are reset so the next snapshot
        begins from zero. The caller is expected to publish the attestation
        on the ``decon.attest`` topic.

        ``extra_*`` arguments let a caller (e.g. the IcebergWriter) merge
        per-snapshot counts that were accumulated in a separate gate
        instance (the curate dataflow's gate is the one that actually
        scans documents; the writer aggregates from the buffered
        :class:`GoldRecord` rows). Merged counts are summed into the
        local accumulators so a single attestation reflects the full
        per-snapshot contamination signal.
        """
        committed = committed_at or datetime.now(UTC)
        per_bench: dict[BenchmarkName, int] = dict(self._state.per_benchmark_hits)
        if extra_per_benchmark_hits:
            for k, v in extra_per_benchmark_hits.items():
                per_bench[k] = per_bench.get(k, 0) + int(v)
        rejected = list(self._state.rejected_doc_hashes)
        if extra_rejected_doc_hashes:
            rejected.extend(extra_rejected_doc_hashes)
        tokens_scanned = self._state.tokens_scanned + max(0, int(extra_tokens_scanned))
        tokens_flagged = self._state.tokens_flagged + max(0, int(extra_tokens_flagged))
        unsigned_payload = {
            "snapshot_id": snapshot_id,
            "committed_at": committed.isoformat().replace("+00:00", "Z"),
            "benchmark_set_version": self._set_version,
            "benchmarks": list(self._blooms.keys()),
            "tokens_scanned": tokens_scanned,
            "tokens_flagged": tokens_flagged,
            "rejected_doc_hashes": rejected,
            "per_benchmark_hits": per_bench,
        }
        canonical = json.dumps(unsigned_payload, sort_keys=True, separators=(",", ":")).encode(
            "utf-8"
        )
        sign_result = self._signer.sign(canonical)
        attestation = DeconAttestation(
            snapshot_id=snapshot_id,
            committed_at=committed,
            benchmark_set_version=self._set_version,
            benchmarks=list(self._blooms.keys()),
            tokens_scanned=tokens_scanned,
            tokens_flagged=tokens_flagged,
            rejected_doc_hashes=rejected,
            per_benchmark_hits=per_bench,
            signature=sign_result.signature_b64,
            signer_cert=sign_result.cert_pem,
        )
        self._state = _ScanState()
        return attestation


class _EmbeddingSketch:
    """E5-small ONNX sketch index with deterministic hash-based fallback.

    Public methods:
      - ``add(benchmark, text)`` indexes a prompt.
      - ``query(text)`` returns iterable ``(benchmark, max_cos_sim)`` pairs.

    The fallback never reports cos-sim >= ``DEFAULT_EMBED_THRESHOLD`` so it
    is functionally a no-op gate when the real model is unavailable - this
    is intentional: the n-gram Bloom remains the authoritative signal in
    development environments.
    """

    def __init__(
        self,
        model_dir: str | Path | None,
        *,
        revision: str | None = None,
        allow_fallback: bool = True,
    ) -> None:
        self._model_dir = Path(model_dir) if model_dir else None
        self._allow_fallback = allow_fallback
        self._session, self._tokenizer = self._load(self._model_dir)
        if not allow_fallback and not self.is_model_loaded:
            raise RuntimeError("the pinned E5-small-v2 ONNX model is required")
        self._index: dict[BenchmarkName, list[list[float]]] = {}
        self._revision = revision or (
            "intfloat/e5-small-v2-onnx" if self.is_model_loaded else "hash-fallback"
        )

    @property
    def is_model_loaded(self) -> bool:
        """Whether the semantic gate is backed by the real E5 model."""
        return self._session is not None and self._tokenizer is not None

    @property
    def revision(self) -> str:
        return self._revision

    @property
    def backend(self) -> str:
        return "onnxruntime-cpu" if self.is_model_loaded else "hash-fallback"

    @staticmethod
    def _load(model_dir: Path | None) -> tuple[object | None, object | None]:
        if model_dir is None or not model_dir.is_dir():
            return None, None
        try:
            import onnxruntime as ort  # type: ignore[import-untyped]
            from transformers import AutoTokenizer  # type: ignore[import-untyped]
        except Exception:
            return None, None
        model_file = model_dir / "model.onnx"
        if not model_file.is_file():
            return None, None
        sess = ort.InferenceSession(str(model_file), providers=["CPUExecutionProvider"])
        tok = AutoTokenizer.from_pretrained(str(model_dir), local_files_only=True)
        return sess, tok

    def add(self, benchmark: BenchmarkName, text: str) -> None:
        vec = self._embed(text)
        self._index.setdefault(benchmark, []).append(vec)

    def query(self, text: str) -> Iterable[tuple[BenchmarkName, float]]:
        if not self._index:
            return []
        q = self._embed(text)
        out: list[tuple[BenchmarkName, float]] = []
        for bench, vecs in self._index.items():
            best = 0.0
            for v in vecs:
                sim = _cosine(q, v)
                if sim > best:
                    best = sim
            out.append((bench, best))
        return out

    def _embed(self, text: str) -> list[float]:
        if self._session is None or self._tokenizer is None:
            return _hash_embedding(text)
        try:
            encoded = self._tokenizer(  # type: ignore[misc]
                f"query: {text}",
                max_length=512,
                truncation=True,
                padding="max_length",
                return_tensors="np",
            )
            input_names = {
                value.name
                for value in self._session.get_inputs()  # type: ignore[union-attr]
            }
            feeds = {
                key: encoded[key].astype("int64")
                for key in ("input_ids", "attention_mask", "token_type_ids")
                if key in encoded and key in input_names
            }
            outputs = self._session.run(None, feeds)  # type: ignore[union-attr]
            hidden = outputs[0]
            mask = encoded["attention_mask"].astype("float32")[:, :, None]
            denominator = mask.sum(axis=1).clip(1.0, None)
            arr = ((hidden * mask).sum(axis=1) / denominator).reshape(-1)
            norm = math.sqrt(float((arr * arr).sum()))
            if norm == 0:
                return [0.0] * len(arr)
            return list((arr / norm).tolist())
        except Exception:
            if not self._allow_fallback:
                raise
            return _hash_embedding(text)


def _cosine(a: list[float], b: list[float]) -> float:
    if not a or not b or len(a) != len(b):
        return 0.0
    num = sum(x * y for x, y in zip(a, b, strict=True))
    da = math.sqrt(sum(x * x for x in a))
    db = math.sqrt(sum(y * y for y in b))
    if da == 0 or db == 0:
        return 0.0
    return num / (da * db)


def _semantic_chunks(text: str, *, max_chunks: int = 8, target_words: int = 320) -> list[str]:
    """Build bounded section/paragraph chunks for E5 instead of truncating a paper."""
    paragraphs = [value.strip() for value in text.split("\n\n") if value.strip()]
    chunks: list[str] = []
    current: list[str] = []
    words = 0
    for paragraph in paragraphs:
        count = len(_WORD.findall(paragraph))
        if current and words + count > target_words:
            chunks.append("\n\n".join(current))
            current = []
            words = 0
            if len(chunks) >= max_chunks:
                break
        current.append(paragraph)
        words += count
    if current and len(chunks) < max_chunks:
        chunks.append("\n\n".join(current))
    return chunks or [text]


def _hash_embedding(text: str, dim: int = 64) -> list[float]:
    """Deterministic projection: blake2b -> dim floats in [-1, 1]."""
    digest = hashlib.blake2b(text.encode("utf-8"), digest_size=dim).digest()
    raw = [(b - 128) / 128.0 for b in digest]
    norm = math.sqrt(sum(x * x for x in raw)) or 1.0
    return [x / norm for x in raw]
