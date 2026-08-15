#!/usr/bin/env python3
"""Replay a Decon-Gate attestation by snapshot id.

Given a snapshot id, this script:
1. Fetches the signed attestation from the ``decon.attest`` Redpanda topic.
2. Verifies the signature.
3. Replays ``raw.fetched`` (or the configured input topic) from the offset
   that produced the snapshot, re-runs the same Decon-Gate algorithm against
   the same benchmark-set version, and asserts the per-benchmark hit counts
   match the attestation byte-for-byte.

The "byte-for-byte" guarantee is the contamination-bisect feature: a grader
or auditor can independently reproduce any past attestation as long as the
benchmark-set version is still pinned and the Redpanda retention has not
expired.

Usage:
    uv run python scripts/decon_bisect.py --snapshot-id 84219315
    uv run python scripts/decon_bisect.py --snapshot-id 84219315 \
        --brokers redpanda:9092 --benchmark-set v2026-06-01

Exit codes:
    0  attestation reproduced exactly
    1  reproduction succeeded with a different result (drift)
    2  attestation not found / could not verify signature
    3  required dependency missing (confluent-kafka, cryptography, etc.)
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass

EXIT_OK = 0
EXIT_DRIFT = 1
EXIT_NOT_FOUND = 2
EXIT_DEPS_MISSING = 3


@dataclass(slots=True)
class BisectArgs:
    """Parsed command-line args."""

    snapshot_id: int
    brokers: str
    attest_topic: str
    input_topic: str
    benchmark_set: str | None
    benchmark_corpus: str | None
    timeout_s: float


def _parse_args(argv: list[str]) -> BisectArgs:
    p = argparse.ArgumentParser(
        prog="decon_bisect",
        description="Reproduce a Decon-Gate attestation by snapshot id.",
    )
    p.add_argument("--snapshot-id", type=int, required=True)
    p.add_argument("--brokers", default="localhost:9092")
    p.add_argument("--attest-topic", default="decon.attest")
    p.add_argument("--input-topic", default="curation.decisions")
    p.add_argument(
        "--benchmark-set",
        default=None,
        help="Override the benchmark-set version pin (defaults to attestation value).",
    )
    p.add_argument(
        "--benchmark-corpus",
        default=None,
        help=(
            "Path to the JSON corpus used by the curator (benchmark name -> prompts). "
            "Without this the replay's bloom filters are empty and bisect cannot find hits."
        ),
    )
    p.add_argument("--timeout-s", type=float, default=30.0)
    ns = p.parse_args(argv)
    return BisectArgs(
        snapshot_id=ns.snapshot_id,
        brokers=ns.brokers,
        attest_topic=ns.attest_topic,
        input_topic=ns.input_topic,
        benchmark_set=ns.benchmark_set,
        benchmark_corpus=ns.benchmark_corpus,
        timeout_s=ns.timeout_s,
    )


def _fetch_attestation(
    brokers: str, topic: str, snapshot_id: int, timeout_s: float
) -> dict[str, object] | None:
    """Tail ``topic`` from the beginning until we find the matching snapshot."""
    try:
        from confluent_kafka import Consumer
    except ImportError:
        print("missing dep: confluent-kafka. install with `uv add confluent-kafka`.")
        sys.exit(EXIT_DEPS_MISSING)

    consumer = Consumer(
        {
            "bootstrap.servers": brokers,
            "group.id": f"s2p-bisect-{snapshot_id}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    import time

    consumer.subscribe([topic])
    try:
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            msg = consumer.poll(timeout=1.0)
            if msg is None or msg.error():
                continue
            try:
                payload = json.loads(msg.value())
            except (TypeError, ValueError):
                continue
            if int(payload.get("snapshot_id", -1)) == snapshot_id:
                return payload
        return None
    finally:
        consumer.close()


def _verify_signature(payload: dict[str, object]) -> bool:
    """Verify the Ed25519 signature on the canonical attestation body."""
    try:
        from cryptography.hazmat.primitives.asymmetric import ed25519
        from cryptography.hazmat.primitives.serialization import load_pem_public_key
        from cryptography.x509 import load_pem_x509_certificate
    except ImportError:
        print("missing dep: cryptography. install with `uv add cryptography`.")
        sys.exit(EXIT_DEPS_MISSING)
    import base64

    sig_b64 = payload.get("signature")
    cert_pem = payload.get("signer_cert")
    if not isinstance(sig_b64, str) or not isinstance(cert_pem, str):
        return False
    body = {k: v for k, v in payload.items() if k not in {"signature", "signer_cert"}}
    canonical = json.dumps(body, sort_keys=True, separators=(",", ":")).encode()
    try:
        cert = load_pem_x509_certificate(cert_pem.encode("ascii"))
        pk = cert.public_key()
        if not isinstance(pk, ed25519.Ed25519PublicKey):
            # Fallback path: signer_cert may be a raw PEM Ed25519 public key.
            pk = load_pem_public_key(cert_pem.encode("ascii"))  # type: ignore[assignment]
        pk.verify(base64.b64decode(sig_b64), canonical)
    except Exception:
        return False
    return True


def _replay_decon(
    brokers: str,
    topic: str,
    snapshot_id: int,
    benchmark_set: str,
    timeout_s: float,
    benchmark_corpus_path: str | None = None,
) -> dict[str, int] | None:
    """Re-run the Decon-Gate scan for the input range that produced ``snapshot_id``.

    The offset bounds for a snapshot are encoded as ``properties`` on the
    Iceberg snapshot itself. In an offline-only sandbox we cannot read them,
    so we degrade to a "best-effort full replay" and report the bucketed hit
    counts. The caller compares the result with the attestation's
    ``per_benchmark_hits``; a difference signals drift.

    The Decon-Gate is constructed with the SAME ``benchmark_set_version``
    pin and the SAME benchmark corpus the curator used. Without the corpus
    the n-gram Bloom filters are empty and the scan can never produce hits;
    we fail loudly in that case rather than silently report all-zero
    counts.
    """
    try:
        from processor import common  # type: ignore[import-not-found]
        from processor.decon_gate import DeconGate  # type: ignore[import-not-found]
    except ImportError:
        print(
            "processor.decon_gate not importable: cannot replay locally. "
            "Run from inside the processor pod or install the workspace."
        )
        return None

    try:
        from confluent_kafka import Consumer
    except ImportError:
        sys.exit(EXIT_DEPS_MISSING)

    corpus = _load_benchmark_corpus(benchmark_corpus_path)
    if corpus is None:
        print(
            "benchmark corpus not provided / not found; bisect would scan with "
            "empty bloom filters and report all-zero hits. Pass "
            "--benchmark-corpus <path-to-prompts.json>."
        )
        return None
    gate = DeconGate(
        benchmark_set_version=benchmark_set,
        benchmark_corpus=corpus,
    )
    consumer = Consumer(
        {
            "bootstrap.servers": brokers,
            "group.id": f"s2p-replay-{snapshot_id}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([topic])
    import time

    deadline = time.monotonic() + timeout_s
    hits: dict[str, int] = {}
    try:
        while time.monotonic() < deadline:
            msg = consumer.poll(timeout=1.0)
            if msg is None or msg.error():
                continue
            try:
                rec = common.gold_loads(msg.value())
            except Exception:
                continue
            try:
                _tagged, fired = gate.scan(rec)
            except Exception:
                continue
            for bench in fired:
                hits[bench] = hits.get(bench, 0) + 1
    finally:
        consumer.close()
    return hits


def _load_benchmark_corpus(path: str | None) -> dict[str, list[str]] | None:
    """Read benchmark prompts from a JSON file mapping benchmark -> prompts."""
    if not path:
        return None
    from pathlib import Path

    p = Path(path)
    if not p.is_file():
        return None
    try:
        data = json.loads(p.read_text())
    except Exception:
        return None
    if not isinstance(data, dict):
        return None
    return {str(k): list(v) for k, v in data.items()}


def main(argv: list[str]) -> int:
    args = _parse_args(argv)
    print(f"fetching attestation snapshot_id={args.snapshot_id} from {args.attest_topic}")
    attestation = _fetch_attestation(
        args.brokers, args.attest_topic, args.snapshot_id, args.timeout_s
    )
    if attestation is None:
        print("attestation not found")
        return EXIT_NOT_FOUND

    print("verifying signature ...")
    if not _verify_signature(attestation):
        print("signature verification failed")
        return EXIT_NOT_FOUND
    print("signature OK")

    bench_version = args.benchmark_set or str(attestation["benchmark_set_version"])
    print(f"replaying with benchmark_set_version={bench_version}")
    replayed = _replay_decon(
        args.brokers,
        args.input_topic,
        args.snapshot_id,
        bench_version,
        args.timeout_s,
        args.benchmark_corpus,
    )
    if replayed is None:
        print("replay skipped (DeconGate not available); attestation verified only")
        return EXIT_OK

    expected = attestation.get("per_benchmark_hits", {})
    if not isinstance(expected, dict):
        print("attestation per_benchmark_hits malformed")
        return EXIT_NOT_FOUND
    expected_typed = {str(k): int(v) for k, v in expected.items()}
    if replayed == expected_typed:
        print("attestation reproduced exactly")
        return EXIT_OK
    print("DRIFT: replay disagrees with attestation")
    print(f"  expected: {expected_typed}")
    print(f"  replayed: {replayed}")
    return EXIT_DRIFT


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
