"""Rollout gate for replica distribution and exact quality-batch semantics."""

from __future__ import annotations

import argparse
import json
import urllib.request
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from typing import Any


def _request(
    opener: urllib.request.OpenerDirector,
    url: str,
    *,
    payload: dict[str, Any] | None = None,
) -> tuple[dict[str, Any], str]:
    body = None if payload is None else json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=body,
        headers={
            "Accept": "application/json",
            "Connection": "close",
            **({"Content-Type": "application/json"} if body is not None else {}),
        },
        method="POST" if body is not None else "GET",
    )
    with opener.open(request, timeout=180) as response:
        value = json.loads(response.read())
        backend = response.headers.get("X-S2P-Model-Backend", "").strip()
    if not isinstance(value, dict):
        raise RuntimeError(f"model service returned a non-object for {url}")
    if not backend:
        raise RuntimeError(f"model service omitted X-S2P-Model-Backend for {url}")
    return value, backend


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", required=True)
    parser.add_argument("--model-family", required=True)
    parser.add_argument("--expected-backends", required=True, type=int)
    parser.add_argument("--distribution-requests", type=int, default=60)
    parser.add_argument("--concurrency", type=int, default=12)
    args = parser.parse_args()
    if (
        args.expected_backends < 1
        or args.distribution_requests < args.expected_backends
        or args.concurrency < 1
    ):
        raise SystemExit("backend and request counts must be positive and internally consistent")

    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    base_url = args.base_url.rstrip("/")
    metadata, _ = _request(opener, f"{base_url}/v1/metadata")
    if metadata.get("ready") is not True:
        raise RuntimeError("model service did not report ready")
    expected_revision = str(
        metadata.get("quality", {}).get(args.model_family, {}).get("revision", "")
    )
    if not expected_revision:
        raise RuntimeError("model service metadata omitted the requested classifier revision")

    def probe(index: int) -> str:
        # A private opener guarantees one independent TCP connection for this
        # actual inference request, matching the curator client contract.
        probe_opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        result, backend = _request(
            probe_opener,
            f"{base_url}/v1/quality",
            payload={
                "model_family": args.model_family,
                "text": f"Scientific classifier distribution probe {index}.",
            },
        )
        if str(result.get("revision", "")) != expected_revision:
            raise RuntimeError("one model backend returned a different classifier revision")
        return backend

    counts: Counter[str] = Counter()
    with ThreadPoolExecutor(max_workers=min(args.concurrency, args.distribution_requests)) as pool:
        counts.update(pool.map(probe, range(args.distribution_requests)))

    if len(counts) < args.expected_backends:
        raise RuntimeError(
            f"only {len(counts)} of {args.expected_backends} ready backends received traffic: "
            f"{dict(counts)}"
        )
    minimum_share = min(counts.values()) / args.distribution_requests
    if args.expected_backends > 1 and minimum_share < 0.10:
        raise RuntimeError(f"one model backend received less than 10% of requests: {dict(counts)}")

    texts = [
        "A controlled scientific explanation with one result.",
        "A reproducible implementation documents its evaluation protocol.",
    ]
    batch, _ = _request(
        opener,
        f"{base_url}/v1/quality:batch",
        payload={"model_family": args.model_family, "texts": texts},
    )
    results = batch.get("results")
    if not isinstance(results, list) or len(results) != len(texts):
        raise RuntimeError("bounded batch returned the wrong result count")
    singletons = []
    for text in texts:
        singleton, _ = _request(
            opener,
            f"{base_url}/v1/quality",
            payload={"model_family": args.model_family, "text": text},
        )
        singletons.append(singleton)
    if results != singletons:
        raise RuntimeError(
            "quality batch changed an ordered one-by-one score or classifier revision"
        )

    print(
        json.dumps(
            {
                "backend_requests": dict(sorted(counts.items())),
                "concurrency": args.concurrency,
                "distribution_requests": args.distribution_requests,
                "expected_backends": args.expected_backends,
                "minimum_backend_share": minimum_share,
                "model_family": args.model_family,
                "ordered_batch_matches_singletons": True,
                "revision": expected_revision,
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
