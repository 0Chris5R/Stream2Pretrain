"""Rollout gate for replica distribution and exact quality-batch semantics."""

from __future__ import annotations

import argparse
import json
import urllib.parse
import urllib.request
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from processor.model_client import resolved_endpoint_urls


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
    parser.add_argument("--headless-host")
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

    direct_backends: set[str] = set()
    direct_endpoint_count = 0
    if args.headless_host:
        parsed = urllib.parse.urlsplit(base_url)
        if parsed.port is None:
            raise RuntimeError("base URL must contain the model service port")
        endpoint_urls = resolved_endpoint_urls(
            args.headless_host,
            scheme=parsed.scheme,
            port=parsed.port,
        )
        if len(endpoint_urls) != args.expected_backends:
            raise RuntimeError(
                f"headless service resolved {len(endpoint_urls)} endpoints; "
                f"expected {args.expected_backends}"
            )

        def direct_probe(endpoint: str) -> str:
            direct_opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
            result, backend = _request(
                direct_opener,
                f"{endpoint}/v1/quality",
                payload={
                    "model_family": args.model_family,
                    "text": f"Direct classifier endpoint probe for {endpoint}.",
                },
            )
            if str(result.get("revision", "")) != expected_revision:
                raise RuntimeError("one direct model endpoint returned a different revision")
            return backend

        with ThreadPoolExecutor(max_workers=len(endpoint_urls)) as pool:
            direct_backends.update(pool.map(direct_probe, endpoint_urls))
        direct_endpoint_count = len(endpoint_urls)
        if len(direct_backends) != direct_endpoint_count:
            raise RuntimeError(
                "headless endpoint routing did not reach one distinct backend per ready Pod: "
                f"{sorted(direct_backends)}"
            )

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

    # Keep enough independent connections per ready backend for the
    # distribution check to remain meaningful when KEDA scales beyond the
    # original three-Pod profile.
    counts: Counter[str] = Counter()
    distribution_requests = 0
    minimum_share = 0.0
    minimum_required_share = 1.0
    for round_index in range(3):
        # KEDA can add one endpoint between reading readyReplicas and starting
        # this gate. Accumulate another full sample when that new backend has
        # not yet had time to receive a representative share.
        observed_backends = max(args.expected_backends, len(counts))
        round_requests = max(args.distribution_requests, observed_backends * 20)
        start_index = distribution_requests
        with ThreadPoolExecutor(max_workers=min(args.concurrency, round_requests)) as pool:
            counts.update(pool.map(probe, range(start_index, start_index + round_requests)))
        distribution_requests += round_requests

        if len(counts) < args.expected_backends:
            if round_index < 2:
                continue
            raise RuntimeError(
                f"only {len(counts)} of {args.expected_backends} ready backends received traffic: "
                f"{dict(counts)}"
            )
        observed_backends = len(counts)
        minimum_share = min(counts.values()) / distribution_requests
        minimum_required_share = min(0.10, 0.5 / observed_backends)
        if observed_backends == 1 or minimum_share >= minimum_required_share:
            break
    else:
        raise RuntimeError(
            "one model backend received less than half its uniform share "
            f"({minimum_required_share:.4f}): {dict(counts)}"
        )

    texts = [
        "A controlled scientific explanation with one result.",
        "A reproducible implementation documents its evaluation protocol.",
    ]
    batch, batch_backend = _request(
        opener,
        f"{base_url}/v1/quality:batch",
        payload={"model_family": args.model_family, "texts": texts},
    )
    results = batch.get("results")
    if not isinstance(results, list) or len(results) != len(texts):
        raise RuntimeError("bounded batch returned the wrong result count")
    singletons = []
    for text in texts:
        # Batch parity is a property of one immutable runtime. A ClusterIP may
        # route these independent connections to CPUs with slightly different
        # kernels, so compare against the exact Pod that served the batch. The
        # distribution probe above separately proves all ready Pods receive
        # traffic and expose the same pinned revision.
        for _ in range(max(10, args.expected_backends * 10)):
            singleton, backend = _request(
                opener,
                f"{base_url}/v1/quality",
                payload={"model_family": args.model_family, "text": text},
            )
            if backend == batch_backend:
                singletons.append(singleton)
                break
        else:
            raise RuntimeError(
                f"could not route singleton parity probe to batch backend {batch_backend}"
            )
    if results != singletons:
        raise RuntimeError(
            "quality batch changed an ordered one-by-one score or classifier revision "
            f"on backend {batch_backend}: batch={results!r}, singletons={singletons!r}"
        )

    print(
        json.dumps(
            {
                "backend_requests": dict(sorted(counts.items())),
                "batch_parity_backend": batch_backend,
                "concurrency": args.concurrency,
                "distribution_requests": distribution_requests,
                "direct_backends": sorted(direct_backends),
                "direct_endpoint_count": direct_endpoint_count,
                "expected_backends": args.expected_backends,
                "observed_backends": len(counts),
                "minimum_backend_share": minimum_share,
                "minimum_required_backend_share": minimum_required_share,
                "model_family": args.model_family,
                "ordered_batch_matches_singletons": True,
                "revision": expected_revision,
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
