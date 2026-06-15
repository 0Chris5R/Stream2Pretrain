"""Feed loader: SourceFeed CRDs in cluster, YAML files in dev.

Production reads ``stream2pretrain.io/v1alpha1`` SourceFeed CRDs through the
Kubernetes API. Dev reads the same shape from a YAML file pointed at by
``S2P_FEED_CONFIG``. The YAML schema is exactly ``[SourceFeedSpec, ...]``.
"""

from __future__ import annotations

from pathlib import Path

import yaml

from schemas.sourcefeed import SourceFeedSpec


def load_feeds_from_yaml(path: str | Path) -> list[SourceFeedSpec]:
    """Parse a list of ``SourceFeedSpec`` from a YAML file.

    Top-level shape supports either ``feeds: [...]`` or a bare list.
    """
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"feed config not found: {p}")
    raw = yaml.safe_load(p.read_text(encoding="utf-8"))
    if isinstance(raw, dict) and "feeds" in raw:
        items = raw["feeds"]
    elif isinstance(raw, list):
        items = raw
    else:
        raise ValueError("feed config must be a list or {feeds: [...]}")
    return [SourceFeedSpec.model_validate(it) for it in items]


def load_feeds_from_kube(
    namespace: str = "stream2pretrain",
    *,
    label_selector: str | None = None,
) -> list[SourceFeedSpec]:
    """Read SourceFeed CRD instances from the cluster.

    Returns an empty list if the kubernetes client is not installed (dev path).
    Errors at runtime if the CRD is missing.
    """
    try:
        from kubernetes import client, config
    except ImportError:
        return []

    try:
        config.load_incluster_config()
    except Exception:
        config.load_kube_config()

    api = client.CustomObjectsApi()
    resp = api.list_namespaced_custom_object(
        group="stream2pretrain.io",
        version="v1alpha1",
        namespace=namespace,
        plural="sourcefeeds",
        label_selector=label_selector or "",
    )
    out: list[SourceFeedSpec] = []
    for item in resp.get("items", []):
        spec = item.get("spec", {})
        # Ensure CRD-required name lives on spec for our Pydantic model.
        spec.setdefault("name", item.get("metadata", {}).get("name", "unnamed"))
        out.append(SourceFeedSpec.model_validate(spec))
    return out


def feeds_by_protocol(feeds: list[SourceFeedSpec], protocol: str) -> list[SourceFeedSpec]:
    """Filter ``feeds`` to those with ``protocol`` and ``enabled=True``."""
    return [f for f in feeds if f.protocol == protocol and f.enabled]
