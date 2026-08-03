"""Collect Stream2Pretrain capacity evidence from a target k3s cluster.

This script intentionally records measured values only. If a data source is not
available in the current kube context, the report says ``needs-measurement``
instead of filling a guessed value.
"""

from __future__ import annotations

import argparse
import json
import subprocess
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

TOPICS = ("raw.fetched", "docs.normalized", "docs.curated", "decon.attest")


@dataclass(frozen=True, slots=True)
class CommandResult:
    command: list[str]
    returncode: int
    stdout: str
    stderr: str


def run(command: list[str], *, timeout: int = 30) -> CommandResult:
    """Run a command and capture output without raising."""
    try:
        proc = subprocess.run(
            command,
            check=False,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        return CommandResult(command, proc.returncode, proc.stdout, proc.stderr)
    except (OSError, subprocess.TimeoutExpired) as exc:
        return CommandResult(command, 124, "", str(exc))


def parse_cpu_quantity(value: str) -> int | None:
    """Parse a Kubernetes CPU quantity into millicores."""
    if value == "":
        return None
    if value.endswith("n"):
        return _scaled_int(value[:-1], 1 / 1_000_000)
    if value.endswith("u"):
        return _scaled_int(value[:-1], 1 / 1_000)
    if value.endswith("m"):
        return _scaled_int(value[:-1], 1)
    return _scaled_int(value, 1000)


def parse_byte_quantity(value: str) -> int | None:
    """Parse a Kubernetes memory/storage quantity into bytes."""
    if value == "":
        return None
    suffixes = {
        "Ki": 1024,
        "Mi": 1024**2,
        "Gi": 1024**3,
        "Ti": 1024**4,
        "K": 1000,
        "M": 1000**2,
        "G": 1000**3,
        "T": 1000**4,
    }
    for suffix, multiplier in suffixes.items():
        if value.endswith(suffix):
            return _scaled_int(value[: -len(suffix)], multiplier)
    return _scaled_int(value, 1)


def _scaled_int(value: str, multiplier: float) -> int | None:
    try:
        return int(float(value) * multiplier)
    except ValueError:
        return None


def kubectl_json(args: list[str], *, namespace: str | None = None) -> tuple[dict[str, Any] | None, str | None]:
    command = ["kubectl"]
    if namespace:
        command.extend(["-n", namespace])
    command.extend(args)
    command.extend(["-o", "json"])
    result = run(command)
    if result.returncode != 0:
        return None, result.stderr.strip() or result.stdout.strip() or "kubectl command failed"
    try:
        parsed = json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        return None, f"invalid JSON from {' '.join(command)}: {exc}"
    if not isinstance(parsed, dict):
        return None, f"unexpected JSON shape from {' '.join(command)}"
    return parsed, None


def node_capacity() -> dict[str, Any]:
    data, err = kubectl_json(["get", "nodes"])
    if err or data is None:
        return {"status": "needs-measurement", "reason": err}
    nodes: list[dict[str, Any]] = []
    total_cpu_m = 0
    total_memory_bytes = 0
    for item in data.get("items", []):
        status = item.get("status", {})
        allocatable = status.get("allocatable", {})
        cpu_m = parse_cpu_quantity(str(allocatable.get("cpu", "")))
        memory_bytes = parse_byte_quantity(str(allocatable.get("memory", "")))
        if cpu_m is not None:
            total_cpu_m += cpu_m
        if memory_bytes is not None:
            total_memory_bytes += memory_bytes
        nodes.append(
            {
                "name": item.get("metadata", {}).get("name"),
                "allocatable_cpu_m": cpu_m,
                "allocatable_memory_bytes": memory_bytes,
            }
        )
    return {
        "status": "measured",
        "node_count": len(nodes),
        "allocatable_cpu_m": total_cpu_m,
        "allocatable_memory_bytes": total_memory_bytes,
        "nodes": nodes,
    }


def workload_resources(namespace: str) -> dict[str, Any]:
    data, err = kubectl_json(["get", "pods"], namespace=namespace)
    if err or data is None:
        return {"status": "needs-measurement", "reason": err}
    pods: list[dict[str, Any]] = []
    request_cpu_m = 0
    request_memory_bytes = 0
    limit_cpu_m = 0
    limit_memory_bytes = 0
    for item in data.get("items", []):
        pod_request_cpu_m = 0
        pod_request_memory_bytes = 0
        pod_limit_cpu_m = 0
        pod_limit_memory_bytes = 0
        for container in item.get("spec", {}).get("containers", []):
            resources = container.get("resources", {})
            requests = resources.get("requests", {})
            limits = resources.get("limits", {})
            pod_request_cpu_m += parse_cpu_quantity(str(requests.get("cpu", ""))) or 0
            pod_request_memory_bytes += parse_byte_quantity(str(requests.get("memory", ""))) or 0
            pod_limit_cpu_m += parse_cpu_quantity(str(limits.get("cpu", ""))) or 0
            pod_limit_memory_bytes += parse_byte_quantity(str(limits.get("memory", ""))) or 0
        request_cpu_m += pod_request_cpu_m
        request_memory_bytes += pod_request_memory_bytes
        limit_cpu_m += pod_limit_cpu_m
        limit_memory_bytes += pod_limit_memory_bytes
        pods.append(
            {
                "name": item.get("metadata", {}).get("name"),
                "phase": item.get("status", {}).get("phase"),
                "request_cpu_m": pod_request_cpu_m,
                "request_memory_bytes": pod_request_memory_bytes,
                "limit_cpu_m": pod_limit_cpu_m,
                "limit_memory_bytes": pod_limit_memory_bytes,
            }
        )
    return {
        "status": "measured",
        "pod_count": len(pods),
        "request_cpu_m": request_cpu_m,
        "request_memory_bytes": request_memory_bytes,
        "limit_cpu_m": limit_cpu_m,
        "limit_memory_bytes": limit_memory_bytes,
        "pods": pods,
    }


def pvc_capacity(namespace: str) -> dict[str, Any]:
    data, err = kubectl_json(["get", "pvc"], namespace=namespace)
    if err or data is None:
        return {"status": "needs-measurement", "reason": err}
    claims = []
    for item in data.get("items", []):
        requested = item.get("spec", {}).get("resources", {}).get("requests", {}).get("storage")
        capacity = item.get("status", {}).get("capacity", {}).get("storage")
        claims.append(
            {
                "name": item.get("metadata", {}).get("name"),
                "phase": item.get("status", {}).get("phase"),
                "storage_class": item.get("spec", {}).get("storageClassName"),
                "requested_bytes": parse_byte_quantity(str(requested or "")),
                "capacity_bytes": parse_byte_quantity(str(capacity or "")),
            }
        )
    seed_claims = [c for c in claims if "seed" in str(c["name"]).lower() or "hf" in str(c["name"]).lower()]
    return {"status": "measured", "claims": claims, "seed_loader_claims": seed_claims}


def redpanda_topics(namespace: str) -> dict[str, Any]:
    pods, err = kubectl_json(["get", "pods", "-l", "app.kubernetes.io/name=redpanda"], namespace=namespace)
    if err or pods is None or not pods.get("items"):
        return {"status": "needs-measurement", "reason": err or "no Redpanda pods found"}
    pod_name = pods["items"][0].get("metadata", {}).get("name")
    topics: dict[str, Any] = {}
    for topic in TOPICS:
        result = run(
            [
                "kubectl",
                "-n",
                namespace,
                "exec",
                str(pod_name),
                "--",
                "rpk",
                "topic",
                "describe",
                topic,
                "-f",
                "json",
            ],
            timeout=30,
        )
        if result.returncode != 0:
            topics[topic] = {"status": "needs-measurement", "reason": result.stderr.strip()}
            continue
        try:
            topics[topic] = {"status": "measured", "raw": json.loads(result.stdout)}
        except json.JSONDecodeError:
            topics[topic] = {"status": "measured", "raw_text": result.stdout.strip()}
    return {"status": "measured", "pod": pod_name, "topics": topics}


def minio_capacity(namespace: str) -> dict[str, Any]:
    pods, err = kubectl_json(["get", "pods", "-l", "app=minio"], namespace=namespace)
    if err or pods is None or not pods.get("items"):
        return {"status": "needs-measurement", "reason": err or "no MinIO pods found"}
    return {
        "status": "measured",
        "pods": [item.get("metadata", {}).get("name") for item in pods.get("items", [])],
        "throughput": "needs-measurement: run MinIO warp or equivalent against this cluster",
    }


def collect(namespace: str, redpanda_namespace: str, minio_namespace: str) -> dict[str, Any]:
    return {
        "generated_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "namespace": namespace,
        "node_capacity": node_capacity(),
        "workload_resources": workload_resources(namespace),
        "pvc_capacity": pvc_capacity(namespace),
        "redpanda_topics": redpanda_topics(redpanda_namespace),
        "minio_capacity": minio_capacity(minio_namespace),
    }


def render_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# Stream2Pretrain Capacity Report",
        "",
        f"Generated: `{report['generated_at']}`",
        f"Namespace: `{report['namespace']}`",
        "",
        "## Summary",
        "",
    ]
    nodes = report["node_capacity"]
    workloads = report["workload_resources"]
    pvcs = report["pvc_capacity"]
    topics = report["redpanda_topics"]
    minio = report["minio_capacity"]
    lines.extend(
        [
            f"- Node allocatable CPU/RAM: `{_summary(nodes, 'allocatable_cpu_m')}` / `{_summary(nodes, 'allocatable_memory_bytes')}`",
            f"- Stream2Pretrain pod requests CPU/RAM: `{_summary(workloads, 'request_cpu_m')}` / `{_summary(workloads, 'request_memory_bytes')}`",
            f"- Stream2Pretrain pod limits CPU/RAM: `{_summary(workloads, 'limit_cpu_m')}` / `{_summary(workloads, 'limit_memory_bytes')}`",
            f"- PVC count: `{len(pvcs.get('claims', [])) if pvcs.get('status') == 'measured' else 'needs-measurement'}`",
            f"- Redpanda topics: `{topics.get('status')}`",
            f"- MinIO throughput: `{minio.get('throughput', minio.get('status'))}`",
            "",
            "## Raw Evidence",
            "",
            "```json",
            json.dumps(report, indent=2, sort_keys=True),
            "```",
            "",
        ]
    )
    return "\n".join(lines)


def _summary(section: dict[str, Any], key: str) -> str:
    if section.get("status") != "measured":
        return "needs-measurement"
    value = section.get(key)
    return str(value) if value is not None else "needs-measurement"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Collect k3s capacity evidence for Stream2Pretrain.")
    parser.add_argument("--namespace", default="stream2pretrain")
    parser.add_argument("--redpanda-namespace", default="redpanda")
    parser.add_argument("--minio-namespace", default="minio")
    parser.add_argument("--out", default="docs/capacity-report.generated.md")
    parser.add_argument("--json-out", default="docs/capacity-report.generated.json")
    args = parser.parse_args(argv)

    report = collect(args.namespace, args.redpanda_namespace, args.minio_namespace)
    json_path = Path(args.json_out)
    md_path = Path(args.out)
    json_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    md_path.write_text(render_markdown(report), encoding="utf-8")
    print(f"wrote {json_path} and {md_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
