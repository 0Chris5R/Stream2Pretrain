"""Create or validate the one-time native-consumer to Bytewax cutover marker.

The marker lives on the same retained state volume as Bytewax recovery.  A
deployment retry can therefore distinguish a completed broker-offset handoff
from an unattempted migration without relying on the lifetime of a Pod or a
workflow run.
"""

from __future__ import annotations

import json
import os
import sys
from datetime import UTC, datetime
from pathlib import Path

CUTOVER_ID = "native-consumer-to-bytewax-v2"
SCHEMA_VERSION = 1


def required_env(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(f"{name} is required")
    return value


def expected_marker() -> dict[str, object]:
    starting_offset = required_env("S2P_KAFKA_START_OFFSET").lower()
    if starting_offset != "stored":
        raise RuntimeError("the Bytewax cutover requires S2P_KAFKA_START_OFFSET=stored")
    try:
        recovery_partitions = int(required_env("S2P_BYTEWAX_RECOVERY_PARTITIONS"))
    except ValueError as exc:
        raise RuntimeError("S2P_BYTEWAX_RECOVERY_PARTITIONS must be a positive integer") from exc
    if recovery_partitions < 1:
        raise RuntimeError("S2P_BYTEWAX_RECOVERY_PARTITIONS must be a positive integer")
    return {
        "schema_version": SCHEMA_VERSION,
        "cutover_id": CUTOVER_ID,
        "component": required_env("S2P_CUTOVER_COMPONENT"),
        "topic": required_env("S2P_CUTOVER_TOPIC"),
        "consumer_group": required_env("S2P_CONSUMER_GROUP"),
        "flow_name": required_env("S2P_BYTEWAX_FLOW_NAME"),
        "recovery_name": required_env("S2P_BYTEWAX_RECOVERY_NAME"),
        "recovery_partitions": recovery_partitions,
        "starting_offset": starting_offset,
    }


def marker_path(state_dir: Path, component: str) -> Path:
    return state_dir / "cutovers" / CUTOVER_ID / f"{component}.json"


def validate_marker(path: Path, expected: dict[str, object]) -> dict[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise LookupError(f"cutover marker is absent: {path}") from exc
    except (OSError, ValueError) as exc:
        raise RuntimeError(f"cutover marker is unreadable: {path}") from exc
    if not isinstance(value, dict):
        raise RuntimeError(f"cutover marker is not a JSON object: {path}")
    mismatches = {
        key: {"expected": expected_value, "stored": value.get(key)}
        for key, expected_value in expected.items()
        if value.get(key) != expected_value
    }
    if mismatches:
        detail = json.dumps(mismatches, sort_keys=True)
        raise RuntimeError(f"cutover marker does not match the deployed identity: {detail}")
    return {str(key): item for key, item in value.items()}


def ensure_marker(path: Path, expected: dict[str, object]) -> dict[str, object]:
    if path.exists():
        return validate_marker(path, expected)
    path.parent.mkdir(parents=True, exist_ok=True)
    marker = {
        **expected,
        "created_at": datetime.now(UTC).isoformat(),
    }
    try:
        # Exclusive creation makes this a true one-time marker. A concurrent
        # deployment may validate the winner, but it can never overwrite it.
        with path.open("x", encoding="utf-8") as marker_file:
            serialized = (
                json.dumps(marker, sort_keys=True, separators=(",", ":")) + "\n"
            )
            marker_file.write(serialized)
            marker_file.flush()
            os.fsync(marker_file.fileno())
    except FileExistsError:
        return validate_marker(path, expected)
    return validate_marker(path, expected)


def main() -> None:
    mode = os.environ.get("S2P_CUTOVER_MARKER_MODE", "check").strip().lower()
    expected = expected_marker()
    state_dir = Path(os.environ.get("S2P_STATE_DIR", "/var/lib/s2p"))
    path = marker_path(state_dir, str(expected["component"]))
    try:
        if mode == "check":
            value = validate_marker(path, expected)
        elif mode == "ensure":
            value = ensure_marker(path, expected)
        else:
            raise RuntimeError("S2P_CUTOVER_MARKER_MODE must be check or ensure")
    except LookupError as exc:
        print(str(exc), file=sys.stderr)
        raise SystemExit(3) from exc
    print(json.dumps({"path": str(path), "marker": value}, sort_keys=True))


if __name__ == "__main__":
    main()
