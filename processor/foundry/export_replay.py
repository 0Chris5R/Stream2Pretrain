"""Export one completed live job as a deterministic local replay fixture."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from processor.foundry.store import FoundryStore
from processor.foundry.util import canonical_json, sha256


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--job-id", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    state_dir = os.environ.get("S2P_FOUNDRY_STATE_DIR", "/var/lib/s2p/foundry")
    store = FoundryStore(str(Path(state_dir) / "control.sqlite3"))
    try:
        fixture = store.replay_fixture(job_id=args.job_id)
    finally:
        store.close()
    if not fixture:
        raise SystemExit(f"job {args.job_id!r} has no recorded provider results")
    target = Path(args.output)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(canonical_json(fixture) + b"\n")
    print(
        json.dumps(
            {
                "job_id": args.job_id,
                "output": str(target),
                "providers": sorted(fixture),
                "fixture_hash": sha256(fixture),
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()


__all__ = ["main"]
