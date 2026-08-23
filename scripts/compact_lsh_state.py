"""Compact the curator's LevelDB state without changing its live records."""

from __future__ import annotations

import argparse
import os
from collections.abc import Callable
from pathlib import Path
from typing import Protocol


class LevelDB(Protocol):
    """Operations needed from a LevelDB handle."""

    def compact_range(self) -> None: ...

    def close(self) -> None: ...


DatabaseFactory = Callable[[str], LevelDB]


def bytes_on_disk(path: Path) -> int:
    """Return the physical file bytes below *path*."""
    return sum(
        entry.stat().st_size
        for root, _dirs, files in os.walk(path)
        for name in files
        if (entry := Path(root, name)).is_file()
    )


def compact_state(state: Path, database_factory: DatabaseFactory | None = None) -> tuple[int, int]:
    """Compact one closed LevelDB and return its before/after sizes."""
    if not state.is_dir():
        raise FileNotFoundError(f"LevelDB state is missing: {state}")
    if database_factory is None:
        import plyvel  # type: ignore[import-untyped]

        def open_database(path: str) -> LevelDB:
            return plyvel.DB(path, create_if_missing=False)  # type: ignore[no-any-return]

        database_factory = open_database

    before = bytes_on_disk(state)
    database = database_factory(str(state))
    try:
        database.compact_range()
    finally:
        database.close()
    return before, bytes_on_disk(state)


def main() -> None:
    """Run lossless compaction against a mounted curator checkpoint."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "state",
        nargs="?",
        type=Path,
        default=Path("/var/lib/s2p/checkpoint/lshbloom/leveldb"),
    )
    args = parser.parse_args()
    before, after = compact_state(args.state)
    print(f"leveldb_size_before_bytes={before}", flush=True)
    print(f"leveldb_size_after_bytes={after}", flush=True)
    print(f"leveldb_reclaimed_bytes={max(0, before - after)}", flush=True)


if __name__ == "__main__":
    main()
