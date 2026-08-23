from pathlib import Path

from scripts.compact_lsh_state import compact_state


class FakeLevelDB:
    def __init__(self, state: Path) -> None:
        self.state = state
        self.compacted = False
        self.closed = False

    def compact_range(self) -> None:
        self.compacted = True
        (self.state / "obsolete.sst").unlink()

    def close(self) -> None:
        self.closed = True


def test_compact_state_reclaims_obsolete_files_and_closes_database(tmp_path: Path) -> None:
    live = tmp_path / "live.sst"
    obsolete = tmp_path / "obsolete.sst"
    live.write_bytes(b"live")
    obsolete.write_bytes(b"obsolete")
    opened: list[FakeLevelDB] = []

    def factory(path: str) -> FakeLevelDB:
        database = FakeLevelDB(Path(path))
        opened.append(database)
        return database

    before, after = compact_state(tmp_path, factory)

    assert before == 12
    assert after == 4
    assert opened[0].compacted is True
    assert opened[0].closed is True


def test_compact_state_rejects_a_missing_database(tmp_path: Path) -> None:
    missing = tmp_path / "missing"

    try:
        compact_state(missing)
    except FileNotFoundError as exc:
        assert str(missing) in str(exc)
    else:
        raise AssertionError("missing LevelDB should not be created during recovery")
