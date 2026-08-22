from __future__ import annotations

from pathlib import Path


def test_processor_entrypoint_does_not_inject_command_name_into_argparse() -> None:
    root = Path(__file__).resolve().parents[1]
    script = (root / "processor" / "container_entrypoint.sh").read_text(encoding="utf-8")

    assert 'exec python -c "from ${module} import main; main()" "$@"' in script
    assert '"$command_name" "$@"' not in script
