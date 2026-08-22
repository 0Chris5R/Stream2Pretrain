from __future__ import annotations

import pytest

from scripts.validate_foundry_live import (
    ValidationError,
    find_run,
    validate_created_run,
    validate_terminal_run,
)


def test_validate_created_run_requires_a_compatible_single_candidate_run() -> None:
    run_id, run = validate_created_run(
        {"created": True, "run": {"run_id": "manual-1", "max_candidates": 1}}
    )

    assert run_id == "manual-1"
    assert run["max_candidates"] == 1

    resumed_id, resumed = validate_created_run(
        {
            "created": False,
            "run": {
                "run_id": "manual-1",
                "max_candidates": 1,
                "candidate_count": 1,
                "processed_count": 0,
                "state": "pending",
            },
        }
    )
    assert resumed_id == "manual-1"
    assert resumed["state"] == "pending"

    with pytest.raises(ValidationError, match="incompatible"):
        validate_created_run(
            {
                "created": False,
                "run": {
                    "run_id": "manual-1",
                    "max_candidates": 1,
                    "candidate_count": 2,
                    "processed_count": 0,
                    "state": "pending",
                },
            }
        )
    with pytest.raises(ValidationError, match="unbounded"):
        validate_created_run({"created": True, "run": {"run_id": "manual-2", "max_candidates": 2}})


def test_find_and_validate_terminal_run() -> None:
    run = {"run_id": "manual-1", "state": "completed", "processed_count": 1}

    assert find_run({"manual_runs": [run]}, "manual-1") == run
    validate_terminal_run(run)

    with pytest.raises(ValidationError, match="exactly one"):
        validate_terminal_run({**run, "processed_count": 0})
