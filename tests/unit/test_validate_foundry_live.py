from __future__ import annotations

import pytest

from scripts.validate_foundry_live import (
    ValidationError,
    artifact_counts,
    find_run,
    validate_artifact_increases,
    validate_created_run,
    validate_terminal_run,
)


def test_validate_created_run_requires_a_compatible_bounded_run() -> None:
    run_id, run = validate_created_run(
        {
            "created": True,
            "run": {
                "run_id": "manual-1",
                "max_candidates": 3,
                "candidate_count": 3,
                "processed_count": 0,
            },
        }
    )

    assert run_id == "manual-1"
    assert run["max_candidates"] == 3

    resumed_id, resumed = validate_created_run(
        {
            "created": False,
            "run": {
                "run_id": "manual-1",
                "max_candidates": 3,
                "candidate_count": 3,
                "processed_count": 1,
                "state": "pending",
            },
        }
    )
    assert resumed_id == "manual-1"
    assert resumed["state"] == "pending"

    with pytest.raises(ValidationError, match="eligible candidates"):
        validate_created_run(
            {
                "created": False,
                "run": {
                    "run_id": "manual-1",
                    "max_candidates": 3,
                    "candidate_count": 4,
                    "processed_count": 0,
                    "state": "pending",
                },
            }
        )
    with pytest.raises(ValidationError, match="unbounded"):
        validate_created_run(
            {
                "created": True,
                "run": {
                    "run_id": "manual-2",
                    "max_candidates": 2,
                    "candidate_count": 2,
                    "processed_count": 0,
                },
            }
        )


def test_find_and_validate_terminal_run() -> None:
    run = {
        "run_id": "manual-1",
        "state": "completed",
        "candidate_count": 3,
        "processed_count": 3,
    }

    assert find_run({"manual_runs": [run]}, "manual-1") == run
    validate_terminal_run(run)

    with pytest.raises(ValidationError, match="every candidate"):
        validate_terminal_run({**run, "processed_count": 2})


def test_artifact_validation_requires_new_sft_and_rl_outputs() -> None:
    before = {"sft_trajectory:accepted": 4, "rl_environment:accepted": 2}
    after = {"sft_trajectory:accepted": 5, "rl_environment:accepted": 3}

    assert artifact_counts({"artifacts": after}) == after
    validate_artifact_increases(before, after)

    with pytest.raises(ValidationError, match="rl_environment:accepted"):
        validate_artifact_increases(before, {**after, "rl_environment:accepted": 2})
