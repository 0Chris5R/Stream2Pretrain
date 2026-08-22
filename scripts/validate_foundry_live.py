"""Run one fail-closed live Foundry candidate from inside the API container."""

from __future__ import annotations

import json
import os
import sys
import time
import urllib.error
import urllib.request
from typing import Any

DEFAULT_BASE_URL = "http://[::1]:8092"
DEFAULT_MAX_CANDIDATES = 3
TERMINAL_STATES = frozenset({"completed", "quota_exhausted", "failed"})
REQUIRED_ACCEPTED_ARTIFACTS = frozenset({"sft_trajectory:accepted", "rl_environment:accepted"})


class ValidationError(RuntimeError):
    """The bounded validation contract was not satisfied."""


def _request_json(
    opener: urllib.request.OpenerDirector,
    base_url: str,
    path: str,
    *,
    token: str | None = None,
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    body = None if payload is None else json.dumps(payload).encode("utf-8")
    headers = {"Accept": "application/json"}
    if body is not None:
        headers["Content-Type"] = "application/json"
    if token is not None:
        headers["Authorization"] = f"Bearer {token}"
    request = urllib.request.Request(
        f"{base_url.rstrip('/')}/{path.lstrip('/')}",
        data=body,
        headers=headers,
        method="POST" if body is not None else "GET",
    )
    try:
        with opener.open(request, timeout=20) as response:
            decoded = json.load(response)
    except urllib.error.HTTPError as exc:
        raise ValidationError(f"Foundry API returned HTTP {exc.code} for {path}") from exc
    except (OSError, ValueError) as exc:
        raise ValidationError(f"Foundry API request failed for {path}: {exc}") from exc
    if not isinstance(decoded, dict):
        raise ValidationError(f"Foundry API returned a non-object response for {path}")
    return decoded


def validate_created_run(
    response: dict[str, Any], *, max_candidates: int = DEFAULT_MAX_CANDIDATES
) -> tuple[str, dict[str, Any]]:
    run = response.get("run")
    created = response.get("created")
    if created not in {True, False} or not isinstance(run, dict):
        raise ValidationError("Foundry did not return a manual run")
    if run.get("max_candidates") != max_candidates:
        raise ValidationError("Refusing to validate an unbounded manual run")
    candidate_count = run.get("candidate_count")
    processed_count = run.get("processed_count")
    if (
        isinstance(candidate_count, bool)
        or not isinstance(candidate_count, int)
        or not 1 <= candidate_count <= max_candidates
    ):
        raise ValidationError("Bounded Foundry validation has no eligible candidates")
    if (
        isinstance(processed_count, bool)
        or not isinstance(processed_count, int)
        or not 0 <= processed_count <= candidate_count
    ):
        raise ValidationError("Bounded Foundry validation has invalid progress")
    if created is False and (run.get("state") not in {"pending", "running"}):
        raise ValidationError("Refusing to resume an incompatible manual run")
    run_id = run.get("run_id")
    if not isinstance(run_id, str) or not run_id:
        raise ValidationError("The bounded manual run has no run_id")
    return run_id, run


def find_run(dashboard: dict[str, Any], run_id: str) -> dict[str, Any] | None:
    runs = dashboard.get("manual_runs")
    if not isinstance(runs, list):
        raise ValidationError("Foundry dashboard has no manual_runs list")
    for run in runs:
        if isinstance(run, dict) and run.get("run_id") == run_id:
            return run
    return None


def validate_terminal_run(run: dict[str, Any]) -> None:
    if run.get("state") != "completed" or run.get("processed_count") != run.get("candidate_count"):
        raise ValidationError("Bounded Foundry validation did not process every candidate")


def artifact_counts(dashboard: dict[str, Any]) -> dict[str, int]:
    raw_counts = dashboard.get("artifacts")
    if not isinstance(raw_counts, dict):
        raise ValidationError("Foundry dashboard has no artifact counts")
    counts: dict[str, int] = {}
    for key, value in raw_counts.items():
        if isinstance(key, str) and isinstance(value, int) and not isinstance(value, bool):
            counts[key] = value
    return counts


def validate_artifact_increases(before: dict[str, int], after: dict[str, int]) -> None:
    missing = sorted(
        key for key in REQUIRED_ACCEPTED_ARTIFACTS if after.get(key, 0) <= before.get(key, 0)
    )
    if missing:
        raise ValidationError(
            "Bounded Foundry validation produced no new accepted artifacts for: "
            + ", ".join(missing)
        )


def main() -> int:
    token = os.environ.get("S2P_FOUNDRY_CONTROL_TOKEN", "")
    if not token:
        print("S2P_FOUNDRY_CONTROL_TOKEN is not configured", file=sys.stderr)
        return 1
    base_url = os.environ.get("S2P_FOUNDRY_LOCAL_URL", DEFAULT_BASE_URL)
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    try:
        health = _request_json(opener, base_url, "/healthz")
        if health.get("status") != "ok":
            raise ValidationError("Foundry API is not healthy")
        initial_dashboard = _request_json(opener, base_url, "/api/foundry/dashboard")
        initial_artifacts = artifact_counts(initial_dashboard)
        try:
            max_candidates = int(
                os.environ.get("S2P_FOUNDRY_VALIDATION_MAX_CANDIDATES", DEFAULT_MAX_CANDIDATES)
            )
        except (TypeError, ValueError) as exc:
            raise ValidationError("Foundry validation candidate limit must be an integer") from exc
        if not 1 <= max_candidates <= 10:
            raise ValidationError("Foundry validation candidate limit must be between 1 and 10")
        response = _request_json(
            opener,
            base_url,
            "/api/foundry/runs/manual",
            token=token,
            payload={"max_candidates": max_candidates},
        )
        run_id, created_run = validate_created_run(response, max_candidates=max_candidates)
        print(
            json.dumps(
                {"created": response["created"], "run": created_run},
                sort_keys=True,
            )
        )

        timeout_seconds = int(os.environ.get("S2P_FOUNDRY_VALIDATION_TIMEOUT_SECONDS", "2700"))
        deadline = time.monotonic() + timeout_seconds
        terminal_run: dict[str, Any] | None = None
        while time.monotonic() < deadline:
            dashboard = _request_json(opener, base_url, "/api/foundry/dashboard")
            run = find_run(dashboard, run_id)
            if run is not None and run.get("state") in TERMINAL_STATES:
                terminal_run = run
                break
            time.sleep(5)
        if terminal_run is None:
            raise ValidationError("Bounded Foundry validation did not reach a terminal state")
        print(json.dumps(terminal_run, sort_keys=True))
        validate_terminal_run(terminal_run)
        final_dashboard = _request_json(opener, base_url, "/api/foundry/dashboard")
        final_artifacts = artifact_counts(final_dashboard)
        validate_artifact_increases(initial_artifacts, final_artifacts)
        print(
            json.dumps(
                {
                    "accepted_artifact_increases": {
                        key: final_artifacts.get(key, 0) - initial_artifacts.get(key, 0)
                        for key in sorted(REQUIRED_ACCEPTED_ARTIFACTS)
                    }
                },
                sort_keys=True,
            )
        )
    except ValidationError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
