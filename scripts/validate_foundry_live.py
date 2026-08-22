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
TERMINAL_STATES = frozenset({"completed", "quota_exhausted", "failed"})


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


def validate_created_run(response: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    run = response.get("run")
    if response.get("created") is not True or not isinstance(run, dict):
        raise ValidationError("Refusing to validate through an existing manual run")
    if run.get("max_candidates") != 1:
        raise ValidationError("Refusing to validate an unbounded manual run")
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
    if run.get("state") != "completed" or run.get("processed_count") != 1:
        raise ValidationError("Bounded Foundry validation did not process exactly one candidate")


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
        response = _request_json(
            opener,
            base_url,
            "/api/foundry/runs/manual",
            token=token,
            payload={"max_candidates": 1},
        )
        run_id, created_run = validate_created_run(response)
        print(json.dumps({"created": True, "run": created_run}, sort_keys=True))

        deadline = time.monotonic() + 900
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
    except ValidationError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
