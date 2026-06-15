"""Shared pytest fixtures for the Stream2Pretrain integration suite.

The integration tests in ``tests/integration`` need a running Redpanda
broker and a MinIO instance. Two strategies are supported:

1. ``S2P_USE_RUNNING_STACK=1``: assume ``docker-compose.dev.yml`` is already
   up and reachable on ``localhost:9092`` / ``localhost:9000``. This is the
   fastest path on a developer laptop.
2. Default: shell out to ``docker compose -f docker-compose.dev.yml up -d``
   on session start and tear it down on session end.

Tests skip gracefully when neither path is available (no docker daemon, no
network) so the suite stays runnable in CI sandboxes without root.
"""

from __future__ import annotations

import os
import shutil
import socket
import subprocess
import time
from collections.abc import Iterator
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
COMPOSE_FILE = REPO_ROOT / "docker-compose.dev.yml"

REDPANDA_HOST = "localhost"
REDPANDA_PORT = 9092
MINIO_HOST = "localhost"
MINIO_PORT = 9000
SUBMIT_API_HOST = os.environ.get("S2P_SUBMIT_API_HOST", "localhost")
SUBMIT_API_PORT = int(os.environ.get("S2P_SUBMIT_API_PORT", "8000"))


@dataclass(frozen=True, slots=True)
class StackEndpoints:
    """Connection coordinates for the dev stack."""

    redpanda_brokers: str
    minio_endpoint: str
    minio_access_key: str
    minio_secret_key: str
    submit_api_url: str


def _port_open(host: str, port: int, timeout: float = 1.0) -> bool:
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sock:
        sock.settimeout(timeout)
        try:
            sock.connect((host, port))
        except OSError:
            return False
        return True


def _wait_for(host: str, port: int, deadline_s: float) -> bool:
    """Poll a TCP port until it accepts connections or the deadline passes."""
    end = time.monotonic() + deadline_s
    while time.monotonic() < end:
        if _port_open(host, port):
            return True
        time.sleep(0.5)
    return False


def _docker_available() -> bool:
    return shutil.which("docker") is not None


def _compose_up() -> None:
    subprocess.run(
        ["docker", "compose", "-f", str(COMPOSE_FILE), "up", "-d"],
        cwd=str(REPO_ROOT),
        check=True,
    )


def _compose_down() -> None:
    subprocess.run(
        ["docker", "compose", "-f", str(COMPOSE_FILE), "down"],
        cwd=str(REPO_ROOT),
        check=False,
    )


@pytest.fixture(scope="session")
def dev_stack() -> Iterator[StackEndpoints]:
    """Yield endpoints for a running Redpanda + MinIO dev stack.

    Skips the test if neither a pre-running stack nor a usable docker daemon
    is available. Tearing the stack down is opt-in (``S2P_TEARDOWN_STACK=1``)
    so successive test runs do not pay the start-up cost on a laptop.
    """
    pre_running = _port_open(REDPANDA_HOST, REDPANDA_PORT) and _port_open(
        MINIO_HOST, MINIO_PORT
    )
    started_here = False
    if not pre_running:
        if os.environ.get("S2P_USE_RUNNING_STACK") == "1":
            pytest.skip("S2P_USE_RUNNING_STACK=1 set but stack not reachable")
        if not _docker_available():
            pytest.skip("docker not available and dev stack not pre-running")
        try:
            _compose_up()
        except subprocess.CalledProcessError as exc:
            pytest.skip(f"docker compose up failed: {exc}")
        started_here = True
        if not _wait_for(REDPANDA_HOST, REDPANDA_PORT, deadline_s=60.0):
            _compose_down()
            pytest.skip("Redpanda did not become ready within 60s")
        if not _wait_for(MINIO_HOST, MINIO_PORT, deadline_s=30.0):
            _compose_down()
            pytest.skip("MinIO did not become ready within 30s")

    endpoints = StackEndpoints(
        redpanda_brokers=f"{REDPANDA_HOST}:{REDPANDA_PORT}",
        minio_endpoint=f"http://{MINIO_HOST}:{MINIO_PORT}",
        minio_access_key="minioadmin",
        minio_secret_key="minioadmin",
        submit_api_url=f"http://{SUBMIT_API_HOST}:{SUBMIT_API_PORT}",
    )
    try:
        yield endpoints
    finally:
        if started_here and os.environ.get("S2P_TEARDOWN_STACK") == "1":
            _compose_down()


@pytest.fixture(scope="session")
def submit_api_reachable(dev_stack: StackEndpoints) -> bool:
    """True iff the submit API is currently listening. Tests skip otherwise."""
    return _port_open(SUBMIT_API_HOST, SUBMIT_API_PORT)


@pytest.fixture
def trace_id() -> str:
    """Fresh W3C-style 16-byte trace id for tests that need one."""
    import secrets

    return secrets.token_hex(16)
