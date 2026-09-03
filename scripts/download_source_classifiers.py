"""Fetch checksum-pinned release weights at image build time, not on Pod startup."""

from __future__ import annotations

import hashlib
import json
import sys
import tarfile
import tempfile
import urllib.request
from pathlib import Path


def main() -> None:
    manifest_path, destination = map(Path, sys.argv[1:])
    manifest = json.loads(manifest_path.read_text())
    for task, artifact in manifest["models"].items():
        print(f"Fetching pinned {task}", flush=True)
        with tempfile.TemporaryFile() as archive:
            digest = hashlib.sha256()
            with urllib.request.urlopen(artifact["url"], timeout=180) as response:
                while chunk := response.read(1024 * 1024):
                    digest.update(chunk)
                    archive.write(chunk)
            if digest.hexdigest() != artifact["archive_sha256"]:
                raise RuntimeError(f"Archive checksum mismatch: {task}")
            archive.seek(0)
            with tarfile.open(fileobj=archive) as bundle:
                for member in bundle.getmembers():
                    if (
                        not member.isfile()
                        or Path(member.name).parts[:1] != (task,)
                        or ".." in Path(member.name).parts
                    ):
                        raise RuntimeError(f"Unexpected model archive entry: {member.name}")
                bundle.extractall(destination, filter="data")
        with (destination / task / "model.safetensors").open("rb") as weights:
            if hashlib.file_digest(weights, "sha256").hexdigest() != artifact["weights_sha256"]:
                raise RuntimeError(f"Weights checksum mismatch: {task}")


if __name__ == "__main__":
    main()
