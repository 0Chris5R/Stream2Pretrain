"""GitHub release tarball fetcher (v0.2.0).

Consumes the existing ``github_releases`` poller's emissions on the
``raw.fetched`` topic (filter ``source_feed=github-releases``) and, per
release, fetches the source tarball from
``https://api.github.com/repos/{owner}/{repo}/tarball/{tag}`` into MinIO.

The fetcher then stream-extracts the ``tar.gz`` (no full archive in memory),
emits one :class:`schemas.bronze.BronzeRecord` with ``source_format="code"``
per allow-listed source file, and writes the file bytes to MinIO under
``s3://bronze/code/repo=<owner>__<repo>/ref=<tag>/<path>``.

Budget: stays under the GitHub REST 5000 req/h ceiling because exactly one
``/tarball`` request is issued per release. The shared
``ingest/common/rate_limit.py`` token bucket provides per-pod politeness.
"""

from __future__ import annotations

__all__: list[str] = []
