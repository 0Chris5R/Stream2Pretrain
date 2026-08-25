from __future__ import annotations

from scripts.remove_github_source_objects import _delete_matching, _is_github_key


class _Paginator:
    def paginate(self, *, Bucket: str):
        assert Bucket == "bronze"
        return [
            {
                "Contents": [
                    {"Key": "code/owner/repo/ref/file.py", "Size": 12},
                    {
                        "Key": "year=2026/source=github-releases/doc.html.gz",
                        "Size": 18,
                    },
                    {"Key": "source=github-release-tarballs/doc.json", "Size": 20},
                    {"Key": "ingest-cursors/github-releases.json", "Size": 4},
                    {"Key": "year=2026/source=arxiv-html/doc.html.gz", "Size": 99},
                ]
            }
        ]


class _Client:
    def __init__(self) -> None:
        self.deleted: list[str] = []

    def get_paginator(self, name: str) -> _Paginator:
        assert name == "list_objects_v2"
        return _Paginator()

    def delete_objects(self, *, Bucket: str, Delete: dict):
        assert Bucket == "bronze"
        self.deleted.extend(item["Key"] for item in Delete["Objects"])


def test_github_key_predicate_is_source_bounded() -> None:
    assert _is_github_key("code/owner/repo/ref/file.py")
    assert _is_github_key("source=github-releases/doc.json")
    assert _is_github_key("year=2026/source=github-release-tarballs/doc.json")
    assert _is_github_key("ingest-cursors/github-releases.json")
    assert not _is_github_key("year=2026/source=arxiv-html/doc.html.gz")
    assert not _is_github_key("cards/github-model-readme.md")


def test_delete_matching_reports_exact_scope() -> None:
    client = _Client()

    result = _delete_matching(client, "bronze")

    assert result == {"bucket": "bronze", "objects_deleted": 4, "bytes_deleted": 54}
    assert client.deleted == [
        "code/owner/repo/ref/file.py",
        "year=2026/source=github-releases/doc.html.gz",
        "source=github-release-tarballs/doc.json",
        "ingest-cursors/github-releases.json",
    ]
