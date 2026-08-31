from __future__ import annotations

from typing import Any

from botocore.exceptions import ClientError

from processor.object_lifecycle import RULE_ID, configure_bucket, desired_rules, lifecycle_rule


class FakeS3:
    def __init__(self, rules: list[dict[str, Any]] | None = None) -> None:
        self.rules = rules
        self.puts: list[tuple[str, list[dict[str, Any]]]] = []

    def get_bucket_lifecycle_configuration(self, **kwargs: Any) -> dict[str, Any]:
        assert kwargs["Bucket"]
        if self.rules is None:
            raise ClientError(
                {"Error": {"Code": "NoSuchLifecycleConfiguration", "Message": "missing"}},
                "GetBucketLifecycleConfiguration",
            )
        return {"Rules": self.rules}

    def put_bucket_lifecycle_configuration(self, **kwargs: Any) -> None:
        self.rules = kwargs["LifecycleConfiguration"]["Rules"]
        self.puts.append((kwargs["Bucket"], self.rules))


def test_rule_expires_transient_objects_and_incomplete_uploads() -> None:
    assert lifecycle_rule(1) == {
        "ID": RULE_ID,
        "Status": "Enabled",
        "Filter": {"Prefix": ""},
        "Expiration": {"Days": 1},
        "AbortIncompleteMultipartUpload": {"DaysAfterInitiation": 1},
    }


def test_desired_rules_preserve_unrelated_bucket_policy() -> None:
    existing = [
        {"ID": "operator-policy", "Status": "Enabled", "Filter": {"Prefix": "keep/"}},
        {"ID": RULE_ID, "Status": "Disabled", "Filter": {"Prefix": "old/"}},
    ]

    assert desired_rules(existing, days=1) == [existing[0], lifecycle_rule(1)]


def test_configure_bucket_installs_and_verifies_missing_policy() -> None:
    client = FakeS3()

    report = configure_bucket(client, "s2p-bronze", days=1, apply=True)

    assert report == {"bucket": "s2p-bronze", "changed": True, "retention_days": 1}
    assert client.puts == [("s2p-bronze", [lifecycle_rule(1)])]


def test_configure_bucket_is_idempotent() -> None:
    client = FakeS3([lifecycle_rule(1)])

    report = configure_bucket(client, "s2p-silver", days=1, apply=True)

    assert report["changed"] is False
    assert client.puts == []
