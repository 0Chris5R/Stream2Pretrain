from __future__ import annotations

import pytest

from processor.common import ProcessorConfig
from processor.iceberg_catalog import _polaris_properties, iceberg_maintenance_properties


def test_polaris_access_delegation_can_be_disabled(
    cfg: ProcessorConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("S2P_ICEBERG_ACCESS_DELEGATION", "none")

    properties = _polaris_properties(cfg)

    assert properties["header.X-Iceberg-Access-Delegation"] == "none"
    assert properties["py-io-impl"] == "pyiceberg.io.pyarrow.PyArrowFileIO"
    assert properties["s3.connect-timeout"] == "10"
    assert properties["s3.request-timeout"] == "60"


def test_scheduled_maintenance_is_the_only_metadata_delete_owner() -> None:
    properties = iceberg_maintenance_properties()

    assert properties["write.metadata.delete-after-commit.enabled"] == "false"
