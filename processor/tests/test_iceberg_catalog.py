from __future__ import annotations

import pytest

from processor.common import ProcessorConfig
from processor.iceberg_catalog import _polaris_properties


def test_polaris_access_delegation_can_be_disabled(
    cfg: ProcessorConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("S2P_ICEBERG_ACCESS_DELEGATION", "none")

    properties = _polaris_properties(cfg)

    assert properties["header.X-Iceberg-Access-Delegation"] == "none"
    assert properties["py-io-impl"] == "pyiceberg.io.pyarrow.PyArrowFileIO"
