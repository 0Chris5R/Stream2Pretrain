"""Stream2Pretrain test suite.

Contains:
- ``integration/``: end-to-end black-box tests booted against the local dev
  stack defined in ``docker-compose.dev.yml``.
- ``load/``: k6 scripts run separately from pytest.

Unit tests live alongside their components (``ingest/<x>/tests``,
``processor/tests``, etc.). This top-level package only carries the
cross-component integration coverage.
"""
