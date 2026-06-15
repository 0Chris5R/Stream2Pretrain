"""arXiv native-HTML fetcher for Stream2Pretrain.

Converts arXiv ids (sourced from a Redpanda subscription on
``docs.normalized`` or a static backfill list) into full-paper-body Bronze
records by fetching ``https://arxiv.org/html/<id>`` and falling back to
``https://ar5iv.labs.arxiv.org/html/<id>`` for older papers without native
HTML.

Public entrypoint: :func:`ingest.arxiv_html_fetcher.fetcher.main` (CronJob /
Deployment ENTRYPOINT).
"""

from __future__ import annotations

__all__ = ["fetcher", "extractor"]
