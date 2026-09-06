"""arXiv native-HTML fetcher for Stream2Pretrain.

Converts arXiv ids sourced from the live Redpanda discovery stream into full-paper-body Bronze
records by fetching ``https://arxiv.org/html/<id>`` and falling back to
``https://ar5iv.labs.arxiv.org/html/<id>`` for older papers without native
HTML.

Public entrypoint: :func:`ingest.arxiv_html_fetcher.fetcher.main`.
"""

from __future__ import annotations

__all__ = ["extractor", "fetcher"]
