"""OpenReview ingest module (Stream2Pretrain v0.2.0).

Two entrypoints:

- ``live``: poll ``api2.openreview.net`` for fresh ICLR / NeurIPS / ICML / COLM
  submissions; stash the binary PDF in MinIO bronze and emit one
  ``BronzeRecord`` per submission (``source_format="pdf"``,
  ``extraction_pipeline="openreview-pdf-pending-marker"``). Each review,
  decision, and rebuttal note attached to the same forum is also persisted as
  ``source_format="review"`` so the prose flows through Silver and Gold without
  a PDF parser.
- ``backfill``: stream the REVIEWARENA HuggingFace dataset (PDFs + reviews +
  decisions for ICLR 2020-2026 / NeurIPS 2021-2025 / ICML 2025 / COLM
  2024-2025) and emit BronzeRecords with the same shape as live mode.

The PDF parser sidecar (marker) is deferred to a Phase-2 release; for v0.2.0
the PDF body lives in MinIO and the BronzeRecord is the durable pointer.
"""

from __future__ import annotations

__all__ = ["live", "backfill"]
