"""Stream2Pretrain ingest layer.

Subpackages:

- ``common``: shared HTTP client, Kafka producer, MinIO writer, structlog,
  OTel tracer, content hashing, robots.txt cache, rate limiter, feed loader.
- ``rss_poller``: RSS / Atom CronJob poller.
- ``oaipmh_poller``: OAI-PMH 2.0 CronJob poller (arXiv default).
- ``hf_poller``: Hugging Face Hub model-card and dataset-card poller.
- ``arxiv_html_fetcher``: native arXiv ``/html/<id>`` fetcher with
  ``ar5iv.labs.arxiv.org`` fallback (v0.2.0).
"""
