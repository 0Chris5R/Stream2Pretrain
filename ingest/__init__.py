"""Stream2Pretrain ingest layer.

Subpackages:

- ``common``: shared HTTP client, Kafka producer, MinIO writer, structlog,
  OTel tracer, content hashing, robots.txt cache, rate limiter, feed loader.
- ``rss_poller``: RSS / Atom CronJob poller.
- ``oaipmh_poller``: OAI-PMH 2.0 CronJob poller (arXiv default).
- ``sitemap_poller``: optional sitemap adapter, absent from the active catalogue.
- ``github_releases``: GitHub Releases Atom CronJob poller.
- ``github_release_tarball_fetcher``: per-release source tarball fetcher
  emitting one ``CodeFileRecord`` per allow-listed file (v0.2.0).
- ``hf_poller``: Hugging Face Hub REST CronJob poller.
- ``arxiv_html_fetcher``: native arXiv ``/html/<id>`` fetcher with
  ``ar5iv.labs.arxiv.org`` fallback (v0.2.0).
"""
