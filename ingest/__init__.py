"""Stream2Pretrain ingest layer.

Subpackages:

- ``common``: shared HTTP client, Kafka producer, MinIO writer, structlog,
  OTel tracer, content hashing, robots.txt cache, rate limiter, feed loader.
- ``rss_poller``: RSS / Atom CronJob poller.
- ``oaipmh_poller``: OAI-PMH 2.0 CronJob poller (arXiv default).
- ``sitemap_poller``: gzipped sitemap.xml CronJob poller with index expansion.
- ``github_events``: long-running GitHub Public Events poller.
- ``github_releases``: GitHub Releases Atom CronJob poller.
- ``hf_poller``: Hugging Face Hub REST CronJob poller.
- ``submit_api``: FastAPI ``POST /submit`` endpoint.
"""
