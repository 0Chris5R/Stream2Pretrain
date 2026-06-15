"""FastAPI submit endpoint for Stream2Pretrain.

Lets a human (or an automation) push a single URL into the curation pipeline.
The endpoint validates the request against ``SourceFeedSpec`` semantics, fetches
the URL, lands the bytes in MinIO bronze, and emits a ``BronzeRecord`` on the
``raw.fetched`` topic.
"""
