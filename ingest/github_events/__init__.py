"""GitHub Events long-running poller.

Unlike the other ingest pollers, this is a Deployment (not a CronJob): GitHub's
``X-Poll-Interval`` floor of 60 s makes a per-pass CronJob wasteful. The pod
runs forever, sleeps for ``X-Poll-Interval`` between calls, and honours the
authenticated 5000 req/h budget.
"""
