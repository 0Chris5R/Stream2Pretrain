# Stream2Pretrain MinIO chart

This chart makes the object-store tier part of the repository's declarative
deployment. It creates a single-node MinIO StatefulSet, ClusterIP Service,
persistent volume, health probes, ServiceMonitor and an idempotent bucket
bootstrap Job.

The chart expects Secret `minio-root` in namespace `minio` with keys
`accessKey` and `secretKey`. Credential values remain operator supplied and are
never stored in Git.

The course profile is intentionally a single-instance stateful deployment. It
is sufficient for the demonstrated prototype but is not a claim of MinIO high
availability. A larger deployment must use a distributed object store and
measured failure-domain, capacity and restore settings.

For a fresh cluster, deploy it through the storage tier:

```bash
./scripts/setup_dhbw_demo.sh storage
```

The chart owns the stable `minio-data` claim name. The DHBW values keep its
measured 10 GiB request because the cluster's `local-path` provisioner cannot
expand the retained volume online. Other environments can set a larger request
before first installation.
