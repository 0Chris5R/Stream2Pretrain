# Stream2Pretrain MinIO chart

This chart makes the object-store tier part of the repository's declarative
deployment. It creates a distributed MinIO StatefulSet with one persistent
volume per member, a client Service, a headless peer Service, health probes,
a ServiceMonitor and an idempotent bucket bootstrap Job.

The chart expects Secret `minio-root` in namespace `minio` with keys
`accessKey` and `secretKey`. Credential values remain operator supplied and are
never stored in Git.

The chart rejects fewer than four members because that would fall back to a
standalone topology. Pods prefer different Kubernetes nodes. The course
cluster has three nodes, so one node can host more than one member. Failure
domain placement, aggregate disk capacity and restore time must be measured
before deploying this change to the retained installation.

For a fresh cluster, deploy it through the storage tier:

```bash
./scripts/setup_dhbw_demo.sh storage
```

Each StatefulSet member owns a stable `data-<pod-name>` claim. The DHBW values
retain the measured 10 GiB request per claim. The old standalone `minio-data`
claim is not mounted by the distributed topology. Its objects require a
verified copy and checksum procedure before an existing installation switches
the client Service.
