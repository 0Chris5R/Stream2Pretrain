#!/bin/sh
set -eu

command_name="$(basename "$0")"
case "$command_name" in
  s2p-fetcher) module="processor.fetcher" ;;
  s2p-curate) module="processor.curate" ;;
  s2p-curator-model-service) module="processor.model_service" ;;
  s2p-iceberg-writer) module="processor.iceberg_writer" ;;
  s2p-iceberg-maintenance) module="processor.iceberg_maintenance" ;;
  s2p-decon-api) module="processor.decon_api" ;;
  s2p-duckdb-api) module="processor.duckdb_api" ;;
  s2p-local-sources-api) module="processor.local_sources_api" ;;
  s2p-mixture-controller) module="processor.mixture_controller.controller" ;;
  s2p-seed-loader) module="processor.seed_loader" ;;
  s2p-foundry) module="processor.foundry.worker" ;;
  s2p-foundry-api) module="processor.foundry.api" ;;
  s2p-foundry-export-replay) module="processor.foundry.export_replay" ;;
  s2p-foundry-build-oracle) module="processor.foundry.oracle_build" ;;
  *)
    echo "Unknown Stream2Pretrain entrypoint: $command_name" >&2
    exit 64
    ;;
esac

# ``python -c`` already supplies ``-c`` as sys.argv[0].  Pass only the
# container arguments so argparse-based commands see exactly what Kubernetes
# put in ``args`` (for example, ``--apply`` for Iceberg maintenance).
exec python -c "from ${module} import main; main()" "$@"
