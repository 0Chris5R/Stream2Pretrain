from __future__ import annotations

import json
import shutil
import subprocess
import sys
import tomllib
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
INGEST_PACKAGES = sorted(
    path.parent
    for path in (ROOT / "ingest").glob("*/pyproject.toml")
    if path.parent.name != "common"
)
LOCAL_PACKAGE_PATHS = {
    "stream2pretrain": ".",
    "stream2pretrain-ingest-common": "./ingest/common",
}


def test_processor_ci_image_uses_an_immutable_dependency_base() -> None:
    full = (ROOT / "processor" / "Dockerfile").read_text(encoding="utf-8")
    app = (ROOT / "processor" / "Dockerfile.app").read_text(encoding="utf-8")
    model_app = (ROOT / "processor" / "Dockerfile.model-service.app").read_text(encoding="utf-8")
    entrypoint = (ROOT / "processor" / "container_entrypoint.sh").read_text(encoding="utf-8")
    workflow = (ROOT / ".github" / "workflows" / "deploy-main.yml").read_text(encoding="utf-8")

    assert "AS runtime-base" in full
    assert "FROM runtime-base AS runtime" in full
    assert "ARG PROCESSOR_BASE_IMAGE=" in app
    assert "FROM ${PROCESSOR_BASE_IMAGE} AS runtime" in app
    assert "uv sync" not in app
    assert "apt-get" not in app
    assert 'ENTRYPOINT ["python", "-m", "processor.model_service"]' in model_app
    assert "COPY processor                  /app/processor" not in model_app
    assert "processor/model_service.py" in model_app
    assert "processor/operators/quality.py" in model_app
    assert "processor/Dockerfile.model-service.app" in workflow
    assert "S2P_INSTALL_EXTRA=${{ matrix.extra }}" in workflow
    assert "target: runtime-base" in workflow
    assert "dockerfile: processor/Dockerfile.app" in workflow
    assert "type=gha,scope=${{ matrix.image }}" in workflow
    assert "Build and publish thin processor image" in workflow
    thin_processor_step = workflow.split("Build and publish thin processor image", maxsplit=1)[1]
    thin_processor_step = thin_processor_step.split(
        "Smoke-test Python runtime imports", maxsplit=1
    )[0]
    assert "cache-from:" not in thin_processor_step
    assert "cache-to:" not in thin_processor_step
    assert 's2p-curator-model-service) module="processor.model_service"' in entrypoint
    assert 'command_name="$1"' in entrypoint
    assert "RUN python -c" not in app
    assert "RUN case" not in model_app


def test_processor_model_images_are_component_specific_and_immutable() -> None:
    dockerfile = (ROOT / "processor" / "Dockerfile").read_text(encoding="utf-8")
    workflow = (ROOT / ".github" / "workflows" / "deploy-main.yml").read_text(encoding="utf-8")
    values = (ROOT / "charts" / "stream2pretrain" / "values.yaml").read_text(encoding="utf-8")
    curate_template = (
        ROOT / "charts" / "stream2pretrain" / "templates" / "processor-curate.yaml"
    ).read_text(encoding="utf-8")
    model_service_template = (
        ROOT / "charts" / "stream2pretrain" / "templates" / "processor-model-service.yaml"
    ).read_text(encoding="utf-8")
    network_policy_template = (
        ROOT / "charts" / "stream2pretrain" / "templates" / "networkpolicies.yaml"
    ).read_text(encoding="utf-8")
    fetcher_template = (
        ROOT / "charts" / "stream2pretrain" / "templates" / "processor-fetcher.yaml"
    ).read_text(encoding="utf-8")

    assert "ARG S2P_MODEL_PROFILE=none" in dockerfile
    assert 'case "${S2P_MODEL_PROFILE}"' in dockerfile
    assert "processor-base-quality" in workflow
    assert "processor-base-kenlm" in workflow
    assert "processor-base-fetcher" in workflow
    assert "extra: fetcher-service" in workflow
    assert "processor-quality-model" in workflow
    assert "processor-kenlm-model" in workflow
    assert "processor-fetcher-model" in workflow
    assert "COPY processor/pdf_worker.py" in (
        ROOT / "processor" / "Dockerfile.fetcher.app"
    ).read_text(encoding="utf-8")
    assert "processor/pdf_worker.py" in workflow
    assert "image: stream2pretrain/processor-quality-model" in values
    assert "image: stream2pretrain/processor-kenlm-model" in values
    assert "image: stream2pretrain/processor-fetcher-model" in values
    assert "bootstrap: false" in values
    assert 'ternary "/models" "/opt/models" $externalModels' in curate_template
    assert "S2P_QUALITY_MODEL_SERVICE_URL" in curate_template
    assert "S2P_KENLM_MODEL_SERVICE_URL" in curate_template
    assert curate_template.count("MODEL_SERVICE_DISCOVERY_HOST") == 2
    assert "kind: ScaledObject" in model_service_template
    assert "clusterIP: None" in model_service_template
    assert "s2p_model_active_requests" in model_service_template
    assert "s2p_model_waiting_requests" in model_service_template
    assert "s2p_curator_model_waiting_requests" in model_service_template
    # Direct headless-Service Pod IPs remain governed by the same label-based
    # same-namespace ingress and egress rules as ClusterIP traffic.
    assert network_policy_template.count("app.kubernetes.io/part-of: stream2pretrain") >= 4
    assert "- podSelector:" in network_policy_template
    assert "requiredDuringSchedulingIgnoredDuringExecution" not in model_service_template
    assert "preferredDuringSchedulingIgnoredDuringExecution" in model_service_template
    assert "$curatorComponent" in model_service_template
    assert "type: Recreate" in model_service_template
    assert "maxSurge:" not in model_service_template
    assert "from docling.document_converter import DocumentConverter" in dockerfile
    assert "hasattr(torch.ops.torchvision, 'nms')" in dockerfile
    assert fetcher_template.count("S2P_REQUIRE_REAL_MODELS") == 2


def test_fetcher_image_has_an_isolated_application_and_dependency_profile() -> None:
    processor_project = (ROOT / "processor" / "pyproject.toml").read_text(encoding="utf-8")
    fetcher_app = (ROOT / "processor" / "Dockerfile.fetcher.app").read_text(encoding="utf-8")
    workflow = (ROOT / ".github" / "workflows" / "deploy-main.yml").read_text(encoding="utf-8")

    assert "fetcher-service = [" in processor_project
    assert '"docling==2.114.0"' in processor_project
    assert "processor/Dockerfile.fetcher.app" in workflow
    assert (
        "processor-fetcher-model\n            context: .\n            dockerfile: processor/Dockerfile.fetcher.app"
        in workflow
    )
    assert "from processor.fetcher import main" not in fetcher_app
    assert 'ENTRYPOINT ["s2p-entrypoint", "s2p-fetcher"]' in fetcher_app
    assert "processor/curate.py" not in fetcher_app
    assert "processor/iceberg_writer.py" not in fetcher_app
    assert "COPY processor/expired_inputs.py" in fetcher_app
    assert "processor/expired_inputs.py" in (ROOT / ".github/workflows/deploy-main.yml").read_text()
    assert "COPY processor/work_cutoff.py" in fetcher_app
    assert "processor/work_cutoff.py" in (ROOT / ".github/workflows/deploy-main.yml").read_text()


def test_ingest_common_facade_does_not_eagerly_import_poller_dependencies() -> None:
    facade = (ROOT / "ingest" / "common" / "__init__.py").read_text(encoding="utf-8")

    assert "def __getattr__" in facade
    assert "from ingest.common.kafka_producer import BronzeProducer" not in facade
    probe = subprocess.run(
        [
            sys.executable,
            "-c",
            "import sys; import ingest.common.license_admission; "
            "assert 'ingest.common.kafka_producer' not in sys.modules; "
            "assert 'kubernetes' not in sys.modules",
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    assert probe.returncode == 0, probe.stderr


def test_curator_startup_probe_guards_slow_model_initialization() -> None:
    template = (
        ROOT / "charts" / "stream2pretrain" / "templates" / "processor-curate.yaml"
    ).read_text(encoding="utf-8")

    startup_probe = """          startupProbe:
            httpGet: {path: /healthz, port: metrics}
            periodSeconds: 10
            timeoutSeconds: 3
            failureThreshold: 90"""
    assert startup_probe in template
    assert template.index("startupProbe:") < template.index("livenessProbe:")
    assert "initialDelaySeconds: 60" not in template


def test_curator_uses_bounded_runtime_micro_batches_without_new_recovery_state() -> None:
    curate = (ROOT / "processor" / "curate.py").read_text(encoding="utf-8")
    template = (
        ROOT / "charts" / "stream2pretrain" / "templates" / "processor-curate.yaml"
    ).read_text(encoding="utf-8")
    values = (ROOT / "charts" / "stream2pretrain" / "values.yaml").read_text(encoding="utf-8")

    assert 'op.flat_map_batch("flat_map_batch", up, _batch_step)' in curate
    assert '_curate_run("curate_run", inp)' in curate
    assert 'op.filter("curate_drop_none", mapped' in curate
    assert 'op.collect("curate_run"' not in curate
    assert "S2P_CURATOR_DOCUMENT_BATCH_SIZE" in template
    assert "sourceBatchSize: 4" in values
    assert 'value: "s2p-curate-live-v5"' in template
    assert 'value: "curate-live-v5"' in template


def test_horizontal_profile_renders_every_application_component_for_scale_out() -> None:
    helm = shutil.which("helm")
    if helm is None:
        pytest.skip("Helm is required for the rendered topology contract")
    rendered = subprocess.run(
        [
            helm,
            "template",
            "stream2pretrain",
            str(ROOT / "charts" / "stream2pretrain"),
            "--namespace",
            "stream2pretrain",
            "--values",
            str(ROOT / "charts" / "stream2pretrain" / "values-dev.yaml"),
            "--values",
            str(ROOT / "infra" / "helmfile-values" / "stream2pretrain.dev.yaml"),
            "--values",
            str(ROOT / "infra" / "helmfile-values" / "stream2pretrain.horizontal-scaling.yaml"),
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    resources = [item for item in yaml.safe_load_all(rendered.stdout) if item]
    by_name = {(item.get("kind"), item.get("metadata", {}).get("name")): item for item in resources}
    workloads = {
        "stream2pretrain-processor-fetcher": (
            "fetcher",
            {
                "S2P_BYTEWAX_FLOW_NAME": "s2p-fetcher-live-v5",
                "S2P_BYTEWAX_RECOVERY_NAME": "fetcher-live-v5",
            },
        ),
        "stream2pretrain-processor-curate": (
            "curate",
            {
                "S2P_BYTEWAX_FLOW_NAME": "s2p-curate-live-v5",
                "S2P_BYTEWAX_RECOVERY_NAME": "curate-live-v5",
            },
        ),
        "stream2pretrain-processor-iceberg-writer": (
            "iceberg-writer",
            {
                "S2P_BYTEWAX_FLOW_NAME": "s2p-iceberg-writer-live-v3",
                "S2P_BYTEWAX_RECOVERY_NAME": "iceberg-writer-live-v3",
            },
        ),
        "stream2pretrain-foundry-worker": ("worker", {}),
    }
    for name, (container_name, expected_env) in workloads.items():
        statefulset = by_name[("StatefulSet", name)]
        assert statefulset["spec"]["replicas"] == 2
        assert statefulset["spec"]["serviceName"] == name
        assert statefulset["spec"]["podManagementPolicy"] == "Parallel"
        assert statefulset["spec"]["updateStrategy"]["type"] == "OnDelete"
        service = by_name[("Service", name)]
        assert service["spec"]["clusterIP"] == "None"
        checkpoint = by_name[("PersistentVolumeClaim", f"{name}-checkpoint")]
        assert checkpoint["spec"]["accessModes"] == ["ReadWriteMany"]
        containers = statefulset["spec"]["template"]["spec"]["containers"]
        container = next(item for item in containers if item["name"] == container_name)
        env = {item["name"]: item.get("value") for item in container["env"]}
        assert env["BYTEWAX_STATEFULSET_NAME"] == name
        assert env["BYTEWAX_HOSTFILE_PATH"] == "/etc/bytewax/hostfile.txt"
        assert env["S2P_BYTEWAX_RECOVERY_ROOT"] == "/var/lib/s2p/recovery"
        for key, value in expected_env.items():
            assert env[key] == value
        init = statefulset["spec"]["template"]["spec"]["initContainers"][0]
        assert init["securityContext"]["allowPrivilegeEscalation"] is False
        assert init["securityContext"]["readOnlyRootFilesystem"] is True
        assert init["securityContext"]["capabilities"]["drop"] == ["ALL"]
        command = "\n".join(init["command"])
        assert 'while [ "$index" -lt "2" ]' in command
        assert f"{name}-${{index}}.{name}.stream2pretrain.svc.cluster.local:9999" in command

    for name in (
        "stream2pretrain-ingest-arxiv-html",
        "stream2pretrain-ingest-hf-cards",
        "stream2pretrain-duckdb",
        "stream2pretrain-foundry-api",
        "stream2pretrain-source-controller",
        "stream2pretrain-ui",
    ):
        assert by_name[("Deployment", name)]["spec"]["replicas"] == 2

    for profile in ("quality", "kenlm"):
        name = f"stream2pretrain-processor-model-service-{profile}"
        assert by_name[("ScaledObject", name)]["spec"]["minReplicaCount"] == 2

    duckdb = by_name[("Deployment", "stream2pretrain-duckdb")]
    duckdb_volumes = duckdb["spec"]["template"]["spec"]["volumes"]
    serving_index = next(item for item in duckdb_volumes if item["name"] == "serving-index")
    assert "emptyDir" in serving_index

    for name in ("stream2pretrain-ingest-rss", "stream2pretrain-ingest-oaipmh"):
        cronjob = by_name[("CronJob", name)]
        container = cronjob["spec"]["jobTemplate"]["spec"]["template"]["spec"]["containers"][0]
        env = {item["name"]: item.get("value") for item in container["env"]}
        assert env["S2P_CURSOR_LEASE_BACKEND"] == "kubernetes"

    source_controller = by_name[("Deployment", "stream2pretrain-source-controller")]
    controller_env = {
        item["name"]: item.get("value")
        for item in source_controller["spec"]["template"]["spec"]["containers"][0]["env"]
    }
    assert controller_env["S2P_CURSOR_LEASE_BACKEND"] == "kubernetes"
    assert ("PodDisruptionBudget", "stream2pretrain-source-controller") in by_name

    curator = by_name[("StatefulSet", "stream2pretrain-processor-curate")]
    curator_env = curator["spec"]["template"]["spec"]["containers"][0]["env"]
    coordination = next(
        item for item in curator_env if item["name"] == "S2P_COORDINATION_DATABASE_URL"
    )
    assert coordination["valueFrom"]["secretKeyRef"] == {
        "name": "stream2pretrain-coordination",
        "key": "url",
        "optional": False,
    }


def test_manual_setup_restarts_bytewax_as_a_coordinated_execution() -> None:
    setup = (ROOT / "scripts" / "setup_dhbw_demo.sh").read_text(encoding="utf-8")
    scaling = yaml.safe_load(
        (ROOT / "infra" / "helmfile-values" / "stream2pretrain.horizontal-scaling.yaml").read_text(
            encoding="utf-8"
        )
    )

    for workload in (
        "stream2pretrain-processor-fetcher",
        "stream2pretrain-processor-curate",
        "stream2pretrain-processor-iceberg-writer",
        "stream2pretrain-foundry-worker",
    ):
        assert f"statefulset/{workload}" in setup
    assert '"statefulset/$workload" --timeout=300s' in setup
    assert "quiesce_bytewax_executions" in setup
    assert 'scale "$resource" --replicas=0' in setup
    assert "wait --for=delete pod" in setup
    assert "restore_quiesced_bytewax_executions" in setup
    assert "apply_application_tier" in setup
    assert "stream2pretrain.horizontal-scaling.yaml" in setup
    for component, replica_key, checkpoint_key in (
        ("fetcher", "replicas", "checkpoint"),
        ("curate", "replicas", "checkpoint"),
        ("iceberg", "replicas", "checkpoint"),
        ("foundry", "workerReplicas", "state"),
    ):
        checkpoint = scaling["processor"][component][checkpoint_key]
        assert scaling["processor"][component][replica_key] == 2
        assert checkpoint["accessMode"] == "ReadWriteMany"
        assert checkpoint["storageClass"] == "longhorn"
    assert scaling["processor"]["foundry"]["apiReplicas"] == 2
    assert scaling["processor"]["duckdbApi"]["replicas"] == 2
    assert scaling["sourceController"]["replicas"] == 2
    assert scaling["ui"]["replicas"] == 2


def test_manual_setup_blocks_incompatible_checkpoint_migrations_before_quiescing() -> None:
    setup = (ROOT / "scripts" / "setup_dhbw_demo.sh").read_text(encoding="utf-8")
    guard = setup.split("validate_bytewax_checkpoint_storage() {", 1)[1].split(
        "\n}\n\nrestore_quiesced_bytewax_executions()", 1
    )[0]
    application = setup.split("  application)\n", 1)[1].split("  verify)\n", 1)[0]

    assert "uv run --frozen python -" in guard
    assert "if replicas > 1:" in guard
    assert 'if "ReadWriteMany" not in access_modes:' in guard
    assert 'if not storage_class or storage_class == "local-path":' in guard
    assert 'if [[ "$managed" == "false" ]]' in guard
    assert 'if [[ ",$actual_access_modes," != *,ReadWriteMany,* ]]' in guard
    assert 'if [[ "$actual_storage_class" == "local-path" ]]' in guard
    assert 'if [[ "$actual_storage_class" != "$desired_storage_class" ]]' in guard
    assert "checkpoint-stream2pretrain-processor-curate-0" in guard
    assert "state-stream2pretrain-foundry-0" in guard
    assert 'if [[ "$managed" == "true" ]]' in guard
    assert "stream2pretrain.io/migrated-from" in guard
    assert "stream2pretrain.io/migration-verified" in guard
    assert "Migrate recovery plus local decision and duplicate state" in guard
    assert "stream2pretrain.io/foundry-control-migrated-from" in guard
    assert "stream2pretrain.io/foundry-control-migration-verified" in guard
    assert "stream2pretrain.io/foundry-control-migration-manifest-sha256" in guard
    assert "^[0-9A-Fa-f]{64}$" in guard
    assert "delete persistentvolumeclaim" not in guard
    assert application.index("validate_bytewax_checkpoint_storage") < application.index(
        "ensure_foundry_signing_identity"
    )
    assert application.index("validate_bytewax_checkpoint_storage") < application.index(
        "apply_application_tier"
    )
    apply_application = setup.split("apply_application_tier() {", 1)[1].split(
        "\n}\n\napply_named_release()", 1
    )[0]
    assert apply_application.index("quiesce_bytewax_executions") < apply_application.index(
        "delete_legacy_bytewax_statefulsets"
    )
    assert apply_application.index("delete_legacy_bytewax_statefulsets") < apply_application.index(
        "apply_tier application"
    )


def test_workflow_guards_legacy_bytewax_state_before_any_deploy_mutation() -> None:
    workflow = (ROOT / ".github" / "workflows" / "deploy-main.yml").read_text(encoding="utf-8")
    guard_start = workflow.index('if [[ "$DEPLOY_MODE" == "deploy" ]]; then')
    guard_end = workflow.index('if [[ "$DNS_INFRA_CHANGED" == "true" ]]', guard_start)
    guard = workflow[guard_start:guard_end]

    assert "checkpoint-stream2pretrain-processor-curate-0" in guard
    assert "state-stream2pretrain-foundry-0" in guard
    assert 'if [[ "$managed" == "true" ]]' in guard
    assert "stream2pretrain.io/migrated-from" in guard
    assert "stream2pretrain.io/migration-verified" in guard
    assert "Migrate recovery plus local decision and duplicate state" in guard
    assert "stream2pretrain.io/foundry-control-migrated-from" in guard
    assert "stream2pretrain.io/foundry-control-migration-verified" in guard
    assert "stream2pretrain.io/foundry-control-migration-manifest-sha256" in guard
    assert "^[0-9A-Fa-f]{64}$" in guard
    assert guard_start < workflow.index("kubectl create namespace stream2pretrain")
    assert guard_start < workflow.index("kubectl -n kube-system patch deployment/coredns")

    recreate = workflow.split('legacy_curator_statefulset="$(', 1)[1].split(
        "# GitHub serialises this workflow", 1
    )[0]
    assert ".spec.volumeClaimTemplates // [] | length" in recreate
    assert "delete" in recreate
    assert "statefulset/stream2pretrain-processor-curate" in recreate
    assert "statefulset/stream2pretrain-foundry" in recreate


def test_workflow_uses_split_foundry_workloads_and_isolates_curator_canary() -> None:
    workflow = (ROOT / ".github" / "workflows" / "deploy-main.yml").read_text(encoding="utf-8")

    assert "exec -i statefulset/stream2pretrain-foundry -c api" not in workflow
    assert (
        "rollout status \\\n              statefulset/stream2pretrain-foundry --timeout"
        not in workflow
    )
    assert "deployment/stream2pretrain-foundry-api -c api" in workflow
    assert "statefulset/stream2pretrain-foundry-worker --timeout=180s" in workflow
    assert "stream2pretrain-foundry-worker-0:worker" in workflow

    canary = workflow.split('curator_canary_job="s2p-curate-smoke-', 1)[1].split(
        'production_curator_limits="$(', 1
    )[0]
    assert 'select(.name != "bytewax-hostfile")' in canary
    assert 'and .name != "BYTEWAX_POD_NAME"' in canary
    assert 'and .name != "BYTEWAX_STATEFULSET_NAME"' in canary
    assert 'and .name != "BYTEWAX_HOSTFILE_PATH"' in canary
    assert 'and .name != "S2P_COORDINATION_DATABASE_URL"' in canary
    assert '.name != "recovery"' in canary
    assert 'name: "recovery"' in canary
    assert 'name: "checkpoint"' not in canary


def test_cloudnative_pg_creates_coordination_database_with_bootstrap_superuser() -> None:
    helm = shutil.which("helm")
    if helm is None:
        pytest.skip("Helm is required for the CloudNativePG render contract")

    template = ROOT / "charts" / "polaris-postgres" / "templates" / "cluster.yaml"
    source = template.read_text(encoding="utf-8")
    assert "postInitSQL:" in source
    assert "postInitApplicationSQL:" not in source

    rendered = subprocess.run(
        [
            helm,
            "template",
            "polaris-postgres",
            str(ROOT / "charts" / "polaris-postgres"),
            "--namespace",
            "polaris",
            "--values",
            str(ROOT / "infra" / "helmfile-values" / "polaris-postgres.dev.yaml"),
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    cluster = next(
        item
        for item in yaml.safe_load_all(rendered.stdout)
        if item and item.get("kind") == "Cluster"
    )
    initdb = cluster["spec"]["bootstrap"]["initdb"]
    assert initdb["postInitSQL"] == ["CREATE DATABASE stream2pretrain OWNER polaris"]
    assert "postInitApplicationSQL" not in initdb


def test_source_controller_lease_duration_is_an_explicit_controller_setting() -> None:
    values = yaml.safe_load(
        (ROOT / "charts" / "stream2pretrain" / "values.yaml").read_text(encoding="utf-8")
    )
    template = (
        ROOT / "charts" / "stream2pretrain" / "templates" / "sourcecontroller.yaml"
    ).read_text(encoding="utf-8")

    assert values["sourceController"]["leaseDurationSeconds"] == 600
    assert ".Values.sourceController.leaseDurationSeconds" in template
    assert ".Values.sources.huggingface.models.pollIntervalSeconds" not in template


def test_storage_migration_guards_precede_every_destructive_reconciliation_step() -> None:
    setup = (ROOT / "scripts" / "setup_dhbw_demo.sh").read_text(encoding="utf-8")
    workflow = (ROOT / ".github" / "workflows" / "deploy-main.yml").read_text(encoding="utf-8")

    minio_guard = setup.split("require_minio_data_migration() {", 1)[1].split(
        "\n}\n\nrequire_polaris_postgres_migration()", 1
    )[0]
    postgres_guard = setup.split("require_polaris_postgres_migration() {", 1)[1].split(
        "\n}\n\nvalidate_bytewax_checkpoint_storage()", 1
    )[0]
    for guard in (minio_guard, postgres_guard):
        for mutation in (
            "kubectl apply",
            "kubectl create",
            "kubectl delete",
            "kubectl patch",
            "kubectl label",
            "kubectl annotate",
            "helmfile",
            "apply_tier",
        ):
            assert mutation not in guard

    storage_case = setup.split("  storage)\n", 1)[1].split("  edge)\n", 1)[0]
    assert storage_case.index("require_minio_data_migration") < storage_case.index(
        "apply_tier storage"
    )

    catalog_case = setup.split("  catalog)\n", 1)[1].split("  topics)\n", 1)[0]
    guard_index = catalog_case.index("require_polaris_postgres_migration")
    for mutation in (
        "required_catalog_secret",
        "ensure_polaris_persistence_identity",
        "adopt_polaris_release_resources",
        "apply_tier catalog",
    ):
        assert guard_index < catalog_case.index(mutation)

    catalog_reconciliation = workflow.split('if [[ "$CATALOG_CHANGED" == "true" ]]; then', 1)[
        1
    ].split('coordination_username="$(', 1)[0]
    workflow_guard_index = catalog_reconciliation.index(
        "if kubectl -n polaris get statefulset/polaris-postgres"
    )
    namespace_index = catalog_reconciliation.index("kubectl create namespace polaris")
    workflow_guard = catalog_reconciliation[workflow_guard_index:namespace_index]
    for mutation in (
        "kubectl apply",
        "kubectl create",
        "kubectl delete",
        "kubectl patch",
        "kubectl label",
        "kubectl annotate",
        "helmfile",
    ):
        assert mutation not in workflow_guard
    for mutation in (
        "kubectl create namespace polaris",
        "create secret generic polaris-persistence",
        "patch secret polaris-persistence",
        'label "$resource"',
        "delete deployment/polaris",
        "delete service/polaris",
    ):
        assert workflow_guard_index < catalog_reconciliation.index(mutation)


def test_redpanda_chart_and_rendered_replication_contract() -> None:
    helm = shutil.which("helm")
    if helm is None:
        pytest.skip("Helm is required for the Redpanda render contract")

    lock = yaml.safe_load((ROOT / "helmfile.lock").read_text(encoding="utf-8"))
    redpanda_dependency = next(item for item in lock["dependencies"] if item["name"] == "redpanda")
    assert redpanda_dependency["version"] == "25.3.10"
    helmfile_source = (ROOT / "helmfile.yaml").read_text(encoding="utf-8")
    assert "version: 25.3.10" in helmfile_source

    values_path = ROOT / "infra" / "helmfile-values" / "redpanda.dev.yaml"
    values = yaml.safe_load(values_path.read_text(encoding="utf-8"))
    assert "post_upgrade_job" not in values

    rendered = subprocess.run(
        [
            helm,
            "template",
            "redpanda",
            "redpanda/redpanda",
            "--version",
            "25.3.10",
            "--namespace",
            "redpanda",
            "--values",
            str(values_path),
        ],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    resources = [item for item in yaml.safe_load_all(rendered.stdout) if item]
    by_kind = {(item.get("kind"), item.get("metadata", {}).get("name")): item for item in resources}

    statefulset = by_kind[("StatefulSet", "redpanda")]
    assert statefulset["spec"]["replicas"] == 3
    required_anti_affinity = statefulset["spec"]["template"]["spec"]["affinity"]["podAntiAffinity"][
        "requiredDuringSchedulingIgnoredDuringExecution"
    ]
    assert len(required_anti_affinity) == 1
    assert required_anti_affinity[0]["topologyKey"] == "kubernetes.io/hostname"

    claim = statefulset["spec"]["volumeClaimTemplates"][0]
    assert claim["metadata"]["name"] == "datadir"
    assert claim["spec"]["accessModes"] == ["ReadWriteOnce"]
    assert claim["spec"]["storageClassName"] == "local-path"
    assert claim["spec"]["resources"]["requests"]["storage"] == "50Gi"

    config = by_kind[("ConfigMap", "redpanda")]
    bootstrap = json.loads(config["data"][".bootstrap.json.in"])
    assert bootstrap["default_topic_replications"] == "3"

    service = by_kind[("Service", "redpanda")]
    ports = {item["name"]: item["port"] for item in service["spec"]["ports"]}
    assert ports == {
        "admin": 9644,
        "http": 8082,
        "kafka": 9093,
        "rpc": 33145,
        "schemaregistry": 8081,
    }

    disruption_budget = by_kind[("PodDisruptionBudget", "redpanda")]
    assert disruption_budget["spec"]["maxUnavailable"] == 1
    monitor = by_kind[("ServiceMonitor", "redpanda")]
    endpoint = monitor["spec"]["endpoints"][0]
    assert endpoint["path"] == "/public_metrics"
    assert endpoint["port"] == "admin"


def test_manual_setup_creates_shared_coordination_identity_without_echoing_it() -> None:
    setup = (ROOT / "scripts" / "setup_dhbw_demo.sh").read_text(encoding="utf-8")
    coordination = setup.split("ensure_coordination_identity() {", 1)[1].split(
        "\n}\n\nadopt_polaris_release_resources()", 1
    )[0]

    assert coordination.count("'$value | @uri'") == 2
    assert "stream2pretrain-coordination" in coordination
    assert "polaris-postgres.polaris.svc.cluster.local:5432/stream2pretrain" in coordination
    assert '--from-literal=url="$coordination_url"' in coordination
    assert "--dry-run=client -o yaml | kubectl apply -f -" in coordination
    assert (
        "unset username password encoded_username encoded_password coordination_url" in coordination
    )
    assert "printf" not in coordination.replace(
        "printf 'Polaris persistence credentials are required for worker coordination.\\n' >&2",
        "",
    )


def test_workload_alerts_follow_controller_ownership() -> None:
    rules = (ROOT / "charts" / "stream2pretrain" / "templates" / "prometheusrules.yaml").read_text(
        encoding="utf-8"
    )

    assert 'deployment=~"{{ $fullName }}-(duckdb|ui)"' in rules
    assert 'statefulset=~"{{ $fullName }}-processor-(fetcher|curate|iceberg-writer)"' in rules
    assert 'statefulset="{{ $fullName }}-foundry-worker"' in rules
    assert 'deployment="{{ $fullName }}-foundry-api"' in rules
    assert 'statefulset="{{ $fullName }}-foundry"' not in rules


def test_catalog_bootstrap_precedes_application_rollout() -> None:
    template = (
        ROOT / "charts" / "stream2pretrain" / "templates" / "job-polaris-bootstrap.yaml"
    ).read_text(encoding="utf-8")
    dockerfile = (ROOT / "processor" / "Dockerfile.app").read_text(encoding="utf-8")

    assert '"helm.sh/hook": pre-install,pre-upgrade' in template
    assert '"helm.sh/hook-weight": "-10"' in template
    assert "python -m processor.polaris_bootstrap" in template
    assert "--apply --register-missing --register-only" in template
    workflow = (ROOT / ".github" / "workflows" / "deploy-main.yml").read_text(encoding="utf-8")
    assert "s2p-entrypoint s2p-object-lifecycle --apply" in workflow
    helper = (ROOT / "charts" / "stream2pretrain" / "templates" / "_helpers.tpl").read_text(
        encoding="utf-8"
    )
    assert "S2P_TRANSIENT_OBJECT_RETENTION_DAYS" in helper
    assert "activeDeadlineSeconds: 300" in template
    assert "COPY processor" in dockerfile


def test_release_images_are_deployed_by_content_digest() -> None:
    workflow = (ROOT / ".github" / "workflows" / "deploy-main.yml").read_text(encoding="utf-8")
    helper = (ROOT / "charts" / "stream2pretrain" / "templates" / "_helpers.tpl").read_text(
        encoding="utf-8"
    )

    assert "image-pin:" in workflow
    assert "needs.image-pin.result == 'success'" in workflow
    assert "pin_component processor_quality_model processor-quality-model" in workflow
    assert "processor-quality-model@${IMAGE_DIGEST_PROCESSOR_QUALITY}" in workflow
    assert '"quality source-pretrain-quality"' in workflow
    assert "scripts/benchmark_model_service.py" in workflow
    assert "ui.image=stream2pretrain/ui@${IMAGE_DIGEST_UI}" in workflow
    assert "service/stream2pretrain-ui 18080:http" in workflow
    assert "minimum_rootfs_available=$((6 * 1024 * 1024 * 1024))" in workflow
    assert "Existing unschedulable Pods will be reconciled by this release" in workflow
    unschedulable_gate = workflow[
        workflow.index('if [[ -n "$unschedulable_pods" ]]') : workflow.index(
            'worker_nodes="$(', workflow.index('if [[ -n "$unschedulable_pods" ]]')
        )
    ]
    assert "report_deploy_failure" not in unschedulable_gate
    assert "exit 1" not in unschedulable_gate
    assert 'contains "@sha256:" .image' in helper
    assert 'printf "%s/%s" $ctx.Values.image.registry .image' in helper
    assert 'delete "scaledobject/$model"' in workflow
    assert '"horizontalpodautoscaler/keda-hpa-$model"' in workflow
    assert 'scale "deployment/$model" --replicas=1' in workflow
    assert "--field-selector=status.phase=Failed" in workflow
    assert "release_applied=false" in workflow
    assert "helm_apply_status=${PIPESTATUS[0]}" in workflow
    assert "release: already exists|another operation .* is in progress" in workflow
    assert 'if [[ "$release_applied" != true ]]' in workflow
    helm_release = workflow[
        workflow.index("release_applied=false") : workflow.index(
            'finish_phase "Changed workload readiness"'
        )
    ]
    assert "\n              sync \\\n" in helm_release
    assert "--wait=false" in helm_release
    assert "--wait-for-jobs=false" in helm_release
    assert "workload_timeout=60" in helm_release
    assert '"$workload" == statefulset/stream2pretrain-processor-fetcher*' in helm_release
    assert "workload_timeout=180" in helm_release
    assert '"$workload" == deployment/stream2pretrain-processor-model-service-*' in helm_release
    assert "workload_timeout=600" in helm_release
    helmfile = (ROOT / "helmfile.yaml").read_text(encoding="utf-8")
    application_release = helmfile.split("  - name: stream2pretrain", maxsplit=1)[1]
    assert "    wait: false" in application_release
    assert "    waitForJobs: false" in application_release
    assert ') >"$log" 2>&1 &' in helm_release


def test_fetcher_uses_matching_official_cpu_vision_wheels() -> None:
    processor_project = (ROOT / "processor" / "pyproject.toml").read_text(encoding="utf-8")
    lock = (ROOT / "uv.lock").read_text(encoding="utf-8")

    assert '"torchvision>=0.18,<1"' in processor_project
    assert 'name = "torchvision"\nversion = "0.28.0+cpu"' in lock
    assert 'source = { registry = "https://download.pytorch.org/whl/cpu" }' in lock


def test_pdf_fallback_uses_the_bounded_cpu_tableformer_mode() -> None:
    scientific = (ROOT / "processor" / "scientific.py").read_text(encoding="utf-8")

    assert "mode=TableFormerMode.FAST" in scientific
    assert "mode=TableFormerMode.ACCURATE" not in scientific
    assert "options.do_formula_enrichment = False" in scientific


def test_cpu_pdf_fallback_does_not_load_the_codeformula_vlm() -> None:
    scientific = (ROOT / "processor" / "scientific.py").read_text(encoding="utf-8")

    assert "options.do_formula_enrichment = False" in scientific


def test_release_preserves_stream_offsets() -> None:
    workflow = (ROOT / ".github" / "workflows" / "deploy-main.yml").read_text()
    assert "rpk group seek" not in workflow
    assert "S2P_CORE_TOPIC_PARTITIONS=4 bash scripts/reconcile_topic_partitions.sh" in workflow


def test_curator_canary_keeps_limits_but_uses_a_measured_scheduler_request() -> None:
    workflow = (ROOT / ".github" / "workflows" / "deploy-main.yml").read_text(encoding="utf-8")
    start = workflow.index("# Clone the exact deployed curator Pod spec")
    end = workflow.index('finish_phase "Core canary"')
    canary = workflow[start:end]

    assert "| .resources.requests = {" in canary
    assert 'cpu: "100m"' in canary
    assert 'memory: "1Gi"' in canary
    assert "| .resources.limits =" not in canary
    assert "canary_curator_limits" in canary
    assert "production_curator_limits" in canary
    assert "--for=condition=PodScheduled" in canary
    assert "--timeout=30s" in canary
    assert "describe pod" in canary
    assert '-l "job-name=$curator_canary_job"' in canary


def test_model_service_content_hash_ignores_unrelated_processor_code() -> None:
    workflow = (ROOT / ".github" / "workflows" / "deploy-main.yml").read_text(encoding="utf-8")

    model_input = (
        "inputs: .dockerignore schemas processor/Dockerfile.model-service.app "
        "processor/__init__.py processor/common.py processor/model_service.py processor/model_jobs.py "
        "processor/operators/__init__.py "
        "processor/operators/quality.py processor/operators/source_classifiers.py processor/operators/kenlm_score.py"
    )
    assert workflow.count(model_input) == 2
    assert "inputs: .dockerignore pyproject.toml uv.lock tests/pyproject.toml" not in workflow


def test_foundry_has_an_independent_application_image() -> None:
    workflow = (ROOT / ".github" / "workflows" / "deploy-main.yml").read_text(encoding="utf-8")

    assert "image: processor-foundry" in workflow
    assert "dockerfile: processor/Dockerfile.foundry.app" in workflow
    assert "processor/Dockerfile.foundry.app processor/__init__.py" in workflow
    assert "processor/foundry docs/provider-terms" in workflow
    foundry_dockerfile = (ROOT / "processor" / "Dockerfile.foundry.app").read_text()
    assert "COPY docs/provider-terms             /app/docs/provider-terms" in foundry_dockerfile


@pytest.mark.parametrize("package_dir", INGEST_PACKAGES, ids=lambda path: path.name)
def test_ingest_dockerfile_declares_an_isolated_local_install(package_dir: Path) -> None:
    metadata = tomllib.loads((package_dir / "pyproject.toml").read_text(encoding="utf-8"))
    dependencies = metadata["project"]["dependencies"]
    dockerfile = (package_dir / "Dockerfile").read_text(encoding="utf-8")

    assert "uv pip install --system --no-cache --no-config --no-sources" in dockerfile
    for package_name, local_path in LOCAL_PACKAGE_PATHS.items():
        if package_name == "stream2pretrain" or any(
            dependency.split(" ", 1)[0] == package_name for dependency in dependencies
        ):
            assert f"-e {local_path}" in dockerfile
            if local_path not in {".", "./ingest/common"}:
                source_path = local_path.removeprefix("./")
                assert f"COPY {source_path} /src/{source_path}" in dockerfile
