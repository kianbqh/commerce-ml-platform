"""Real Dagster/dagster-dbt adapter. Load only in the isolated pinned runtime.

The environment supplies existing manifests. Loading definitions parses small JSON
only: no dbt parse, data scans, training, API lifecycle or schedules are started.
"""
from __future__ import annotations

from contextlib import contextmanager
import copy
import json
import os
from pathlib import Path
import sys
import time

from dagster import (AllPartitionMapping, AssetCheckResult, AssetCheckSeverity, AssetDep, AssetKey,
                     AssetSelection, DefaultScheduleStatus, DefaultSensorStatus, Definitions,
                     MaterializeResult, ScheduleDefinition, SkipReason, StaticPartitionsDefinition,
                     asset, asset_check, define_asset_job, job, op, sensor)
from dagster_dbt import (DagsterDbtTranslator, DagsterDbtTranslatorSettings, DbtCliResource,
                         DbtProject, dbt_assets)

from .gates import emit_local_alert, read_json
from .provenance import sha256_file, utc_now
from .runtime import Runtime


@contextmanager
def scoped_environment(changes):
    previous = {k: os.environ.get(k) for k in changes}
    os.environ.update(changes)
    try:
        yield
    finally:
        for key, value in previous.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


def annotated_manifest(path):
    """Explicitly target every singular dbt test; retain original manifest on disk.

    Multi-model singular tests otherwise become observations rather than checks.
    Target preference is its declared meta, then the lexically first model dep.
    Generic tests retain dbt's attached_node. This is a deliberate translation
    wrapper; it does not rewrite any legacy SQL/test file.
    """
    manifest = copy.deepcopy(read_json(path))
    nodes = manifest["nodes"]
    expected = []
    for unique_id, node in nodes.items():
        if node.get("resource_type") != "test":
            continue
        models = sorted(dep for dep in node.get("depends_on", {}).get("nodes", [])
                        if nodes.get(dep, {}).get("resource_type") == "model")
        if not models:
            raise ValueError("test without a model target needs an explicit reviewed translation: " + unique_id)
        meta = node.setdefault("meta", {})
        nested = meta.setdefault("dagster", {})
        if not nested.get("ref") and not node.get("attached_node"):
            target = nodes[models[0]]
            nested["ref"] = {"name": target["name"], "package_name": target["package_name"]}
        node.setdefault("config", {}).setdefault("meta", {}).update(meta)
        expected.append(unique_id)
    return manifest, expected


class TerminalTranslator(DagsterDbtTranslator):
    def get_asset_key(self, dbt_resource_props):
        return super().get_asset_key(dbt_resource_props).with_prefix(["m4", "terminal"])

    def get_asset_spec(self, manifest, unique_id, project=None):
        spec = super().get_asset_spec(manifest, unique_id, project)
        if manifest["nodes"].get(unique_id, {}).get("resource_type") == "model":
            return spec.merge_attributes(deps=[AssetDep(AssetKey("terminal_gate"))])
        return spec


def build_definitions(rt, manifest_path, *, coordinator=True):
    partitions = StaticPartitionsDefinition([b["key"] for b in rt.manifest["batches"]])
    manifest, expected_checks = annotated_manifest(manifest_path)
    project = DbtProject(project_dir=rt.warehouse)

    def guarded(context, label, function):
        # AssetCheckExecutionContext exposes run, not the asset-only run_id alias.
        run_id = context.run.run_id
        try:
            return function(run_id)
        except Exception as exc:
            rt.fail(run_id, context.partition_key, label, exc)
            raise

    @asset(partitions_def=partitions, group_name="arrival_replay")
    def arrival_ingest(context):
        receipt = guarded(context, "ingest", lambda run_id: rt.ingest(context.partition_key, run_id))
        return MaterializeResult(metadata={"receipt": str(rt.attempt(context.run_id) / "ingest.json"),
                                          "simulated_day": next(b["received_day_simulated"] for b in rt.manifest["batches"] if b["key"] == context.partition_key)})

    @asset(partitions_def=partitions, deps=[arrival_ingest], group_name="arrival_replay")
    def dbt_staging(context):
        guarded(context, "staging", rt.stage)
        return MaterializeResult(metadata={"attempt": context.run_id})

    @asset_check(asset=dbt_staging, name="input_quality", blocking=True, partitions_def=partitions)
    def input_quality(context):
        passed = guarded(context, "dbt_input_quality", rt.input_checks)
        return AssetCheckResult(passed=passed, severity=AssetCheckSeverity.ERROR,
                                metadata={"evidence": str(rt.attempt(context.run.run_id) / "input-check.json")})

    @asset(partitions_def=partitions, deps=[dbt_staging], group_name="arrival_replay")
    def operational_marts(context):
        guarded(context, "marts", rt.marts)
        return MaterializeResult(metadata={"attempt": context.run_id})

    @asset_check(asset=operational_marts, name="quality_and_processing_freshness", blocking=True,
                 partitions_def=partitions)
    def quality_and_processing_freshness(context):
        passed = guarded(context, "output_quality", rt.output_checks)
        return AssetCheckResult(passed=passed, severity=AssetCheckSeverity.ERROR,
                                metadata={"evidence": str(rt.attempt(context.run.run_id) / "output-checks.json"),
                                          "maximum_processing_age_seconds": rt.config["maximum_processing_age_seconds"]})

    @asset(partitions_def=partitions, deps=[operational_marts], group_name="arrival_replay")
    def batch_promoted(context):
        receipt = guarded(context, "promotion", rt.promote)
        return MaterializeResult(metadata={"database": receipt["database"], "checkpoint": str(rt.root / "promoted.json")})

    @asset(deps=[AssetDep(batch_promoted, partition_mapping=AllPartitionMapping())], group_name="terminal")
    def reference_full(context):
        reference = rt.full_reference()
        return MaterializeResult(metadata={"database": reference})

    @asset(deps=[reference_full], group_name="terminal")
    def full_incremental_parity(context):
        reference = read_json(rt.root / "full-reference/reference.json")["database"]
        result = rt.parity(reference)
        return MaterializeResult(metadata={"passed": result["passed"], "evidence": str(rt.root / "full-incremental-parity.json")})

    @asset(deps=[full_incremental_parity], group_name="spark")
    def spark_parquet(context):
        sys.path.insert(0, str(rt.repo / "spark"))
        from instacart_job import compare, run
        output = run(rt, rt.root / "spark-original")
        parity = compare(rt, output, rt.root / "spark-original/spark-dbt-parity.json")
        return MaterializeResult(metadata={"passed": parity["passed"], "manifest": str(rt.root / "spark-original/spark-run.json")})

    @asset(deps=[full_incremental_parity, spark_parquet], group_name="terminal")
    def terminal_gate(context):
        database = rt.terminal_gate()
        return MaterializeResult(metadata={"database": str(database), "terminal_original_only": True})

    @dbt_assets(manifest=manifest, name="terminal_warehouse_dbt", project=project,
                dagster_dbt_translator=TerminalTranslator(settings=DagsterDbtTranslatorSettings(enable_asset_checks=True)))
    def terminal_warehouse_dbt(context, dbt: DbtCliResource):
        directory = rt.root / "terminal" / ("dbt-" + context.run_id)
        directory.mkdir(parents=True, exist_ok=False)
        receipt = {**rt.state(), "database": str(rt.root / "terminal/warehouse.duckdb")}
        arguments = ["build", "--vars", json.dumps(rt.variables(receipt), sort_keys=True),
                     "--threads", str(rt.config["dbt_threads"])]
        # The DbtCliResource streams actual dbt materialization/check events.
        with scoped_environment(rt.environment(receipt["database"], directory)):
            started, started_utc = time.perf_counter(), utc_now()
            invocation = dbt.cli(arguments, context=context, target_path=directory / "dbt-target")
            try:
                yield from invocation.stream()
            finally:
                for name in ("manifest.json", "run_results.json"):
                    path = directory / "dbt-target" / name
                    if path.exists():
                        rt.evidence(f"terminal/dbt-{context.run_id}/{name}.identity.json", {"path": str(path),
                                    "sha256": sha256_file(path),
                                    "expected_check_nodes": expected_checks})
                rt.evidence(f"terminal/dbt-{context.run_id}/process.json", {"command": ["dbt", *arguments],
                            "started_utc": started_utc, "finished_utc": utc_now(),
                            "wall_seconds": time.perf_counter()-started, "returncode": invocation.process.poll(),
                            "timer_scope": "real dagster-dbt CLI process, streamed events and artifact recording"})

    actual_checks = terminal_warehouse_dbt.check_keys
    if len(actual_checks) != len(expected_checks):
        raise ValueError(f"dbt checks import incomplete: {len(actual_checks)} != {len(expected_checks)}")

    @asset(deps=list(terminal_warehouse_dbt.keys), group_name="terminal")
    def feature_export(context):
        from .downstream import export_terminal
        result = export_terminal(rt)
        return MaterializeResult(metadata={"feature_fingerprint": result["feature_fingerprint"],
                                          "split_sha256": result["split_sha256"]})

    @asset(deps=[feature_export], group_name="optional_model")
    def recsys_train_eval(context):
        from .downstream import train_eval
        result = train_eval(rt)
        return MaterializeResult(metadata={"status": result["status"], "evidence": str(rt.root / "model-run.json")})

    @asset(deps=[recsys_train_eval], group_name="optional_model")
    def api_benchmark(context):
        from .downstream import benchmark_api
        result = benchmark_api(rt)
        return MaterializeResult(metadata={"status": result["status"], "evidence": str(rt.root / "api-run.json")})

    @asset(deps=[api_benchmark], group_name="demonstrations")
    def manual_demonstrations(context):
        from .manual import run_demonstrations
        result = run_demonstrations(rt, manifest_path, context.instance, parent_id=context.run_id,
                                   first=rt.config["backfill_first"], last=rt.config["backfill_last"])
        return MaterializeResult(metadata=result)

    arrival_assets = [arrival_ingest, dbt_staging, operational_marts, batch_promoted]
    terminal_assets = [reference_full, full_incremental_parity, spark_parquet, terminal_gate, terminal_warehouse_dbt,
                       feature_export, recsys_train_eval, api_benchmark, manual_demonstrations]
    arrival_job = define_asset_job("arrival_batch", selection=AssetSelection.assets(*arrival_assets), partitions_def=partitions)
    terminal_job = define_asset_job("terminal_complete", selection=AssetSelection.assets(*terminal_assets))

    @sensor(name="local_quality_alert", default_status=DefaultSensorStatus.STOPPED)
    def local_quality_alert(context):
        # Reading retained failure records is sufficient; no cloud transmission or RunRequest.
        paths = sorted((rt.root / "attempts").glob("*/failure.json"))
        cursor = context.cursor or ""
        for path in paths:
            failure = read_json(path)
            emit_local_alert(rt.root / "alerts", failure, cursor=cursor, now=utc_now(), log=context.log.error)
        context.update_cursor(str(len(paths)))
        return SkipReason("Local alerts evaluated manually; no runs requested")

    jobs = [arrival_job, terminal_job]
    schedules = []
    if coordinator:
        @op
        def manual_complete(context):
            from .manual import execute_complete
            return execute_complete(rt, definitions, context.instance, context.run_id)

        @job(name="commerce_all")
        def commerce_all():
            manual_complete()

        jobs.append(commerce_all)
        schedules.append(ScheduleDefinition(name="daily_replay_definition_only", job=commerce_all,
                            cron_schedule="0 0 * * *", execution_timezone="UTC",
                            default_status=DefaultScheduleStatus.STOPPED,
                            should_execute=lambda _context: False,
                            description="Definition only; all requested runs are manual or CI. No daemon."))

    definitions = Definitions(assets=arrival_assets + terminal_assets,
                             asset_checks=[input_quality, quality_and_processing_freshness], jobs=jobs,
                             schedules=schedules, sensors=[local_quality_alert],
                             resources={"dbt": DbtCliResource(project_dir=project.project_dir,
                                                              profiles_dir=rt.warehouse, target="duckdb")})
    return definitions


def load():
    # Every required path must already exist; import performs no materialization.
    return build_definitions(Runtime(os.environ["M4_RUN_ROOT"]), os.environ["M4_DBT_MANIFEST"])
