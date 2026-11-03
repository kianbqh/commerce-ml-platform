"""Manual suite: child Dagster runs, idempotence, correction, failure and recovery."""
from __future__ import annotations

from datetime import timedelta
import json
import os
from pathlib import Path
import time

from . import duck
from .gates import processing_freshness, read_json, utc
from .provenance import utc_now
from .replay import backfill_range, json_hash, verify_changed_partitions


def run_child(rt, definitions, instance, job_name, *, parent_id, partition=None, expected_failure=False):
    started = time.perf_counter()
    job = definitions.get_job_def(job_name)
    kwargs = {"instance": instance, "raise_on_error": False,
              "tags": {"m4/parent_run_id": parent_id, "m4/scenario": rt.scenario}}
    if partition is not None:
        kwargs["partition_key"] = partition
    result = job.execute_in_process(**kwargs)
    # Preserve actual run/check/materialization identities, including failed runs.
    events = []
    for event in result.all_events:
        data = event.event_specific_data
        asset_key = getattr(data, "asset_key", None)
        if asset_key is None and hasattr(data, "materialization"):
            asset_key = data.materialization.asset_key
        events.append({"event_type": event.event_type_value, "step_key": event.step_key,
                       "message": event.message, "asset_key": list(asset_key.path) if asset_key else None,
                       "check_passed": getattr(data, "passed", None), "check_name": getattr(data, "check_name", None)})
    rt.evidence(f"children/{result.run_id}.json", {"parent_run_id": parent_id, "child_run_id": result.run_id,
                "job": job_name, "partition": partition, "success": result.success,
                "expected_failure": expected_failure, "wall_seconds": time.perf_counter() - started,
                "events": events})
    rt.release_lock(result.run_id)
    if expected_failure == result.success:
        raise RuntimeError(f"unexpected child state: {job_name} {partition}, success={result.success}")
    return result


def execute_complete(rt, definitions, instance, parent_id):
    keys = [b["key"] for b in rt.manifest["batches"]]
    children = []
    for partition in keys:
        result = run_child(rt, definitions, instance, "arrival_batch", parent_id=parent_id, partition=partition)
        children.append(result.run_id)
    # Repeat a loaded batch, audit every partition and both corrected facts.
    before = duck.partition_digests(rt.state()["database"], rt.manifest, connection_options=rt.duck_options)
    repeated = run_child(rt, definitions, instance, "arrival_batch", parent_id=parent_id, partition=keys[-1])
    after = duck.partition_digests(rt.state()["database"], rt.manifest, connection_options=rt.duck_options)
    if before != after:
        raise ValueError("repeated batch changed logical values")
    rt.evidence("replay-idempotence.json", {"passed": True, "repeated_partition": keys[-1],
                 "child_run_id": repeated.run_id, "before": before, "after": after})
    terminal = run_child(rt, definitions, instance, "terminal_complete", parent_id=parent_id)
    rt.evidence("coordinator.json", {"parent_run_id": parent_id, "arrival_child_run_ids": children,
                 "idempotence_child_run_id": repeated.run_id, "terminal_child_run_id": terminal.run_id,
                 "schedule_activated": False, "daemon_started": False})
    return {"parent_run_id": parent_id, "terminal_child_run_id": terminal.run_id}


def run_demonstrations(rt, manifest_path, instance, *, parent_id, first, last):
    from .definitions import build_definitions
    from dagster import build_sensor_context
    selected = backfill_range(rt.manifest, first, last)
    original = rt.state()
    before = duck.partition_digests(original["database"], rt.manifest, connection_options=rt.duck_options)
    plan = rt.correction_plan(selected)
    corrected = rt.fork("corrected", overlay=plan)
    definitions = build_definitions(corrected, manifest_path, coordinator=False)
    corrected_children = []
    for partition in selected:
        result = run_child(corrected, definitions, instance, "arrival_batch", parent_id=parent_id, partition=partition)
        corrected_children.append(result.run_id)
    after = duck.partition_digests(corrected.state()["database"], corrected.manifest, connection_options=corrected.duck_options)
    changed = verify_changed_partitions(before, after, selected)
    # Both facts must show an actual value change in every corrected partition.
    for partition in selected:
        for table in ("marts.fct_orders", "marts.fct_order_lines"):
            if before[partition][table]["sha256"] == after[partition][table]["sha256"]:
                raise ValueError("correction did not propagate to " + table)
    reference = corrected.full_reference(label="corrected-full-reference")
    parity = corrected.parity(reference, label="corrected-full-incremental-parity")
    corrected.evidence("backfill-proof.json", {"selected": selected, **changed, "before": before, "after": after,
                       "child_run_ids": corrected_children, "corrected_parity_passed": parity["passed"],
                       "global_rollup_change_expected": True,
                       "raw_inputs_changed": False, "arrivals_and_corrections_simulated": True})

    # A dedicated invalid-product overlay must really fail the staging quality gate.
    row = dict(plan["rows"][-1])
    bad_plan = {"encoding": "m4-bad-batch-v1", "simulation": True,
                "scope": "required product foreign key replaced by unknown -1", "rows": [{**row, "new_product_id": -1}]}
    bad_plan["overlay_sha256"] = json_hash(bad_plan)
    bad = rt.fork("bad", overlay=bad_plan)
    bad_defs = build_definitions(bad, manifest_path, coordinator=False)
    failed = run_child(bad, bad_defs, instance, "arrival_batch", parent_id=parent_id,
                       partition=row["batch"], expected_failure=True)
    if bad.state() != original:
        raise ValueError("bad batch changed the promoted pointer")
    forbidden = {"operational_marts", "batch_promoted", "feature_export", "recsys_train_eval", "api_benchmark"}
    materialized = {event.step_key for event in failed.all_events if event.event_type_value == "ASSET_MATERIALIZATION"}
    if materialized & forbidden or not (bad.attempt(failed.run_id) / "failure.json").exists():
        raise ValueError("bad batch was not demonstrably blocked")
    failed_checks = [event for event in failed.all_events if event.event_type_value == "ASSET_CHECK_EVALUATION"
                     and getattr(event.event_specific_data, "passed", None) is False]
    if not failed_checks:
        raise ValueError("bad run has no actual failed Dagster check evaluation")
    try:
        # Explicit negative terminal feature gate; absence alone is not evidence.
        from .gates import require_terminal
        require_terminal(bad.manifest, bad.state(), parity=True, quality=True, original=bad.scenario == "original")
    except ValueError:
        ml_rejected = True
    else:
        raise ValueError("bad branch unexpectedly allowed ML export")
    # Evaluate the stopped sensor manually. Its idempotent alert may already exist.
    alert_sensor = bad_defs.get_sensor_def("local_quality_alert")
    with build_sensor_context(instance=instance) as context:
        tick = alert_sensor.evaluate_tick(context)
        if tick.run_requests:
            raise ValueError("alert sensor unexpectedly requested a run")
        sensor_cursor = context.cursor
    bad.evidence("bad-batch-proof.json", {"failed_run_id": failed.run_id, "run_success": failed.success,
                 "promoted_pointer_unchanged": bad.state() == original, "materialized_steps": sorted(materialized),
                 "downstream_blocked": True, "ml_terminal_gate_rejected": ml_rejected,
                 "failed_check_evaluations": len(failed_checks), "sensor_cursor": sensor_cursor,
                 "sensor_run_requests": 0, "alerts": str(bad.root / "alerts/alerts.jsonl")})

    # Recovery has a fresh owned attempt; invalid DB/logs and alert remain retained.
    recovered = rt.fork("corrected", overlay={"rows": [], "overlay_sha256": json_hash([]), "simulation": True})
    recovery_defs = build_definitions(recovered, manifest_path, coordinator=False)
    recovery = run_child(recovered, recovery_defs, instance, "arrival_batch", parent_id=parent_id, partition=row["batch"])
    recovery_parity = duck.compare_databases(original["database"], recovered.state()["database"], connection_options=recovered.duck_options)
    if not recovery_parity["passed"]:
        raise ValueError("clean recovery differs from original source state")
    bad.evidence("recovery-proof.json", {"failed_run_id": failed.run_id, "recovery_run_id": recovery.run_id,
                 "recovery_branch": str(recovered.root), "failed_artifacts_retained": True,
                 "original_input_parity": recovery_parity})

    materialized_at = original["materialized_at"]
    clock = utc(materialized_at)
    arguments = {"materialized_at": materialized_at,
                 "maximum_age_seconds": rt.config["maximum_processing_age_seconds"],
                 "actual_prefix": original["visible_prefix"], "expected_prefix": [b["key"] for b in rt.manifest["batches"]]}
    fresh = processing_freshness(**arguments, now=clock)
    stale = processing_freshness(**arguments, now=clock + timedelta(seconds=rt.config["maximum_processing_age_seconds"] + 1))
    if not fresh["passed"] or stale["passed"]:
        raise ValueError("controlled processing freshness demo failed")
    rt.evidence("freshness-demo.json", {"fresh": fresh, "stale": stale,
                 "clock_injected": True, "no_real_event_timestamps": True})
    rt.evidence("demonstrations.json", {"backfill_branch": str(corrected.root), "bad_batch_branch": str(bad.root),
                 "recovery_branch": str(recovered.root), "selected_partitions": selected})
    # Only selected Spark partitions are written anew. Unchanged physical files
    # remain referenced from the original immutable Parquet inventory.
    import sys
    sys.path.insert(0, str(rt.repo / "spark"))
    from instacart_job import compare, run
    base = read_json(rt.root / "spark-original/spark-run.json")
    corrected_spark = run(corrected, corrected.root / "spark-backfill", selected=selected, base=base)
    compare(corrected, corrected_spark, corrected.root / "spark-backfill/spark-dbt-parity.json")
    return {"corrected": str(corrected.root), "bad": str(bad.root), "recovered": str(recovered.root)}
