"""Append-only selected-asset completion against a closed data-only parent."""
from __future__ import annotations

import os
from pathlib import Path
import sys
import time

from .accepted_cache import (cache_snapshot, database_stamp, make_binding, parent_identity, read, sha256,
                             validate_binding, verify_parent_unchanged)
from .gates import write_json_new
from .provenance import collect, utc_now


def validate_config(parent_config, requested):
    expected = {**parent_config, "training_enabled": True}
    if requested != expected:
        raise ValueError("full-models config must retain all full settings except training_enabled=true")
    if requested["training_seeds"] != [42, 43, 44] or requested["http_seed"] != 42:
        raise ValueError("predeclared seeds/HTTP seed changed")


def completion_events(result):
    events = []
    for event in result.all_events:
        data = event.event_specific_data
        key = getattr(data, "asset_key", None)
        if key is None and hasattr(data, "materialization"):
            key = data.materialization.asset_key
        events.append({"event_type": event.event_type_value, "step_key": event.step_key,
                       "message": event.message, "asset_key": list(key.path) if key else None})
    return events


def require_materializations(events):
    keys = [event["asset_key"] for event in events if event["event_type"] == "ASSET_MATERIALIZATION"]
    expected = [["recsys_train_eval"], ["api_benchmark"]]
    if sorted(keys) != sorted(expected):
        raise ValueError("selected child must materialize exactly the two actual downstream assets")


def build_downstream_definitions(rt, manifest_path):
    """Production selected-job builder, also resolved by the actual CI contract."""
    from dagster import AssetSelection, Definitions, define_asset_job
    from .definitions import build_definitions
    base = build_definitions(rt, manifest_path, coordinator=False)
    selected = define_asset_job("recsys_downstream_only",
                                selection=AssetSelection.keys("recsys_train_eval", "api_benchmark"))
    definitions = Definitions.merge(base, Definitions(jobs=[selected]))
    Definitions.validate_loadable(definitions)
    return definitions


def complete(repo, parent_root, config_path, run_root, *, model_python=None):
    started_utc, started = utc_now(), time.perf_counter()
    from .runtime import Runtime
    repo, parent_root, run_root = (Path(path).resolve() for path in (repo, parent_root, run_root))
    parent, parent_config = parent_identity(parent_root, repo)
    config = read(config_path)
    validate_config(parent_config, config)
    allowed = Path(config["runtime_root"]).resolve()
    if allowed not in run_root.parents or parent_root == run_root or parent_root in run_root.parents:
        raise ValueError("child needs a new sibling G-backed runtime directory, outside parent")
    binding = make_binding(repo, parent_root / "terminal/warehouse.duckdb", parent_root / "terminal/features",
                           parent_root / "terminal/feature-export.json", parent=parent)
    for config_key, binding_key in (("frozen_split_sha256", "split_sha256"),
                                    ("frozen_feature_fingerprint", "feature_fingerprint")):
        if config.get(config_key) and config[config_key] != binding[binding_key]:
            raise ValueError("accepted full terminal differs from frozen feature identity")
    manifest_path = parent_root / "manifest-parse/dbt-target/manifest.json"
    if not manifest_path.is_file():
        raise ValueError("accepted parent dbt manifest is required; no parse/replay fallback")
    run_root.mkdir(parents=True, exist_ok=False)
    (run_root / "tmp").mkdir()
    (run_root / "dagster-home").mkdir()
    effective = {**config, "cache_only": True, "model_python": str(model_python or sys.executable)}
    provenance = collect(repo, effective, sys.argv)
    provenance["orchestrator_executable"] = sys.executable
    provenance["parent_data_run"] = parent
    initial = {**read(parent_root / "initialization.json"), "repo": str(repo),
               "scenario": "original", "parent_data_run": parent,
               "scope": "accepted input references only; no data initialization/replay"}
    for name, value in (("config.json", effective), ("provenance.json", provenance),
                         ("initialization.json", initial), ("input-binding.json", binding),
                         ("arrival_manifest.json", read(parent_root / "arrival_manifest.json")),
                         ("promoted.json", read(parent_root / "promoted.json"))):
        write_json_new(run_root / name, value)
    write_json_new(run_root / "inputs-before.json", {"database_stamp": binding["database_stamp"],
                   "cache_snapshot": binding["cache_snapshot"], "parent": parent})
    rt = Runtime(run_root)
    result, error, verified = None, None, False
    try:
        from dagster import DagsterInstance
        definitions = build_downstream_definitions(rt, manifest_path)
        with DagsterInstance.local_temp(tempdir=str(run_root / "dagster-home")) as instance:
            result = definitions.get_job_def("recsys_downstream_only").execute_in_process(
                instance=instance, raise_on_error=False,
                tags={"m4/parent_data_run_id": parent["manual_run_id"],
                      "m4/parent_data_run_root": str(parent_root), "m4/scope": "downstream_only"})
        events = completion_events(result)
        write_json_new(run_root / "downstream-events.json", {"events": events})
        if not result.success:
            raise RuntimeError("selected downstream run failed; original parent retained")
        require_materializations(events)
    except Exception as exc:
        error = repr(exc)
    finally:
        after_cache, after_stamp = None, None
        try:
            after_cache = cache_snapshot(binding["feature_cache"])
            after_stamp = database_stamp(binding["database"], repo)
            validate_binding(binding, repo)
            if after_cache != binding["cache_snapshot"] or after_stamp != binding["database_stamp"]:
                raise ValueError("accepted input bytes/stamp changed during downstream execution")
            verify_parent_unchanged(parent)
            verified = True
        except Exception as exc:
            error = error or repr(exc)
        write_json_new(run_root / "input-verification.json", {"unchanged": verified,
                       "parent": parent, "binding_sha256": sha256(run_root / "input-binding.json"),
                       "database_stamp": after_stamp, "cache_snapshot": after_cache,
                       "verification": "after-run hashes/stamps compared with inputs-before; no rewrite/export fallback",
                       "error": error})
        write_json_new(run_root / "downstream-run.json", {"success": result is not None and result.success and verified and error is None,
                       "run_id": result.run_id if result else None, "parent": parent,
                       "scope": "independent append-only selected Dagster assets; data-only parent unchanged",
                       "selected_assets": ["recsys_train_eval", "api_benchmark"],
                       "config_file": str(Path(config_path).resolve()), "config_file_sha256": sha256(config_path),
                       "started_utc": started_utc, "finished_utc": utc_now(),
                       "wall_seconds": time.perf_counter() - started,
                       "timer_scope": "complete() entry through post-run input verification; excludes CLI startup and final receipt write",
                       "error": error})
    if error:
        raise RuntimeError(error)
    return run_root
