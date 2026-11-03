"""Manual entry point. Requires a fresh root and explicit isolated dependencies."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import shutil
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from m4.gates import write_json_new
from m4.provenance import run_command, utc_now
from m4.runtime import initialize


def prepare_manifest(rt):
    directory = rt.root / "manifest-parse"
    receipt = {"visible_prefix": [b["key"] for b in rt.manifest["batches"]],
               "partition": rt.manifest["batches"][-1]["key"]}
    binary = shutil.which("dbt")
    if not binary:
        raise RuntimeError("isolated pinned dbt unavailable")
    command = [binary, "parse", "--project-dir", str(rt.warehouse), "--profiles-dir", str(rt.warehouse),
               "--target", "duckdb", "--vars", json.dumps(rt.variables(receipt), sort_keys=True),
               "--target-path", str(directory / "dbt-target")]
    run_command(command, cwd=rt.repo, env=rt.environment(rt.root / "parse-unused.duckdb", directory),
                destination=directory, provenance=rt.provenance)
    return directory / "dbt-target/manifest.json"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo", default=str(Path(__file__).resolve().parents[2]))
    parser.add_argument("--config", required=True)
    parser.add_argument("--run-root", required=True)
    parser.add_argument("--raw-dir")
    parser.add_argument("--definitions-only", action="store_true")
    args = parser.parse_args()
    rt = initialize(args.repo, args.config, args.run_root, raw_dir=args.raw_dir)
    manifest = prepare_manifest(rt)
    os.environ["M4_RUN_ROOT"] = str(rt.root)
    os.environ["M4_DBT_MANIFEST"] = str(manifest)
    os.environ["DAGSTER_HOME"] = str(rt.root / "dagster-home")
    Path(os.environ["DAGSTER_HOME"]).mkdir()
    from dagster import DagsterInstance, Definitions
    from m4.definitions import build_definitions
    definitions = build_definitions(rt, manifest)
    Definitions.validate_loadable(definitions)
    rt.evidence("definitions-load.json", {"passed": True, "manifest": str(manifest),
                 "schedules_default_stopped": True, "sensors_default_stopped": True,
                 "daemon_started": False, "materialization_performed": False})
    if args.definitions_only:
        return
    with DagsterInstance.local_temp(tempdir=str(rt.root / "dagster-home")) as instance:
        result = definitions.get_job_def("commerce_all").execute_in_process(instance=instance, raise_on_error=False)
        rt.evidence("manual-run.json", {"run_id": result.run_id, "success": result.success,
                     "run_is_manual": True, "scheduled_jobs_activated": 0})
        if not result.success:
            raise SystemExit(1)


if __name__ == "__main__":
    main()
