"""CI/local manual fixture suite. Never downloads data."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import uuid

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from m4.gates import read_json, write_json_new
from m4.provenance import sha256_file


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--fixture", required=True)
    parser.add_argument("--runtime-root", required=True)
    parser.add_argument("--enable-training", action="store_true",
                        help="Opt in to actual three-seed fitting and HTTP in this same fixture commerce_all run")
    args = parser.parse_args()
    fixture, base = Path(args.fixture).resolve(), Path(args.runtime_root).resolve()
    if os.environ.get("GITHUB_ACTIONS"):
        allowed = Path(os.environ["RUNNER_TEMP"]).resolve()
        if allowed != base and allowed not in base.parents:
            raise ValueError("CI writes must stay beneath this runner's temporary directory")
    elif not (str(base).lower().startswith("g:") or str(base).startswith("/mnt/g/") or
              str(base).startswith("/opt/codex-resume/all-modules-v2/m4")):
        raise ValueError("local scratch must stay on the configured G-backed paths")
    identity = read_json(fixture / "attribution.json")
    data_kind = identity.get("data_kind")
    if data_kind not in (None, "synthetic_test"):
        raise ValueError("unsupported fixture data_kind")
    for name, record in identity["files"].items():
        if sha256_file(fixture / name) != record["sha256"]:
            raise ValueError("fixture identity mismatch: " + name)
    config = read_json(Path(__file__).resolve().parents[1] / "config/fixture.json")
    config.update({"runtime_root": str(base), "raw_dir": str(fixture)})
    if data_kind == "synthetic_test":
        config["data_kind"] = "synthetic_test"
    if args.enable_training:
        if identity.get("data_kind") != "synthetic_test":
            raise ValueError("training CI option requires the declared synthetic fixture")
        config["training_enabled"] = True
    base.mkdir(parents=True, exist_ok=True)
    identifier = "fixture-" + uuid.uuid4().hex
    path = write_json_new(base / (identifier + ".config.json"), config)
    env = os.environ.copy()
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    env["TMPDIR"] = str(base)
    command = [sys.executable, str(Path(__file__).with_name("run_manual.py")), "--config", str(path),
               "--run-root", str(base / identifier), "--raw-dir", str(fixture)]
    completed = subprocess.run(command, env=env, check=False)
    if completed.returncode:
        raise SystemExit(completed.returncode)
    manifest = read_json(base / identifier / "arrival_manifest.json")
    if data_kind == "synthetic_test":
        provenance = read_json(base / identifier / "provenance.json")
        if manifest.get("data_kind") != data_kind or provenance.get("data_kind") != data_kind:
            raise ValueError("synthetic fixture label lost at runtime boundary")
    if [b["key"] for b in manifest["batches"]] != ["000000", "000001", "000002"] or any(b["lines"] == 0 for b in manifest["batches"]):
        raise ValueError("fixture did not exercise three nonempty arrival batches")
    model = read_json(base / identifier / "model-run.json")
    api = read_json(base / identifier / "api-run.json")
    if args.enable_training:
        if model["status"] != "completed" or model["seeds"] != [42, 43, 44] or api["status"] != "completed":
            raise ValueError("opt-in fixture did not complete three-seed model/API assets")
        raw = read_json(base / identifier / "api/raw-http.json")
        if raw["requests"] != 20 or raw["successful_responses"] != 20 or raw["errors"] != 0 or raw["warmup_requests"] != 50:
            raise ValueError("opt-in fixture HTTP contract failed")
        coordinator = read_json(base / identifier / "coordinator.json")
        terminal = read_json(base / identifier / "children" / (coordinator["terminal_child_run_id"] + ".json"))
        materialized = [event.get("asset_key") for event in terminal["events"]
                        if event["event_type"] == "ASSET_MATERIALIZATION"]
        if any(materialized.count([key]) != 1 for key in ("recsys_train_eval", "api_benchmark")):
            raise ValueError("actual downstream Dagster materializations are missing/duplicated")
    elif model["status"] != "not run" or api["status"] != "not run":
        raise ValueError("light fixture unexpectedly trained a model/API")
    print(base / identifier)


if __name__ == "__main__":
    main()
