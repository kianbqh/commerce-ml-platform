"""Terminal-only adapters to the unchanged recommendation implementation."""
from __future__ import annotations

import json
import os
from pathlib import Path
import socket
import statistics
import subprocess
import sys
import time
import urllib.request

from .gates import read_json, require_terminal
from .provenance import run_command, sha256_file, utc_now


def export_terminal(rt):
    require_terminal(rt.manifest, rt.state(), parity=read_json(rt.root / "full-incremental-parity.json")["passed"],
                     quality=True, original=rt.scenario == "original")
    sys.path.insert(0, str(rt.repo / "recsys"))
    from recsys.data import export
    started = time.perf_counter()
    ds = export(rt.root / "terminal/warehouse.duckdb", rt.root / "terminal/features")
    manifest = ds.manifest
    for config_key, result_key in (("frozen_split_sha256", "split_sha256"),
                                   ("frozen_feature_fingerprint", "feature_fingerprint")):
        if rt.config.get(config_key) and rt.config[config_key] != manifest[result_key]:
            raise ValueError("unchanged source/split feature identity differs from frozen baseline: " + result_key)
    rt.evidence("terminal/feature-export.json", {"wall_seconds": time.perf_counter() - started,
                "split_sha256": manifest["split_sha256"], "feature_fingerprint": manifest["feature_fingerprint"],
                "feature_manifest": str(rt.root / "terminal/features/manifest.json"),
                "feature_manifest_sha256": sha256_file(rt.root / "terminal/features/manifest.json"),
                "only_complete_original_snapshot": True, "training_only_priors_unchanged": True,
                "rec_order_split_sql_sha256": sha256_file(rt.warehouse / "models/recommendations/rec_order_split.sql")})
    return manifest


def train_eval(rt):
    if not rt.config["training_enabled"]:
        result = {"status": "not run", "reason": "new fitting requires explicit run configuration; M4 data acceptance does not retrain"}
        rt.evidence("model-run.json", result)
        return result
    if rt.config["training_seeds"] != [42, 43, 44] or rt.config["http_seed"] != 42:
        raise ValueError("predeclared seeds or HTTP model selection changed")
    results = []
    for seed in rt.config["training_seeds"]:
        directory = rt.root / "models" / f"seed-{seed}"
        command = [sys.executable, "-m", "recsys.pipeline", "--database", str(rt.root / "terminal/warehouse.duckdb"),
                   "--cache", str(rt.root / "terminal/features"), "--artifacts", str(directory / "artifacts"),
                   "--out", str(directory / "metrics.json"), "--epochs", str(rt.config["training_epochs"]),
                   "--rounds", str(rt.config["ranker_rounds"]), "--seed", str(seed),
                   "--candidates", str(rt.config["candidates"])]
        env = os.environ.copy()
        env["PYTHONPATH"] = str(rt.repo / "recsys")
        env["TMPDIR"] = str(rt.root / "tmp")
        run_command(command, cwd=rt.repo, env=env, destination=directory / "process", provenance=rt.provenance)
        metrics = read_json(directory / "metrics.json")
        if metrics["dataset"]["split_sha256"] != read_json(rt.root / "terminal/features/manifest.json")["split_sha256"]:
            raise ValueError("seed run changed held-out split")
        results.append({"seed": seed, "path": str(directory / "metrics.json"), "metrics": metrics})
    # Store exact individual records. Any public table must copy the produced values.
    summaries = {}
    for method in results[0]["metrics"]["test"]:
        summaries[method] = {}
        for metric in results[0]["metrics"]["test"][method]:
            values = [r["metrics"]["test"][method][metric] for r in results]
            if all(isinstance(v, (int, float)) for v in values):
                summaries[method][metric] = {"mean": statistics.mean(values), "std": statistics.pstdev(values),
                                             "std_definition": "population ddof=0", "values": values}
    result = {"status": "completed", "seeds": [r["seed"] for r in results],
              "per_seed_paths": [r["path"] for r in results], "summary": summaries,
              "http_seed_predeclared": 42}
    rt.evidence("model-run.json", result)
    return result


def benchmark_api(rt):
    model = read_json(rt.root / "model-run.json")
    if model["status"] != "completed":
        result = {"status": "not run", "reason": "new three-seed model path is disabled/not complete"}
        rt.evidence("api-run.json", result)
        return result
    port = int(rt.config["http_port"])
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", port))
    directory = rt.root / "api"
    directory.mkdir(parents=True, exist_ok=False)
    env = os.environ.copy()
    env.update({"PYTHONPATH": str(rt.repo / "recsys"), "COMMERCE_CACHE": str(rt.root / "terminal/features"),
                "RECSYS_ARTIFACTS": str(rt.root / "models/seed-42/artifacts"), "TMPDIR": str(rt.root / "tmp")})
    server_command = [sys.executable, "-m", "uvicorn", "recsys.serve:app", "--host", "127.0.0.1",
                      "--port", str(port), "--workers", "1"]
    owned = None
    started = time.perf_counter()
    try:
        with (directory / "server.stdout.log").open("x", encoding="utf-8") as stdout, \
             (directory / "server.stderr.log").open("x", encoding="utf-8") as stderr:
            owned = subprocess.Popen(server_command, cwd=rt.repo, env=env, stdout=stdout, stderr=stderr)
            deadline = time.monotonic() + 60
            while time.monotonic() < deadline:
                if owned.poll() is not None:
                    raise RuntimeError("owned API exited before readiness")
                try:
                    with urllib.request.urlopen(f"http://127.0.0.1:{port}/health", timeout=1) as response:
                        if response.status == 200:
                            break
                except OSError:
                    time.sleep(0.1)
            else:
                raise RuntimeError("owned API readiness timed out")
            command = [sys.executable, str(rt.repo / "recsys/scripts/bench_api.py"), "--url", f"http://127.0.0.1:{port}",
                       "--cache", str(rt.root / "terminal/features"), "-n", str(rt.config["http_requests"]),
                       "-c", str(rt.config["http_concurrency"]), "-k", "10", "--out", str(directory / "raw-http.json")]
            run_command(command, cwd=rt.repo, env=env, destination=directory / "benchmark-process", provenance=rt.provenance)
    finally:
        if owned is not None and owned.poll() is None:
            owned.terminate()
            try:
                owned.wait(timeout=10)
            except subprocess.TimeoutExpired:
                owned.kill()
                owned.wait(timeout=10)
        rt.evidence("api-lifecycle.json", {"server_command": server_command, "owned_pid": owned.pid if owned else None,
                    "owned_process_terminated": owned is None or owned.poll() is not None,
                    "wall_seconds": time.perf_counter() - started})
    result = {"status": "completed", "raw_http": str(directory / "raw-http.json"), "predeclared_seed": 42,
              "excluded_warmups": 50, "repetitions": 1, "concurrency": rt.config["http_concurrency"],
              "client_timer": "request start through complete response body; existing validator retained",
              "handler_timer": "retrieval, features, ranking and response items; unchanged implementation"}
    rt.evidence("api-run.json", result)
    return result
