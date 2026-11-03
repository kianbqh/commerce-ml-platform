"""Runtime-only evidence collection. Importing this module runs no commands."""
from __future__ import annotations

from datetime import datetime, timezone
import hashlib
from importlib import metadata
import json
import os
from pathlib import Path
import platform
import shutil
import subprocess
import sys
import time

from .gates import write_json_new
from .replay import json_hash

PACKAGES = ("dagster", "dagster-dbt", "dbt-core", "dbt-duckdb", "duckdb", "pyspark",
            "torch", "numpy", "faiss-cpu", "lightgbm", "fastapi")


def sha256_file(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(8 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def output_command(command, cwd=None):
    try:
        completed = subprocess.run(command, cwd=cwd, text=True, stdout=subprocess.PIPE,
                                   stderr=subprocess.STDOUT, timeout=20, check=False)
        return {"command": command, "returncode": completed.returncode, "output": completed.stdout}
    except (OSError, subprocess.TimeoutExpired) as exc:
        return {"command": command, "status": "not available", "reason": str(exc)}


def collect(repo, config, command):
    cpu = platform.processor()
    proc = Path("/proc/cpuinfo")
    if proc.exists():
        cpu = next((line.split(":", 1)[1].strip() for line in proc.read_text().splitlines()
                    if line.startswith("model name")), cpu)
    versions = {}
    for name in PACKAGES:
        try:
            versions[name] = metadata.version(name)
        except metadata.PackageNotFoundError:
            versions[name] = "not installed"
    git = output_command(["git", "rev-parse", "HEAD"], cwd=repo)
    if git.get("returncode") != 0:
        raise RuntimeError("real source git SHA is required for runtime evidence")
    return {"git_sha": git["output"].strip(), "config_sha256": json_hash(config),
            "packages": versions, "python": sys.version, "executable": sys.executable,
            "cpu": cpu or "unreported", "logical_cpus": os.cpu_count(),
            "gpu_driver": output_command(["nvidia-smi", "--query-gpu=name,driver_version", "--format=csv,noheader"]),
            "cuda": output_command(["nvcc", "--version"]), "device_scope": "CPU data pipeline; GPU not used",
            "platform": platform.platform(),
            "environment": "WSL2" if "microsoft" in platform.release().lower() else
                           "GitHub-hosted runner" if os.environ.get("GITHUB_ACTIONS") else platform.system(),
            "command": list(command), "timestamp_utc": utc_now(),
            "clock": "perf_counter monotonic for elapsed; aware UTC for timestamps"}


def run_command(command, *, cwd, env, destination, provenance, allow_failure=False):
    """No shell interpolation; create-only logs, retained on every failure."""
    directory = Path(destination)
    directory.mkdir(parents=True, exist_ok=False)
    start_utc, start = utc_now(), time.perf_counter()
    code, error = None, None
    try:
        with (directory / "stdout.log").open("x", encoding="utf-8") as stdout, \
             (directory / "stderr.log").open("x", encoding="utf-8") as stderr:
            completed = subprocess.run(command, cwd=cwd, env=env, stdout=stdout, stderr=stderr, check=False)
            code = completed.returncode
    except Exception as exc:
        error = repr(exc)
    result = {"provenance": provenance, "command": list(command), "cwd": str(cwd),
              "started_utc": start_utc, "finished_utc": utc_now(),
              "wall_seconds": time.perf_counter() - start, "returncode": code, "error": error,
              "timer_includes": "process startup, command execution, stdout/stderr file writes",
              "environment_overrides": {k: env[k] for k in ("RAW_DIR", "DUCKDB_PATH", "DBT_LOG_PATH",
                                       "DBT_TARGET_PATH", "DBT_PROFILES_DIR", "TMPDIR", "SPARK_LOCAL_DIRS") if k in env}}
    write_json_new(directory / "process.json", result)
    if (code != 0 or error) and not allow_failure:
        raise RuntimeError(f"command failed; retained evidence {directory / 'process.json'}")
    return result
