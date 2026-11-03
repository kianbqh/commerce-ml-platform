"""Explicit finite completion; does not replay arrival/dbt/Spark stages."""
from __future__ import annotations

import argparse
import os
from pathlib import Path
import sys

sys.dont_write_bytecode = True
os.environ.setdefault("PYTHONDONTWRITEBYTECODE", "1")
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from m4.downstream_completion import complete


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo", default=str(Path(__file__).resolve().parents[2]))
    parser.add_argument("--parent-run-root", required=True)
    parser.add_argument("--config", required=True)
    parser.add_argument("--run-root", required=True)
    parser.add_argument("--model-python", help="Existing read-only fit/API Python; orchestration remains in current Dagster environment")
    args = parser.parse_args()
    print(complete(args.repo, args.parent_run_root, args.config, args.run_root, model_python=args.model_python))
