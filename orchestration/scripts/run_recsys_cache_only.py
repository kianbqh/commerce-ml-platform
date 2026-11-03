"""Run the unchanged recommender with a fail-closed, read-only cache loader."""
from __future__ import annotations

import argparse
from importlib import import_module, metadata
import json
from pathlib import Path
import platform
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "recsys"))
from m4.accepted_cache import read, validate_binding
from m4.gates import write_json_new


def inspect_runtime(destination):
    packages = {}
    for package, module in (("numpy", "numpy"), ("torch", "torch"), ("faiss-cpu", "faiss"),
                            ("lightgbm", "lightgbm"), ("duckdb", "duckdb"), ("fastapi", "fastapi"),
                            ("uvicorn", "uvicorn"), ("httpx", "httpx"), ("scipy", "scipy"), ("pandas", "pandas")):
        imported = import_module(module)
        packages[package] = {"metadata_version": metadata.version(package),
                             "imported_version": str(getattr(imported, "__version__", "unreported")),
                             "module_file": str(getattr(imported, "__file__", "unreported"))}
    write_json_new(destination, {"python": sys.version, "executable": sys.executable,
                   "platform": platform.platform(), "packages": packages,
                   "scope": "actual import identity only; no training/API/data export"})


def run_cached(*, binding_path, database, cache, **kwargs):
    binding = read(binding_path)
    repo = Path(__file__).resolve().parents[2]
    validate_binding(binding, repo, database=database, cache=cache)
    from recsys import pipeline
    from recsys.data import Dataset

    def accepted_only(actual_database, actual_cache, force=False):
        if force:
            raise ValueError("accepted terminal cache cannot be force-exported")
        validate_binding(binding, repo, database=actual_database, cache=actual_cache)
        # The original export() is never called, even on cache miss/stamp drift.
        return Dataset(binding["feature_cache"])

    pipeline.export = accepted_only
    return pipeline.run(database=database, cache=cache, **kwargs)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--inspect-only", action="store_true")
    parser.add_argument("--runtime-receipt")
    parser.add_argument("--binding")
    parser.add_argument("--database")
    parser.add_argument("--cache")
    parser.add_argument("--artifacts")
    parser.add_argument("--out")
    parser.add_argument("--epochs", type=int, default=3)
    parser.add_argument("--rounds", type=int, default=120)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--candidates", type=int, default=100)
    args = parser.parse_args()
    if args.inspect_only:
        if not args.runtime_receipt:
            parser.error("--inspect-only requires --runtime-receipt")
        inspect_runtime(args.runtime_receipt)
        return
    if any(not getattr(args, key) for key in ("binding", "database", "cache", "artifacts", "out")):
        parser.error("cache-only fitting requires binding/database/cache/artifacts/out")
    run_cached(binding_path=args.binding, database=args.database, cache=args.cache,
               artifacts=args.artifacts, out=args.out, epochs=args.epochs, rounds=args.rounds,
               seed=args.seed, candidates=args.candidates)


if __name__ == "__main__":
    main()
