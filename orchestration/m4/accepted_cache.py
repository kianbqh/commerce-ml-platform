"""Read-only identities for accepted terminal features; no engine imports."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path


def sha256(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(8 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def read(path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def definition_hash(repo):
    # Match unchanged recsys.data.definition_hash exactly; no data/SQL edits.
    repo = Path(repo).resolve()
    files = sorted((repo / "warehouse/models/recommendations").glob("*.sql"))
    files += [repo / "recsys/recsys/data.py", repo / "recsys/recsys/features.py"]
    digest = hashlib.sha256()
    for path in files:
        digest.update(path.name.encode())
        digest.update(path.read_bytes())
    return digest.hexdigest()


def database_stamp(database, repo):
    path = Path(database).resolve(strict=True)
    stat = path.stat()
    return {"path": str(path), "bytes": stat.st_size, "mtime_ns": stat.st_mtime_ns,
            "feature_definition_sha256": definition_hash(repo)}


def cache_snapshot(cache):
    """Only this small feature-cache tree, never raw CSV/DB/data trees."""
    root = Path(cache).resolve(strict=True)
    paths = sorted(path for path in root.rglob("*") if path.is_file())
    if not paths or len(paths) > 1024:
        raise ValueError("unexpected terminal feature-cache file count")
    result = {}
    for path in paths:
        if path.is_symlink() or root not in path.resolve().parents:
            raise ValueError("feature-cache member escapes accepted directory")
        before = path.stat()
        digest = sha256(path)
        after = path.stat()
        if (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns):
            raise ValueError("feature-cache member changed during identity read")
        result[path.relative_to(root).as_posix()] = {"bytes": after.st_size, "sha256": digest}
    return result


def make_binding(repo, database, cache, feature_receipt, *, parent=None):
    database, cache = Path(database).resolve(strict=True), Path(cache).resolve(strict=True)
    manifest_path = cache / "manifest.json"
    manifest, receipt = read(manifest_path), read(feature_receipt)
    if receipt.get("only_complete_original_snapshot") is not True:
        raise ValueError("features were not exported from a complete original terminal")
    if receipt.get("feature_manifest_sha256") != sha256(manifest_path):
        raise ValueError("accepted feature receipt/manifest identity mismatch")
    for key in ("split_sha256", "feature_fingerprint"):
        if receipt.get(key) != manifest.get(key):
            raise ValueError("accepted feature receipt differs: " + key)
    binding = {"schema": "m4-accepted-terminal-cache-v1", "database": str(database),
               "feature_cache": str(cache), "database_stamp": database_stamp(database, repo),
               "feature_manifest_sha256": sha256(manifest_path),
               "split_sha256": manifest["split_sha256"],
               "feature_fingerprint": manifest["feature_fingerprint"],
               "cache_snapshot": cache_snapshot(cache), "parent": parent}
    validate_binding(binding, repo)
    return binding


def validate_binding(binding, repo, *, database=None, cache=None, verify_files=False):
    if binding.get("schema") != "m4-accepted-terminal-cache-v1":
        raise ValueError("unrecognized accepted cache binding")
    db = Path(database or binding["database"]).resolve(strict=True)
    root = Path(cache or binding["feature_cache"]).resolve(strict=True)
    if str(db) != binding["database"] or str(root) != binding["feature_cache"]:
        raise ValueError("cache-only arguments differ from accepted resolved paths")
    manifest_path = root / "manifest.json"
    if sha256(manifest_path) != binding["feature_manifest_sha256"]:
        raise ValueError("accepted feature manifest changed")
    manifest = read(manifest_path)
    stamp = database_stamp(db, repo)
    if stamp != binding["database_stamp"] or manifest["database"] != stamp:
        raise ValueError("cache-only database/definition stamp mismatch; export forbidden")
    for key in ("split_sha256", "feature_fingerprint"):
        if manifest.get(key) != binding[key]:
            raise ValueError("accepted feature identity mismatch: " + key)
    if verify_files and cache_snapshot(root) != binding["cache_snapshot"]:
        raise ValueError("accepted feature-cache bytes changed")
    return manifest


def parent_identity(root, repo, *, require_closed=True):
    """Validate retained receipts only; no data replay, dbt or Spark execution."""
    from .gates import require_terminal
    from .replay import validate_manifest
    root = Path(root).resolve(strict=True)
    config, initial = read(root / "config.json"), read(root / "initialization.json")
    manifest = validate_manifest(read(root / "arrival_manifest.json"))
    parity, gate = read(root / "full-incremental-parity.json"), read(root / "terminal/gate.json")
    spark = read(root / "spark-original/spark-dbt-parity.json")
    require_terminal(manifest, read(root / "promoted.json"), parity=parity.get("passed") is True,
                     quality=gate.get("passed") is True, original=initial.get("scenario") == "original")
    if gate.get("original_source_only") is not True or spark.get("passed") is not True:
        raise ValueError("terminal/Spark acceptance is incomplete")
    manual = read(root / "manual-run.json") if require_closed else None
    if require_closed and (manual.get("success") is not True or not manual.get("run_id")):
        raise ValueError("parent full run has not completed successfully")
    if require_closed and (config.get("fixture") or config.get("training_enabled") is not False):
        raise ValueError("append-only child requires the original data-only full parent")
    if require_closed and any(read(root / name).get("status") != "not run" for name in ("model-run.json", "api-run.json")):
        raise ValueError("parent must retain its explicit data-only model/API records")
    names = ["config.json", "initialization.json", "arrival_manifest.json", "promoted.json",
             "provenance.json", "full-incremental-parity.json", "terminal/gate.json",
             "terminal/feature-export.json", "spark-original/spark-dbt-parity.json"]
    if require_closed:
        names += ["manual-run.json", "model-run.json", "api-run.json"]
    identity = {"run_root": str(root), "manual_run_id": manual["run_id"] if manual else None,
                "source_git_sha": read(root / "provenance.json")["git_sha"],
                "receipt_sha256": {name: sha256(root / name) for name in names}}
    return identity, config


def verify_parent_unchanged(identity):
    root = Path(identity["run_root"]).resolve(strict=True)
    if any(sha256(root / name) != digest for name, digest in identity["receipt_sha256"].items()):
        raise ValueError("accepted parent receipt changed")


def prepare_training_binding(rt):
    """Reuse child binding, or bind this one-job fixture's terminal export."""
    from .gates import read_json
    if rt.config.get("cache_only"):
        path = rt.root / "input-binding.json"
        binding = read_json(path)
    else:
        parent, _ = parent_identity(rt.root, rt.repo, require_closed=False)
        binding = make_binding(rt.repo, rt.root / "terminal/warehouse.duckdb",
                               rt.root / "terminal/features", rt.root / "terminal/feature-export.json",
                               parent=parent)
        path = rt.evidence("training-input-binding.json", binding)
    validate_binding(binding, rt.repo, verify_files=True)
    return path, binding
