"""Fail-closed freshness, promotion and local alert contracts (stdlib only)."""
from __future__ import annotations

from datetime import datetime, timezone
import json
import os
from pathlib import Path
from .replay import json_hash, prefix


def utc(value):
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00")) if isinstance(value, str) else value
    if parsed.tzinfo is None:
        raise ValueError("timezone-aware UTC clock required")
    return parsed.astimezone(timezone.utc)


def processing_freshness(materialized_at, *, now, maximum_age_seconds, actual_prefix, expected_prefix):
    age = (utc(now) - utc(materialized_at)).total_seconds()
    return {"passed": 0 <= age <= maximum_age_seconds and actual_prefix == expected_prefix,
            "age_seconds": age, "maximum_age_seconds": maximum_age_seconds,
            "actual_prefix": actual_prefix, "expected_prefix": expected_prefix,
            "scope": "processing age and simulated replay completeness; not real event freshness"}


def require_checks(checks, expected_names):
    names = [c["name"] for c in checks]
    if len(names) != len(set(names)) or set(names) != set(expected_names):
        raise ValueError("missing, extra or duplicate quality results")
    failed = [c["name"] for c in checks if c.get("passed") is not True]
    if failed:
        raise ValueError("blocking checks failed: " + ",".join(failed))


def require_terminal(manifest, receipt, *, parity, quality, original):
    expected = [b["key"] for b in manifest["batches"]]
    if receipt.get("visible_prefix") != expected or receipt.get("arrival_manifest_sha256") != manifest["manifest_sha256"]:
        raise ValueError("partial or unrelated arrival cannot export ML features")
    if not original or receipt.get("overlay_sha256") or receipt.get("scenario") != "original":
        raise ValueError("corrected/bad-batch branches cannot enter ML data")
    if not parity or not quality or receipt.get("status") != "promoted":
        raise ValueError("ML requires promoted quality/parity success")


def write_json_new(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("x", encoding="utf-8", newline="\n") as stream:
        json.dump(value, stream, indent=2, ensure_ascii=False, allow_nan=False)
        stream.write("\n")
    return path


def read_json(path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def promote(root, candidate, receipt, *, expected_pointer, checks, expected_names):
    """Single writer, immutable candidates; atomic pointer is the only promoted state.

    All candidate DB writers must be closed before this function. Caller retains a
    writer lock for the whole attempt; this compare-and-swap rejects stale runs.
    """
    root, candidate = Path(root).resolve(), Path(candidate).resolve()
    if root not in candidate.parents or not candidate.is_file():
        raise ValueError("candidate must exist within isolated run root")
    require_checks(checks, expected_names)
    pointer = root / "promoted.json"
    before = read_json(pointer) if pointer.exists() else None
    if before != expected_pointer:
        raise ValueError("promotion conflict: prior checkpoint changed")
    if receipt.get("status") != "checked" or receipt.get("database") != str(candidate):
        raise ValueError("only a checked candidate may be promoted")
    promoted = {**receipt, "status": "promoted"}
    immutable = candidate.parent / "promoted-receipt.json"
    write_json_new(immutable, promoted)
    temp = root / ("pointer-" + receipt["attempt_id"] + ".json")
    write_json_new(temp, promoted)
    os.replace(temp, pointer)
    return promoted


def alert_record(failure, *, cursor, now):
    identity = {k: failure.get(k) for k in ("run_id", "check", "partition", "attempt_id", "reason")}
    return {"alert_id": json_hash(identity), "schema": "m4-local-alert-v1", "severity": "ERROR",
            "timestamp_utc": utc(now).isoformat(), "cursor": cursor,
            "delivery": "local JSONL only", "provenance": failure.get("provenance", {"scope": "pure contract fixture"}), **identity}


def emit_local_alert(directory, failure, *, cursor, now, log=print):
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    record = alert_record(failure, cursor=cursor, now=now)
    path = directory / (record["alert_id"] + ".json")
    try:
        write_json_new(path, record)
    except FileExistsError:
        return read_json(path), False
    with (directory / "alerts.jsonl").open("a", encoding="utf-8", newline="\n") as stream:
        stream.write(json.dumps(record, sort_keys=True, ensure_ascii=False) + "\n")
    log(json.dumps(record, sort_keys=True, ensure_ascii=False))
    return record, True
