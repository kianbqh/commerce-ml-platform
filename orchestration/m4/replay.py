"""Pure replay protocol: metadata is simulated; basket contents remain real data."""
from __future__ import annotations

from collections import Counter
from datetime import date, timedelta
import hashlib
import json
from pathlib import Path

ENCODING = "m4-arrivals-v1"


def json_hash(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"),
                                     ensure_ascii=False, allow_nan=False).encode("utf-8")).hexdigest()


def batch_index(order_number, width):
    if isinstance(order_number, bool) or isinstance(width, bool) or int(order_number) != order_number or int(width) != width:
        raise ValueError("order_number and width must be integers")
    if order_number < 1 or width < 1:
        raise ValueError("positive order number and width required")
    return (int(order_number) - 1) // int(width)


def key(index):
    if index < 0 or int(index) != index:
        raise ValueError("nonnegative integer batch required")
    return f"{index:06d}"


def make_manifest(batch_counts, *, width, sources, seed=42, start_day="2026-01-01", delays=None):
    """Counts come from runtime metadata SQL, never invented full-data constants.

    A delayed partition is received late in a simulated daily receipt calendar.
    Exposure still follows the ordered prefix; this is not arbitrary out-of-order CDC.
    No sleeping or real calendar scheduler is involved.
    """
    if width < 1 or not batch_counts:
        raise ValueError("positive width and nonempty metadata counts required")
    delays = delays or {}
    batches = []
    previous_received = None
    for index, counts in sorted(batch_counts.items()):
        if index < 0 or counts["orders"] < 1 or counts["lines"] < 0:
            raise ValueError("invalid batch metadata")
        partition = key(index)
        expected = date.fromisoformat(start_day) + timedelta(days=index)
        delay = int(delays.get(partition, 0))
        if delay < 0:
            raise ValueError("receipt delay cannot be negative")
        received = expected + timedelta(days=delay)
        if previous_received is not None and received <= previous_received:
            received = previous_received + timedelta(days=1)
        batches.append({"key": partition, "index": index, "orders": int(counts["orders"]),
                        "lines": int(counts["lines"]), "scheduled_day_simulated": expected.isoformat(),
                        "received_day_simulated": received.isoformat(),
                        "late_by_days_simulated": (received - expected).days})
        previous_received = received
    value = {"encoding": ENCODING, "source": "Instacart 2017 static CSVs", "orders_per_batch": width,
             "batch_formula": "(order_number-1)//orders_per_batch", "seed": seed,
             "simulation": "arrival calendar and late receipt metadata only; ordered prefix exposure; not production CDC",
             "sources": sources, "batches": batches}
    value["manifest_sha256"] = json_hash(value)
    return value


def validate_manifest(value):
    copied = dict(value)
    digest = copied.pop("manifest_sha256")
    if json_hash(copied) != digest or value["encoding"] != ENCODING:
        raise ValueError("arrival manifest identity mismatch")
    indices = [b["index"] for b in value["batches"]]
    if indices != sorted(set(indices)) or indices != list(range(max(indices) + 1)):
        raise ValueError("ordered contiguous finite partition metadata required")
    if any(b["key"] != key(b["index"]) for b in value["batches"]):
        raise ValueError("partition key mismatch")
    return value


def prefix(manifest, partition):
    validate_manifest(manifest)
    keys = [b["key"] for b in manifest["batches"]]
    if partition not in keys:
        raise ValueError("partition is outside immutable arrival manifest")
    return keys[:keys.index(partition) + 1]


def backfill_range(manifest, first, last):
    keys = [b["key"] for b in validate_manifest(manifest)["batches"]]
    if first not in keys or last not in keys or keys.index(first) > keys.index(last):
        raise ValueError("invalid bounded backfill")
    selected = keys[keys.index(first):keys.index(last) + 1]
    if len(selected) < 2 or any(not b["lines"] for b in manifest["batches"] if b["key"] in selected):
        raise ValueError("correction demo needs at least two nonempty real partitions")
    return selected


def correction_overlay(candidates, selected, *, seed=42):
    """Candidates are existing source lines. Keys never change; raw input is untouched."""
    chosen = {}
    for row in candidates:
        partition = row["batch"]
        if partition not in selected or row["order_number"] <= 1:
            continue
        if row["reordered"] not in (0, 1):
            raise ValueError("invalid reordered flag")
        pair = (row["order_id"], row["product_id"])
        if partition not in chosen or pair < (chosen[partition]["order_id"], chosen[partition]["product_id"]):
            chosen[partition] = dict(row)
    if set(chosen) != set(selected):
        raise ValueError("selected partitions lack an eligible existing-key correction")
    rows = [{"batch": p, "order_id": chosen[p]["order_id"], "product_id": chosen[p]["product_id"],
             "old_reordered": chosen[p]["reordered"], "new_reordered": 1 - chosen[p]["reordered"]}
            for p in selected]
    value = {"encoding": "m4-correction-v1", "seed": seed, "simulation": True,
             "scope": "existing-key reordered-flag flips; no deletes or primary-key edits", "rows": rows}
    value["overlay_sha256"] = json_hash(value)
    return value


def verify_changed_partitions(before, after, selected):
    if set(before) != set(after):
        raise ValueError("partition closure changed")
    changed = sorted(p for p in before if before[p] != after[p])
    if changed != sorted(selected):
        raise ValueError(f"changed partitions {changed} != selected {sorted(selected)}")
    return {"changed": changed, "unchanged": sorted(set(before) - set(changed))}


def expected_prefix_counts(manifest, partition):
    selected = set(prefix(manifest, partition))
    return {name: sum(b[name] for b in manifest["batches"] if b["key"] in selected)
            for name in ("orders", "lines")}


def reference_upsert(existing, incoming, columns):
    """Small-fixture oracle only: reject duplicates before keyed replacement."""
    def indexed(rows):
        output = {}
        for row in rows:
            grain = tuple(row[c] for c in columns)
            if any(v is None for v in grain) or grain in output:
                raise ValueError("null or duplicate unique key")
            output[grain] = dict(row)
        return output
    merged = indexed(existing)
    merged.update(indexed(incoming))
    return [merged[k] for k in sorted(merged)]
