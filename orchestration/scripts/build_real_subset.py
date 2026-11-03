"""Deferred fixture capture from already resident full real CSVs; no downloads.

This reads large sources and must only run after root releases the M4 I/O window.
No arrival/correction metadata is represented as real-world event time.
"""
from __future__ import annotations

import argparse
from collections import Counter
import csv
import json
from pathlib import Path
import subprocess
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from m4.gates import read_json, write_json_new
from m4.provenance import sha256_file, utc_now


def capture(raw, destination, source_manifest, *, users=8):
    raw, destination = Path(raw), Path(destination)
    destination.mkdir(parents=True, exist_ok=False)
    started = time.perf_counter()
    source = read_json(source_manifest)["sources"]
    counts, final_at_six = Counter(), set()
    with (raw / "orders.csv").open(encoding="utf-8", newline="") as stream:
        for row in csv.DictReader(stream):
            if int(row["order_number"]) <= 6 and row["eval_set"] != "test":
                counts[int(row["user_id"])] += 1
                if row["eval_set"] == "train" and int(row["order_number"]) == 6:
                    final_at_six.add(int(row["user_id"]))
    eligible = sorted(u for u, n in counts.items() if n == 6)
    chosen = eligible[:users]
    if not chosen or len(chosen) != users:
        raise ValueError("not enough users with six actual labeled source baskets")
    if not set(chosen) & final_at_six:
        chosen[-1] = min(final_at_six)
        chosen.sort()
    chosen_set = set(chosen)
    order_ids, product_ids, aisle_ids, department_ids = set(), set(), set(), set()
    output_counts = {}
    def subset(name, predicate, record):
        count = 0
        with (raw / (name + ".csv")).open(encoding="utf-8", newline="") as incoming, \
             (destination / (name + ".csv")).open("x", encoding="utf-8", newline="") as outgoing:
            reader = csv.DictReader(incoming)
            writer = csv.DictWriter(outgoing, fieldnames=reader.fieldnames)
            writer.writeheader()
            for row in reader:
                if predicate(row):
                    writer.writerow(row)
                    record(row)
                    count += 1
        output_counts[name] = count
    subset("orders", lambda r: int(r["user_id"]) in chosen_set and int(r["order_number"]) <= 6 and r["eval_set"] != "test",
           lambda r: order_ids.add(int(r["order_id"])))
    for name in ("order_products__prior", "order_products__train"):
        subset(name, lambda r: int(r["order_id"]) in order_ids, lambda r: product_ids.add(int(r["product_id"])))
    def catalog(row):
        aisle_ids.add(int(row["aisle_id"]))
        department_ids.add(int(row["department_id"]))
    subset("products", lambda r: int(r["product_id"]) in product_ids, catalog)
    subset("aisles", lambda r: int(r["aisle_id"]) in aisle_ids, lambda r: None)
    subset("departments", lambda r: int(r["department_id"]) in department_ids, lambda r: None)
    hashes = {p.name: {"bytes": p.stat().st_size, "sha256": sha256_file(p)} for p in sorted(destination.glob("*.csv"))}
    git = subprocess.run(["git", "rev-parse", "HEAD"], text=True, capture_output=True, check=True)
    identity = {"source": "Instacart 2017 real CSV subset", "source_url": "https://huggingface.co/datasets/attik/Instacart-Market-Basket-Analysis",
                "attribution": "Instacart Market Basket Analysis 2017; no synthetic grocery baskets",
                "arrival_metadata": "simulated only; order_number width 2 gives exactly three batches",
                "selection": "smallest eligible user IDs with six labeled baskets, including a source train basket; no random row fabrication",
                "chosen_user_ids": chosen, "row_counts": output_counts, "files": hashes,
                "original_source_manifest_sha256": sha256_file(source_manifest), "original_sources": source,
                "git_sha": git.stdout.strip(), "command": sys.argv, "timestamp_utc": utc_now(),
                "wall_seconds": time.perf_counter()-started, "purpose": "correctness fixture only; not performance/quality evidence"}
    write_json_new(destination / "attribution.json", identity)
    return identity


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--raw-dir", required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("--source-manifest", required=True)
    parser.add_argument("--users", type=int, default=8)
    args = parser.parse_args()
    capture(args.raw_dir, args.out, args.source_manifest, users=args.users)
