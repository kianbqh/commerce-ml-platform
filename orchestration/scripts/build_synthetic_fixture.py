"""Generate deterministic toy grocery CSVs for three-batch CI correctness checks.

No dataset, fixture capture or network input is read. The literal arithmetic
follows the existing recommendation tests' synthetic-basket pattern.
"""
from __future__ import annotations

import argparse
import csv
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re


def generate(destination, source_sha):
    if not re.fullmatch(r"[0-9a-f]{40}", source_sha):
        raise ValueError("source-sha must be the caller's actual 40-character checkout SHA")
    root = Path(destination).resolve()
    root.mkdir(parents=True, exist_ok=False)
    counts = {}

    def write(name, header, rows):
        with (root / (name + ".csv")).open("x", encoding="utf-8", newline="") as stream:
            writer = csv.writer(stream, lineterminator="\n")
            writer.writerow(header)
            writer.writerows(rows)
        counts[name] = len(rows)

    write("aisles", ["aisle_id", "aisle"], [[10+i, f"synthetic aisle {i}"] for i in range(4)])
    write("departments", ["department_id", "department"], [[3, "synthetic department 0"], [7, "synthetic department 1"]])
    write("products", ["product_id", "product_name", "aisle_id", "department_id"],
          [[3001+i, f"synthetic grocery {i}", 10+i//6, 3 if i < 12 else 7] for i in range(24)])
    orders, prior, train = [], [], []
    for user in range(8):
        seen = set()
        # Six labeled baskets give exactly three nonempty width-two batches.
        for number in range(1, 7):
            order = 10000 + user*10 + number
            orders.append([order, 1001+user, "train" if number == 6 else "prior",
                           number, number % 7, 10, 0 if number == 1 else 7])
            products = sorted({(user % 4)*6, (user % 4)*6+1,
                               (user+number) % 24, (user+2*number) % 24})
            target = train if number == 6 else prior
            for position, product in enumerate(products, 1):
                target.append([order, 3001+product, position, int(product in seen)])
            seen.update(products)
    write("orders", ["order_id", "user_id", "eval_set", "order_number", "order_dow",
                     "order_hour_of_day", "days_since_prior_order"], orders)
    header = ["order_id", "product_id", "add_to_cart_order", "reordered"]
    write("order_products__prior", header, prior)
    write("order_products__train", header, train)
    files = {p.name: {"bytes": p.stat().st_size,
                     "sha256": hashlib.sha256(p.read_bytes()).hexdigest()}
             for p in sorted(root.glob("*.csv"))}
    identity = {
        "schema": "m4-synthetic-fixture-v1", "data_kind": "synthetic_test",
        "source": "Project-authored deterministic synthetic grocery baskets",
        "attribution": "Generated from literal arithmetic; no Instacart records copied or transformed",
        "generation": "8 toy users, 6 labeled baskets each; product IDs and reordered flags derive only from user/order arithmetic and a local seen set",
        "arrival_metadata": "simulated width-two ordered arrivals; exactly 000000, 000001, 000002",
        "users": [1001+i for i in range(8)], "row_counts": counts, "files": files,
        "source_checkout_sha": source_sha,
        "generator_script": "orchestration/scripts/build_synthetic_fixture.py",
        "generator_script_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "purpose": "synthetic integration correctness only; not historical-data or performance evidence",
        "real_data_inputs_read": False,
    }
    with (root / "attribution.json").open("x", encoding="utf-8", newline="\n") as stream:
        json.dump(identity, stream, indent=2, ensure_ascii=False)
        stream.write("\n")
    return identity


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", required=True, help="new output directory")
    parser.add_argument("--source-sha", required=True, help="actual caller checkout SHA; generator bytes are hashed separately")
    arguments = parser.parse_args()
    record = generate(arguments.out, arguments.source_sha)
    print(json.dumps({"data_kind": record["data_kind"], "row_counts": record["row_counts"],
                      "generator_script_sha256": record["generator_script_sha256"]}, indent=2))
