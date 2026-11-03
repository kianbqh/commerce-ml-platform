"""Read existing M4 outputs only; never triggers engines, fitting or services."""
from __future__ import annotations

import argparse
import csv
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from m4.gates import read_json, write_json_new
from m4.runtime import Runtime


def summarize(rt):
    coordinator = read_json(rt.root / "coordinator.json")
    timings = []
    for run_id in coordinator["arrival_child_run_ids"]:
        attempt = rt.attempt(run_id)
        receipt = read_json(attempt / "ingest.json")
        row = {"partition": receipt["partition"], "run_id": run_id,
               "setup_wall_seconds": receipt["setup_wall_seconds"]}
        for label in ("dbt-stage", "dbt-input-tests", "dbt-marts"):
            row[label + "_wall_seconds"] = read_json(attempt / label / "process.json")["wall_seconds"]
        row["etl_and_test_wall_seconds"] = sum(row[k] for k in row if k.startswith("dbt-"))
        row["digest_wall_seconds_excluded"] = read_json(attempt / "partition-digests.json")["wall_seconds"]
        row["quality_audit_wall_seconds_excluded"] = read_json(attempt / "output-checks.json")["wall_seconds"]
        timings.append(row)
    full = read_json(rt.root / "full-reference/reference.json")
    parity = read_json(rt.root / "full-incremental-parity.json")
    spark = read_json(rt.root / "spark-original/spark-run.json")
    spark_parity = read_json(rt.root / "spark-original/spark-dbt-parity.json")
    demonstrations = read_json(rt.root / "demonstrations.json")
    corrected = Path(demonstrations["backfill_branch"])
    bad = Path(demonstrations["bad_batch_branch"])
    result = {"provenance": rt.provenance, "arrival_manifest_sha256": rt.manifest["manifest_sha256"],
              "incremental_equals_full": parity["passed"],
              "full_refresh_etl_and_tests_wall_seconds": full["process"]["wall_seconds"],
              "incremental_etl_and_tests_wall_seconds_total": sum(r["etl_and_test_wall_seconds"] for r in timings),
              "per_batch": timings, "timing_protocol": "one declared repetition; setup/digest/export kept separately; idempotence rerun excluded from replay total",
              "backfill": read_json(corrected / "backfill-proof.json"),
              "quality_gate": read_json(bad / "bad-batch-proof.json"),
              "recovery": read_json(bad / "recovery-proof.json"),
              "freshness": read_json(rt.root / "freshness-demo.json"),
              "spark_exact_parity": spark_parity["passed"],
              "spark_etl_wall_seconds": spark["etl_wall_seconds_excluding_physical_hashes"],
              "spark_process_scope_wall_seconds": spark["wall_seconds"],
              "spark_physical_hash_wall_seconds": spark["physical_hash_wall_seconds"],
              "spark_master_actual": spark["master_actual"],
              "spark_tables": {name: {k: value[k] for k in ("rows", "nonempty_partition_count", "physical")}
                               for name, value in spark["tables"].items()},
              "spark_backfill_exact_parity": read_json(corrected / "spark-backfill/spark-dbt-parity.json")["passed"],
              "model": read_json(rt.root / "model-run.json"), "api": read_json(rt.root / "api-run.json"),
              "gcp": {"status": "not run", "reason": "optional; no GCP project/account supplied; no cloud deployment"},
              "remote_ci": {"status": "not run by this reader", "reason": "root must add verified final-commit CI URL"}}
    write_json_new(rt.root / "summary.json", result)
    with (rt.root / "build-timings.csv").open("x", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(timings[0]))
        writer.writeheader()
        writer.writerows(timings)
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-root", required=True)
    args = parser.parse_args()
    summarize(Runtime(args.run_root))
