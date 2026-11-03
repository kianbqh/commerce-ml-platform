"""Explicit manual runtime adapters. Never imported by pure tests to run engines."""
from __future__ import annotations

import csv
import json
import os
from pathlib import Path
import shutil
import sys
import time
import uuid

from . import duck
from .gates import (emit_local_alert, processing_freshness, promote, read_json, require_checks,
                    require_terminal, write_json_new)
from .provenance import collect, run_command, sha256_file, utc_now
from .replay import (backfill_range, correction_overlay, json_hash, make_manifest, prefix,
                     validate_manifest, verify_changed_partitions)

RAW_NAMES = ("orders", "order_products__prior", "order_products__train", "products", "aisles", "departments")
OPERATIONAL = ["path:models/staging", "path:models/intermediate", "path:models/marts",
               "path:models/orchestration", "path:tests/singular", "tag:m4_input", "tag:m4_output"]


def initialize(repo, config_path, root, *, raw_dir=None):
    """Runtime gate: hashes all six real sources, scans metadata, creates unique run."""
    repo, root = Path(repo).resolve(), Path(root).resolve()
    config = read_json(config_path)
    allowed = Path(config["runtime_root"]).resolve()
    if allowed not in root.parents:
        raise ValueError("new run must be beneath configured isolated G-backed runtime root")
    root.mkdir(parents=True, exist_ok=False)
    (root / "tmp").mkdir()
    audit_options = duck.connection_options(config, root / "tmp")
    raw = Path(raw_dir or config["raw_dir"]).resolve()
    provenance = collect(repo, config, sys.argv)
    if config.get("data_kind") == "synthetic_test":
        provenance["data_kind"] = "synthetic_test"
    started = time.perf_counter()
    sources = {name: {"path": str(raw / (name + ".csv")), "bytes": (raw / (name + ".csv")).stat().st_size,
                      "mtime_ns": (raw / (name + ".csv")).stat().st_mtime_ns,
                      "sha256": sha256_file(raw / (name + ".csv"))} for name in RAW_NAMES}
    source_hash_seconds = time.perf_counter() - started
    con = duck.connect(":memory:", read_only=False, **audit_options)
    try:
        width = int(config["orders_per_batch"])
        con.execute(f"create temp table metadata_orders as select cast(order_id as bigint) order_id, cast(order_number as integer) order_number from read_csv_auto({duck.quote(raw / 'orders.csv')},header=true)")
        nulls, n, unique = con.execute("select count(*) filter(where order_id is null or order_number is null or order_number<1),count(*),count(distinct order_id) from metadata_orders").fetchone()
        if nulls or n != unique:
            raise ValueError("raw order metadata has null/duplicate keys")
        line_sql = " union all ".join(f"select cast(order_id as bigint) order_id,cast(product_id as bigint) product_id from read_csv_auto({duck.quote(raw / (name + '.csv'))},header=true)" for name in RAW_NAMES[1:3])
        invalid = con.execute(f"with lines as ({line_sql}) select count(*) from lines l left join metadata_orders o using(order_id) where l.order_id is null or l.product_id is null or o.order_id is null").fetchone()[0]
        if invalid:
            raise ValueError("raw order lines have null/orphan order keys")
        counts = {int(i): {"orders": int(n), "lines": 0} for i, n in con.execute(f"select cast(floor((order_number-1)/{width}.0) as bigint),count(*) from metadata_orders group by 1").fetchall()}
        for i, n in con.execute(f"with lines as ({line_sql}) select cast(floor((o.order_number-1)/{width}.0) as bigint),count(*) from lines l join metadata_orders o using(order_id) group by 1").fetchall():
            counts[int(i)]["lines"] = int(n)
    finally:
        con.close()
    manifest = make_manifest(counts, width=width, sources=sources, seed=config["seed"],
                             start_day=config["simulated_start_day"], delays=config["simulated_late_receipt_days"],
                             data_kind=config.get("data_kind"))
    validate_manifest(manifest)
    write_json_new(root / "config.json", config)
    write_json_new(root / "arrival_manifest.json", manifest)
    write_json_new(root / "provenance.json", provenance)
    write_json_new(root / "initialization.json", {"provenance": provenance, "source_hash_seconds": source_hash_seconds,
                    "metadata_and_hash_seconds": time.perf_counter() - started, "raw_dir": str(raw),
                    "repo": str(repo), "scenario": "original", "audit_connection_options": audit_options, "manifest_sha256": manifest["manifest_sha256"]})
    return Runtime(root)


class Runtime:
    def __init__(self, root):
        self.root = Path(root).resolve()
        self.config = read_json(self.root / "config.json")
        self.manifest = validate_manifest(read_json(self.root / "arrival_manifest.json"))
        self.provenance = read_json(self.root / "provenance.json")
        self.initial = read_json(self.root / "initialization.json")
        self.repo = Path(self.initial["repo"])
        self.warehouse = self.repo / "warehouse"
        self.raw = Path(self.initial["raw_dir"])
        self.scenario = self.initial["scenario"]
        self.duck_options = duck.connection_options(self.config, self.root / "tmp")

    def evidence(self, relative, data):
        return write_json_new(self.root / relative, {"provenance": self.provenance, **data})

    def state(self):
        path = self.root / "promoted.json"
        return read_json(path) if path.exists() else None

    def attempt(self, run_id):
        return self.root / "attempts" / run_id

    def receipt(self, run_id):
        return read_json(self.attempt(run_id) / "ingest.json")

    def release_lock(self, run_id):
        path = self.root / "writer-lock.json"
        if path.exists() and read_json(path)["run_id"] == run_id:
            path.unlink()

    def fail(self, run_id, partition, check, reason):
        path = self.attempt(run_id) / "failure.json"
        failure = {"provenance": self.provenance, "run_id": run_id, "partition": partition, "attempt_id": run_id,
                   "check": check, "reason": str(reason), "timestamp_utc": utc_now()}
        if not path.exists():
            self.evidence(path.relative_to(self.root), failure)
        emit_local_alert(self.root / "alerts", failure, cursor=run_id, now=utc_now())
        self.release_lock(run_id)

    def ingest(self, partition, run_id):
        keys = prefix(self.manifest, partition)
        prior = self.state()
        if self.scenario == "original":
            previous_keys = prior["visible_prefix"] if prior else []
            if previous_keys != keys[:-1] and previous_keys != keys:
                raise ValueError("arrival jobs must execute serially in manifest order; only current partition may repeat")
        elif prior is None:
            raise ValueError("demonstration branches must fork a completed original snapshot")
        else:
            keys = prior["visible_prefix"]
        write_json_new(self.root / "writer-lock.json", {"run_id": run_id, "pid": os.getpid()})
        directory = self.attempt(run_id)
        directory.mkdir(parents=True, exist_ok=False)
        candidate = directory / "candidate.duckdb"
        setup_start = time.perf_counter()
        if prior:
            shutil.copyfile(prior["database"], candidate)
        write_json_new(directory / "prior.json", prior)
        applied = list(prior.get("applied_correction_batches", [])) if prior else []
        overlay_path, overlay_sha = "", None
        if self.scenario in {"corrected", "bad"}:
            plan = read_json(self.root / "overlay-plan.json")
            if partition not in applied:
                applied.append(partition)
            active = [row for row in plan["rows"] if self.scenario == "bad" or row["batch"] in applied]
            overlay_path = str(directory / "active-overlay.csv")
            with Path(overlay_path).open("x", encoding="utf-8", newline="") as stream:
                writer = csv.writer(stream)
                writer.writerow(["order_id", "product_id", "new_reordered", "new_product_id"])
                for row in active:
                    writer.writerow([row["order_id"], row["product_id"], row.get("new_reordered", ""), row.get("new_product_id", "")])
            overlay_sha = sha256_file(overlay_path)
        receipt = {"provenance": self.provenance, "attempt_id": run_id, "dagster_run_id": run_id,
                   "partition": partition, "visible_prefix": keys, "scenario": self.scenario,
                   "applied_correction_batches": sorted(applied), "database": str(candidate),
                   "arrival_manifest_sha256": self.manifest["manifest_sha256"],
                   "source_sha256": {k: v["sha256"] for k, v in self.manifest["sources"].items()},
                   "overlay_path": overlay_path, "overlay_sha256": overlay_sha,
                   "overlay_plan_sha256": read_json(self.root / "overlay-plan.json").get("overlay_sha256") if overlay_path else None,
                   "prior_receipt_sha256": json_hash(prior) if prior else None,
                   "setup_wall_seconds": time.perf_counter() - setup_start,
                   "status": "ingested", "timestamp_utc": utc_now()}
        write_json_new(directory / "ingest.json", receipt)
        return receipt

    def variables(self, receipt):
        return {"m4_enabled": True, "m4_prefix": int(receipt["visible_prefix"][-1]),
                "m4_orders_per_batch": self.config["orders_per_batch"],
                "m4_affected_batches": [int(receipt["partition"])],
                "m4_overlay_path": receipt.get("overlay_path", ""), "eval_sets": ["prior", "train"]}

    def environment(self, database, artifact_root):
        environment = os.environ.copy()
        environment.update({"RAW_DIR": str(self.raw), "DUCKDB_PATH": str(database),
                            "DBT_PROFILES_DIR": str(self.warehouse), "DBT_LOG_PATH": str(artifact_root / "dbt-logs"),
                            "DBT_TARGET_PATH": str(artifact_root / "dbt-target"),
                            "DBT_SEND_ANONYMOUS_USAGE_STATS": "false", "DO_NOT_TRACK": "1",
                            "DBT_MEMORY_LIMIT": self.config["dbt_memory_limit"],
                            "TMPDIR": str(self.root / "tmp")})
        return environment

    def dbt(self, receipt, label, command, selection, *, full_refresh=False, allow_failure=False, indirect_selection=None):
        for source in self.manifest["sources"].values():
            stat = Path(source["path"]).stat()
            if stat.st_size != source["bytes"] or stat.st_mtime_ns != source["mtime_ns"]:
                raise ValueError("raw source changed after immutable manifest")
        directory = Path(receipt["database"]).parent / label
        binary = shutil.which("dbt")
        if not binary:
            raise RuntimeError("isolated pinned dbt is unavailable")
        argv = [binary, command, "--project-dir", str(self.warehouse), "--profiles-dir", str(self.warehouse),
                "--target", "duckdb", "--threads", str(self.config["dbt_threads"]),
                "--vars", json.dumps(self.variables(receipt), sort_keys=True), "--select", *selection,
                "--target-path", str(directory / "dbt-target")]
        if indirect_selection is not None:
            argv.extend(["--indirect-selection", indirect_selection])
        if full_refresh:
            argv.append("--full-refresh")
        return run_command(argv, cwd=self.repo, env=self.environment(receipt["database"], directory),
                           destination=directory, provenance=self.provenance, allow_failure=allow_failure)

    def stage(self, run_id):
        receipt = self.receipt(run_id)
        return self.dbt(receipt, "dbt-stage", "run", ["path:models/staging", "path:models/intermediate"])

    def input_checks(self, run_id):
        receipt = self.receipt(run_id)
        result = self.dbt(receipt, "dbt-input-tests", "test", ["path:models/staging", "tag:m4_input"], allow_failure=True, indirect_selection="cautious")
        passed = result["returncode"] == 0
        self.evidence(f"attempts/{run_id}/input-check.json", {"name": "dbt_input_quality", "passed": passed,
                                                           "process_evidence": "dbt-input-tests/process.json"})
        if not passed:
            self.fail(run_id, receipt["partition"], "dbt_input_quality", "dbt stage/input tests failed; no mart promotion")
        return passed

    def marts(self, run_id):
        receipt = self.receipt(run_id)
        result = self.dbt(receipt, "dbt-marts", "build", ["path:models/marts", "path:models/orchestration",
                                                        "path:tests/singular", "tag:m4_output"])
        self.evidence(f"attempts/{run_id}/materialization.json", {"materialized_at": utc_now(), "dbt_process": result})
        return result

    def output_checks(self, run_id, *, now=None):
        started = time.perf_counter()
        receipt = self.receipt(run_id)
        checks = duck.audit(receipt["database"], self.manifest, receipt["visible_prefix"][-1], connection_options=self.duck_options)
        materialized = read_json(self.attempt(run_id) / "materialization.json")["materialized_at"]
        freshness = processing_freshness(materialized, now=now or utc_now(),
                    maximum_age_seconds=self.config["maximum_processing_age_seconds"],
                    actual_prefix=receipt["visible_prefix"], expected_prefix=prefix(self.manifest, receipt["visible_prefix"][-1]))
        checks += [{"name": "processing_freshness", **freshness}]
        self.evidence(f"attempts/{run_id}/output-checks.json", {"checks": checks,
                      "wall_seconds": time.perf_counter()-started,
                      "timer_scope": "source/fact exact value hashes, EXCEPT ALL, grains, FK, counts and processing freshness; separate from dbt ETL"})
        try:
            require_checks(checks, duck.AUDIT_NAMES | {"processing_freshness"})
        except ValueError as exc:
            self.fail(run_id, receipt["partition"], "output_quality", exc)
            return False
        return True

    def promote(self, run_id):
        receipt = self.receipt(run_id)
        checks = read_json(self.attempt(run_id) / "output-checks.json")["checks"]
        if not read_json(self.attempt(run_id) / "input-check.json")["passed"]:
            raise ValueError("input tests did not pass")
        # Every batch audits full/per-batch schema, counts, values and keys.
        digest_started = time.perf_counter()
        digests = duck.partition_digests(receipt["database"], self.manifest, connection_options=self.duck_options)
        self.evidence(f"attempts/{run_id}/partition-digests.json", {"partitions": digests,
                      "wall_seconds": time.perf_counter() - digest_started, "timer_scope": "separate audit; not ETL"})
        updated = {**receipt, "status": "checked", "materialized_at": read_json(self.attempt(run_id) / "materialization.json")["materialized_at"]}
        promoted = promote(self.root, receipt["database"], updated,
                           expected_pointer=read_json(self.attempt(run_id) / "prior.json"),
                           checks=checks, expected_names=duck.AUDIT_NAMES | {"processing_freshness"})
        self.release_lock(run_id)
        return promoted

    def full_reference(self, *, label="full-reference"):
        promoted = self.state()
        if promoted is None or promoted["visible_prefix"] != [b["key"] for b in self.manifest["batches"]]:
            raise ValueError("full reference requires complete received input")
        directory = self.root / label
        directory.mkdir(parents=True, exist_ok=False)
        receipt = {**promoted, "database": str(directory / "warehouse.duckdb")}
        process = self.dbt(receipt, "dbt-full", "build", OPERATIONAL, full_refresh=True)
        checks = duck.audit(receipt["database"], self.manifest, receipt["visible_prefix"][-1], connection_options=self.duck_options)
        require_checks(checks, duck.AUDIT_NAMES)
        self.evidence(f"{label}/reference.json", {"database": receipt["database"], "overlay_sha256": receipt["overlay_sha256"],
                      "source_sha256": receipt["source_sha256"], "checks": checks, "process": process,
                      "comparison_input": "exact same latest received source and active correction overlay as incremental"})
        return receipt["database"]

    def parity(self, reference, *, label="full-incremental-parity"):
        result = duck.compare_databases(self.state()["database"], reference, connection_options=self.duck_options)
        self.evidence(label + ".json", result)
        if not result["passed"]:
            raise ValueError("exact full/incremental parity failed; feature export blocked")
        return result

    def fork(self, scenario, *, overlay):
        if scenario not in {"corrected", "bad"}:
            raise ValueError("unsupported isolated demonstration")
        target = self.root / "demonstrations" / (scenario + "-" + uuid.uuid4().hex)
        target.mkdir(parents=True, exist_ok=False)
        for name in ("config.json", "arrival_manifest.json", "provenance.json"):
            shutil.copyfile(self.root / name, target / name)
        write_json_new(target / "initialization.json", {**self.initial, "scenario": scenario, "forked_from": str(self.root)})
        write_json_new(target / "promoted.json", self.state())
        write_json_new(target / "overlay-plan.json", overlay)
        (target / "tmp").mkdir()
        return Runtime(target)

    def correction_plan(self, selected):
        con = duck.connect(self.state()["database"], **self.duck_options)
        try:
            width = self.manifest["orders_per_batch"]
            partitions = ",".join(duck.quote(k) for k in selected)
            cursor = con.execute(f"with candidates as (select lpad(cast(cast(floor((order_number-1)/{width}.0) as bigint) as varchar),6,'0') batch,order_id,product_id,order_number,reordered from marts.fct_order_lines where order_number>1), numbered as (select *,row_number() over(partition by batch order by order_id,product_id) rn from candidates where batch in ({partitions})) select batch,order_id,product_id,order_number,reordered from numbered where rn=1")
            names = [d[0] for d in cursor.description]
            candidates = [dict(zip(names, row)) for row in cursor.fetchall()]
        finally:
            con.close()
        return correction_overlay(candidates, selected, seed=self.config["seed"])

    def terminal_gate(self):
        receipt = self.state()
        result = read_json(self.root / "full-incremental-parity.json")
        require_terminal(self.manifest, receipt, parity=result["passed"], quality=True,
                         original=self.scenario == "original")
        freshness = processing_freshness(receipt["materialized_at"], now=utc_now(),
                         maximum_age_seconds=self.config["maximum_processing_age_seconds"],
                         actual_prefix=receipt["visible_prefix"], expected_prefix=[b["key"] for b in self.manifest["batches"]])
        if not freshness["passed"]:
            raise ValueError("terminal processing freshness failed")
        directory = self.root / "terminal"
        directory.mkdir(parents=True, exist_ok=False)
        shutil.copyfile(receipt["database"], directory / "warehouse.duckdb")
        self.evidence("terminal/gate.json", {"passed": True, "freshness": freshness,
                      "original_source_only": True, "partial_recommendation_materializations": 0})
        return directory / "warehouse.duckdb"
