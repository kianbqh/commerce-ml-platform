"""Real Spark local CSV/Parquet job. Nothing runs on import.

Use the same raw hashes, typed cleaning, simulated arrivals and optional existing-
key corrections as dbt. Integer aggregate targets avoid float-reduction tolerance.
"""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "orchestration"))
from m4 import duck
from m4.gates import read_json, write_json_new
from m4.provenance import sha256_file, utc_now
from m4.runtime import Runtime

TABLES = {"fct_order_lines": "marts.fct_order_lines",
          "batch_department_integer_stats": "orchestration.batch_department_integer_stats",
          "batch_hourly_integer_stats": "orchestration.batch_hourly_integer_stats",
          "batch_product_integer_stats": "orchestration.batch_product_integer_stats"}


def schemas():
    from pyspark.sql.types import DoubleType, IntegerType, LongType, StringType, StructField, StructType
    def schema(fields):
        return StructType([StructField(name, kind(), True) for name, kind in fields])
    return {
        "orders": schema([("order_id", LongType), ("user_id", LongType), ("eval_set", StringType),
                          ("order_number", IntegerType), ("order_dow", IntegerType),
                          ("order_hour_of_day", IntegerType), ("days_since_prior_order", DoubleType)]),
        "lines": schema([("order_id", LongType), ("product_id", LongType),
                         ("add_to_cart_order", IntegerType), ("reordered", IntegerType)]),
        "products": schema([("product_id", LongType), ("product_name", StringType),
                            ("aisle_id", LongType), ("department_id", LongType)]),
        "aisles": schema([("aisle_id", LongType), ("aisle", StringType)]),
        "departments": schema([("department_id", LongType), ("department", StringType)]),
        "overlay": schema([("order_id", LongType), ("product_id", LongType),
                           ("new_reordered", IntegerType), ("new_product_id", LongType)])}


def physical_inventory(root):
    data, metadata = [], []
    for path in sorted(Path(root).rglob("*")):
        if path.is_file():
            record = {"path": str(path.resolve()), "bytes": path.stat().st_size, "sha256": sha256_file(path)}
            (data if path.suffix == ".parquet" else metadata).append(record)
    return {"parquet_files": data, "parquet_file_count": len(data), "parquet_bytes": sum(f["bytes"] for f in data),
            "metadata_files": metadata, "metadata_bytes": sum(f["bytes"] for f in metadata),
            "size_scope": "physical Parquet file bytes; metadata/CRC/SUCCESS tracked separately"}


def assert_base_unchanged(base):
    for inventory in base["tables"].values():
        for record in inventory["physical"]["parquet_files"]:
            path = Path(record["path"])
            if path.stat().st_size != record["bytes"] or sha256_file(path) != record["sha256"]:
                raise ValueError("base Parquet partition changed: " + str(path))


def run(rt, destination, *, selected=None, base=None):
    from pyspark import StorageLevel
    from pyspark.sql import SparkSession, functions as F
    destination = Path(destination).resolve()
    if rt.root not in destination.parents:
        raise ValueError("Spark output/scratch must stay in this unique isolated run")
    destination.mkdir(parents=True, exist_ok=False)
    if selected:
        if base is None:
            raise ValueError("bounded backfill requires original immutable partition inventory")
        selected = sorted(set(selected))
        if any(p not in [b["key"] for b in rt.manifest["batches"]] for p in selected):
            raise ValueError("backfill outside finite partitions")
        assert_base_unchanged(base)
    receipt = rt.state()
    if receipt is None or receipt["visible_prefix"] != [b["key"] for b in rt.manifest["batches"]]:
        raise ValueError("Spark comparison requires a fully promoted received-input snapshot")
    # Recheck real source identities before timing either engine comparison.
    source_started = time.perf_counter()
    for record in rt.manifest["sources"].values():
        if Path(record["path"]).stat().st_size != record["bytes"] or sha256_file(record["path"]) != record["sha256"]:
            raise ValueError("raw data changed after arrival manifest")
    source_seconds = time.perf_counter() - source_started
    scratch = destination / "scratch"
    scratch.mkdir()
    os.environ["SPARK_LOCAL_DIRS"] = str(scratch)
    os.environ["PYSPARK_SUBMIT_ARGS"] = f"--driver-memory {rt.config['spark_driver_memory']} pyspark-shell"
    started_utc, started = utc_now(), time.perf_counter()
    spark = None
    cleaned = None
    physical_hash_seconds = 0.0
    try:
        spark = (SparkSession.builder.master(rt.config["spark_master"]).appName("instacart-manual-arrival-parity")
                 .config("spark.sql.shuffle.partitions", str(rt.config["spark_shuffle_partitions"]))
                 .config("spark.sql.adaptive.enabled", "false").config("spark.sql.ansi.enabled", "true")
                 .config("spark.sql.session.timeZone", "UTC").config("spark.local.dir", str(scratch))
                 .config("spark.sql.warehouse.dir", str(destination / "warehouse-scratch"))
                 .config("spark.jars.ivy", str(destination / "ivy-cache")).getOrCreate())
        schema = schemas()
        def read(name, kind=None):
            return (spark.read.schema(schema[kind or name]).option("header", "true").option("mode", "FAILFAST")
                    .option("encoding", "UTF-8").option("quote", '"').option("escape", '"')
                    .option("multiLine", "true").option("nullValue", "").csv(str(rt.raw / (name + ".csv"))))
        orders, products = read("orders"), read("products")
        for name, frame, grain in (("orders", orders, "order_id"), ("products", products, "product_id")):
            if frame.where(F.col(grain).isNull()).limit(1).count() or frame.groupBy(grain).count().where("count<>1").limit(1).count():
                raise ValueError("null/duplicate " + name + " primary key")
        # All six CSVs participate; catalog integrity is checked rather than dropped.
        aisles, departments = read("aisles"), read("departments")
        if products.join(aisles, "aisle_id", "left_anti").limit(1).count() or products.join(departments, "department_id", "left_anti").limit(1).count():
            raise ValueError("catalog foreign key violation")
        lines = (read("order_products__prior", "lines").withColumn("eval_set", F.lit("prior"))
                 .unionByName(read("order_products__train", "lines").withColumn("eval_set", F.lit("train"))))
        if receipt.get("overlay_path"):
            overlay = (spark.read.schema(schema["overlay"]).option("header", "true").option("mode", "FAILFAST")
                       .csv(receipt["overlay_path"]))
            lines = (lines.alias("l").join(overlay.alias("x"), ["order_id", "product_id"], "left")
                     .select(F.col("order_id"), F.coalesce(F.col("x.new_product_id"), F.col("product_id")).alias("product_id"),
                             F.col("l.add_to_cart_order"), F.coalesce(F.col("x.new_reordered"), F.col("l.reordered")).alias("reordered"),
                             F.col("l.eval_set")))
        if lines.where("order_id is null or product_id is null or reordered is null or add_to_cart_order is null or reordered not in (0,1) or add_to_cart_order<1").limit(1).count():
            raise ValueError("required line key/value is invalid")
        if lines.groupBy("order_id", "product_id").count().where("count<>1").limit(1).count():
            raise ValueError("duplicate line grain")
        if lines.join(orders, "order_id", "left_anti").limit(1).count() or lines.join(products, "product_id", "left_anti").limit(1).count():
            raise ValueError("arrived order/catalog foreign key violation")
        # Match dbt's business column order and types; batch is bookkeeping only.
        cleaned = (lines.alias("l").join(orders.alias("o"), "order_id").join(products.alias("p"), "product_id")
                   .select(F.col("order_id"), F.col("product_id"), F.col("o.user_id"), F.col("o.order_number"),
                           F.col("o.order_dow"), F.col("o.order_hour_of_day"), F.col("p.aisle_id"),
                           F.col("p.department_id"), F.col("l.add_to_cart_order"), F.col("l.reordered"), F.col("l.eval_set"))
                   .withColumn("batch", F.lpad(F.floor((F.col("order_number") - 1) / F.lit(rt.config["orders_per_batch"])).cast("long").cast("string"), 6, "0")))
        if selected:
            cleaned = cleaned.where(F.col("batch").isin(selected))
        cleaned = cleaned.persist(StorageLevel.DISK_ONLY)
        # Integers only: division/rates are derived later using an explicit Decimal contract if needed.
        department = cleaned.groupBy("batch", "department_id").agg(
            F.count("*").cast("long").alias("n_lines"), F.countDistinct("order_id").cast("long").alias("n_orders"),
            F.countDistinct("user_id").cast("long").alias("n_users"), F.sum("reordered").cast("long").alias("n_reordered"),
            F.sum("add_to_cart_order").cast("long").alias("sum_cart_position"))
        hourly = cleaned.groupBy("batch", "order_dow", "order_hour_of_day").agg(
            F.countDistinct("order_id").cast("long").alias("n_orders"), F.count("*").cast("long").alias("n_lines"),
            F.sum("reordered").cast("long").alias("n_reordered")).withColumn("order_dow", F.col("order_dow").cast("long")).withColumn("order_hour_of_day", F.col("order_hour_of_day").cast("long"))
        product = cleaned.groupBy("batch", "product_id").agg(
            F.count("*").cast("long").alias("n_lines"), F.countDistinct("user_id").cast("long").alias("n_buyers"),
            F.sum("reordered").cast("long").alias("n_reordered"))
        frames = {"fct_order_lines": cleaned, "batch_department_integer_stats": department,
                  "batch_hourly_integer_stats": hourly, "batch_product_integer_stats": product}
        outputs = {}
        for name, frame in frames.items():
            table_root = destination / name
            if name != "fct_order_lines":
                frame = frame.persist(StorageLevel.DISK_ONLY)
            counts = {row["batch"]: int(row["count"]) for row in frame.groupBy("batch").count().collect()}
            frame.write.mode("errorifexists").partitionBy("batch").parquet(str(table_root))
            partitions = {batch: str(table_root / ("batch=" + batch) / "*.parquet") for batch in counts}
            all_counts = dict(counts)
            if base:
                partitions = {**base["tables"][name]["partition_globs"], **partitions}
                all_counts = {**base["tables"][name]["all_partition_rows"], **counts}
            hash_started = time.perf_counter()
            physical = physical_inventory(table_root)
            physical_hash_seconds += time.perf_counter() - hash_started
            outputs[name] = {"schema": frame.schema.jsonValue(), "new_partition_rows": counts,
                             "all_partition_rows": all_counts, "rows": sum(all_counts.values()),
                             "nonempty_partition_count": len(partitions),
                             "partition_globs": partitions, "physical": physical}
            if name != "fct_order_lines":
                frame.unpersist()
        effective = dict(spark.sparkContext.getConf().getAll())
        result = {"provenance": rt.provenance, "started_utc": started_utc, "finished_utc": utc_now(),
                  "wall_seconds": time.perf_counter() - started, "source_hash_wall_seconds_excluded": source_seconds,
                  "physical_hash_wall_seconds": physical_hash_seconds,
                  "etl_wall_seconds_excluding_physical_hashes": time.perf_counter()-started-physical_hash_seconds,
                  "timer_scope": "JVM/session startup, CSV parsing, strict checks, joins, disk persistence, aggregates, Parquet writes and physical-file hashing",
                  "spark_version": spark.version, "effective_config": effective,
                  "source_manifest_sha256": rt.manifest["manifest_sha256"], "overlay_sha256": receipt.get("overlay_sha256"),
                  "selected_backfill_partitions": selected, "tables": outputs,
                  "master_actual": spark.sparkContext.master, "default_parallelism_actual": spark.sparkContext.defaultParallelism,
                  "driver_memory_effective": effective.get("spark.driver.memory", "unreported"),
                  "logical_contract": ("synthetic grocery fixture; simulated arrivals; explicit schemas; integer sufficient statistics"
                                       if rt.provenance.get("data_kind") == "synthetic_test" else
                                       "real static Instacart CSVs; ordered simulated arrivals; explicit schemas; integer sufficient statistics")}
        write_json_new(destination / "spark-run.json", result)
        if base:
            assert_base_unchanged(base)
        return result
    except Exception as exc:
        write_json_new(destination / "failure.json", {"provenance": rt.provenance, "error": repr(exc),
                       "wall_seconds": time.perf_counter() - started, "timestamp_utc": utc_now()})
        raise
    finally:
        if cleaned is not None:
            cleaned.unpersist()
        if spark is not None:
            spark.stop()


def compare(rt, output, destination):
    con = duck.connect(":memory:", read_only=False, **rt.duck_options)
    started = time.perf_counter()
    try:
        con.execute(f"attach {duck.quote(rt.state()['database'])} as dbt_reference (read_only)")
        results = {}
        for name, target in TABLES.items():
            globs = list(output["tables"][name]["partition_globs"].values())
            paths_sql = "[" + ",".join(duck.quote(path) for path in globs) + "]"
            relation = f"read_parquet({paths_sql},hive_partitioning=true,hive_types={{'batch':VARCHAR}})"
            target_relation = "dbt_reference." + target
            target_schema = [(r[0], r[1]) for r in con.execute(f"describe select * from {target_relation}").fetchall()]
            source_schema = {r[0]: r[1] for r in con.execute(f"describe select * from {relation}").fetchall()}
            expected = dict(target_schema)
            if name == "fct_order_lines":
                expected["batch"] = "VARCHAR"
            if source_schema != expected:
                raise ValueError(f"Spark/DBT schema mismatch {name}: {source_schema} != {expected}")
            names = ",".join(duck.ident(c[0]) for c in target_schema)
            con.execute(f"create temp view spark_{name} as select {names} from {relation}")
            keys = duck.TABLE_KEYS[target]
            source_hash = duck.table_digest(con, "spark_" + name, keys)
            target_hash = duck.table_digest(con, target_relation, keys)
            left = con.execute(f"select count(*) from ((select {names} from spark_{name}) except all (select {names} from {target_relation}))").fetchone()[0]
            right = con.execute(f"select count(*) from ((select {names} from {target_relation}) except all (select {names} from spark_{name}))").fetchone()[0]
            batch_expression = (f"lpad(cast(cast(floor((order_number-1)/{rt.config['orders_per_batch']}.0) as bigint) as varchar),6,'0')"
                                if name == "fct_order_lines" else "batch")
            partitions = {}
            for batch_record in rt.manifest["batches"]:
                batch = batch_record["key"]
                where = f"{batch_expression}={duck.quote(batch)}"
                partitions[batch] = {"spark": duck.table_digest(con, "spark_" + name, keys, where),
                                     "dbt": duck.table_digest(con, target_relation, keys, where)}
            bad_partition_keys = (con.execute(f"select count(*) from {relation} where batch<>{batch_expression}").fetchone()[0]
                                  if name == "fct_order_lines" else 0)
            passed = source_hash == target_hash and left == right == bad_partition_keys == 0
            passed = passed and all(v["spark"] == v["dbt"] for v in partitions.values())
            results[name] = {"passed": passed, "schema": target_schema, "spark": source_hash, "dbt": target_hash,
                             "spark_except_all": left, "dbt_except_all": right,
                             "invalid_partition_keys": bad_partition_keys, "partitions": partitions}
        result = {"provenance": rt.provenance, "passed": all(r["passed"] for r in results.values()),
                  "tables": results, "wall_seconds": time.perf_counter() - started,
                  "timer_scope": "DuckDB Parquet read-back, schema/EXCEPT ALL/sorted typed-stream hashes; separate from Spark ETL"}
        write_json_new(destination, result)
        if not result["passed"]:
            raise ValueError("exact Spark/dbt parity failed")
        return result
    finally:
        con.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-root", required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("--selected-batches", nargs="+")
    parser.add_argument("--base-spark-manifest")
    args = parser.parse_args()
    rt = Runtime(args.run_root)
    output = run(rt, args.out, selected=args.selected_batches,
                 base=read_json(args.base_spark_manifest) if args.base_spark_manifest else None)
    compare(rt, output, Path(args.out) / "spark-dbt-parity.json")


if __name__ == "__main__":
    main()
