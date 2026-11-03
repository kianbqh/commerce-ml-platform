"""Explicit runtime DuckDB queries. No engine is imported at module load."""
from __future__ import annotations

from pathlib import Path
import re
import time

from .canonical import digest_sorted

TABLE_KEYS = {
    "marts.fct_order_lines": ["order_id", "product_id"],
    "marts.fct_orders": ["order_id"],
    "marts.mart_department_kpis": ["department_id"],
    "orchestration.batch_department_integer_stats": ["batch", "department_id"],
    "orchestration.batch_hourly_integer_stats": ["batch", "order_dow", "order_hour_of_day"],
    "orchestration.batch_product_integer_stats": ["batch", "product_id"],
}


def quote(value):
    return "'" + str(value).replace("'", "''") + "'"


def ident(value):
    return '"' + value.replace('"', '""') + '"'


def connection_options(config, temporary_directory):
    """Use the same declared per-run limits for metadata, audits and read-back."""
    threads, memory = config["dbt_threads"], config["dbt_memory_limit"]
    if isinstance(threads, bool) or not isinstance(threads, int) or threads < 1:
        raise ValueError("dbt_threads must be a positive integer")
    if not isinstance(memory, str) or not re.fullmatch(r"[1-9][0-9]*(?:\.[0-9]+)?\s*(?:KB|MB|GB|TB|KiB|MiB|GiB|TiB)", memory, re.IGNORECASE):
        raise ValueError("dbt_memory_limit must be a positive bounded memory size")
    return {"threads": threads, "memory_limit": memory,
            "temp_directory": str(Path(temporary_directory).resolve())}


def connect(path, read_only=True, *, threads=4, memory_limit="4GB", temp_directory=None):
    # Validate before importing/opening the engine. Quote path/text SQL values.
    options = connection_options({"dbt_threads": threads, "dbt_memory_limit": memory_limit},
                                 temp_directory or Path(path).parent)
    import duckdb
    con = duckdb.connect(str(path), read_only=read_only)
    try:
        con.execute(f"set threads={options['threads']}")
        con.execute(f"set memory_limit={quote(options['memory_limit'])}")
        if temp_directory is not None:
            con.execute(f"set temp_directory={quote(options['temp_directory'])}")
    except Exception:
        con.close()
        raise
    return con


def rows(cursor, chunk_size=8192):
    while True:
        chunk = cursor.fetchmany(chunk_size)
        if not chunk:
            break
        yield from chunk


def table_digest(con, table, keys, where="true"):
    columns = [(r[0], r[1]) for r in con.execute(f"describe select * from {table}").fetchall()]
    fields = ",".join(ident(c[0]) for c in columns)
    order = ",".join(ident(k) for k in keys)
    return digest_sorted(rows(con.execute(f"select {fields} from {table} where {where} order by {order}")), columns, keys)


def compare_databases(left, right, *, connection_options=None):
    started = time.perf_counter()
    con = connect(left, **(connection_options or {}))
    try:
        con.execute(f"attach {quote(Path(right).resolve())} as reference (read_only)")
        result = {}
        for table, keys in TABLE_KEYS.items():
            other = "reference." + table
            a, b = table_digest(con, table, keys), table_digest(con, other, keys)
            if a["columns"] != b["columns"]:
                raise ValueError(f"schema mismatch: {table}")
            names = ",".join(ident(c[0]) for c in a["columns"])
            # EXCEPT ALL catches duplicates; every business column participates.
            l_only = con.execute(f"select count(*) from ((select {names} from {table}) except all (select {names} from {other}))").fetchone()[0]
            r_only = con.execute(f"select count(*) from ((select {names} from {other}) except all (select {names} from {table}))").fetchone()[0]
            key_names = ",".join(ident(k) for k in keys)
            key_diff = con.execute(f"select count(*) from ((select {key_names} from {table}) except (select {key_names} from {other}))").fetchone()[0]
            reverse_key_diff = con.execute(f"select count(*) from ((select {key_names} from {other}) except (select {key_names} from {table}))").fetchone()[0]
            passed = a == b and not (l_only or r_only or key_diff or reverse_key_diff)
            result[table] = {"passed": passed, "incremental": a, "full": b,
                             "left_except_all": l_only, "right_except_all": r_only,
                             "left_only_keys": key_diff, "right_only_keys": reverse_key_diff}
        return {"passed": all(v["passed"] for v in result.values()), "tables": result,
                "wall_seconds": time.perf_counter() - started,
                "timer_scope": "schema/count/key/EXCEPT ALL and canonical sorted stream; excluded from both ETL times"}
    finally:
        con.close()


def partition_digests(database, manifest, *, connection_options=None):
    """Hashes BOTH facts plus integer marts; global rollups are audited separately."""
    con = connect(database, **(connection_options or {}))
    try:
        result = {}
        width = manifest["orders_per_batch"]
        for b in manifest["batches"]:
            values = {}
            for table, keys in TABLE_KEYS.items():
                if table == "marts.mart_department_kpis":
                    continue
                where = (f"cast(floor((order_number-1)/{width}.0) as bigint)={b['index']}"
                         if table.startswith("marts.") else f"batch={quote(b['key'])}")
                values[table] = table_digest(con, table, keys, where)
            result[b["key"]] = values
        return result
    finally:
        con.close()


def audit(database, manifest, partition, *, connection_options=None):
    con = connect(database, **(connection_options or {}))
    try:
        visible = [b for b in manifest["batches"] if b["index"] <= int(partition)]
        width = manifest["orders_per_batch"]
        expected = {b["key"]: b["lines"] for b in visible}
        got = dict(con.execute(f"select lpad(cast(cast(floor((order_number-1)/{width}.0) as bigint) as varchar),6,'0'),count(*) from marts.fct_order_lines group by 1").fetchall())
        got = {k: got.get(k, 0) for k in expected} | {k: v for k, v in got.items() if k not in expected}
        checks = []
        def check(name, value, details):
            checks.append({"name": name, "passed": bool(value), "details": details})
        check("batch_row_reconciliation", got == expected, {"expected": expected, "actual": got})
        nulls = con.execute("select count(*) from marts.fct_order_lines where order_id is null or product_id is null or user_id is null").fetchone()[0]
        check("null_key_rate_zero", nulls == 0, {"null_keys": nulls, "maximum_null_rate": 0})
        duplicates = {}
        for table, keys in TABLE_KEYS.items():
            names = ",".join(ident(k) for k in keys)
            duplicates[table] = con.execute(f"select count(*) from (select {names},count(*) n from {table} group by {names} having n<>1)").fetchone()[0]
        check("unique_grains", not any(duplicates.values()), duplicates)
        fk = con.execute("select count(*) from marts.fct_order_lines l left join staging.stg_orders o using(order_id) left join staging.stg_products p using(product_id) where o.order_id is null or p.product_id is null").fetchone()[0]
        check("arrived_order_catalog_fk", fk == 0, {"invalid_keys": fk})
        closure = con.execute("select count(*) from (select order_id,count(*) basket_size,sum(reordered) n_reordered from marts.fct_order_lines group by 1) l full join marts.fct_orders o using(order_id) where l.order_id is null or o.order_id is null or l.basket_size<>o.basket_size or l.n_reordered<>o.n_reordered").fetchone()[0]
        check("order_line_correction_closure", closure == 0, {"inconsistent_orders": closure})
        orders = con.execute("select count(*) from staging.stg_orders").fetchone()[0]
        check("arrival_completeness", orders == sum(b["orders"] for b in visible), {"orders": orders, "expected": sum(b["orders"] for b in visible)})
        # Validate every business value against arrived staging input at EVERY
        # prefix, not only the final row counts. Raw views stay prefix-filtered.
        con.execute("""create temp view m4_expected_lines as
            select l.order_id,l.product_id,o.user_id,o.order_number,o.order_dow,o.order_hour_of_day,
                   p.aisle_id,p.department_id,l.add_to_cart_order,l.reordered,l.eval_set
            from staging.stg_order_products l join staging.stg_orders o using(order_id)
            join staging.stg_products p using(product_id)""")
        con.execute("""create temp view m4_expected_orders as
            with line_totals as (select order_id,count(*) basket_size,sum(reordered) n_reordered,
                                 count(distinct department_id) n_departments from m4_expected_lines group by 1)
            select o.order_id,o.user_id,o.eval_set,o.order_number,o.order_dow,o.order_hour_of_day,
                   o.days_since_prior_order,o.days_since_first_order,l.basket_size,l.n_reordered,l.n_departments,
                   case when o.order_number=1 then null else cast(l.n_reordered as double)/l.basket_size end reorder_share
            from line_totals l join intermediate.int_orders_timeline o using(order_id)""")
        exact = {}
        for expected_table, fact in (("m4_expected_lines", "marts.fct_order_lines"), ("m4_expected_orders", "marts.fct_orders")):
            keys = TABLE_KEYS[fact]
            source_hash = table_digest(con, expected_table, keys)
            fact_hash = table_digest(con, fact, keys)
            left = con.execute(f"select count(*) from ((select * from {expected_table}) except all (select * from {fact}))").fetchone()[0]
            right = con.execute(f"select count(*) from ((select * from {fact}) except all (select * from {expected_table}))").fetchone()[0]
            exact[fact] = {"expected": source_hash, "actual": fact_hash, "expected_except_all": left,
                           "actual_except_all": right, "passed": source_hash == fact_hash and left == right == 0}
        check("source_fact_exact_values", all(v["passed"] for v in exact.values()), exact)
        return checks
    finally:
        con.close()

AUDIT_NAMES = {"batch_row_reconciliation", "null_key_rate_zero", "unique_grains", "arrived_order_catalog_fk",
               "order_line_correction_closure", "arrival_completeness", "source_fact_exact_values"}
