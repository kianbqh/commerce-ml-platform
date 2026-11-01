"""Row counts, analytic query timings (median of 5 warm runs) and KPI snapshots from the built warehouse."""
import json, os, statistics, time
import duckdb

con = duckdb.connect(os.environ.get("DUCKDB_PATH", "warehouse.duckdb"), read_only=True)
con.execute("set threads=4")
con.execute("set memory_limit='4GB'")
tables = ["fct_order_lines", "fct_orders", "dim_products", "dim_users", "mart_department_kpis",
          "mart_hourly_demand", "mart_user_retention", "mart_product_pairs"]
rows = {t: con.execute(f"select count(*) from marts.{t}").fetchone()[0] for t in tables}

Q = {
    "reorder_rate_by_department (33.8M-line scan)":
        "select department_id, avg(reordered) from marts.fct_order_lines group by 1",
    "top_100_products_by_buyers":
        "select product_id, count(distinct user_id) b from marts.fct_order_lines group by 1 order by b desc limit 100",
    "basket_size_by_dow_hour":
        "select order_dow, order_hour_of_day, avg(basket_size) from marts.fct_orders group by 1,2",
    "user_segment_summary":
        "select segment, count(*), avg(avg_basket_size), avg(reorder_rate) from marts.dim_users group by 1",
    "aisle_share_with_window":
        "select aisle_id, n, n*1.0/sum(n) over () from (select aisle_id, count(*) n from marts.fct_order_lines group by 1)",
    "user_product_reorder_counts (feature table)":
        "select count(*) from (select user_id, product_id, count(*) c, sum(reordered) r from marts.fct_order_lines group by 1,2)",
}
timings, samples = {}, {}
for name, sql in Q.items():
    con.execute(sql).fetchall()
    ts = []
    for _ in range(5):
        t = time.perf_counter(); con.execute(sql).fetchall(); ts.append((time.perf_counter() - t) * 1e3)
    timings[name] = round(statistics.median(ts), 1)
    samples[name] = ts

k = {}
k["orders"] = rows["fct_orders"]; k["lines"] = rows["fct_order_lines"]
k["users"] = rows["dim_users"]
k["avg_basket_size"] = round(con.execute("select avg(basket_size) from marts.fct_orders").fetchone()[0], 2)
k["overall_reorder_rate_excl_first_orders"] = round(con.execute(
    "select avg(reordered) from marts.fct_order_lines where order_number > 1").fetchone()[0], 4)
k["top_departments"] = con.execute("select department_name, round(share_of_lines,4), round(reorder_rate,4) from marts.mart_department_kpis order by n_lines desc limit 5").fetchall()
k["observed_span_share"] = con.execute("select span_threshold_days, round(observed_span_share,4) from marts.mart_user_retention order by span_threshold_days").fetchall()
k["peak_hour"] = con.execute("select order_dow, order_hour_of_day, n_orders from marts.mart_hourly_demand order by n_orders desc limit 1").fetchone()
k["top_lift_pairs"] = con.execute("select product_a_name, product_b_name, n_together, round(lift,2) from marts.mart_product_pairs order by lift desc limit 5").fetchall()
k["n_rule_pairs"] = rows["mart_product_pairs"]
out = {"row_counts": rows, "query_ms_median_of_5": timings, "kpis": k,
       "query_ms_samples": samples,
       "query_protocol": {"threads": 4, "memory_limit": "4GB", "warmup_runs": 1, "measured_runs": 5},
       "db_file_mb": round(os.path.getsize(os.environ.get("DUCKDB_PATH", "warehouse.duckdb")) / 2**20, 1)}
print(json.dumps(out, indent=2, default=str)); json.dump(out, open("results/warehouse_stats.json", "w"), indent=2, default=str)
