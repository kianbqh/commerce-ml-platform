"""Runs the real dbt project against a tiny hand-made fixture dataset in a temp DuckDB file:
the clean fixture must build and pass all data tests; a corrupted fixture must make the right test fail."""
import csv, json, os, subprocess, tempfile
import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def write_fixture(d, corrupt=False):
    w = lambda name, header, rows: csv.writer(open(os.path.join(d, f"{name}.csv"), "w", newline="")).writerows([header] + rows)
    w("departments", ["department_id", "department"], [[1, "produce"], [2, "dairy eggs"]])
    w("aisles", ["aisle_id", "aisle"], [[10, "fresh fruits"], [20, "milk"]])
    w("products", ["product_id", "product_name", "aisle_id", "department_id"],
      [[100, "Banana", 10, 1], [101, "Strawberries", 10, 1], [200, "Whole Milk", 20, 2]])
    w("orders", ["order_id", "user_id", "eval_set", "order_number", "order_dow", "order_hour_of_day", "days_since_prior_order"],
      [[1, 7, "prior", 1, 0, 9, ""], [2, 7, "prior", 2, 3, 10, 7], [3, 7, "train", 3, 5, 14, 30],
       [4, 8, "prior", 1, 1, 8, ""], [5, 8, "train", 2, 2, 20, 12], [6, 8, "test", 3, 2, 21, 5]])
    first_reordered = 1 if corrupt else 0
    w("order_products__prior", ["order_id", "product_id", "add_to_cart_order", "reordered"],
      [[1, 100, 1, first_reordered], [1, 200, 2, 0], [2, 100, 1, 1], [2, 101, 2, 0], [4, 200, 1, 0]])
    w("order_products__train", ["order_id", "product_id", "add_to_cart_order", "reordered"],
      [[3, 100, 1, 1], [3, 101, 2, 1], [5, 200, 1, 1], [5, 100, 2, 0]])


def dbt(args, d):
    env = dict(os.environ, RAW_DIR=d, DUCKDB_PATH=os.path.join(d, "t.duckdb"), DBT_PROFILES_DIR=ROOT,
               DBT_SEND_ANONYMOUS_USAGE_STATS="false")
    return subprocess.run(["dbt", *args, "--target-path", os.path.join(d, "target")], cwd=ROOT, env=env,
                          capture_output=True, text=True)


def test_clean_fixture_builds_and_passes_all_tests():
    with tempfile.TemporaryDirectory() as d:
        write_fixture(d)
        p = dbt(["build"], d)
        assert p.returncode == 0, p.stdout[-2000:]
        import duckdb
        con = duckdb.connect(os.path.join(d, "t.duckdb"), read_only=True)
        assert con.execute("select count(*) from marts.fct_order_lines").fetchone()[0] == 9
        assert con.execute("select sum(basket_size) from marts.fct_orders").fetchone()[0] == 9
        # user 7: day offsets 0, 7, 37 -> observed span 37
        assert con.execute("select observed_order_history_days from marts.dim_users where user_id = 7").fetchone()[0] == 37
        span_share = dict(con.execute("select span_threshold_days, observed_span_share from marts.mart_user_retention").fetchall())
        assert span_share[7] == 1.0 and span_share[14] == 0.5 and span_share[60] == 0.0


def test_corrupted_fixture_is_caught():
    with tempfile.TemporaryDirectory() as d:
        write_fixture(d, corrupt=True)
        p = dbt(["build"], d)
        assert p.returncode != 0
        assert "assert_first_order_has_no_reorders" in p.stdout and "FAIL" in p.stdout
