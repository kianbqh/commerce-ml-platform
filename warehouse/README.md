# analytics-dbt-warehouse

I like the point where a pile of retail CSVs becomes a few tables that answer useful questions. This project builds an Instacart warehouse with dbt and DuckDB, then checks the data contracts, adds a second batch incrementally, and measures a handful of analytical queries.

The executed warehouse contains **33,819,106 order lines**, **3,346,083 orders** and **206,209 users**. Its 14 models form a staging → intermediate → marts pipeline, with two fact tables, two dimensions and four KPI marts.

## What is modeled

| Layer | Models and purpose |
|---|---|
| Staging | Orders, prior/train order lines, products, aisles and departments |
| Intermediate | Per-user day offsets reconstructed from `days_since_prior_order` |
| Facts | Incremental `fct_order_lines` and `fct_orders` |
| Dimensions | Product popularity and user order-history summaries |
| Marts | Department KPIs, encoded day/hour demand, observed order-history span shares, and product-pair support/confidence/lift |

The incremental load appends new orders through anti-joins; dimensions and KPI marts rebuild. Product pairs use products with popularity rank ≤1,000 and at least 200 shared orders.

There are 52 data tests: uniqueness, nulls, relationships, accepted values, ranges, composite keys and reconciliations. The latter check basket totals, join row counts, first-order reorder flags, department shares and monotonic span shares.

## Local results

Measured on 31 October 2026 with an i7-12700KF, Ubuntu 24.04 / WSL2 and Python 3.12.3. dbt uses four threads; DuckDB has a 4 GB memory limit. CSVs are read from the G: mount and the database is on the G-backed native WSL disk. [Environment and versions](results/runtime_environment.json).

**Build lifecycle** — elapsed time per command, including dbt startup and SQL execution. All model runs completed 14/14; both full-data test passes completed 52/52.

| Step | Wall time | Result |
|---|---|---|
| Full refresh, prior only: 32,434,489 lines | 22.45 s | 14 models |
| Incremental addition: 1,384,617 train lines | 19.06 s | 14 models |
| Test after incremental load | 14.28 s | 52 / 52 |
| Full refresh, prior + train | 23.74 s | 14 models |
| Test after full refresh | 43.24 s | 52 / 52 |

The order-line fact's dbt-reported execution time was 3.22 s for the incremental addition and 7.84 s for the full refresh. [Command and model timings](results/build_timings.json) · [validation](results/validation.json).

**Queries** — one warm-up followed by five measured execute-and-fetch runs per query, using four threads and the same 4 GB limit. The table shows the median; all five samples are saved.

| Query | Median |
|---|---|
| Reorder rate by department, 33.8M-line scan | 33.3 ms |
| Top 100 products by distinct buyers | 360.5 ms |
| Basket size by encoded day of week and hour | 4.3 ms |
| User-segment summary | 1.7 ms |
| Aisle shares with a window function | 25.4 ms |
| User × product reorder-count groups | 300.8 ms |

[Raw query samples, row counts and KPI snapshot](results/warehouse_stats.json). The database file was 702.5 MiB.

A few results I found interesting:

- Average basket size is 10.11 items; 62.87% of lines after a user's first order are reorders.
- Produce accounts for 29.24% of lines and dairy/eggs for 16.65%.
- The busiest encoded time slot is `order_dow=1`, hour 10, with 54,719 orders.
- The pair mart contains 81,761 pairs. Its largest lift is 237.01 for two toddler yogurt products, observed together in 2,883 orders.

## Reading the order-history span metric

`mart_user_retention` keeps its original model name. Its columns describe **observed order-history span**: `span_threshold_days`, `n_users`, `n_users_with_span_ge_n` and `observed_span_share`.

For each user, `observed_order_history_days` is the maximum reconstructed day offset among their orders with supplied item rows. A threshold N reports the fraction of users whose span is at least N. For example, the 30-day threshold has an observed span share of 96.41%, and the 90-day threshold 71.17%.

Instacart supplies relative gaps capped at 30 days and no calendar dates, so these reconstructed spans are lower bounds on elapsed spans. This is an empirical span distribution; it does not count a return event on day N or adjust for observation censoring. The held-out test orders have no supplied item rows and are outside the fact tables.

## Fixture and SQL checks

Both pytest cases passed. The clean fixture builds the real 14-model project and checks nine lines across six raw orders, basket totals, a hand-computed 37-day span, and the 7/14/60-day span shares. The second case changes a first-order line to `reordered=1` and confirms that `assert_first_order_has_no_reorders` rejects the build. [Pytest output](results/pytest.log).

The executed engine is DuckDB 2.0.0. A separate syntax check transpiled all **nine intermediate/mart models** to BigQuery SQL with SQLGlot 30.27.0 and parsed all nine successfully. The exporter emitted `NULLS LAST` warnings, recorded in the execution log. Staging has an adapter-specific CSV/source macro. [Check results](results/portability.json) · [generated BigQuery SQL](results/bigquery_sql). BigQuery execution is outside this local run.

## Run

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements-lock.txt
bash scripts/run_all.sh
```

The runner downloads six CSVs, performs the full/incremental lifecycle, measures queries, checks SQL transpilation and runs the two fixture cases. Downloads use Python's standard library and an atomic temporary file. [Downloaded file sizes, row counts and SHA-256 hashes](results/data_manifest.json).

`DUCKDB_PATH` selects the database file, `TMPDIR` selects fixture scratch space, and `RAW_DIR` can point dbt at another copy of the CSVs. GitHub Actions runs the clean/corrupted fixture checks with the locked dependencies.

```bash
export DBT_PROFILES_DIR="$PWD"
dbt build
dbt docs generate
dbt docs serve
```

Data: *The Instacart Online Grocery Shopping Dataset 2017*, downloaded from the [attik/Instacart-Market-Basket-Analysis mirror](https://huggingface.co/datasets/attik/Instacart-Market-Basket-Analysis). Source URLs and hashes are recorded in the data manifest.
