# Instacart temporal features

This module turns six Instacart CSVs into the histories the recommender actually consumes. The default platform run builds five staging views and six recommendation tables with dbt/DuckDB. It keeps raw line aggregation inside DuckDB with a four-gigabyte memory limit and four threads.

| Table | Boundary and content |
|---|---|
| `rec_order_split` | Rank only labeled baskets within each user; final basket test, previous basket validation, earlier baskets train |
| `rec_lines` | Item rows joined to that split and static product categories |
| `rec_history_product` | Counts, last observed order number and reorder counts before validation/test |
| `rec_history_user` | Observed order/item/unique-product counts and cutoff order number |
| `rec_history_category` | Per-user aisle and department purchase counts at each cutoff |
| `rec_product_prior` | All products with training-only purchase/buyer/reorder counts |

Validation history excludes its target basket. Test history adds the same user's completed validation basket and excludes the test target. Global product priors and normalizers fit training only. The exported arrays have content hashes and a feature-definition fingerprint covering SQL and export/feature code; a changed feature definition cannot silently reuse an older trained model.

The complete input has 33,819,106 labeled order lines and 3,346,083 labeled orders, plus 75,000 raw orders without supplied labels. It covers 206,209 users and 49,688 products. Actual phase counts, exclusions, split checksums, normalizers and array hashes are in [dataset.json](../results/dataset.json). Per-user order IDs/cutoffs are retained in a local Parquet audit file.

```bash
# From the repository root, after installing the root dependencies:
./scripts/run_all.sh
```

`RAW_DIR`, `DUCKDB_PATH` and `COMMERCE_CACHE` select input files, the private database and exported arrays. A cached rerun can set `REUSE_FEATURES=1` when the warehouse is unchanged. The full data are reused locally; the root runner downloads only when the configured CSVs are absent.

The recorded run uses a native WSL working copy on the G-backed disk. [The working-copy manifest](../results/native_input_manifest.json) verifies every CSV against the original SHA256, and [the dbt audit](../results/dbt-validation.json) records the 11 models and 30 data tests. The original mounted-file read failure and the targeted native recovery are retained in the execution logs.

The earlier analytical fact/dimension/KPI models remain available in `models/marts/`. Their separately measured full/incremental/query results are [archived](../results/before/warehouse/) and are not recommendation features: in particular, full-history `dim_users` and product aggregates do not feed the learned models.

Source: *The Instacart Online Grocery Shopping Dataset 2017*, from the [public CSV mirror](https://huggingface.co/datasets/attik/Instacart-Market-Basket-Analysis). [Input hashes and row counts](../results/before/warehouse/data_manifest.json). The unified CI runs real dbt against hand-made small fixtures, including a corrupt first-order flag, held-out-label perturbation and exact cutoff-value checks.
