# Synthetic grocery fixture for CI

I generate these toy baskets to exercise the complete data pipeline without
redistributing historical order records. The generator uses literal user/order
arithmetic, invented product names and its own purchase-history sets. It does not
read or transform an Instacart file.

The shape follows the existing recommendation tests: eight toy users, six labeled
baskets each, 24 catalog products, four aisles and two departments. Orders 1–5 are
`prior`, order 6 is `train`, and there is no seventh order. Width two therefore
gives three nonempty batches, `000000` through `000002`. Each batch has eligible
existing-key corrections after a user's first order.

From the repository root:

```bash
python orchestration/scripts/build_synthetic_fixture.py --out "$NEW_FIXTURE" --source-sha "$SOURCE_SHA"
python orchestration/scripts/run_fixture.py --fixture "$NEW_FIXTURE" --runtime-root "$NEW_RUNTIME_ROOT"
```

Both paths must be fresh. The second command runs the actual Dagster/dbt/Spark
adapters, corrections, blocking bad-batch check, recovery and terminal feature
boundary; it keeps model fitting and API benchmarking disabled. CI provisions
Python, Java and the combined dependency set before invoking it.

`attribution.json` records `data_kind=synthetic_test`, all six generated CSV byte
lengths/SHA256 values, the generator's own SHA256 and the caller's checkout SHA.
Runtime provenance and arrival manifests carry that label, and the dashboard
shows its synthetic-test banner. Generated artifacts are correctness evidence,
separate from local historical-data results.

The original local real-data capture keeps its unmodified producer attribution
and separately recorded consumer source identity. It is not included here.
Historical Instacart inputs are used locally under the
[Dataset Terms](https://gist.github.com/jeremystan/582eba13d6ee27ed465c43dc78934700).
The [original announcement](https://tech.instacart.com/3-million-instacart-orders-open-sourced-d40d29ead6f2)
describes that release; the toy records generated here are authored by this
project rather than taken from it.
