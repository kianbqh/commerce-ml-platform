# Local Spark export and exact read-back

I use PySpark to read the same historical Instacart CSVs as dbt, write cleaned
order lines and integer marts to physical Parquet partitions, then read those
files through DuckDB for an exact comparison.

Local runs use historical baskets under the [Dataset Terms](https://gist.github.com/jeremystan/582eba13d6ee27ed465c43dc78934700).
CI uses independently generated synthetic baskets through the same adapter.
Arrival/correction metadata is simulated in both cases, and Parquet is
physically partitioned by arrival `batch`. Synthetic provenance remains visible
in the result and dashboard; local historical-data receipts stay separate.

## Accepted measurement

The full parent at `829250d93e5e436e98e4a5df41296b0d121a52a8` passed original and corrected exact read-back.
Spark 4.2.0 actually used `local[8]`, parallelism 8 and a `4g` driver.
The source is historical data with simulated arrival/correction metadata.

| Output | Rows | Nonempty / declared batches | Original new files / Parquet bytes | Backfill new files / Parquet bytes |
|---|---:|---:|---:|---:|
| `fct_order_lines` | 33,819,106 | 10 / 10 | 160 / 252,345,948 | 32 / 169,067,940 |
| `batch_department_integer_stats` | 210 | 10 / 10 | 119 / 222,563 | 23 / 43,100 |
| `batch_hourly_integer_stats` | 1,678 | 10 / 10 | 160 / 298,259 | 32 / 59,959 |
| `batch_product_integer_stats` | 310,819 | 10 / 10 | 160 / 2,959,850 | 32 / 985,863 |

All outputs have ten nonempty/declared logical batches. Backfill newly writes
only batches `000000`/`000001`; its composite view retains the other eight
original partitions. All 599 original Parquet files were independently rehashed
during acceptance and stayed unchanged. File-byte totals exclude CRC/SUCCESS
metadata. New backfill file totals do not include retained original files.

| Timer | Original | Corrected selected backfill |
|---|---:|---:|
| Spark processing excluding physical hashes | 150.113704 s | 109.407346 s |
| Physical hashing | 0.204950 s | 0.111704 s |
| Process scope including those hashes | 150.318653 s | 109.519048 s |
| Separate source hashing | 1.866623 s | 1.164528 s |
| DuckDB schema/value/EXCEPT ALL/digest read-back | 1,225.006004 s | 1,188.500498 s |

The exact read-back cost exceeded the Spark processing cost. Selected physical
writes also still validate the complete source; this is not partition-local
input I/O or a distributed-throughput result.

[Summary](../results/orchestration/full-20261103/summary.json) · [aggregate counts](../results/orchestration/full-20261103/aggregate-counts.json) · [parent supplement](../results/orchestration/full-20261103/parent-supplement.json) · [per-batch CSV](../results/orchestration/full-20261103/build-timings.csv) · [delivery report](../results/M4_DELIVERY_REPORT.md).
Spark values are under `summary.json#/spark`; exact comparison records are under
`summary.json#/parity/spark_original` and `spark_corrected`. Published evidence
uses the row-free summary rather than native-path physical manifests.

The separate [synthetic CI run](https://github.com/kianbqh/commerce-ml-platform/actions/runs/41601768167)
passed the same adapter on 181 invented lines. Its timings are not substituted
for these historical-data measurements.

The selected model/API child has also completed without rerunning Spark or
exporting features again. The [combined dashboard](../results/orchestration/full-20261103/dashboard/index.html)
and [browser checks](../results/orchestration/full-20261103/dashboard/browser-validation.json) bind the parent
data work to that separate child. See the [child summary](../results/orchestration/full-20261103/downstream/summary.json)
for model/HTTP values.

## Invocation

Install both pinned requirement sets and configure a Linux JDK 17 or later as
shown in the [orchestration guide](../orchestration/README.md). The manual
orchestration command invokes the original Spark pass and selected backfill.
It also supplies the existing runtime state and corrected-branch identity.
`full-models.json` can continue into fitting/HTTP in the same job; the selected
downstream child reuses accepted terminal inputs and does not invoke Spark.
The combined dashboard uses `--run-root "$PARENT" --downstream-root "$CHILD"`.

To reproduce just the original Spark adapter against an existing valid run,
choose a new output directory:

```bash
python spark/instacart_job.py --run-root "$RUN" --out "$RUN/spark-manual-unique-id"
```

For a selected backfill, use the actual corrected runtime path from
`demonstrations.json` and the original manifest:

```bash
python spark/instacart_job.py \
  --run-root "$CORRECTED_RUN" \
  --out "$CORRECTED_RUN/spark-manual-backfill-unique-id" \
  --selected-batches 000000 000001 \
  --base-spark-manifest "$RUN/spark-original/spark-run.json"
```

These commands do not create an arrival replay or a correction plan. They use the
recorded runtime and active overlay. Each output is create-only.

## Data and parity contract

All six CSVs have explicit schemas and strict parsing. Required keys, duplicate
grains and catalog references are checked before writing. The fact preserves
dbt's business columns and adds `batch` as the Parquet partition key.

| Spark output | DuckDB/dbt target |
|---|---|
| Cleaned order lines | `marts.fct_order_lines` |
| Department integer statistics | `orchestration.batch_department_integer_stats` |
| Hourly integer statistics | `orchestration.batch_hourly_integer_stats` |
| Product integer statistics | `orchestration.batch_product_integer_stats` |

The marts retain BIGINT counts and sums rather than floating averages. Read-back
checks column names/types, row counts, complete keys, both directions of
`EXCEPT ALL`, whole-table typed-stream hashes and hashes for every declared batch,
including empty ones. The fact's Hive partition value is checked against the
batch derived from its order number. Exactness concerns these four outputs;
the orchestration layer has a separate six-table database comparison.

The SQL engine sorts the full relations and streams the digest input in chunks.
The adapter collects bounded aggregate metadata rather than millions of fact
rows into Python. Serialization follows the shared `m4-typed-framed-v1` contract.

## Selected physical backfill

Only selected batch partitions are newly written. A composite manifest combines
the corrected files with references to unchanged original files. Original file
identities are checked before and after the backfill. Corrected Parquet is read
back against the corrected dbt branch. Source validation can still scan the
complete CSVs, so selected writes do not imply partition-local source I/O.

`spark-run.json` retains row counts, nonempty partition counts, partition globs
and the physical file inventory for each output. Report Parquet bytes separately
from CRC/success metadata. Large Parquet files stay outside Git; the small
manifest and comparison receipts are sufficient for the published record.

## Timing and scope

The protocol uses one declared repetition with UTC and monotonic timestamps.
JVM/session startup, parsing, validation, joins, persistence, aggregation and
writes are included in the recorded processing scope. Source hashing, physical
Parquet/metadata hashing and DuckDB read-back comparison have separate timings.
Use the manifest's actual master, parallelism and effective configuration rather
than treating the requested `local[8]` or `local[2]` as an observed resource count.

This is local Spark on a single machine with static input. Fixture results apply
to fixture correctness; full-data timings describe that recorded host/run.
Distributed scaling, production throughput and GCP deployment are outside this
experiment.
