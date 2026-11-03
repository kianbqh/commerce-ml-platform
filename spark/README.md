# Local Spark parity

I use PySpark to read the same six real Instacart CSVs, write cleaned order lines as Parquet partitioned by simulated arrival batch and compute department, hourly and product integer marts. DuckDB reads the actual Parquet files back and compares them with dbt.

**Status:** candidate implementation; Java, Spark execution and measured runtime/parity are not run. No performance values are available yet.

Install the candidate pins in `requirements-runtime.txt` in an isolated G-backed WSL environment and verify Java >=17. The orchestration runner invokes this job after successful full/incremental parity. It records actual master, effective configuration, default parallelism, package/hardware provenance, every partition count and physical file inventory. Full config requests `local[8]`; root must reduce it before the run if the shared resource window requires fewer cores.

```bash
python spark/instacart_job.py --run-root "$RUN" --out "$RUN/spark-manual-new"
```

Output is create-only. All six CSVs use explicit schemas and strict parsing. Required keys, duplicate grains and catalog references fail before writing. The cleaned fact retains dbt's business columns; only `batch` is added as a Parquet partition key. The department mart stores line/order/user counts, reorder count and cart-position sum. Hourly stores order/line/reorder counts. Product stores line/buyer/reorder counts. All aggregates use BIGINT. No floating average or hidden approximate comparison is used.

Parity checks explicit types and column names, row counts, unique keys, both directions of `EXCEPT ALL`, canonical SHA-256 hashes of every value and hashes per declared batch, including empty batches. The serializer is shared with the [orchestration contract](../orchestration/README.md). Unicode, nulls, integers and exact binary64 values have explicit encodings. Full facts are sorted by the SQL engine and streamed; millions of rows are not collected into Python. Spark only collects bounded aggregate metadata.

For a correction backfill, pass selected batch keys and the original `spark-run.json`. Only selected Parquet partitions are newly written. A composite manifest refers to unchanged original files and corrected new files. Original files are retained and rehashed before and after. Corrected Parquet is compared with the corrected dbt branch. Source validation still scans full CSVs; selected writes are not a claim of partition-local raw input I/O.

**Protocol:** one declared repetition; monotonic wall times and UTC start/end; source hashing excluded and recorded; physical Parquet/metadata hashing timed separately; JVM startup, checks, joins, aggregation and writes are included in the declared processing scope. Parquet bytes exclude CRC and success metadata, which have a separate inventory. The full data and large Parquet output stay outside Git.

**Limitations:** local Spark on one desktop, static source data, simulated arrival/correction metadata and integer marts. There is no distributed-cluster, production throughput or GCP claim. A fixture is correctness evidence only.

[Apache PySpark 4.2 installation requirements](https://spark.apache.org/docs/4.2.0/api/python/getting_started/install.html) · [Instacart source](https://huggingface.co/datasets/attik/Instacart-Market-Basket-Analysis)
