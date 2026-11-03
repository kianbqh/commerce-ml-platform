# A small result dashboard

I keep a static snapshot of the latest run I choose to inspect: arrival progress, exact incremental/full comparison, build times, quality and processing-freshness checks, Spark Parquet counts, correction recovery, local alerts and the new recommendation results.

The dashboard reads the small JSON/JSONL artifacts already emitted by the orchestration code. It does not run Dagster, dbt, DuckDB, Spark, fitting or the HTTP API. No new dependency or frontend build is needed. The exact file/field mapping is in [SOURCE_SCHEMA.json](SOURCE_SCHEMA.json).

```bash
python orchestration/scripts/build_dashboard.py --run-root "$RUN" --out "$RUN/dashboard-01"
```

Open `dashboard-01/index.html` manually. To refresh, generate another new output folder from the chosen run and open that snapshot. The page's refresh button only reloads the static file. There is no timer, polling, server, daemon or activated schedule. The self-contained page makes no external requests; its only link is the adjacent snapshot JSON.

Missing metrics display **未运行**, with JSON values left null. A missing alert file is not treated as a measured zero. Malformed/oversized inputs show an incomplete view and source diagnostics. No frozen historical number or example data fills a gap. Numbers are displayed without rounding, and source hashes identify the exact bytes read. Reads are bounded and confined to the supplied run directory; failed/corrected branches must also stay within it.

The freshness card shows the recorded check outcome, not a new live SLA evaluation. Controlled clock and invalid-batch demonstrations remain separate from the main run. Recommendation metrics appear only for this run's completed fitting record; disabled fitting stays unrun. Small-subset outputs and synthetic test data have visible labels.

**Status:** source candidate plus pure stdlib verification only. The generated empty preview contains no measured values. The real M4 pipeline is still scheduled last. A live service and native browser rendering have not been run or verified in this preparation.
