"""Transpile every compiled mart/intermediate model from DuckDB SQL to BigQuery SQL with sqlglot.
Staging models are excluded on purpose: on DuckDB they read CSVs, on BigQuery raw_source() switches to source('raw', ...)."""
import glob, json, sys
from pathlib import Path
import sqlglot

files = sorted(glob.glob("target/compiled/instacart_analytics/models/marts/*.sql") +
               glob.glob("target/compiled/instacart_analytics/models/intermediate/*.sql"))
ok, fail = [], {}
Path("results/bigquery_sql").mkdir(parents=True, exist_ok=True)
for f in files:
    try:
        bq = sqlglot.transpile(open(f).read(), read="duckdb", write="bigquery", pretty=True)[0]
        sqlglot.parse_one(bq, read="bigquery"); ok.append(f.split("/")[-1])
        open(f"results/bigquery_sql/{f.split('/')[-1]}", "w").write(bq + "\n")
    except Exception as e:  # noqa
        fail[f] = str(e)[:300]
res = {"models_checked": len(files), "transpiled_and_reparsed_ok": len(ok), "failures": fail}
print(json.dumps(res, indent=2)); json.dump(res, open("results/portability.json", "w"), indent=2)
if len(files) != 9 or fail:
    sys.exit("Expected all nine compiled intermediate/mart models to transpile and re-parse")
