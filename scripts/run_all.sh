#!/usr/bin/env bash
# Download Instacart (~700 MB CSV) -> dbt full refresh (prior) -> incremental (train) -> dbt test
# -> full refresh + test -> query benchmark -> BigQuery portability check -> pytest (fixture data, incl. a negative test)
set -euo pipefail
cd "$(dirname "$0")/.."
export DBT_PROFILES_DIR=$PWD DBT_SEND_ANONYMOUS_USAGE_STATS=false
PY=${PYTHON:-python3}
./scripts/download_data.sh
rm -f -- "${DUCKDB_PATH:-warehouse.duckdb}" "${DUCKDB_PATH:-warehouse.duckdb}.wal"
"$PY" scripts/timed_build.py
"$PY" scripts/benchmark_queries.py
dbt compile -q >/dev/null && "$PY" scripts/portability_check.py
"$PY" -m pytest --capture=sys -q pytests | tee results/pytest.log
