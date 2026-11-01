#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
ROOT=$PWD
PYTHON=${PYTHON:-python3}
export PYTHONPATH="$ROOT/recsys:$ROOT/experiments${PYTHONPATH:+:$PYTHONPATH}"
export RAW_DIR=${RAW_DIR:-"$ROOT/warehouse/data/raw"}
export DUCKDB_PATH=${DUCKDB_PATH:-"$ROOT/data/commerce.duckdb"}
export COMMERCE_CACHE=${COMMERCE_CACHE:-"$ROOT/data/features"}
export RECSYS_ARTIFACTS=${RECSYS_ARTIFACTS:-"$ROOT/artifacts"}
export DBT_PROFILES_DIR="$ROOT/warehouse" DBT_SEND_ANONYMOUS_USAGE_STATS=false
export OMP_NUM_THREADS=4 MKL_NUM_THREADS=4 OPENBLAS_NUM_THREADS=4
mkdir -p results "$(dirname "$DUCKDB_PATH")" "$COMMERCE_CACHE" "$RECSYS_ARTIFACTS"
if [[ ${REUSE_FEATURES:-0} != 1 && ! -f "$RAW_DIR/order_products__prior.csv" ]]; then
  bash warehouse/scripts/download_data.sh
fi
if [[ ${REUSE_FEATURES:-0} != 1 ]]; then
  (cd warehouse; dbt build --select path:models/staging path:models/recommendations --indirect-selection cautious) 2>&1 | tee results/dbt-build.log
fi
"$PYTHON" -u -m recsys.pipeline --epochs "${EPOCHS:-3}" --rounds "${ROUNDS:-120}" --out results/metrics.json 2>&1 | tee results/pipeline.log
"$PYTHON" -m pytest --capture=sys -q recsys/tests warehouse/pytests 2>&1 | tee results/tests.log
(cd experiments; PYTHON="${EXPERIMENT_PYTHON:-$PYTHON}" bash scripts/run_all.sh) 2>&1 | tee results/experiments.log
if [[ ${BENCH_API:-1} == 1 ]]; then bash scripts/bench_api.sh; fi
"$PYTHON" scripts/report.py
printf 'COMMERCE_COMPLETE\n'
