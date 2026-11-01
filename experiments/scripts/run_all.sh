#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
if [[ -f .venv/bin/activate ]]; then source .venv/bin/activate; fi
PY=${PYTHON:-python3}
export OMP_NUM_THREADS=${OMP_NUM_THREADS:-2} OPENBLAS_NUM_THREADS=${OPENBLAS_NUM_THREADS:-2} MKL_NUM_THREADS=${MKL_NUM_THREADS:-2}
export XDG_CACHE_HOME="$PWD/.cache" TMPDIR=${TMPDIR:-"$PWD/.tmp"} MPLCONFIGDIR="$PWD/.cache/matplotlib"
export JUPYTER_CONFIG_DIR="$PWD/.cache/jupyter/config" JUPYTER_DATA_DIR="$PWD/.cache/jupyter/data"
export JUPYTER_RUNTIME_DIR=${JUPYTER_RUNTIME_DIR:-"$TMPDIR/jupyter"} IPYTHONDIR="$PWD/.cache/ipython"
mkdir -p "$TMPDIR" "$MPLCONFIGDIR" "$JUPYTER_CONFIG_DIR" "$JUPYTER_DATA_DIR" "$JUPYTER_RUNTIME_DIR" "$IPYTHONDIR"
"$PY" -m pytest -q --capture=sys tests
"$PY" run_analysis.py
"$PY" notebooks/build_notebook.py
"$PY" -m jupyter nbconvert --execute --to notebook --inplace --ExecutePreprocessor.timeout=600 notebooks/ab_test_readout.ipynb
"$PY" -m jupyter nbconvert --to html --embed-images notebooks/ab_test_readout.ipynb
"$PY" scripts/verify_outputs.py
"$PY" ../scripts/report.py --sync-experiments-only
echo ANALYSIS_COMPLETE
