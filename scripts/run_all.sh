#!/usr/bin/env bash
# Train, evaluate, run all five tests, and benchmark a temporary API process.
set -euo pipefail
cd "$(dirname "$0")/.."
PY=${PYTHON:-python3}
PORT=${PORT:-8088}
export OMP_NUM_THREADS=4 MKL_NUM_THREADS=4 OPENBLAS_NUM_THREADS=1
mkdir -p results
"$PY" -u -m recsys.pipeline --epochs "${EPOCHS:-8}" --out results/metrics.json 2>&1 | tee results/pipeline.log
"$PY" -m pytest --capture=sys -q --junitxml=results/pytest-junit.xml 2>&1 | tee results/pytest.log
"$PY" - <<'PY'
from xml.etree import ElementTree as ET
suites = ET.parse("results/pytest-junit.xml").getroot().findall("testsuite")
assert sum(int(s.get("tests", 0)) for s in suites) == 5
assert all(int(s.get(k, 0)) == 0 for s in suites for k in ("failures", "errors", "skipped"))
PY
"$PY" -m uvicorn recsys.serve:app --host 127.0.0.1 --port "$PORT" --log-level warning > results/api.log 2>&1 &
API_PID=$!
cleanup() { kill "$API_PID" 2>/dev/null || true; wait "$API_PID" 2>/dev/null || true; }
trap cleanup EXIT
"$PY" - "$PORT" "$API_PID" <<'PY'
import os, sys, time, httpx
url = f"http://127.0.0.1:{sys.argv[1]}/health"
for _ in range(120):
    os.kill(int(sys.argv[2]), 0)
    try:
        r = httpx.get(url, timeout=2)
        r.raise_for_status()
        assert r.json()["status"] == "ok"
        break
    except (httpx.HTTPError, AssertionError, ValueError):
        time.sleep(0.5)
else:
    raise SystemExit("API did not become ready")
PY
"$PY" scripts/bench_api.py --url "http://127.0.0.1:$PORT" -n "${REQUESTS:-5000}" -c "${CONCURRENCY:-8}" --out results/api_bench.json
