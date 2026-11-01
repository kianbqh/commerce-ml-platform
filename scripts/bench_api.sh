#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
PYTHON=${PYTHON:-python3}
export PYTHONPATH="$PWD/recsys${PYTHONPATH:+:$PYTHONPATH}"
PORT=${PORT:-8088}
"$PYTHON" - "$PORT" <<'PY'
import socket,sys
with socket.socket() as probe:
    probe.bind(('127.0.0.1',int(sys.argv[1])))
PY
"$PYTHON" -m uvicorn recsys.serve:app --host 127.0.0.1 --port "$PORT" --workers 1 --log-level warning > results/api.log 2>&1 &
API_PID=$!
cleanup() { kill "$API_PID" 2>/dev/null || true; wait "$API_PID" 2>/dev/null || true; }
trap cleanup EXIT
"$PYTHON" - "$PORT" "$API_PID" <<'PY'
import os,sys,time,httpx
for _ in range(120):
    os.kill(int(sys.argv[2]),0)
    try:
        response=httpx.get(f'http://127.0.0.1:{sys.argv[1]}/health',timeout=2)
        response.raise_for_status()
        assert response.json()['dataset']=='Instacart 2017'
        break
    except (httpx.HTTPError,AssertionError): time.sleep(.5)
else: raise SystemExit('Recommendation service did not become ready')
PY
"$PYTHON" recsys/scripts/bench_api.py --url "http://127.0.0.1:$PORT" -n "${REQUESTS:-5000}" -c "${CONCURRENCY:-8}" --out results/api_bench.json
