"""Closed-loop HTTP benchmark; validate every successful recommendation response."""
import argparse
import asyncio
from collections import Counter
from datetime import datetime, timezone
import json
from pathlib import Path
import random
import time

import httpx
import numpy as np


def validate_response(response, uid):
    response.raise_for_status()
    body = response.json()
    items = body["items"]
    ids = [item["item_id"] for item in items]
    if body["user_id"] != uid or len(ids) != 10 or len(set(ids)) != 10:
        raise ValueError("Invalid recommendation IDs or count")
    if not np.isfinite([body["server_ms"], *[item["score"] for item in items]]).all():
        raise ValueError("Non-finite recommendation values")
    return body["server_ms"]


async def main(url, n, c, out):
    lat, srv = [], []
    failures = Counter()
    statuses = Counter()
    started = datetime.now(timezone.utc).isoformat()
    async with httpx.AsyncClient(base_url=url, timeout=10) as client:
        health = await client.get("/health")
        health.raise_for_status()
        metadata = health.json()
        assert metadata["status"] == "ok" and metadata["users"] > 0
        validate_response(await client.get("/recommend/0", params={"k": 10}), 0)
        queue = asyncio.Queue()
        rng = random.Random(0)
        for _ in range(n):
            queue.put_nowait(rng.randrange(metadata["users"]))

        async def worker():
            while True:
                try:
                    uid = queue.get_nowait()
                except asyncio.QueueEmpty:
                    return
                t = time.perf_counter()
                try:
                    response = await client.get(f"/recommend/{uid}", params={"k": 10})
                    statuses[str(response.status_code)] += 1
                    srv.append(validate_response(response, uid))
                except httpx.HTTPStatusError:
                    failures["http"] += 1
                except httpx.RequestError:
                    failures["transport"] += 1
                except (KeyError, TypeError, ValueError):
                    failures["response_validation"] += 1
                finally:
                    lat.append((time.perf_counter() - t) * 1e3)

        t0 = time.perf_counter()
        await asyncio.gather(*(worker() for _ in range(c)))
        elapsed = time.perf_counter() - t0

    def percentiles(values):
        return {f"p{p}": round(float(np.percentile(values, p)), 3) for p in (50, 95, 99)} if values else {}

    result = {
        "started_utc": started, "finished_utc": datetime.now(timezone.utc).isoformat(),
        "url": url, "requests": n, "completed": len(lat), "concurrency": c, "seed": 0,
        "warmup_requests": 1, "users": metadata["users"], "items": metadata["items"],
        "successful_responses": len(srv), "errors": sum(failures.values()),
        "failure_counts": dict(failures), "status_counts": dict(statuses),
        "elapsed_s": round(elapsed, 6), "throughput_rps": round(n / elapsed, 3),
        "client_latency_ms": percentiles(lat), "server_compute_ms": percentiles(srv),
    }
    print(json.dumps(result, indent=2))
    Path(out).parent.mkdir(parents=True, exist_ok=True)
    Path(out).write_text(json.dumps(result, indent=2) + "\n")
    if len(lat) != n or len(srv) != n or failures:
        raise SystemExit("HTTP benchmark failed; inspect the saved result")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="http://127.0.0.1:8088")
    parser.add_argument("-n", type=int, default=5000)
    parser.add_argument("-c", type=int, default=8)
    parser.add_argument("--out", default="results/api_bench.json")
    args = parser.parse_args()
    if args.n < 1 or args.c < 1:
        parser.error("request count and concurrency must be positive")
    asyncio.run(main(args.url, args.n, args.c, args.out))
