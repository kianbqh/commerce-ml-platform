# Validation

The local run on 31 October 2026 used MovieLens-1M in full, eight CPU training epochs and seed 0. The host was an Intel Core i7-12700KF with Ubuntu 24.04 under WSL2 and G-backed storage. Python was 3.12.3; Torch 2.15.0, NumPy 2.5.4, pandas 3.1.0, FAISS CPU 1.15.1 and LightGBM 4.7.0. Complete service versions are in `results/environment.json`.

```bash
bash scripts/run_all.sh
```

- Training: four Torch threads, batch 2,048, eight epochs; pure training elapsed 86.521 seconds. Data loading is timed separately at 4.974 seconds.
- Offline evaluation: all 6,040 test users, exact top-100 unseen candidates; LambdaMART NDCG@10 0.066476 and Recall@10 0.125662.
- FAISS: all 6,040 embeddings for HNSW versus exact top-100 overlap, followed by 2,000 sequential single-query latency measurements per index at one thread. HNSW overlap was 0.998518.
- Tests: **5 passed, 0 skipped**, in 7.20 seconds. One dependency deprecation warning came from Starlette's HTTPX TestClient integration.
- HTTP: one Uvicorn worker, eight closed-loop clients, one excluded warmup recommendation, 5,000 measured requests. All 5,000 returned HTTP 200 and valid response bodies; zero failures. Throughput was 244.332 requests/second. The script stopped the temporary API process afterward.

| Result | Contents |
|---|---|
| `results/metrics.json` | Split counts, offline quality, feature gains, training times and FAISS results |
| `results/api_bench.json` | Timed request counts, status and failure counts, elapsed time and latency percentiles |
| `results/environment.json` | Hardware, software, thread settings and dataset archive hash |
| `results/artifact_hashes.json` | SHA-256 and size of the result JSON and generated model artifacts |

Model embeddings, the FAISS index, ranker and user-history context are generated locally in ignored `artifacts/`. The dataset and user histories are not included in this repository.

CI runs the same script with one training epoch, all five tests and 100 HTTP requests at concurrency four. Its uploaded artifact is a functional check; the tables in the README use the local eight-epoch run.
