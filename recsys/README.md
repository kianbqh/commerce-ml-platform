# recsys-ranking

I built this movie recommender to compare candidate retrieval with a separate ranking stage. A PyTorch two-tower model learns 64-dimensional user and movie embeddings; a LightGBM LambdaMART model then reorders 100 candidates using 16 features. The API uses a FAISS HNSW index, removes watched movies, and returns ranked titles.

## Run

Use Python 3.12 on Linux:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
bash scripts/run_all.sh
```

The script downloads MovieLens-1M, trains for eight epochs, evaluates all 6,040 users, runs all five tests, starts one Uvicorn worker on port 8088, sends 5,000 requests with eight clients, and stops its API process. It fails if a test is skipped or a benchmark response is unsuccessful or malformed. Results go to `results/`; generated models and user histories stay in ignored `artifacts/`.

To keep the API running after training:

```bash
python -m recsys.pipeline --epochs 8
python -m uvicorn recsys.serve:app --host 127.0.0.1 --port 8088
# In another terminal with the same environment:
curl 'http://127.0.0.1:8088/recommend/42?k=10'
python scripts/bench_api.py -n 5000 -c 8
```

The API uses zero-based user IDs (0–6039). `GET /health` reports the loaded catalog size, and `GET /recommend/{user_id}?k=10` returns movie IDs, titles, ranking scores, and handler compute time. An unknown user returns 404.

## Evaluation

MovieLens contains explicit ratings. Here every rating is treated as an implicit interaction, regardless of its value. For each user, a stable timestamp sort assigns the last interaction to test and the second-to-last to validation: 988,129 training interactions, 6,040 validation targets, and 6,040 test targets across 3,706 rated movies. Previously seen movies are excluded. With one test target per user, Recall@K is the hit rate.

The two towers combine ID embeddings with user gender, age and occupation, or movie genres and release year. Training uses in-batch sampled softmax, log-frequency correction, temperature 0.05, batch size 2,048, Adam at 0.003, eight CPU epochs, four Torch threads, and seed 0. Duplicate positive movies in a batch are masked from the negative set.

LambdaMART learns from validation targets that appear among the 100 retrieved candidates. Its features use training history only; a seeded 10% query-group split supplies early stopping. Test features use training plus validation history. The 16 features include embedding score and rank, popularity and recency, shrunk mean rating, genre affinity, similarity to the user's last five movies, year gap, and user demographics. Re-ranking preserves Recall@100 because it only reorders this candidate set.

Offline quality uses exact full-catalog retrieval. The separate FAISS comparison measures unfiltered top-100 overlap between exact inner-product search and HNSW (M=32, efConstruction=200, efSearch=128). Search latency uses the first 2,000 user embeddings, one query at a time and one FAISS thread. The HTTP path uses HNSW, seen-item filtering, features, and LambdaMART.

## Local results

Measured on 31 October 2026 with the settings above; full values are in [`results/metrics.json`](results/metrics.json).

| Model, 6,040 test users | Recall@10 | Recall@50 | Recall@100 | NDCG@10 | MRR@100 |
|---|---:|---:|---:|---:|---:|
| Popularity, unseen movies | 0.0364 | 0.1397 | 0.2258 | 0.0179 | 0.0180 |
| Two-tower retrieval | 0.0623 | 0.2300 | 0.3589 | 0.0316 | 0.0313 |
| Two-tower + LambdaMART | **0.1257** | **0.2904** | 0.3589 | **0.0665** | **0.0573** |

Re-ranking increased NDCG@10 by **2.10× over retrieval** and **3.72× over popularity**. LambdaMART used 2,236 query users (223,600 candidate rows), selected 32 trees, and trained in 0.4 seconds. The largest feature gains came from similarity to recent movies and the two-tower rank. Data loading took 4.974 seconds; pure eight-epoch two-tower training took **86.521 seconds**.

HNSW recovered **99.8518%** of exact top-100 results over all 6,040 queries. At this catalog size, exact search had the lower median search time:

| FAISS search, one thread | p50 | p99 |
|---|---:|---:|
| Exact inner product | 0.0324 ms | 0.0522 ms |
| HNSW | 0.0420 ms | 0.0586 ms |

The actual HTTP run completed **5,000/5,000 valid responses**, with **zero HTTP, transport, or response-validation errors**: **244.332 requests/second** over 20.464 seconds. See [`results/api_bench.json`](results/api_bench.json).

| Latency | p50 | p95 | p99 |
|---|---:|---:|---:|
| Client round trip | 20.980 ms | 93.327 ms | 286.805 ms |
| Handler compute | 0.700 ms | 4.580 ms | 8.166 ms |

Environment: Intel Core i7-12700KF, Ubuntu 24.04 in WSL2, Python 3.12.3, CPU training with four threads, and G-backed storage. Package versions and the dataset archive hash are recorded in `results/environment.json`. Data loading and pure two-tower training have separate elapsed-time fields.

The API benchmark is a closed-loop test over WSL loopback with one warmup recommendation excluded from the timed 5,000 requests. Client latency covers the HTTP round trip; `server_ms` covers recommendation computation inside the handler. Every response is checked for the requested user, ten distinct movie IDs, and finite scores.

## Checks and files

The five tests cover metric arithmetic, exact retrieval and seen-item filtering, the temporal split, feature shape and finite values, and API recommendations against trained artifacts. CI trains a one-epoch model, runs all five tests without skips, and exercises 100 actual HTTP requests. CI results are separate from the local eight-epoch measurements.

| File | Purpose |
|---|---|
| `recsys/data.py` | Verified download, parsing, ID mapping and temporal split |
| `recsys/twotower.py` | User and movie towers with sampled-softmax training |
| `recsys/features.py` | History context, exact retrieval and ranking features |
| `recsys/pipeline.py` | Training, offline evaluation, FAISS comparison and artifacts |
| `recsys/serve.py` | HNSW retrieval and ranking API |
| `scripts/bench_api.py` | HTTP benchmark with response validation |

## Data

[MovieLens-1M](https://grouplens.org/datasets/movielens/1m/) is provided by GroupLens Research, University of Minnesota. The downloader verifies the official archive SHA-256 before extraction. Data and derived user-history artifacts are downloaded or built locally and are not redistributed here. See the dataset's [usage terms](https://files.grouplens.org/datasets/movielens/ml-1m-README.txt), including restrictions on redistribution and commercial use.

Dataset reference: F. Maxwell Harper and Joseph A. Konstan, *The MovieLens Datasets: History and Context*, ACM Transactions on Interactive Intelligent Systems 5(4), 2015. [doi:10.1145/2827872](https://doi.org/10.1145/2827872).
