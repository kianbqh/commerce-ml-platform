# Instacart next-basket results

Full-data run: **206,209 users, 49,688 products and 33,819,106 labeled order lines**. The test baskets contain 2,165,417 positives across all 206,209 users.

| Method | Next-basket NDCG@10 | Recall@10 | Candidate Recall@100 |
|---|---:|---:|---:|
| Training popularity | 0.09762 | 0.06994 | 0.21754 |
| Two-tower + HNSW | 0.10023 | 0.08611 | 0.31730 |
| Two-tower + LambdaMART | 0.30336 | 0.22277 | 0.31730 |

Validation candidate Recall@100: 0.31624 macro / 0.28810 micro. LambdaMART fits 185,588 user groups and monitors 20,621 disjoint groups for early stopping. The selected model has 117 trees from a 120-round cap. HNSW's exact top-100 agreement is 0.98454 on a seeded 2,000-user diagnostic.

3 CPU epochs used every one of the 29,524,435 training purchase rows per epoch. These are elapsed execution records: two-tower fitting 429.3s, ranker fitting 101.6s. The full quality evaluation above uses every eligible user; the separate ANN diagnostic is explicitly smaller.

**HTTP:** 5,000/5,000 validated responses, zero errors, concurrency 8, after 50 excluded warmups. Client p50/p95/p99: 69.21/129.73/363.73ms; handler p50/p95/p99: 0.82/1.06/2.38ms. Throughput 56.0 requests/s.

The HTTP run used one Uvicorn worker, one FAISS/LightGBM thread per call and 8 closed-loop clients on an i7-12700KF under WSL2. The client timer ends at complete response-body arrival; the handler timer covers retrieval, feature lookup, reranking and item construction. The recorded WSL process snapshot contained only the task keepalive before launch. [Environment](environment.json) · [WSL process snapshot](api-background-processes.txt).

[Full metrics and fit provenance](metrics.json) · [every HTTP request](api_bench.json) · [split and feature hashes](dataset.json).
