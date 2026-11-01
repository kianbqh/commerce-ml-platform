# Grocery retrieval and ranking

I kept retrieval and ranking separate so I could inspect what is lost when a catalog becomes a shortlist, then see what purchase-history features recover within that shortlist. Both stages now use the full Instacart grocery data and the [warehouse's dbt cutoff tables](../warehouse/README.md).

The 32-dimensional user tower adds a learned user embedding to a projection of observed basket counts, basket sizes and aisle/department affinities. The product tower combines learned product/aisle/department embeddings with training-only popularity, buyer counts and reorder shares. Every training purchase row is used in each epoch. Four uniform product IDs per positive supply implicit negatives; exact positive collisions are replaced, while other observed products can still be negative samples. The balanced logistic objective uses normalized dot products divided by 0.1. This CPU run uses three epochs, batch 4,096, seed 42 and four threads.

FAISS HNSW32 retrieves 100 candidates from all 49,688 products, including products the user has purchased. LambdaMART sees 16 features: tower score/rank, training product priors, history sizes, user-product purchase count/frequency/recency/reorder share, candidate-specific category affinities and category IDs. It fits validation-basket labels using a deterministic 90/10 split of users for fitting and early stopping. Test labels never fit either stage.

Early stopping uses LightGBM's built-in NDCG over the retrieved candidate labels. The published next-basket metrics below use the complete held-out basket, including positives absent from the shortlist.

The [root result table](../results/table.md) reports full-user next-basket NDCG@10 and Recall@10/100, the training-popularity baseline, validation candidate-positive coverage and an explicitly sized HNSW-versus-exact diagnostic. Feature matrices are written in user batches to native memory-mapped arrays; raw CSV strings and the complete candidate feature table are not held as pandas frames.

## Run and serve

```bash
# From the repository root:
./scripts/run_all.sh
PYTHONPATH=recsys uvicorn recsys.serve:app --host 127.0.0.1 --port 8088
```

`GET /recommend/42?k=10` returns original Instacart user/product IDs, product names, rank scores and `handler_ms`. `GET /health` reports the actual user and catalog counts. `RECSYS_ARTIFACTS` and `COMMERCE_CACHE` select local model/feature directories. Scoring uses the completed validation basket as each user's latest available history. One HNSW thread and one LightGBM thread serve each call.

The root HTTP runner starts one temporary Uvicorn process, performs 50 excluded warmups and 5,000 validated requests at concurrency eight, saves every client/handler latency, then stops that process. Its timing protocol and failures are in `results/api_bench.json`. [Contract checks](VALIDATION.md) include actual miniature dbt training and API execution in the unified workflow.

Input: *The Instacart Online Grocery Shopping Dataset 2017*. [Dataset files and source hashes](../results/before/warehouse/data_manifest.json). The small CI fixture is synthetic and separate from published quality. Earlier MovieLens measurements live under [before/movielens](../results/before/movielens/).
