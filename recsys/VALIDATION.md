# Recommendation contracts

The root runner executes actual dbt cutoff tables, learned retrieval/ranking, synthetic contract tests and a temporary HTTP service. Current Instacart quality and full execution scope are in [root results](../results/); earlier MovieLens measurements are explicitly historical under `results/before/movielens/`.

The CI fixture has 40 eligible users, 24 grocery products and six labeled baskets per eligible user, plus unlabeled orders and an excluded short history. It executes dbt rather than recreating its features in Python. Tests check temporal boundaries, exact dbt values, held-out-label perturbation invariance, actual learned weights and history-feature consumption, stale-model rejection, disjoint ranking groups, multi-positive metric arithmetic and actual API responses with external IDs.

The full run uses every eligible Instacart user and the entire product catalog. The HTTP benchmark validates every response's status, expected user, requested count, unique catalog IDs, nonempty names and finite scores. Its client timer ends when the response body arrives; the separate handler timer covers retrieval, features, ranking and item construction. Every measured request's user ID and timings are saved.
