import numpy as np


def rank_of_target(ranked: np.ndarray, target: np.ndarray) -> np.ndarray:
    """ranked: (U, K) item ids; target: (U,). Returns 0-based rank, or -1 if absent."""
    hit = ranked == target[:, None]
    return np.where(hit.any(1), hit.argmax(1), -1)


def recall_at_k(ranks: np.ndarray, k: int) -> float:
    return float(((ranks >= 0) & (ranks < k)).mean())


def ndcg_at_k(ranks: np.ndarray, k: int) -> float:
    ok = (ranks >= 0) & (ranks < k)
    g = np.zeros(len(ranks)); g[ok] = 1.0 / np.log2(ranks[ok] + 2)
    return float(g.mean())


def mrr(ranks: np.ndarray) -> float:
    g = np.zeros(len(ranks)); ok = ranks >= 0; g[ok] = 1.0 / (ranks[ok] + 1)
    return float(g.mean())
