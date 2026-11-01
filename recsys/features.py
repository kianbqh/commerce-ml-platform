"""Ranking features for (user, candidate) pairs. All statistics come from the history frame passed in,
so the re-ranker is trained on validation targets with train-only history and evaluated on test targets
with train+val history (no leakage)."""
from __future__ import annotations
from dataclasses import dataclass
import numpy as np
import pandas as pd
from .data import GENRES

FEATURES = ["tt_score", "tt_rank", "item_pop_log", "item_recent_pop_log", "item_mean_rating", "item_year",
            "user_n_log", "user_mean_rating", "user_gender", "user_age", "user_occ", "genre_affinity",
            "sim_last1", "sim_last5_max", "sim_last5_mean", "year_gap"]


@dataclass
class Context:
    item_pop_log: np.ndarray; item_recent_pop_log: np.ndarray; item_mean_rating: np.ndarray; item_year: np.ndarray
    item_gen: np.ndarray; user_n_log: np.ndarray; user_mean_rating: np.ndarray; user_demo: np.ndarray
    user_genre: np.ndarray; user_last5: np.ndarray; user_mean_year: np.ndarray; item_emb: np.ndarray
    seen: list  # list[np.ndarray] of seen item ids per user


def build_context(hist: pd.DataFrame, ds, item_emb: np.ndarray) -> Context:
    I, U = ds.n_items, ds.n_users
    cnt = np.bincount(hist.iid, minlength=I)
    cutoff = hist.ts.quantile(0.9)
    recent = np.bincount(hist[hist.ts >= cutoff].iid, minlength=I)
    rsum = np.bincount(hist.iid, weights=hist.rating, minlength=I)
    item_mean = np.where(cnt > 0, (rsum + 3.5 * 10) / (cnt + 10), 3.5)  # shrunk mean rating
    gen = ds.items.sort_values("iid")[GENRES].values.astype(np.float32)
    year = ds.items.sort_values("iid").year.values.astype(np.float32)
    ucnt = np.bincount(hist.uid, minlength=U)
    umean = np.bincount(hist.uid, weights=hist.rating, minlength=U) / np.maximum(ucnt, 1)
    ug = np.zeros((U, len(GENRES)), np.float32); np.add.at(ug, hist.uid.values, gen[hist.iid.values])
    ug /= np.maximum(ug.sum(1, keepdims=True), 1)
    uy = np.bincount(hist.uid, weights=year[hist.iid.values], minlength=U) / np.maximum(ucnt, 1)
    h = hist.sort_values(["uid", "ts"], kind="stable")
    last5 = np.full((U, 5), -1, np.int64); seen = [np.empty(0, np.int64)] * U
    for uid, g in h.groupby("uid").iid:
        v = g.values; seen[uid] = v; t = v[-5:]; last5[uid, :len(t)] = t[::-1]
    demo = ds.users.sort_values("uid")[["gender", "age", "occ"]].values
    return Context(np.log1p(cnt), np.log1p(recent), item_mean, year, gen, np.log1p(ucnt), umean, demo, ug, last5,
                   uy, item_emb, seen)


def make_features(uids: np.ndarray, cands: np.ndarray, scores: np.ndarray, ctx: Context) -> np.ndarray:
    """uids (N,), cands (N,K), scores (N,K) -> (N*K, F) float32."""
    N, K = cands.shape
    u = np.repeat(uids, K); c = cands.ravel()
    rank = np.tile(np.arange(K), N)
    ie = ctx.item_emb
    l5 = ctx.user_last5[uids]                       # (N,5)
    valid = l5 >= 0
    sims = np.einsum("nkd,njd->nkj", ie[cands], ie[np.where(valid, l5, 0)])  # (N,K,5)
    sims = np.where(valid[:, None, :], sims, np.nan)
    with np.errstate(all="ignore"):
        s_max = np.nanmax(sims, -1); s_mean = np.nanmean(sims, -1)
    cols = [scores.ravel(), rank, ctx.item_pop_log[c], ctx.item_recent_pop_log[c], ctx.item_mean_rating[c],
            ctx.item_year[c], ctx.user_n_log[u], ctx.user_mean_rating[u], ctx.user_demo[u, 0], ctx.user_demo[u, 1],
            ctx.user_demo[u, 2], (ctx.user_genre[u] * ctx.item_gen[c]).sum(1), sims[:, :, 0].ravel(),
            s_max.ravel(), s_mean.ravel(), ctx.item_year[c] - ctx.user_mean_year[u]]
    return np.nan_to_num(np.stack(cols, 1).astype(np.float32), nan=0.0)


def retrieve_exact(user_emb: np.ndarray, item_emb: np.ndarray, uids: np.ndarray, seen: list, k: int):
    s = user_emb[uids] @ item_emb.T
    for r, u in enumerate(uids):
        s[r, seen[u]] = -np.inf
    top = np.argpartition(-s, k, axis=1)[:, :k]
    ts = np.take_along_axis(s, top, 1); o = np.argsort(-ts, 1)
    return np.take_along_axis(top, o, 1), np.take_along_axis(ts, o, 1)
