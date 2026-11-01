"""End-to-end: train two-tower -> FAISS index -> LambdaMART re-ranker -> offline evaluation -> artifacts."""
from __future__ import annotations
import argparse, json, os, pickle, time
import numpy as np, pandas as pd, torch, faiss, lightgbm as lgb
from . import data, twotower, metrics
from .features import FEATURES, build_context, make_features, retrieve_exact

K_CAND = 100


def evaluate(ranked, target):
    r = metrics.rank_of_target(ranked, target)
    return {"recall@10": metrics.recall_at_k(r, 10), "recall@50": metrics.recall_at_k(r, 50),
            "recall@100": metrics.recall_at_k(r, 100), "ndcg@10": metrics.ndcg_at_k(r, 10), "mrr@100": metrics.mrr(r)}


def main():
    ap = argparse.ArgumentParser(); ap.add_argument("--epochs", type=int, default=8); ap.add_argument("--out", default="results/metrics.json")
    a = ap.parse_args(); torch.set_num_threads(4); t0 = time.perf_counter()
    data_started = time.perf_counter()
    ds = data.load("data")
    data_load_s = time.perf_counter() - data_started
    print(f"users {ds.n_users} items {ds.n_items} train {len(ds.train)} val {len(ds.val)} test {len(ds.test)}")
    train_threads = torch.get_num_threads()
    train_started = time.perf_counter()
    model = twotower.train(ds, epochs=a.epochs)
    t_tt = time.perf_counter() - train_started
    ue, ie = model.all_users(), model.all_items()
    uids = np.arange(ds.n_users)

    # ---- re-ranker training data: validation targets, history = train only
    ctx_tr = build_context(ds.train, ds, ie)
    c_tr, s_tr = retrieve_exact(ue, ie, uids, ctx_tr.seen, K_CAND)
    y_val = ds.val.sort_values("uid").iid.values
    lab = (c_tr == y_val[:, None]).astype(int)
    keep = lab.any(1)
    X = make_features(uids[keep], c_tr[keep], s_tr[keep], ctx_tr); Y = lab[keep].ravel()
    ku = uids[keep]; rng = np.random.default_rng(0); is_va = rng.random(len(ku)) < 0.1
    rows_va = np.repeat(is_va, K_CAND)
    dtr = lgb.Dataset(X[~rows_va], Y[~rows_va], group=[K_CAND] * int((~is_va).sum()), feature_name=FEATURES)
    dva = lgb.Dataset(X[rows_va], Y[rows_va], group=[K_CAND] * int(is_va.sum()), reference=dtr)
    params = dict(objective="lambdarank", metric="ndcg", ndcg_eval_at=[10], learning_rate=0.05, num_leaves=31,
                  min_data_in_leaf=50, feature_fraction=0.8, lambdarank_truncation_level=20, verbose=-1, num_threads=4, seed=0)
    t1 = time.time()
    ranker = lgb.train(params, dtr, 500, valid_sets=[dva], callbacks=[lgb.early_stopping(30, verbose=False)])
    t_rk = time.time() - t1
    print(f"ranker: {int(keep.sum())} query users with positive in top-{K_CAND}, best_iter {ranker.best_iteration}")

    # ---- test evaluation: history = train + val
    hist = pd.concat([ds.train, ds.val]); ctx_te = build_context(hist, ds, ie)
    y_te = ds.test.sort_values("uid").iid.values
    c_te, s_te = retrieve_exact(ue, ie, uids, ctx_te.seen, K_CAND)
    t2 = time.time()
    p = ranker.predict(make_features(uids, c_te, s_te, ctx_te), num_iteration=ranker.best_iteration).reshape(-1, K_CAND)
    t_score = time.time() - t2
    reranked = np.take_along_axis(c_te, np.argsort(-p, 1), 1)
    pop = np.argsort(-np.bincount(hist.iid, minlength=ds.n_items))
    pop_ranked = np.stack([pop[~np.isin(pop, ctx_te.seen[u])][:K_CAND] for u in uids])
    res = {"popularity": evaluate(pop_ranked, y_te), "two_tower": evaluate(c_te, y_te), "two_tower+lambdamart": evaluate(reranked, y_te)}

    # ---- FAISS: exact (IndexFlatIP) vs HNSW, recall of ANN vs exact top-100 and per-query latency
    flat = faiss.IndexFlatIP(ie.shape[1]); flat.add(ie)
    hnsw = faiss.IndexHNSWFlat(ie.shape[1], 32, faiss.METRIC_INNER_PRODUCT); hnsw.hnsw.efConstruction = 200; hnsw.add(ie)
    hnsw.hnsw.efSearch = 128
    _, ex = flat.search(ue, K_CAND); _, ap_ = hnsw.search(ue, K_CAND)
    ann_recall = float(np.mean([len(np.intersect1d(ex[i], ap_[i])) / K_CAND for i in range(len(ue))]))
    faiss.omp_set_num_threads(1); lat = {}
    for name, idx in [("flat", flat), ("hnsw", hnsw)]:
        q = ue[:2000]; ts = []
        for i in range(len(q)):
            s = time.perf_counter(); idx.search(q[i:i + 1], K_CAND); ts.append((time.perf_counter() - s) * 1e3)
        lat[name] = {"p50_ms": float(np.percentile(ts, 50)), "p99_ms": float(np.percentile(ts, 99))}

    imp = dict(sorted(zip(FEATURES, ranker.feature_importance("gain").round(1).tolist()), key=lambda x: -x[1]))
    out = {"dataset": {"name": "MovieLens-1M", "users": ds.n_users, "items": ds.n_items, "train": len(ds.train),
                       "val": len(ds.val), "test": len(ds.test), "split": "per-user temporal leave-last-two-out"},
           "test_metrics": res,
           "lift_vs_popularity": {k: res["two_tower+lambdamart"][k] / res["popularity"][k] for k in ["recall@10", "ndcg@10"]},
           "lift_rerank_vs_two_tower": {k: res["two_tower+lambdamart"][k] / res["two_tower"][k] for k in ["recall@10", "ndcg@10"]},
           "faiss": {"hnsw_recall@100_vs_exact": ann_recall, "latency_single_query_1thread": lat},
           "ranker": {"query_users": int(keep.sum()), "rows": int(len(Y)), "best_iteration": ranker.best_iteration,
                      "train_s": round(t_rk, 1), "batch_score_6040x100_s": round(t_score, 2), "feature_gain": imp},
           "data_load_s": round(data_load_s, 3), "two_tower_train_s": round(t_tt, 3),
           "training": {"device": str(next(model.parameters()).device), "epochs": a.epochs,
                        "torch_threads": train_threads, "seed": 0},
           "total_s": round(time.perf_counter() - t0, 3)}
    print(json.dumps(out, indent=2)); os.makedirs(os.path.dirname(a.out), exist_ok=True)
    json.dump(out, open(a.out, "w"), indent=2)
    os.makedirs("artifacts", exist_ok=True)
    np.save("artifacts/user_emb.npy", ue); np.save("artifacts/item_emb.npy", ie)
    faiss.write_index(hnsw, "artifacts/items_hnsw.faiss"); ranker.save_model("artifacts/ranker.txt", num_iteration=ranker.best_iteration)
    pickle.dump({"ctx": ctx_te, "titles": ds.items.sort_values("iid").title.tolist()}, open("artifacts/context.pkl", "wb"))


if __name__ == "__main__":
    main()
