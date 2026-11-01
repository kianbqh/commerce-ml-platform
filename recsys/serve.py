"""FastAPI serving: user embedding -> FAISS HNSW candidate retrieval -> LambdaMART re-rank -> top-k."""
from __future__ import annotations
import os, pickle, time
import numpy as np, faiss, lightgbm as lgb
from fastapi import FastAPI, HTTPException, Query
from .features import make_features

ART = os.environ.get("RECSYS_ARTIFACTS", "artifacts")
K_CAND = 100


class Recommender:
    def __init__(self, art=ART):
        self.ue = np.load(f"{art}/user_emb.npy"); self.index = faiss.read_index(f"{art}/items_hnsw.faiss")
        self.index.hnsw.efSearch = 128
        self.ranker = lgb.Booster(model_file=f"{art}/ranker.txt")
        d = pickle.load(open(f"{art}/context.pkl", "rb")); self.ctx, self.titles = d["ctx"], d["titles"]
        self.n_items = len(self.titles)

    def candidates(self, uid: int, k: int = K_CAND):
        seen = self.ctx.seen[uid]
        kk = min(self.n_items, k + len(seen))
        s, i = self.index.search(self.ue[uid:uid + 1], kk)
        m = (i[0] >= 0) & ~np.isin(i[0], seen)
        return i[0][m][:k], s[0][m][:k]

    def recommend(self, uid: int, k: int = 10, rerank: bool = True):
        c, s = self.candidates(uid)
        if rerank:
            p = self.ranker.predict(make_features(np.array([uid]), c[None], s[None], self.ctx))
            o = np.argsort(-p); c, s = c[o], p[o]
        return [{"item_id": int(i), "title": self.titles[i], "score": float(x)} for i, x in zip(c[:k], s[:k])]


faiss.omp_set_num_threads(1)
app = FastAPI(title="recsys-ranking")
_rec: Recommender | None = None


def rec() -> Recommender:
    global _rec
    if _rec is None:
        _rec = Recommender()
    return _rec


@app.get("/health")
def health():
    return {"status": "ok", "items": rec().n_items, "users": len(rec().ue)}


@app.get("/recommend/{user_id}")
def recommend(user_id: int, k: int = Query(10, ge=1, le=100), rerank: bool = True):
    r = rec()
    if not 0 <= user_id < len(r.ue):
        raise HTTPException(404, "unknown user")
    t = time.perf_counter(); items = r.recommend(user_id, k, rerank)
    return {"user_id": user_id, "items": items, "server_ms": round((time.perf_counter() - t) * 1e3, 3)}
