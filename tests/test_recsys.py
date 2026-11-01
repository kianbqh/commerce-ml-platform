import os
import numpy as np, pandas as pd, pytest
from recsys import metrics
from recsys.features import retrieve_exact, make_features, build_context, FEATURES

ROOT = os.path.dirname(os.path.dirname(__file__))


def test_metrics_hand_computed():
    ranked = np.array([[5, 1, 2], [3, 4, 9], [7, 8, 0]]); target = np.array([5, 9, 1])
    r = metrics.rank_of_target(ranked, target)
    assert r.tolist() == [0, 2, -1]
    assert metrics.recall_at_k(r, 1) == pytest.approx(1 / 3)
    assert metrics.recall_at_k(r, 3) == pytest.approx(2 / 3)
    assert metrics.ndcg_at_k(r, 3) == pytest.approx((1 + 1 / np.log2(4)) / 3)
    assert metrics.mrr(r) == pytest.approx((1 + 1 / 3) / 3)


def test_retrieve_excludes_seen_and_is_sorted():
    rng = np.random.default_rng(0); ue = rng.normal(size=(4, 8)).astype("float32"); ie = rng.normal(size=(50, 8)).astype("float32")
    seen = [np.arange(10), np.array([], int), np.arange(40), np.array([3])]
    c, s = retrieve_exact(ue, ie, np.arange(4), seen, 5)
    for u in range(4):
        assert not np.isin(c[u], seen[u]).any()
        assert np.all(np.diff(s[u]) <= 1e-6)
    full = ue[1] @ ie.T
    assert set(c[1]) == set(np.argsort(-full)[:5])


@pytest.fixture(scope="module")
def ds():
    if not os.path.exists(os.path.join(ROOT, "data/ml-1m/ratings.dat")):
        pytest.skip("MovieLens-1M not downloaded")
    from recsys import data
    cwd = os.getcwd(); os.chdir(ROOT)
    try:
        return data.load("data")
    finally:
        os.chdir(cwd)


def test_split_is_temporal_and_leak_free(ds):
    assert len(ds.val) == len(ds.test) == ds.n_users
    last_train = ds.train.groupby("uid").ts.max()
    v = ds.val.set_index("uid").ts; t = ds.test.set_index("uid").ts
    assert (v >= last_train.reindex(v.index)).all() and (t >= v).all()
    assert len(ds.train) + 2 * ds.n_users == 1_000_209


def test_features_shape_and_finite(ds):
    ie = np.random.default_rng(0).normal(size=(ds.n_items, 16)).astype("float32")
    ctx = build_context(ds.train, ds, ie)
    c = np.tile(np.arange(20), (3, 1)); s = np.zeros((3, 20), "float32")
    X = make_features(np.array([0, 1, 2]), c, s, ctx)
    assert X.shape == (60, len(FEATURES)) and np.isfinite(X).all()


def test_api_end_to_end():
    if not os.path.exists(os.path.join(ROOT, "artifacts/ranker.txt")):
        pytest.skip("run the pipeline first")
    os.environ["RECSYS_ARTIFACTS"] = os.path.join(ROOT, "artifacts")
    from fastapi.testclient import TestClient
    from recsys import serve
    cl = TestClient(serve.app)
    assert cl.get("/health").json()["status"] == "ok"
    r = cl.get("/recommend/10", params={"k": 10}).json()
    ids = [x["item_id"] for x in r["items"]]
    assert len(ids) == 10 == len(set(ids))
    assert not np.isin(ids, serve.rec().ctx.seen[10]).any()
    assert cl.get("/recommend/999999").status_code == 404
