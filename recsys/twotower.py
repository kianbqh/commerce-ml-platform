"""Two-tower retrieval model trained with in-batch sampled softmax + logQ correction."""
from __future__ import annotations
import numpy as np
import torch, torch.nn as nn, torch.nn.functional as F
from .data import GENRES


class UserTower(nn.Module):
    def __init__(self, n_users, dim=64):
        super().__init__()
        self.id = nn.Embedding(n_users, dim); self.g = nn.Embedding(2, 8); self.a = nn.Embedding(7, 8); self.o = nn.Embedding(21, 8)
        self.mlp = nn.Sequential(nn.Linear(dim + 24, 128), nn.ReLU(), nn.Linear(128, dim))

    def forward(self, uid, gender, age, occ):
        x = torch.cat([self.id(uid), self.g(gender), self.a(age), self.o(occ)], -1)
        return F.normalize(self.mlp(x), dim=-1)


class ItemTower(nn.Module):
    def __init__(self, n_items, dim=64, n_years=100):
        super().__init__()
        self.id = nn.Embedding(n_items, dim); self.y = nn.Embedding(n_years, 8); self.gen = nn.Linear(len(GENRES), 16)
        self.mlp = nn.Sequential(nn.Linear(dim + 24, 128), nn.ReLU(), nn.Linear(128, dim))

    def forward(self, iid, year_idx, genres):
        x = torch.cat([self.id(iid), self.y(year_idx), self.gen(genres)], -1)
        return F.normalize(self.mlp(x), dim=-1)


class TwoTower(nn.Module):
    def __init__(self, ds, dim=64, temperature=0.05):
        super().__init__()
        self.user, self.item = UserTower(ds.n_users, dim), ItemTower(ds.n_items, dim)
        self.t = temperature
        u = ds.users.sort_values("uid")
        self.register_buffer("u_feat", torch.tensor(u[["gender", "age", "occ"]].values, dtype=torch.long))
        it = ds.items.sort_values("iid")
        self.register_buffer("i_year", torch.tensor((it.year.values - 1919).clip(0, 99), dtype=torch.long))
        self.register_buffer("i_gen", torch.tensor(it[GENRES].values, dtype=torch.float32))

    def user_emb(self, uid):
        f = self.u_feat[uid]
        return self.user(uid, f[:, 0], f[:, 1], f[:, 2])

    def item_emb(self, iid):
        return self.item(iid, self.i_year[iid], self.i_gen[iid])

    @torch.no_grad()
    def all_items(self):
        self.eval(); return self.item_emb(torch.arange(len(self.i_year))).numpy().astype("float32")

    @torch.no_grad()
    def all_users(self):
        self.eval(); return self.user_emb(torch.arange(len(self.u_feat))).numpy().astype("float32")


def train(ds, epochs=8, batch=2048, lr=3e-3, dim=64, seed=0, log=print):
    torch.manual_seed(seed); np.random.seed(seed)
    model = TwoTower(ds, dim)
    uid = torch.tensor(ds.train.uid.values); iid = torch.tensor(ds.train.iid.values)
    freq = np.bincount(ds.train.iid.values, minlength=ds.n_items) / len(ds.train)
    logq = torch.tensor(np.log(freq + 1e-9), dtype=torch.float32)
    opt = torch.optim.Adam(model.parameters(), lr=lr)
    n = len(uid)
    for ep in range(epochs):
        model.train(); perm = torch.randperm(n); tot = 0.0
        for s in range(0, n, batch):
            b = perm[s:s + batch]; ub, ib = uid[b], iid[b]
            logits = model.user_emb(ub) @ model.item_emb(ib).T / model.t - logq[ib][None, :]
            # mask in-batch duplicates of the positive item (false negatives)
            same = (ib[None, :] == ib[:, None]) & ~torch.eye(len(b), dtype=torch.bool)
            logits = logits.masked_fill(same, -1e9)
            loss = F.cross_entropy(logits, torch.arange(len(b)))
            opt.zero_grad(); loss.backward(); opt.step(); tot += loss.item() * len(b)
        log(f"epoch {ep + 1}/{epochs} loss {tot / n:.4f}")
    return model
