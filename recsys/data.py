"""MovieLens-1M loading + temporal leave-last-two-out split (implicit feedback)."""
from __future__ import annotations
import hashlib, os, urllib.request, zipfile
from pathlib import Path
from dataclasses import dataclass
import numpy as np
import pandas as pd

ML1M_URL = "https://files.grouplens.org/datasets/movielens/ml-1m.zip"
ML1M_SHA256 = "a6898adb50b9ca05aa231689da44c217cb524e7ebd39d264c56e2832f2c54e20"
GENRES = ["Action", "Adventure", "Animation", "Children's", "Comedy", "Crime", "Documentary", "Drama",
          "Fantasy", "Film-Noir", "Horror", "Musical", "Mystery", "Romance", "Sci-Fi", "Thriller", "War", "Western"]


def download(data_dir: str = "data") -> str:
    path = os.path.join(data_dir, "ml-1m")
    if not os.path.exists(os.path.join(path, "ratings.dat")):
        os.makedirs(data_dir, exist_ok=True)
        zp = os.path.join(data_dir, "ml-1m.zip")
        if not os.path.exists(zp):
            urllib.request.urlretrieve(ML1M_URL, zp + ".part")
            os.replace(zp + ".part", zp)
        if hashlib.sha256(Path(zp).read_bytes()).hexdigest() != ML1M_SHA256:
            raise ValueError(f"MovieLens archive checksum mismatch: {zp}")
        root = Path(data_dir).resolve()
        with zipfile.ZipFile(zp) as archive:
            for member in archive.infolist():
                target = (root / member.filename).resolve()
                if not target.is_relative_to(root) or not member.filename.startswith("ml-1m/"):
                    raise ValueError(f"Unexpected archive member: {member.filename}")
            archive.extractall(root)
    return path


@dataclass
class Dataset:
    train: pd.DataFrame      # uid, iid, rating, ts (0-indexed ids)
    val: pd.DataFrame        # one row per user (second-to-last interaction)
    test: pd.DataFrame       # one row per user (last interaction)
    users: pd.DataFrame      # uid, gender, age, occ (ints)
    items: pd.DataFrame      # iid, year, genre multi-hot columns
    n_users: int
    n_items: int


def load(data_dir: str = "data") -> Dataset:
    path = download(data_dir)
    r = pd.read_csv(os.path.join(path, "ratings.dat"), sep="::", engine="python", names=["user", "movie", "rating", "ts"])
    m = pd.read_csv(os.path.join(path, "movies.dat"), sep="::", engine="python", names=["movie", "title", "genres"], encoding="latin-1")
    u = pd.read_csv(os.path.join(path, "users.dat"), sep="::", engine="python", names=["user", "gender", "age", "occ", "zip"])
    m = m[m.movie.isin(r.movie.unique())].reset_index(drop=True)
    movie2iid = {mv: i for i, mv in enumerate(m.movie)}
    user2uid = {us: i for i, us in enumerate(sorted(r.user.unique()))}
    r["uid"] = r.user.map(user2uid); r["iid"] = r.movie.map(movie2iid)
    r = r.sort_values(["uid", "ts"], kind="stable").reset_index(drop=True)
    pos_from_end = r.groupby("uid").cumcount(ascending=False)
    test = r[pos_from_end == 0][["uid", "iid", "rating", "ts"]].reset_index(drop=True)
    val = r[pos_from_end == 1][["uid", "iid", "rating", "ts"]].reset_index(drop=True)
    train = r[pos_from_end >= 2][["uid", "iid", "rating", "ts"]].reset_index(drop=True)
    items = pd.DataFrame({"iid": np.arange(len(m)), "title": m.title})
    items["year"] = m.title.str.extract(r"\((\d{4})\)\s*$")[0].astype(float).fillna(1990).astype(int)
    for g in GENRES:
        items[g] = m.genres.str.split("|").apply(lambda gs, g=g: int(g in gs))
    u["uid"] = u.user.map(user2uid)
    u = u.dropna(subset=["uid"]).astype({"uid": int}).sort_values("uid").reset_index(drop=True)
    users = pd.DataFrame({"uid": u.uid, "gender": (u.gender == "M").astype(int),
                          "age": u.age.astype("category").cat.codes, "occ": u.occ})
    return Dataset(train, val, test, users, items, len(user2uid), len(m))
