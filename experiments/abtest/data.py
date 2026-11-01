"""Kevin Hillstrom / MineThatData e-mail experiment (64,000 customers, randomized 1/3 each to
Mens e-mail, Womens e-mail, No e-mail; outcomes measured over the 2 weeks after the campaign)."""
from pathlib import Path
import hashlib
import urllib.request

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
PATH = ROOT / "data" / "hillstrom.csv.gz"
URL = "https://hillstorm1.s3.us-east-2.amazonaws.com/hillstorm_no_indices.csv.gz"  # public mirror used by scikit-uplift
SHA256 = "bab6578f60db5d792f1c2372c502f029152a5249cf5ea84390f3b7f885d7234f"
ARMS = {"No E-Mail": "control", "Mens E-Mail": "mens", "Womens E-Mail": "womens"}


def load() -> pd.DataFrame:
    if not PATH.exists():
        PATH.parent.mkdir(exist_ok=True)
        urllib.request.urlretrieve(URL, PATH)
    if hashlib.sha256(PATH.read_bytes()).hexdigest() != SHA256:
        raise ValueError("dataset checksum mismatch")
    df = pd.read_csv(PATH)
    df["arm"] = df["segment"].map(ARMS)
    return df


def pre_covariates(df: pd.DataFrame) -> pd.DataFrame:
    """Pre-experiment covariates only (measured before randomization) -> safe for CUPED."""
    x = df[["history", "recency", "mens", "womens", "newbie"]].astype(float)
    x = x.join(pd.get_dummies(df[["zip_code", "channel"]], drop_first=True, dtype=float))
    return x
