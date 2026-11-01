"""Statistical building blocks for online-experiment analysis (NumPy / SciPy / statsmodels)."""
from __future__ import annotations

from dataclasses import asdict, dataclass

import numpy as np
from scipy import stats
from statsmodels.stats.power import NormalIndPower, TTestIndPower
from statsmodels.stats.proportion import confint_proportions_2indep, proportion_effectsize, proportions_ztest


# ---------------------------------------------------------------- power / sample size
def sample_size_proportions(p_base: float, mde_abs: float, alpha: float = 0.05, power: float = 0.8) -> int:
    """Per-arm n to detect an absolute lift `mde_abs` on a conversion rate (two-sided, Cohen's h)."""
    h = abs(proportion_effectsize(p_base + mde_abs, p_base))
    return int(np.ceil(NormalIndPower().solve_power(effect_size=h, alpha=alpha, power=power, ratio=1.0)))


def mde_proportions(p_base: float, n_per_arm: int, alpha: float = 0.05, power: float = 0.8) -> float:
    """Minimum detectable absolute lift for a conversion rate given per-arm n (normal approximation)."""
    z = stats.norm.ppf(1 - alpha / 2) + stats.norm.ppf(power)
    return float(z * np.sqrt(2 * p_base * (1 - p_base) / n_per_arm))


def sample_size_means(sd: float, mde_abs: float, alpha: float = 0.05, power: float = 0.8) -> int:
    d = mde_abs / sd
    return int(np.ceil(TTestIndPower().solve_power(effect_size=d, alpha=alpha, power=power, ratio=1.0)))


# ---------------------------------------------------------------- hypothesis tests
@dataclass
class TestResult:
    metric: str
    control: float
    treatment: float
    diff: float
    rel_lift: float
    ci_low: float
    ci_high: float
    stat: float
    p_value: float

    def dict(self):
        return asdict(self)


def two_proportion_ztest(x_t: np.ndarray, x_c: np.ndarray, metric: str = "rate", alpha: float = 0.05) -> TestResult:
    s_t, n_t, s_c, n_c = int(x_t.sum()), len(x_t), int(x_c.sum()), len(x_c)
    z, p = proportions_ztest([s_t, s_c], [n_t, n_c], alternative="two-sided")
    lo, hi = confint_proportions_2indep(s_t, n_t, s_c, n_c, method="wald", alpha=alpha)
    pt, pc = s_t / n_t, s_c / n_c
    return TestResult(metric, pc, pt, pt - pc, (pt - pc) / pc, float(lo), float(hi), float(z), float(p))


def welch_ttest(y_t: np.ndarray, y_c: np.ndarray, metric: str = "mean", alpha: float = 0.05) -> TestResult:
    y_t, y_c = np.asarray(y_t, float), np.asarray(y_c, float)
    r = stats.ttest_ind(y_t, y_c, equal_var=False)
    diff = y_t.mean() - y_c.mean()
    se = np.sqrt(y_t.var(ddof=1) / len(y_t) + y_c.var(ddof=1) / len(y_c))
    df = r.df
    q = stats.t.ppf(1 - alpha / 2, df)
    return TestResult(metric, y_c.mean(), y_t.mean(), diff, diff / y_c.mean(), diff - q * se, diff + q * se,
                      float(r.statistic), float(r.pvalue))


# ---------------------------------------------------------------- CUPED
def cuped_theta(y: np.ndarray, x: np.ndarray) -> np.ndarray:
    """theta = Cov(X, Y) Var(X)^-1, estimated on pooled data (X: pre-experiment covariate(s), n x k)."""
    x = np.asarray(x, float).reshape(len(y), -1)
    xc = x - x.mean(0)
    yc = np.asarray(y, float) - np.mean(y)
    return np.linalg.lstsq(xc, yc, rcond=None)[0]


def cuped_adjust(y: np.ndarray, x: np.ndarray, theta: np.ndarray | None = None) -> np.ndarray:
    """Y_cuped = Y - (X - mean(X)) theta, using pre-treatment X and a pooled least-squares fit.

    A fixed theta preserves the randomized mean effect; fitting theta on the same sample is regression adjustment.
    """
    x = np.asarray(x, float).reshape(len(y), -1)
    theta = cuped_theta(y, x) if theta is None else theta
    return np.asarray(y, float) - (x - x.mean(0)) @ theta


def variance_reduction(y: np.ndarray, y_adj: np.ndarray) -> float:
    return float(1 - np.var(y_adj, ddof=1) / np.var(y, ddof=1))


# ---------------------------------------------------------------- SRM
def srm_check(counts: dict[str, int], expected_ratio: dict[str, float] | None = None, threshold: float = 0.001):
    names = list(counts)
    obs = np.array([counts[k] for k in names], float)
    ratio = np.array([expected_ratio[k] for k in names], float) if expected_ratio else np.ones(len(names))
    exp = obs.sum() * ratio / ratio.sum()
    chi2, p = stats.chisquare(obs, exp)
    return {"counts": counts, "expected": dict(zip(names, exp.round(1).tolist())),
            "chi2": float(chi2), "p_value": float(p), "srm_detected": bool(p < threshold)}


# ---------------------------------------------------------------- bootstrap
def bootstrap_diff_ci(y_t, y_c, stat=np.mean, n_boot: int = 5000, alpha: float = 0.05, seed: int = 0,
                      relative: bool = False):
    """Percentile bootstrap CI for stat(T) - stat(C) (or relative lift), resampling each arm independently."""
    rng = np.random.default_rng(seed)
    y_t, y_c = np.asarray(y_t, float), np.asarray(y_c, float)
    # Keep the original RNG order (all treatment resamples, then all control resamples),
    # while avoiding two full n_boot x n index matrices in memory.
    samples = []
    for y in (y_t, y_c):
        values = np.empty(n_boot)
        for start in range(0, n_boot, 128):
            end = min(start + 128, n_boot)
            indices = rng.integers(0, len(y), size=(end - start, len(y)))
            values[start:end] = stat(y[indices], axis=1)
        samples.append(values)
    st, sc = samples
    d = (st - sc) / sc if relative else st - sc
    lo, hi = np.quantile(d, [alpha / 2, 1 - alpha / 2])
    return float(lo), float(hi)


# ---------------------------------------------------------------- sequential testing
def msprt_pvalues(y_t, y_c, tau2: float, sigma2: float) -> np.ndarray:
    """Gaussian two-stream mSPRT with known common per-observation variance sigma2.

    Observations arrive in independent pairs; the effect prior is N(0, tau2), fixed before monitoring.
    The always-valid guarantee applies to the Gaussian known-variance model. Applying this formula to
    binary observations is a Gaussian approximation, whose calibration must be assessed separately.
    Neither variance parameter is estimated from future observations.
    """
    y_t, y_c = np.asarray(y_t, float), np.asarray(y_c, float)
    if y_t.ndim != 1 or y_c.ndim != 1 or not np.isfinite(y_t).all() or not np.isfinite(y_c).all():
        raise ValueError("observations must be finite one-dimensional arrays")
    if not np.isfinite(tau2) or tau2 <= 0 or not np.isfinite(sigma2) or sigma2 <= 0:
        raise ValueError("tau2 and sigma2 must be finite and positive, fixed before monitoring")
    n = min(len(y_t), len(y_c))
    y_t, y_c = y_t[:n], y_c[:n]
    k = np.arange(1, n + 1)
    delta = np.cumsum(y_t) / k - np.cumsum(y_c) / k
    v = 2 * sigma2  # variance of a single paired difference
    log_lam = -0.5 * np.log1p(k * tau2 / v) + k ** 2 * tau2 * delta ** 2 / (2 * v * (v + k * tau2))
    return np.exp(-np.maximum.accumulate(np.maximum(0.0, log_lam)))


def naive_peeking_pvalues(y_t, y_c, looks: np.ndarray) -> np.ndarray:
    """Fixed-horizon Welch p-value recomputed at each look (what a dashboard does). Inflates false positives."""
    return np.array([stats.ttest_ind(y_t[:m], y_c[:m], equal_var=False).pvalue for m in looks])


def holm(pvals: dict[str, float]) -> dict[str, float]:
    keys = sorted(pvals, key=pvals.get)
    m, out, running = len(keys), {}, 0.0
    for i, k in enumerate(keys):
        running = max(running, min(1.0, (m - i) * pvals[k]))
        out[k] = running
    return out
