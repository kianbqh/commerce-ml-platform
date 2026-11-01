import numpy as np
import pytest
from scipy import stats
from statsmodels.stats.proportion import proportions_ztest

from abtest import stats as S

def test_ztest_matches_statsmodels_and_ci_covers_truth():
    rng = np.random.default_rng(0)
    t = rng.binomial(1, 0.12, 20000); c = rng.binomial(1, 0.10, 20000)
    r = S.two_proportion_ztest(t, c)
    z, p = proportions_ztest([t.sum(), c.sum()], [len(t), len(c)])
    assert r.stat == pytest.approx(z) and r.p_value == pytest.approx(p)
    assert r.ci_low < 0.02 < r.ci_high


def test_welch_matches_scipy():
    rng = np.random.default_rng(1)
    t = rng.normal(1.0, 3, 500); c = rng.normal(0.5, 1, 800)
    r = S.welch_ttest(t, c)
    ref = stats.ttest_ind(t, c, equal_var=False)
    assert r.p_value == pytest.approx(ref.pvalue) and r.ci_low < 0.5 < r.ci_high


def test_sample_size_and_mde_are_consistent():
    n = S.sample_size_proportions(0.10, 0.01)
    assert 14000 < n < 16000                          # textbook value ~14.7k per arm
    assert S.mde_proportions(0.10, n) == pytest.approx(0.01, rel=0.05)


def test_cuped_reduces_variance_and_recovers_simulated_effect():
    rng = np.random.default_rng(2)
    n, rho = 40000, 0.7
    x = rng.normal(size=n); y = rho * x + np.sqrt(1 - rho ** 2) * rng.normal(size=n)
    treat = rng.random(n) < 0.5; y = y + 0.05 * treat
    ya = S.cuped_adjust(y, x)
    assert S.variance_reduction(y, ya) == pytest.approx(rho ** 2, abs=0.02)
    est = ya[treat].mean() - ya[~treat].mean()
    assert est == pytest.approx(0.05, abs=0.015)


def test_srm_detects_imbalance():
    assert not S.srm_check({"a": 10000, "b": 10050})["srm_detected"]
    assert S.srm_check({"a": 10000, "b": 10600})["srm_detected"]


def test_bootstrap_ci_close_to_analytic():
    rng = np.random.default_rng(3)
    t = rng.normal(1.2, 1, 3000); c = rng.normal(1.0, 1, 3000)
    lo, hi = S.bootstrap_diff_ci(t, c, n_boot=2000)
    a = S.welch_ttest(t, c)
    assert lo == pytest.approx(a.ci_low, abs=0.02) and hi == pytest.approx(a.ci_high, abs=0.02)


def test_gaussian_msprt_null_simulation_with_known_variance():
    fp = 0
    for s in range(100):
        r = np.random.default_rng(s)
        a, b = r.normal(0, 1, 5000), r.normal(0, 1, 5000)
        fp += (S.msprt_pvalues(a, b, tau2=0.01, sigma2=1.0) < 0.05).any()
    assert fp / 100 <= 0.08


def test_msprt_detects_real_effect_and_is_monotone():
    rng = np.random.default_rng(4)
    a = rng.binomial(1, 0.15, 8000).astype(float); b = rng.binomial(1, 0.10, 8000).astype(float)
    p = S.msprt_pvalues(a, b, tau2=1e-4, sigma2=0.09)
    assert p[-1] < 0.01 and np.all(np.diff(p) <= 0)


def test_holm():
    adj = S.holm({"a": 0.01, "b": 0.04, "c": 0.03})
    assert adj == {"a": 0.03, "c": 0.06, "b": 0.06}


def test_msprt_prefix_does_not_depend_on_future_observations():
    rng = np.random.default_rng(5)
    a, b = rng.normal(size=300), rng.normal(size=300)
    full = S.msprt_pvalues(a, b, tau2=0.01, sigma2=1.0)
    prefix = S.msprt_pvalues(a[:100], b[:100], tau2=0.01, sigma2=1.0)
    np.testing.assert_array_equal(full[:100], prefix)


@pytest.mark.parametrize("tau2,sigma2", [(0, 1), (1, 0), (-1, 1), (1, float("nan"))])
def test_msprt_rejects_invalid_variances(tau2, sigma2):
    with pytest.raises(ValueError):
        S.msprt_pvalues([0, 1], [1, 0], tau2=tau2, sigma2=sigma2)


def test_msprt_log_space_handles_large_evidence():
    with np.errstate(over="raise", invalid="raise"):
        p = S.msprt_pvalues(np.ones(2000), np.zeros(2000), tau2=1.0, sigma2=0.01)
    assert np.isfinite(p).all() and p[-1] == 0 and np.all(np.diff(p) <= 0)


def test_bootstrap_batching_matches_full_resamples():
    t, c = np.arange(10.0), np.arange(12.0)
    rng = np.random.default_rng(42)
    st = t[rng.integers(0, len(t), (300, len(t)))].mean(1)
    sc = c[rng.integers(0, len(c), (300, len(c)))].mean(1)
    reference = np.quantile(st - sc, [0.025, 0.975])
    np.testing.assert_allclose(S.bootstrap_diff_ci(t, c, n_boot=300, seed=42), reference)
