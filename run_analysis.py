"""End-to-end experiment readout. Usage: python run_analysis.py  -> reports/results.json, reports/figures/*.png"""
import json
import importlib.metadata
import os
import platform
import time
from datetime import datetime, timezone
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
from scipy import stats

from abtest import stats as S
from abtest.data import SHA256, URL, load, pre_covariates

ROOT = Path(__file__).resolve().parent
OUT, FIG = ROOT / "reports", ROOT / "reports" / "figures"
SEED, ALPHA, POWER = 42, 0.05, 0.8
AA_SEED, BOOTSTRAP_REPS, AA_SIMS = 43, 4000, 400
PLANNING_VISIT_RATE, SEQUENTIAL_TAU2 = 0.10, 0.01 ** 2
METRICS = {"visit": "binary", "conversion": "binary", "spend": "continuous"}


def main():
    t0 = time.time()
    FIG.mkdir(parents=True, exist_ok=True)
    df = load()
    X = pre_covariates(df)
    res = {
        "parameters": {"seed": SEED, "aa_seed": AA_SEED, "alpha": ALPHA, "power": POWER,
                       "bootstrap_reps": BOOTSTRAP_REPS, "aa_sims": AA_SIMS,
                       "sequential_planning_visit_rate": PLANNING_VISIT_RATE,
                       "sequential_tau2": SEQUENTIAL_TAU2},
        "dataset": {"rows": len(df), "arms": df["arm"].value_counts().to_dict(),
                    "source_url": URL, "sha256": SHA256,
                    "missing_values": {k: int(v) for k, v in df.isna().sum().items()},
                    "descriptives": {
                        arm: {m: {"mean": float(g[m].mean()), "sd": float(g[m].std()),
                                  "median": float(g[m].median()), "zero_fraction": float((g[m] == 0).mean())}
                              for m in METRICS}
                        for arm, g in df.groupby("arm")}},
    }

    # 1) SRM: designed as an equal 1/3 split
    res["srm"] = S.srm_check(df["arm"].value_counts().to_dict())

    # 2) Power analysis on the control baseline
    c = df[df.arm == "control"]
    p0 = c.visit.mean()
    n_arm = int(df.arm.value_counts().min())
    res["power"] = {
        "baseline_visit_rate": p0,
        "n_per_arm_available": n_arm,
        "mde_visit_abs_at_n": S.mde_proportions(p0, n_arm, ALPHA, POWER),
        "n_per_arm_for_1pp_visit_lift": S.sample_size_proportions(p0, 0.01, ALPHA, POWER),
        "n_per_arm_for_0.1pp_conversion_lift": S.sample_size_proportions(c.conversion.mean(), 0.001, ALPHA, POWER),
        "n_per_arm_for_$0.25_spend_lift": S.sample_size_means(c.spend.std(), 0.25, ALPHA, POWER),
    }

    # 3) Fixed-horizon tests + CUPED + bootstrap, each treatment arm vs control
    res["tests"], pvals = {}, {}
    for arm in ["mens", "womens"]:
        sub = df[df.arm.isin([arm, "control"])]
        is_t = (sub.arm == arm).to_numpy()
        Xs = X.loc[sub.index].to_numpy()
        for m, kind in METRICS.items():
            y = sub[m].to_numpy(float)
            base = (S.two_proportion_ztest(y[is_t], y[~is_t], m) if kind == "binary"
                    else S.welch_ttest(y[is_t], y[~is_t], m))
            y_h = S.cuped_adjust(y, Xs[:, [0]])          # CUPED with pre-period spend (history)
            y_m = S.cuped_adjust(y, Xs)                  # multivariate CUPED (all pre-period covariates)
            cup_h = S.welch_ttest(y_h[is_t], y_h[~is_t], m + "_cuped_history")
            cup_m = S.welch_ttest(y_m[is_t], y_m[~is_t], m + "_cuped_multi")
            boot = S.bootstrap_diff_ci(y[is_t], y[~is_t], n_boot=BOOTSTRAP_REPS, seed=SEED)
            boot_rel = S.bootstrap_diff_ci(y[is_t], y[~is_t], n_boot=BOOTSTRAP_REPS, seed=SEED, relative=True)
            key = f"{arm}_vs_control/{m}"
            res["tests"][key] = {
                "test": "two-proportion z" if kind == "binary" else "Welch t",
                "fixed": base.dict(),
                "bootstrap_ci_diff": boot, "bootstrap_ci_rel_lift": boot_rel,
                "cuped_history": {**cup_h.dict(), "variance_reduction": S.variance_reduction(y, y_h)},
                "cuped_multi": {**cup_m.dict(), "variance_reduction": S.variance_reduction(y, y_m),
                                "ci_width_ratio": (cup_m.ci_high - cup_m.ci_low) / (base.ci_high - base.ci_low)},
            }
            pvals[key] = base.p_value
    res["holm_adjusted_p"] = S.holm(pvals)

    # 4) Sequential testing. No timestamps in the data, so arrival order = a seeded random permutation.
    rng = np.random.default_rng(SEED)
    t_all = df[df.arm == "mens"].visit.to_numpy(float)
    t_ = t_all[rng.permutation(len(t_all))][:n_arm]
    c_ = c.visit.to_numpy(float)[rng.permutation(len(c))][:n_arm]
    tau2 = SEQUENTIAL_TAU2
    sigma2 = PLANNING_VISIT_RATE * (1 - PLANNING_VISIT_RATE)
    pv = S.msprt_pvalues(t_, c_, tau2=tau2, sigma2=sigma2)
    stop = int(np.argmax(pv < ALPHA)) + 1 if (pv < ALPHA).any() else None
    res["sequential"] = {"method": "Gaussian mSPRT approximation, mens vs control, binary visit",
                         "tau2": tau2, "sigma2": sigma2,
                         "parameter_selection": "fixed planning baseline 0.10 and effect-prior SD 0.01",
                         "arrival_order": "seeded independent random permutation of each arm",
                         "stopped_at_n_per_arm": stop, "fraction_of_full_sample": stop / n_arm if stop else None,
                         "final_running_p": float(pv[-1])}
    fig, ax = plt.subplots(figsize=(7, 3.5))
    ax.semilogy(np.arange(1, n_arm + 1), pv); ax.axhline(ALPHA, ls="--", c="r", label="alpha = 0.05")
    ax.set_ylim(max(float(pv[-1]) * 0.1, np.finfo(float).tiny), 1.0)
    ax.legend(loc="upper right")
    ax.set_xlabel("users per arm"); ax.set_ylabel("running p (Gaussian approximation)")
    ax.set_title("Visit replay: Mens e-mail vs control")
    fig.tight_layout(); fig.savefig(FIG / "msprt_visit.png", dpi=120); plt.close(fig)

    # 5) A/A simulation: why peeking needs a sequential test
    n_sims, looks = AA_SIMS, np.linspace(500, len(c) // 2, 20).astype(int)
    rng = np.random.default_rng(AA_SEED)
    yc = c.visit.to_numpy(float)
    naive_fp = seq_fp = fixed_fp = 0
    for _ in range(n_sims):
        perm = rng.permutation(len(yc)); h = len(yc) // 2
        a, b = yc[perm[:h]], yc[perm[h:2 * h]]
        naive = S.naive_peeking_pvalues(a, b, looks)
        naive_fp += (naive < ALPHA).any(); fixed_fp += naive[-1] < ALPHA
        seq_fp += (S.msprt_pvalues(a, b, tau2=tau2, sigma2=sigma2)[looks - 1] < ALPHA).any()
    counts = {"fixed_horizon": int(fixed_fp), "naive_peeking": int(naive_fp), "gaussian_msprt": int(seq_fp)}
    res["aa_simulation"] = {"n_sims": n_sims, "looks": len(looks),
                            "fixed_horizon_fpr": fixed_fp / n_sims,
                            "naive_peeking_fpr": naive_fp / n_sims, "msprt_peeking_fpr": seq_fp / n_sims,
                            "rejections": counts,
                            "rejection_rate_ci95": {
                                k: list(stats.binomtest(v, n_sims).proportion_ci(confidence_level=0.95))
                                for k, v in counts.items()}}

    # figure: CUPED CI widths
    fig, ax = plt.subplots(figsize=(7, 3.5))
    keys = [k for k in res["tests"] if k.endswith("spend")]
    for i, k in enumerate(keys):
        for j, (lab, d) in enumerate([("raw", res["tests"][k]["fixed"]), ("CUPED", res["tests"][k]["cuped_multi"])]):
            ax.errorbar(d["diff"], i + j * 0.2, xerr=[[d["diff"] - d["ci_low"]], [d["ci_high"] - d["diff"]]],
                        fmt="o", label=lab if i == 0 else None, color="C%d" % j)
    ax.axvline(0, c="k", lw=0.5); ax.set_yticks(range(len(keys)))
    ax.set_yticklabels([k.split("_vs_")[0].capitalize() + " e-mail" for k in keys])
    ax.set_xlabel("spend difference ($/customer, 95% CI)"); ax.legend(); fig.tight_layout()
    fig.savefig(FIG / "cuped_spend_ci.png", dpi=120); plt.close(fig)

    res["runtime_s"] = round(time.time() - t0, 1)
    res["environment"] = {
        "run_utc": datetime.now(timezone.utc).isoformat(), "platform": platform.platform(),
        "python": platform.python_version(),
        "thread_limits": {name: os.environ.get(name)
                          for name in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS")},
        "packages": {p: importlib.metadata.version(p)
                     for p in ("numpy", "pandas", "scipy", "statsmodels", "matplotlib", "pytest", "nbconvert")},
    }
    (OUT / "results.json").write_text(json.dumps(res, indent=2, default=float))
    print(json.dumps({k: res[k] for k in ["srm", "power", "sequential", "aa_simulation", "runtime_s"]}, indent=1, default=float))
    for k, v in res["tests"].items():
        f, cm = v["fixed"], v["cuped_multi"]
        print(f"{k:28s} diff={f['diff']:+.5f} rel={f['rel_lift']:+.1%} p={f['p_value']:.2e} "
              f"| CUPED var red hist={v['cuped_history']['variance_reduction']:.1%} multi={cm['variance_reduction']:.1%} "
              f"CIwidth x{cm['ci_width_ratio']:.3f}")


if __name__ == "__main__":
    main()
