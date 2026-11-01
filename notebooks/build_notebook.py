"""Build the readout notebook; execute it after run_analysis.py has written the report."""
from pathlib import Path

import nbformat as nbf


cells = [
    ("md", """# E-mail campaign experiment readout

I used the Hillstrom / MineThatData experiment to work through an A/B readout: design checks, effect sizes,
power, CUPED, and repeated monitoring. There are 64,000 customers and three randomized arms, with visit,
conversion, and spend measured over the next two weeks.

This notebook reads the generated report and recomputes the tests, bootstrap intervals, and CUPED values
with its recorded parameters. The assertions keep the notebook and command-line analysis in sync."""),
    ("code", """import json, sys
from pathlib import Path
import numpy as np
import pandas as pd
from IPython.display import Image, display

root = Path.cwd()
if not (root / 'abtest').is_dir():
    root = root.parent
sys.path.insert(0, str(root))
from abtest import stats as S
from abtest.data import load, pre_covariates

res = json.loads((root / 'reports/results.json').read_text())
params = res['parameters']
df = load()
X = pre_covariates(df)
assert len(df) == res['dataset']['rows']
df.groupby('arm')[['visit', 'conversion', 'spend']].agg(['mean', 'std', 'count']).round(4)"""),
    ("md", "## 1. Assignment counts and power\nThe SRM check uses the designed equal three-way allocation. Power uses the control rate as a planning baseline."),
    ("code", """srm = S.srm_check(df.arm.value_counts().to_dict())
np.testing.assert_allclose(srm['p_value'], res['srm']['p_value'])
c = df[df.arm == 'control']
n = int(df.arm.value_counts().min())
np.testing.assert_allclose(S.mde_proportions(c.visit.mean(), n), res['power']['mde_visit_abs_at_n'])
display(pd.Series(srm))
pd.Series(res['power'])"""),
    ("md", """## 2. Fixed-horizon effects

Binary outcomes use two-proportion z-tests and Wald intervals for the absolute rate difference. Spend uses
Welch's t-test. Bootstrap intervals resample customers independently within each arm, using 4,000 draws
and seed 42. The 95% intervals are marginal; Holm adjusts the six fixed-horizon p-values as one family."""),
    ("code", """rows = []
for arm in ['mens', 'womens']:
    sub = df[df.arm.isin([arm, 'control'])]
    t = (sub.arm == arm).to_numpy()
    for metric in ['visit', 'conversion', 'spend']:
        y = sub[metric].to_numpy(float)
        key = f'{arm}_vs_control/{metric}'
        saved = res['tests'][key]
        test = S.two_proportion_ztest(y[t], y[~t], metric) if metric != 'spend' else S.welch_ttest(y[t], y[~t], metric)
        for field in ['control', 'treatment', 'diff', 'ci_low', 'ci_high', 'p_value']:
            np.testing.assert_allclose(getattr(test, field), saved['fixed'][field], rtol=1e-12, atol=0)
        for relative, field in [(False, 'bootstrap_ci_diff'), (True, 'bootstrap_ci_rel_lift')]:
            boot = S.bootstrap_diff_ci(y[t], y[~t], n_boot=params['bootstrap_reps'], seed=params['seed'], relative=relative)
            np.testing.assert_allclose(boot, saved[field], rtol=1e-12, atol=0)
        rows.append({'comparison': key, **test.dict(), 'bootstrap_abs': saved['bootstrap_ci_diff'],
                     'bootstrap_relative': saved['bootstrap_ci_rel_lift'], 'holm_p': res['holm_adjusted_p'][key]})
tab = pd.DataFrame(rows)
adjusted = S.holm(dict(zip(tab.comparison, tab.p_value)))
np.testing.assert_allclose([adjusted[k] for k in tab.comparison], tab.holm_p, rtol=1e-12, atol=0)
print('Verified all 6 fixed-horizon results and both bootstrap intervals per comparison.')
tab[['comparison', 'control', 'treatment', 'diff', 'ci_low', 'ci_high', 'holm_p', 'bootstrap_relative']]"""),
    ("md", """## 3. CUPED

The covariates describe customers before the campaign: past spend, recency, earlier product-line purchases,
new-customer status, ZIP category, and channel. The adjustment is fitted on the pooled treatment/control
sample. The reported variance reduction is the in-sample residual reduction; Welch intervals compare the
adjusted group means. Weak prediction of the two-week outcomes leaves only a small precision gain."""),
    ("code", """cuped_rows = []
for arm in ['mens', 'womens']:
    sub = df[df.arm.isin([arm, 'control'])]
    t = (sub.arm == arm).to_numpy()
    xs = X.loc[sub.index].to_numpy()
    for metric in ['visit', 'conversion', 'spend']:
        y = sub[metric].to_numpy(float)
        key = f'{arm}_vs_control/{metric}'
        entry = {'comparison': key}
        for name, covariates in [('cuped_history', xs[:, [0]]), ('cuped_multi', xs)]:
            adjusted_y = S.cuped_adjust(y, covariates)
            test = S.welch_ttest(adjusted_y[t], adjusted_y[~t])
            reduction = S.variance_reduction(y, adjusted_y)
            saved = res['tests'][key][name]
            np.testing.assert_allclose([test.diff, test.ci_low, test.ci_high, reduction],
                                       [saved['diff'], saved['ci_low'], saved['ci_high'], saved['variance_reduction']],
                                       rtol=1e-12, atol=1e-14)
            entry[name + '_variance_reduction'] = reduction
        entry['ci_width_ratio'] = res['tests'][key]['cuped_multi']['ci_width_ratio']
        cuped_rows.append(entry)
print('Verified history and multivariate CUPED for all 6 comparisons.')
pd.DataFrame(cuped_rows).round(5)"""),
    ("md", """## 4. Sequential replay and A/A calibration

The data contains no arrival timestamps, so the replay independently shuffles each arm with seed 42.
The Gaussian mSPRT uses a fixed 0.10 planning visit rate (`sigma2=0.09`) and an effect-prior SD of 1 pp
(`tau2=0.0001`). Its exact always-valid result assumes Gaussian observations with known common variance;
visits use the Gaussian approximation. The A/A experiment reports empirical rejection frequencies over
400 control-group splits at 20 looks, with an independent seed 43 and Monte Carlo intervals."""),
    ("code", """rng = np.random.default_rng(params['seed'])
t_all = df[df.arm == 'mens'].visit.to_numpy(float)
t = t_all[rng.permutation(len(t_all))][:n]
control = c.visit.to_numpy(float)[rng.permutation(len(c))][:n]
pv = S.msprt_pvalues(t, control, tau2=res['sequential']['tau2'], sigma2=res['sequential']['sigma2'])
crossing = int(np.flatnonzero(pv < params['alpha'])[0]) + 1 if (pv < params['alpha']).any() else None
assert crossing == res['sequential']['stopped_at_n_per_arm']
np.testing.assert_allclose(pv[-1], res['sequential']['final_running_p'], rtol=1e-12, atol=0)
display(pd.Series(res['sequential']))
res['aa_simulation']"""),
    ("code", """display(Image(filename=str(root / 'reports/figures/msprt_visit.png')))
display(Image(filename=str(root / 'reports/figures/cuped_spend_ci.png')))
print('All notebook/report consistency checks passed.')"""),
]

nb = nbf.v4.new_notebook()
nb.cells = [nbf.v4.new_markdown_cell(text) if kind == "md" else nbf.v4.new_code_cell(text)
            for kind, text in cells]
nb.metadata["kernelspec"] = {"name": "python3", "display_name": "Python 3", "language": "python"}
nbf.write(nb, Path(__file__).with_name("ab_test_readout.ipynb"))
