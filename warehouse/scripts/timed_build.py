"""Run the dbt lifecycle with wall-clock timings -> results/build_timings.json.
1) full refresh with only the 'prior' file, 2) incremental run adding the 'train' file,
3) dbt test, 4) full refresh of everything for comparison, 5) dbt test again."""
import json, re, subprocess, sys, time

STEPS = [
    ("full_refresh_prior_only", ["dbt", "run", "--full-refresh", "--vars", '{eval_sets: ["prior"]}']),
    ("incremental_add_train", ["dbt", "run"]),
    ("dbt_test_after_incremental", ["dbt", "test"]),
    ("full_refresh_all", ["dbt", "run", "--full-refresh"]),
    ("dbt_test_after_full_refresh", ["dbt", "test"]),
]
out = {}
for name, cmd in STEPS:
    t = time.perf_counter(); p = subprocess.run(cmd, capture_output=True, text=True); el = time.perf_counter() - t
    open(f"results/{name}.log", "w").write(p.stdout + p.stderr)
    done = re.findall(r"Done\. PASS=(\d+) WARN=(\d+) ERROR=(\d+)", p.stdout)
    model_times = dict(re.findall(r"OK created sql \w+ model [\w.]+?\.(\w+) .*?in ([\d.]+)s", p.stdout))
    out[name] = {"seconds": round(el, 2), "exit": p.returncode,
                 "pass_warn_error": [int(x) for x in done[-1]] if done else None,
                 "model_seconds": {k: float(v) for k, v in model_times.items()}}
    print(name, out[name]["seconds"], "s", out[name]["pass_warn_error"])
    if p.returncode != 0:
        print(p.stdout[-3000:]); sys.exit(p.returncode)
json.dump(out, open("results/build_timings.json", "w"), indent=2)
