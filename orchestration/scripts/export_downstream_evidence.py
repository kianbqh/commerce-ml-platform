"""Publish JSON aggregates and ID-free HTTP timings from a closed M4 child.

This stdlib-only extractor never opens databases, arrays, model weights, CSVs,
or services. It preserves the private originals and creates a new output tree.
Run only after independent acceptance of the actual full downstream child.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import math
from pathlib import Path
import re
import statistics

SEEDS = [42, 43, 44]
METHODS = ("two_tower_hnsw", "two_tower_lambdamart", "training_popularity")
COUNT_METRICS = ("users", "heldout_positive_products")
SCORE_METRICS = tuple(f"{name}@{k}" for k in (10, 100) for name in ("recall", "ndcg", "micro_recall"))
PARENT_RECEIPTS = (
    "config.json", "initialization.json", "arrival_manifest.json", "promoted.json",
    "provenance.json", "full-incremental-parity.json", "terminal/gate.json",
    "terminal/feature-export.json", "spark-original/spark-dbt-parity.json",
    "manual-run.json", "model-run.json", "api-run.json",
)
MODEL_PACKAGES = ("numpy", "torch", "faiss-cpu", "lightgbm", "duckdb", "fastapi", "uvicorn", "httpx", "scipy", "pandas")
ORCHESTRATOR_PACKAGES = ("dagster", "dagster-dbt", "dbt-core", "dbt-duckdb", "duckdb", "pyspark", "torch", "numpy", "faiss-cpu", "lightgbm", "fastapi")
MAX_JSON_BYTES = 16 * 1024 * 1024
CHECK_ULPS = 32


def require(value, message):
    if not value:
        raise ValueError(message)


def fields(record, names):
    require(isinstance(record, dict), "expected JSON object")
    return {name: record[name] for name in names}


def number(value, *, integer=False, positive=False, unit=False):
    require(not isinstance(value, bool) and isinstance(value, (int, float)), "missing/invalid numeric field")
    require(math.isfinite(value) and value >= 0, "nonfinite/negative numeric field")
    require(not integer or isinstance(value, int), "expected integer field")
    require(not positive or value > 0, "expected positive field")
    require(not unit or value <= 1, "score outside [0,1]")
    return value


def text(value):
    require(isinstance(value, str) and bool(value), "missing/invalid text field")
    return value


def digest(value, length=64):
    require(isinstance(value, str) and re.fullmatch(r"[0-9a-f]{" + str(length) + "}", value), "invalid identity hash")
    return value


def canonical_hash(value):
    payload = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def bytes_hash(payload):
    return hashlib.sha256(payload).hexdigest()


def near(original, recomputed):
    """32 binary64 ulps at max(1, |a|, |b|); never replace original values."""
    number(original)
    number(recomputed)
    tolerance = CHECK_ULPS * math.ulp(max(1.0, abs(original), abs(recomputed)))
    require(abs(original - recomputed) <= tolerance, "aggregate does not match original samples")


def time_pair(record):
    result = fields(record, ("started_utc", "finished_utc"))
    start, finish = (datetime.fromisoformat(text(result[key])) for key in ("started_utc", "finished_utc"))
    require(start.tzinfo is not None and finish.tzinfo is not None and finish >= start, "invalid aware UTC interval")
    require(start.utcoffset().total_seconds() == finish.utcoffset().total_seconds() == 0, "interval must use UTC")
    return result


def _object(pairs):
    result = {}
    for key, value in pairs:
        require(key not in result, "duplicate JSON field")
        result[key] = value
    return result


def _finite_tree(value):
    if isinstance(value, float):
        require(math.isfinite(value), "nonfinite JSON number")
    elif isinstance(value, dict):
        for item in value.values():
            _finite_tree(item)
    elif isinstance(value, list):
        for item in value:
            _finite_tree(item)


class Evidence:
    def __init__(self):
        self.sources = {}
        self.paths = {}

    def read(self, root, relative, label):
        root = Path(root).resolve(strict=True)
        path = (root / relative).resolve(strict=True)
        require(root in path.parents and path.suffix == ".json", "JSON reference escapes its accepted evidence root")
        if label in self.sources:
            require(self.paths[label] == path, "duplicate source label with another path")
            return self.sources[label]["value"]
        before = path.stat()
        require(0 < before.st_size <= MAX_JSON_BYTES, "JSON evidence exceeds bounded read")
        with path.open("rb") as stream:
            payload = stream.read(MAX_JSON_BYTES + 1)
        after = path.stat()
        require(len(payload) == before.st_size == after.st_size and before.st_mtime_ns == after.st_mtime_ns,
                "source changed during extraction")
        value = json.loads(payload.decode("utf-8-sig"), object_pairs_hook=_object)
        require(isinstance(value, dict), "JSON evidence must be an object")
        _finite_tree(value)
        self.sources[label] = {"bytes": len(payload), "sha256": bytes_hash(payload), "value": value}
        self.paths[label] = path
        return value

    def sha(self, label):
        return self.sources[label]["sha256"]

    def verify_unchanged(self):
        for label, path in self.paths.items():
            with path.open("rb") as stream:
                payload = stream.read(MAX_JSON_BYTES + 1)
            require(bytes_hash(payload) == self.sha(label), "private source changed before publication: " + label)

    def identities(self):
        return {label: fields(record, ("bytes", "sha256")) for label, record in self.sources.items()}


def same_reference(reference, expected):
    require(Path(text(reference)).resolve() == Path(expected).resolve(), "receipt reference belongs to another artifact")


def same_provenance(record, provenance):
    require(record["provenance"] == provenance, "artifact source/config/environment lineage differs")


def process_timer(record, provenance):
    same_provenance(record, provenance)
    require(record["returncode"] == 0 and not isinstance(record["returncode"], bool) and record["error"] is None,
            "required process did not succeed")
    return {**time_pair(record), "wall_seconds": number(record["wall_seconds"]),
            "timer_includes": text(record["timer_includes"])}


def metric_values(record, users=None):
    result = {key: number(record[key], integer=True, positive=True) for key in COUNT_METRICS}
    require(users is None or result["users"] == users, "metric user count differs from accepted dataset")
    result.update({key: number(record[key], unit=True) for key in SCORE_METRICS})
    result["definition"] = text(record["definition"])
    return result


def percentiles(values):
    ordered = sorted(values)
    result = {}
    for percentile in (50, 95, 99):
        # NumPy linear quantiles first convert p to binary64 q=p/100.
        position = (len(ordered) - 1) * (percentile / 100)
        low = math.floor(position)
        high = math.ceil(position)
        result[f"p{percentile}"] = ordered[low] + (ordered[high] - ordered[low]) * (position - low)
    return result


def export(run_root, out):
    root, output = Path(run_root).resolve(strict=True), Path(out).resolve()
    require(not output.exists(), "output must be a new directory")
    require(output != root and root not in output.parents, "do not write inside accepted child")
    ev = Evidence()
    run = ev.read(root, "downstream-run.json", "child/downstream-run.json")
    require(run["success"] is True and run["error"] is None, "downstream child did not finish successfully")
    run_id = text(run["run_id"])
    require(run["selected_assets"] == ["recsys_train_eval", "api_benchmark"], "wrong selected assets")
    child_time = {**time_pair(run), "wall_seconds": number(run["wall_seconds"]), "timer_scope": text(run["timer_scope"])}
    digest(run["config_file_sha256"])
    events = ev.read(root, "downstream-events.json", "child/downstream-events.json")["events"]
    require(isinstance(events, list) and all(isinstance(event, dict) for event in events), "invalid materialization events")
    keys = [event["asset_key"] for event in events if event["event_type"] == "ASSET_MATERIALIZATION"]
    require(sorted(keys) == [["api_benchmark"], ["recsys_train_eval"]], "need exactly two actual downstream materializations")
    binding = ev.read(root, "input-binding.json", "child/input-binding.json")
    before = ev.read(root, "inputs-before.json", "child/inputs-before.json")
    verified = ev.read(root, "input-verification.json", "child/input-verification.json")
    provenance = ev.read(root, "provenance.json", "child/provenance.json")
    config = ev.read(root, "config.json", "child/config.json")
    parent = binding["parent"]
    require(binding["schema"] == "m4-accepted-terminal-cache-v1", "wrong input binding schema")
    require(run["parent"] == verified["parent"] == before["parent"] == provenance["parent_data_run"] == parent,
            "child receipts disagree on parent lineage")
    require(verified["unchanged"] is True and verified["error"] is None, "accepted inputs were not verified unchanged")
    require(verified["binding_sha256"] == ev.sha("child/input-binding.json"), "input binding hash mismatch")
    for key in ("database_stamp", "cache_snapshot"):
        require(before[key] == verified[key] == binding[key], "accepted before/after input identity mismatch")
    require(isinstance(binding["cache_snapshot"], dict) and binding["cache_snapshot"], "missing accepted cache identity")
    for record in binding["cache_snapshot"].values():
        number(record["bytes"], integer=True)
        digest(record["sha256"])
    stamp = binding["database_stamp"]
    require(binding["database"] == stamp["path"], "bound database path/stamp differs")
    number(stamp["bytes"], integer=True, positive=True)
    number(stamp["mtime_ns"], integer=True, positive=True)
    digest(stamp["feature_definition_sha256"])
    for key in ("feature_manifest_sha256", "split_sha256", "feature_fingerprint"):
        digest(binding[key])
    parent_root = Path(parent["run_root"]).resolve(strict=True)
    require(root != parent_root and parent_root not in root.parents and root not in parent_root.parents,
            "parent and child must be separate evidence roots")
    require(output != parent_root and parent_root not in output.parents, "do not write inside accepted parent")
    same_reference(binding["database"], parent_root / "terminal/warehouse.duckdb")
    same_reference(binding["feature_cache"], parent_root / "terminal/features")
    require(set(parent["receipt_sha256"]) == set(PARENT_RECEIPTS), "parent receipt set is incomplete or unexpected")
    parent_records = {}
    for name in PARENT_RECEIPTS:
        parent_records[name] = ev.read(parent_root, name, "parent/" + name)
        require(ev.sha("parent/" + name) == digest(parent["receipt_sha256"][name]), "accepted parent receipt changed: " + name)
    parent_config = parent_records["config.json"]
    require(parent_records["manual-run.json"]["success"] is True and parent_records["manual-run.json"]["run_id"] == parent["manual_run_id"],
            "parent did not close successfully")
    require(parent_records["provenance.json"]["git_sha"] == digest(parent["source_git_sha"], 40), "wrong parent source")
    require(parent_config["training_enabled"] is False and not parent_config.get("fixture") and parent_config.get("data_kind") != "synthetic_test",
            "export requires the original full data-only parent")
    require(parent_records["model-run.json"]["status"] == parent_records["api-run.json"]["status"] == "not run", "parent data-only records changed")
    require(parent_records["initialization.json"]["scenario"] == "original", "parent is not the original data run")
    for name in ("full-incremental-parity.json", "terminal/gate.json", "spark-original/spark-dbt-parity.json"):
        require(parent_records[name]["passed"] is True, "parent acceptance incomplete: " + name)
    require(parent_records["terminal/gate.json"]["original_source_only"] is True, "parent terminal is not original-only")
    feature = parent_records["terminal/feature-export.json"]
    require(feature["only_complete_original_snapshot"] is True, "parent features are incomplete")
    for key in ("split_sha256", "feature_fingerprint", "feature_manifest_sha256"):
        require(feature[key] == binding[key], "parent feature receipt differs from child binding")
    require(sum(batch["lines"] for batch in parent_records["arrival_manifest.json"]["batches"]) == 33819106,
            "full parent must account for all declared labeled lines")
    expected_config = {**parent_config, "training_enabled": True, "cache_only": True, "model_python": config["model_python"]}
    require(config == expected_config, "child effective settings differ from accepted parent protocol")
    require(config["training_enabled"] is True and config["cache_only"] is True, "training/cache-only flags must be true")
    for key, expected in (("training_seeds", SEEDS), ("http_seed", 42), ("training_epochs", 3),
                          ("ranker_rounds", 120), ("candidates", 100), ("http_requests", 5000), ("http_concurrency", 8)):
        require(config[key] == expected, "full protocol mismatch: " + key)
    require(provenance["config_sha256"] == canonical_hash(config), "effective config hash mismatch")
    digest(provenance["git_sha"], 40)
    require(provenance["orchestrator_executable"] == provenance["executable"], "orchestrator identity differs")
    model = ev.read(root, "model-run.json", "child/model-run.json")
    api = ev.read(root, "api-run.json", "child/api-run.json")
    for record in (model, api):
        same_provenance(record, provenance)
        same_reference(record["input_binding"], root / "input-binding.json")
        require(record["model_python"] == config["model_python"], "model interpreter identity differs")
    require(model["status"] == api["status"] == "completed" and model["seeds"] == SEEDS and model["cache_only"] is True,
            "three-seed model/API path incomplete")
    require(model["http_seed_predeclared"] == api["predeclared_seed"] == 42, "wrong serving seed")
    require(api["excluded_warmups"] == 50 and api["repetitions"] == 1 and api["concurrency"] == 8, "wrong HTTP protocol")
    same_reference(model["model_python_identity"], root / "model-python-identity.json")
    model_env = ev.read(root, "model-python-identity.json", "child/model-python-identity.json")
    require(model_env["executable"] == config["model_python"], "actual model interpreter differs from requested interpreter")
    preflight = process_timer(ev.read(root, "model-python-preflight/process.json", "child/model-python-preflight/process.json"), provenance)
    require(len(model["per_seed_paths"]) == len(SEEDS), "missing per-seed metric references")
    seeds, ids, weight_ids, dataset_public = [], [], [], None
    for seed, reference in zip(SEEDS, model["per_seed_paths"]):
        directory = f"models/seed-{seed}"
        same_reference(reference, root / directory / "metrics.json")
        metrics = ev.read(root, directory + "/metrics.json", "child/" + directory + "/metrics.json")
        data, tower, ranker, ann = (metrics[key] for key in ("dataset", "two_tower", "ranker", "faiss"))
        require(data["database"] == stamp and all(data[key] == binding[key] for key in ("split_sha256", "feature_fingerprint")), "seed dataset differs from binding")
        dataset = {key: number(data[key], integer=True) for key in ("users", "products", "raw_orders", "excluded_users_fewer_than_three_labeled_baskets", "unlabeled_orders", "excluded_labeled_orders")}
        for key in ("users", "products"):
            number(dataset[key], integer=True, positive=True)
        for key in ("line_counts", "order_counts"):
            dataset[key] = {split: number(data[key][split], integer=True, positive=True) for split in ("train", "validation", "test")}
        dataset.update(fields(data, ("split_sha256", "feature_fingerprint")))
        dataset.update({key: text(data[key]) for key in ("split_rule", "test_history", "global_priors")})
        require(dataset_public is None or dataset == dataset_public, "per-seed dataset aggregates differ")
        dataset_public = dataset
        require(tower["seed"] == seed and tower["dimension"] == 32 and tower["device"] == "cpu", "wrong tower run identity")
        require(len(tower["epochs"]) == 3, "missing training epochs")
        positive_rows = number(tower["positive_rows_per_epoch"], integer=True, positive=True)
        require(positive_rows == dataset["line_counts"]["train"], "training did not cover declared positive rows")
        epochs = []
        for index, epoch in enumerate(tower["epochs"], 1):
            number(epoch["epoch"], integer=True, positive=True)
            number(epoch["examples"], integer=True, positive=True)
            require(epoch["epoch"] == index and epoch["examples"] == positive_rows, "epoch identity/coverage differs")
            epochs.append({"epoch": index, "examples": positive_rows, "loss": number(epoch["loss"]), "seconds": number(epoch["seconds"])})
        initial, final = (digest(tower[key]) for key in ("initial_weights_sha256", "final_weights_sha256"))
        require(initial != final, "weights did not change during fitting")
        identity = ranker["identity"]
        for key, expected in (("seed", seed), ("epochs", 3), ("dimension", 32), ("candidates", 100), ("rounds", 120),
                              ("split_sha256", binding["split_sha256"]), ("feature_fingerprint", binding["feature_fingerprint"])):
            require(identity[key] == expected, "ranker identity mismatch: " + key)
        for key in ("model_definition_sha256", "fit_user_sha256", "early_stop_user_sha256", "ranking_definition_sha256"):
            digest(identity[key])
        model_id = digest(metrics["model_run_id"])
        calculated_id = bytes_hash(json.dumps({"ranker": identity, "weights": final}, sort_keys=True).encode())
        require(model_id == calculated_id, "model ID does not bind ranker and final tower identity")
        saved_ranker = ev.read(root, directory + "/artifacts/ranker.json", "child/" + directory + "/artifacts/ranker.json")
        require(saved_ranker == ranker, "metrics and saved ranker receipt differ")
        serving = ev.read(root, directory + "/artifacts/serving.json", "child/" + directory + "/artifacts/serving.json")
        require(serving["model_run_id"] == model_id and serving["candidates"] == 100 and serving["history_stage"] == "test", "saved serving identity differs")
        same_reference(serving["feature_cache"], binding["feature_cache"])
        require(all(serving[key] == binding[key] for key in ("split_sha256", "feature_fingerprint")), "serving feature identity differs")
        require(set(metrics["test"]) == set(METHODS), "missing or unexpected test method")
        test = {method: metric_values(metrics["test"][method], dataset["users"]) for method in METHODS}
        require(len({record["heldout_positive_products"] for record in test.values()}) == 1, "test methods disagree on held-out positives")
        rank_counts = {key: number(ranker[key], integer=True) for key in ("fit_users", "early_stop_users", "fit_candidate_rows", "early_stop_candidate_rows", "fit_positive_rows", "early_stop_positive_rows", "best_iteration")}
        require(rank_counts["fit_users"] + rank_counts["early_stop_users"] == dataset["users"], "ranker user-group counts differ")
        for group in ("fit", "early_stop"):
            require(rank_counts[group + "_candidate_rows"] == rank_counts[group + "_users"] * 100, "ranker candidate count differs")
            require(rank_counts[group + "_positive_rows"] <= rank_counts[group + "_candidate_rows"], "invalid ranker positive count")
        require(rank_counts["best_iteration"] <= 120, "ranker iteration exceeds declared rounds")
        coverage = fields(ranker["candidate_coverage"], COUNT_METRICS)
        coverage.update({key: ranker["candidate_coverage"][key] for key in ("recall@100", "ndcg@100", "micro_recall@100", "definition")})
        for key in COUNT_METRICS:
            number(coverage[key], integer=True, positive=True)
        require(coverage["users"] == dataset["users"], "ranker coverage user count differs")
        for key in ("recall@100", "ndcg@100", "micro_recall@100"):
            number(coverage[key], unit=True)
        text(coverage["definition"])
        require(isinstance(ranker["features"], list) and all(isinstance(name, str) and name for name in ranker["features"]), "missing ranker feature names")
        require(set(ranker["feature_gain"]) == set(ranker["features"]), "ranker feature importance names differ")
        gain = {name: number(ranker["feature_gain"][name]) for name in ranker["features"]}
        require(ann["catalog_products"] == dataset["products"] and ann["reorders_eligible"] is True, "FAISS catalog identity differs")
        ann_public = fields(ann, ("index", "efConstruction", "efSearch", "catalog_products", "reorders_eligible", "exact_topk_agreement", "agreement_users", "agreement_user_sha256"))
        text(ann_public["index"])
        for key in ("efConstruction", "efSearch", "catalog_products", "agreement_users"):
            number(ann_public[key], integer=True, positive=True)
        require(ann_public["agreement_users"] == min(2000, dataset["users"]), "FAISS diagnostic aggregate count differs")
        number(ann_public["exact_topk_agreement"], unit=True)
        digest(ann_public["agreement_user_sha256"])
        runtime = fields(metrics["runtime"], ("python", "torch", "numpy", "faiss", "lightgbm", "threads", "device", "peak_process_rss_mib"))
        text(runtime["python"])
        require(runtime["device"] == "cpu" and tower["threads"] == runtime["threads"], "actual model device/thread identity differs")
        number(runtime["threads"], integer=True, positive=True)
        number(runtime["peak_process_rss_mib"])
        for key, package in (("torch", "torch"), ("numpy", "numpy"), ("faiss", "faiss-cpu"), ("lightgbm", "lightgbm")):
            require(runtime[key] == model_env["packages"][package]["imported_version"], "actual model import/runtime versions differ")
        process = process_timer(ev.read(root, directory + "/process/process.json", "child/" + directory + "/process/process.json"), provenance)
        tower_public = {"seed": seed, "epochs": epochs, "seconds": number(tower["seconds"]),
                        "positive_rows_per_epoch": positive_rows, "initial_weights_sha256": initial, "final_weights_sha256": final,
                        **{key: number(tower[key], integer=True, positive=True) for key in ("batch_size", "dimension", "negatives_per_positive", "threads")},
                        **{key: text(tower[key]) for key in ("negative_rule", "objective", "device")}}
        seeds.append({"seed": seed, "model_run_id": model_id, "test": test, "two_tower": tower_public,
                      "ranker": {"identity": fields(identity, ("split_sha256", "feature_fingerprint", "model_definition_sha256", "epochs", "seed", "dimension", "candidates", "rounds", "fit_user_sha256", "early_stop_user_sha256", "ranking_definition_sha256")),
                                 **rank_counts, "candidate_coverage": coverage, "feature_gain": gain,
                                 "feature_seconds": number(ranker["feature_seconds"]), "fit_seconds": number(ranker["fit_seconds"]),
                                 "labels": text(ranker["labels"]), "early_stopping_metric": text(ranker["early_stopping_metric"])},
                      "faiss": ann_public, "runtime": runtime,
                      "evaluation_seconds": number(metrics["evaluation_seconds"]), "total_pipeline_seconds": number(metrics["total_pipeline_seconds"]),
                      "timing_scope": text(metrics["timing_scope"]), "process": process})
        ids.append(model_id)
        weight_ids.append(final)
    require(len(set(ids)) == len(set(weight_ids)) == 3, "three seeds do not have distinct fitted identities")
    aggregate = {}
    require(set(model["summary"]) == set(METHODS), "model summary method set differs")
    for method in METHODS:
        aggregate[method] = {}
        require(set(model["summary"][method]) == set(COUNT_METRICS + SCORE_METRICS), "model summary metric set differs")
        for metric in COUNT_METRICS + SCORE_METRICS:
            values = [record["test"][method][metric] for record in seeds]
            original = model["summary"][method][metric]
            require(original["values"] == values and original["std_definition"] == "population ddof=0", "model summary samples/estimator differ")
            for value in original["values"]:
                number(value)
            near(original["mean"], statistics.mean(values))
            near(original["std"], statistics.pstdev(values))
            aggregate[method][metric] = fields(original, ("mean", "std", "std_definition", "values"))
    same_reference(api["raw_http"], root / "api/raw-http.json")
    raw = ev.read(root, "api/raw-http.json", "child/api/raw-http.json")
    for key in ("requests", "successful_responses", "errors", "concurrency", "warmup_requests", "seed", "k", "users", "products"):
        number(raw[key], integer=True)
    http = {**time_pair(raw), **fields(raw, ("requests", "successful_responses", "errors", "failures", "status_counts", "concurrency", "warmup_requests", "seed", "k", "users", "products", "model_run_id", "elapsed_seconds", "throughput_rps", "client_latency_ms", "handler_latency_ms", "protocol"))}
    require(raw["requests"] == raw["successful_responses"] == 5000 and raw["errors"] == 0 and raw["failures"] == {}, "full HTTP sample count/errors differ")
    require(raw["status_counts"] == {"200": 5000} and raw["warmup_requests"] == 50 and raw["concurrency"] == 8 and raw["seed"] == 0 and raw["k"] == 10, "full HTTP protocol differs")
    require(raw["users"] == dataset_public["users"] and raw["products"] == dataset_public["products"] and raw["model_run_id"] == ids[0], "HTTP service differs from selected seed42 dataset/model")
    require(isinstance(raw["raw_requests"], list) and len(raw["raw_requests"]) == 5000, "missing measured HTTP samples")
    samples = []
    for index, sample in enumerate(raw["raw_requests"]):
        require(sample["request"] == index and not isinstance(sample["request"], bool), "HTTP sample indices missing/duplicated/reordered")
        require(sample["status"] == 200 and not isinstance(sample["status"], bool) and "error" not in sample, "raw HTTP sample did not validate successfully")
        samples.append({"request": sample["request"], "status": sample["status"],
                        "client_ms": number(sample["client_ms"]), "handler_ms": number(sample["handler_ms"])})
    for key, sample_key in (("client_latency_ms", "client_ms"), ("handler_latency_ms", "handler_ms")):
        calculated = percentiles([sample[sample_key] for sample in samples])
        require(set(raw[key]) == {"p50", "p95", "p99"}, "missing HTTP percentile")
        for percentile in calculated:
            near(raw[key][percentile], calculated[percentile])
    near(raw["throughput_rps"], 5000 / number(raw["elapsed_seconds"], positive=True))
    text(http["protocol"])
    lifecycle = ev.read(root, "api-lifecycle.json", "child/api-lifecycle.json")
    same_provenance(lifecycle, provenance)
    number(lifecycle["owned_pid"], integer=True, positive=True)
    require(lifecycle["owned_process_terminated"] is True, "owned API process did not terminate")
    http["lifecycle"] = {"owned_process_started": True, "owned_process_terminated": True, "wall_seconds": number(lifecycle["wall_seconds"])}
    http["process"] = process_timer(ev.read(root, "api/benchmark-process/process.json", "child/api/benchmark-process/process.json"), provenance)
    http.update({"serving_seed": 42, "repetitions": api["repetitions"], "client_timer": text(api["client_timer"]), "handler_timer": text(api["handler_timer"])})
    orchestrator_env = {key: text(provenance[key]) for key in ("environment", "python", "platform", "cpu", "device_scope", "clock")}
    orchestrator_env["logical_cpus"] = number(provenance["logical_cpus"], integer=True, positive=True)
    orchestrator_env["packages"] = {name: text(provenance["packages"][name]) for name in ORCHESTRATOR_PACKAGES}
    fit_env = {key: text(model_env[key]) for key in ("python", "platform", "scope")}
    fit_env["packages"] = {name: {key: text(model_env["packages"][name][key]) for key in ("metadata_version", "imported_version")} for name in MODEL_PACKAGES}
    summary = {"schema": "m4-downstream-public-evidence-v1", "data_scope": "accepted full original terminal; independent downstream child",
               "lineage": {"parent_run_id": text(parent["manual_run_id"]), "child_run_id": run_id,
                           "parent_source_git_sha": parent["source_git_sha"], "child_source_git_sha": provenance["git_sha"],
                           "effective_config_sha256": provenance["config_sha256"], "requested_config_file_sha256_recorded": run["config_file_sha256"],
                           "parent_receipt_sha256": parent["receipt_sha256"], "input_binding_sha256": ev.sha("child/input-binding.json")},
               "accepted_inputs": {**fields(binding, ("feature_manifest_sha256", "split_sha256", "feature_fingerprint")),
                                   "database_bytes": stamp["bytes"], "feature_definition_sha256": stamp["feature_definition_sha256"],
                                   "database_stamp_sha256": canonical_hash(stamp), "cache_snapshot_sha256": canonical_hash(binding["cache_snapshot"]), "unchanged": True},
               "protocol": fields(config, ("training_seeds", "training_epochs", "ranker_rounds", "candidates", "http_seed", "http_requests", "http_concurrency")),
               "environment": {"orchestrator": orchestrator_env, "model_python": fit_env, "model_preflight": preflight},
               "child_timing": child_time, "dataset": dataset_public, "per_seed": seeds,
               "test_aggregate": aggregate, "http": http,
               "verification": {"materialized_assets": ["recsys_train_eval", "api_benchmark"],
                                "population_summary_checked": True, "http_aggregates_checked_against_all_samples": True,
                                "numeric_comparison": {"maximum_binary64_ulps": CHECK_ULPS, "scale": "max(1,abs(original),abs(recomputed))", "formula": "abs(original-recomputed) <= 32*math.ulp(scale)",
                                                       "published_values": "copied from original JSON; no recomputed aggregate replaces measurements"}}}
    public_timings = {"schema": "m4-http-id-free-timings-v1", "model_run_id": ids[0], "serving_seed": 42,
                      "original_raw_http_sha256": ev.sha("child/api/raw-http.json"),
                      "requests": raw["requests"], "raw_requests": samples,
                      "retained_sample_fields": ["request", "status", "error (when present)", "client_ms", "handler_ms"],
                      "scope": "original timing numbers and request indices only; user/product IDs and responses excluded"}
    ev.verify_unchanged()
    output.mkdir(parents=True, exist_ok=False)
    artifacts = {}
    for name, record in (("summary.json", summary), ("http-timings.json", public_timings)):
        payload = (json.dumps(record, indent=2, ensure_ascii=False, allow_nan=False) + "\n").encode("utf-8")
        with (output / name).open("xb") as stream:
            stream.write(payload)
        artifacts[name] = {"bytes": len(payload), "sha256": bytes_hash(payload)}
    manifest = {"schema": "m4-downstream-extraction-manifest-v1", "created_utc": datetime.now(timezone.utc).isoformat(),
                "extractor_sha256": bytes_hash(Path(__file__).read_bytes()), "original_json": ev.identities(), "public_artifacts": artifacts,
                "private_originals_unchanged": True, "scope": "whitelist extraction only; no database/array/model/service execution or copies"}
    with (output / "extraction-manifest.json").open("x", encoding="utf-8", newline="\n") as stream:
        json.dump(manifest, stream, indent=2, ensure_ascii=False, allow_nan=False)
        stream.write("\n")
    return manifest


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-root", required=True, help="Independently accepted, closed full downstream child")
    parser.add_argument("--out", required=True, help="New output directory outside accepted parent/child")
    args = parser.parse_args()
    result = export(args.run_root, args.out)
    print(json.dumps({"status": "completed", "extractor_sha256": result["extractor_sha256"], "public_artifacts": result["public_artifacts"]}, indent=2))
