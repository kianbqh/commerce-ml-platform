"""Synthetic JSON-only extractor contracts; numbers here are not measurements."""
from copy import deepcopy
import hashlib
import importlib.util
import json
import math
import os
from pathlib import Path
import statistics
import sys
import tempfile
import unittest

SCRIPT = Path(__file__).resolve().parents[1] / "scripts/export_downstream_evidence.py"
SPEC = importlib.util.spec_from_file_location("m4_export_downstream_contract", SCRIPT)
exporter = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(exporter)


def h(text):
    return hashlib.sha256(text.encode()).hexdigest()


class DownstreamPublicEvidence(unittest.TestCase):
    def setUp(self):
        base = Path(os.environ.get("M4_EXPORT_TEST_TMP", Path(__file__).parent / "scratch"))
        base.mkdir(parents=True, exist_ok=True)
        self.temp = tempfile.TemporaryDirectory(dir=base)
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.parent, self.child = self.root / "parent", self.root / "child"
        self.output = self.root / "public"
        self.fixture()

    def put(self, path, record):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(record, indent=2) + "\n", encoding="utf-8")
        return record

    def get(self, path):
        return json.loads(path.read_text(encoding="utf-8"))

    def change(self, relative, mutate):
        path = self.child / relative
        value = self.get(path)
        mutate(value)
        self.put(path, value)

    def fixture(self):
        # These deliberately absent DB/cache/array files prove only JSON is read.
        stamp = {"path": str(self.parent / "terminal/warehouse.duckdb"), "bytes": 1000,
                 "mtime_ns": 100000, "feature_definition_sha256": h("synthetic-definition")}
        features = {"split_sha256": h("synthetic-split"), "feature_fingerprint": h("synthetic-features"),
                    "feature_manifest_sha256": h("synthetic-manifest")}
        parent_config = {"training_enabled": False, "training_seeds": [42, 43, 44], "http_seed": 42,
                         "training_epochs": 3, "ranker_rounds": 120, "candidates": 100,
                         "http_requests": 5000, "http_concurrency": 8,
                         "runtime_root": str(self.root), "raw_dir": "/PRIVATE_RAW_SOURCE_PATH"}
        parent_records = {name: {} for name in exporter.PARENT_RECEIPTS}
        parent_records.update({"config.json": parent_config, "initialization.json": {"scenario": "original"},
                               "provenance.json": {"git_sha": h("parent-source")[:40]},
                               "manual-run.json": {"success": True, "run_id": "parent-synthetic"},
                               "model-run.json": {"status": "not run"}, "api-run.json": {"status": "not run"},
                               "arrival_manifest.json": {"batches": [{"lines": 33819106}]},
                               "terminal/gate.json": {"passed": True, "original_source_only": True},
                               "full-incremental-parity.json": {"passed": True},
                               "spark-original/spark-dbt-parity.json": {"passed": True},
                               "terminal/feature-export.json": {"only_complete_original_snapshot": True, **features}})
        for name, value in parent_records.items():
            self.put(self.parent / name, value)
        parent = {"run_root": str(self.parent), "manual_run_id": "parent-synthetic", "source_git_sha": h("parent-source")[:40],
                  "receipt_sha256": {name: hfile(self.parent / name) for name in exporter.PARENT_RECEIPTS}}
        config = {**parent_config, "training_enabled": True, "cache_only": True, "model_python": "/PRIVATE_MODEL_PYTHON/bin/python"}
        provenance = {"git_sha": h("child-source")[:40], "config_sha256": exporter.canonical_hash(config), "parent_data_run": parent,
                      "orchestrator_executable": "/PRIVATE_ORCHESTRATOR/bin/python", "executable": "/PRIVATE_ORCHESTRATOR/bin/python",
                      "environment": "synthetic JSON contract", "python": "3.12.0 synthetic", "platform": "synthetic", "cpu": "synthetic CPU",
                      "device_scope": "CPU data pipeline; GPU not used", "clock": "aware UTC plus perf_counter", "logical_cpus": 4,
                      "packages": {name: "1.0" for name in exporter.ORCHESTRATOR_PACKAGES}}
        self.provenance = provenance
        binding = {"schema": "m4-accepted-terminal-cache-v1", "parent": parent, "database": stamp["path"],
                   "database_stamp": stamp, "feature_cache": str(self.parent / "terminal/features"), **features,
                   "cache_snapshot": {"user_ids.npy": {"bytes": 20, "sha256": h("private-array-never-read")}}}
        self.put(self.child / "input-binding.json", binding)
        self.put(self.child / "inputs-before.json", {"parent": parent, "database_stamp": stamp, "cache_snapshot": binding["cache_snapshot"]})
        self.put(self.child / "input-verification.json", {"unchanged": True, "error": None, "parent": parent,
                 "binding_sha256": hfile(self.child / "input-binding.json"), "database_stamp": stamp, "cache_snapshot": binding["cache_snapshot"]})
        interval = {"started_utc": "2026-01-01T00:00:00+00:00", "finished_utc": "2026-01-01T00:00:01+00:00"}
        self.put(self.child / "downstream-run.json", {"success": True, "error": None, "run_id": "child-synthetic", "parent": parent,
                 "selected_assets": ["recsys_train_eval", "api_benchmark"], "config_file_sha256": h("synthetic-config-file"),
                 **interval, "wall_seconds": 1.25, "timer_scope": "synthetic interval"})
        self.put(self.child / "downstream-events.json", {"events": [{"event_type": "ASSET_MATERIALIZATION", "asset_key": [name]} for name in ("recsys_train_eval", "api_benchmark")]})
        self.put(self.child / "config.json", config)
        self.put(self.child / "provenance.json", provenance)
        environment = {"python": "3.12.0 synthetic", "platform": "synthetic", "scope": "synthetic imports", "executable": config["model_python"],
                       "packages": {name: {"metadata_version": "1.0", "imported_version": "1.0", "module_file": "/PRIVATE_MODULE_PATH/module.py"} for name in exporter.MODEL_PACKAGES}}
        self.put(self.child / "model-python-identity.json", environment)
        process = {"provenance": provenance, "returncode": 0, "error": None, **interval, "wall_seconds": 0.125,
                   "timer_includes": "synthetic process", "command": ["/PRIVATE_COMMAND_PATH"]}
        self.put(self.child / "model-python-preflight/process.json", process)
        dataset = {"database": stamp, "users": 4, "products": 100, "raw_orders": 32,
                   "excluded_users_fewer_than_three_labeled_baskets": 0, "unlabeled_orders": 4, "excluded_labeled_orders": 0,
                   "line_counts": {"train": 12, "validation": 4, "test": 4}, "order_counts": {"train": 12, "validation": 4, "test": 4},
                   **features, "split_rule": "synthetic temporal rule", "test_history": "synthetic completed validation", "global_priors": "synthetic training only",
                   "normalizers": {"PRIVATE_ARRAY_VALUES": [987654321]}, "user_ids": [999999999]}
        self.metrics = []
        for offset, seed in enumerate(exporter.SEEDS):
            tower = {"seed": seed, "dimension": 32, "device": "cpu", "epochs": [{"epoch": epoch, "examples": 12, "loss": 1.0 / epoch + offset * .01, "seconds": .25} for epoch in (1, 2, 3)],
                     "positive_rows_per_epoch": 12, "initial_weights_sha256": h(f"before-{seed}"), "final_weights_sha256": h(f"after-{seed}"),
                     "seconds": .875, "batch_size": 4096, "negatives_per_positive": 4, "threads": 4, "negative_rule": "synthetic negative rule", "objective": "synthetic objective"}
            identity = {"seed": seed, "epochs": 3, "dimension": 32, "candidates": 100, "rounds": 120,
                        "split_sha256": features["split_sha256"], "feature_fingerprint": features["feature_fingerprint"],
                        **{key: h(key) for key in ("model_definition_sha256", "fit_user_sha256", "early_stop_user_sha256", "ranking_definition_sha256")}}
            basket = {"users": 4, "heldout_positive_products": 4, "definition": "synthetic macro definition",
                      **{key: .25 + offset * .01 for key in exporter.SCORE_METRICS}}
            coverage = {key: basket[key] for key in exporter.COUNT_METRICS + ("recall@100", "ndcg@100", "micro_recall@100", "definition")}
            ranker = {"identity": identity, "fit_users": 3, "early_stop_users": 1, "fit_candidate_rows": 300, "early_stop_candidate_rows": 100,
                      "fit_positive_rows": 3, "early_stop_positive_rows": 1, "best_iteration": 2, "candidate_coverage": coverage,
                      "features": ["tt_score", "tt_rank"], "feature_gain": {"tt_score": 1.0, "tt_rank": 0.0},
                      "feature_seconds": .25, "fit_seconds": .125, "labels": "synthetic validation only", "early_stopping_metric": "synthetic NDCG"}
            model_id = exporter.bytes_hash(json.dumps({"ranker": identity, "weights": tower["final_weights_sha256"]}, sort_keys=True).encode())
            result = {"dataset": dataset, "two_tower": tower, "ranker": ranker, "model_run_id": model_id,
                      "test": {name: deepcopy(basket) for name in exporter.METHODS},
                      "faiss": {"index": "synthetic HNSW32", "efConstruction": 100, "efSearch": 128, "catalog_products": 100,
                                "reorders_eligible": True, "exact_topk_agreement": .875, "agreement_users": 4, "agreement_user_sha256": h("synthetic-agreement-users")},
                      "runtime": {"python": "3.12.0", "torch": "1.0", "numpy": "1.0", "faiss": "1.0", "lightgbm": "1.0", "threads": 4, "device": "cpu", "peak_process_rss_mib": 10.0},
                      "evaluation_seconds": .125, "total_pipeline_seconds": 1.0, "timing_scope": "synthetic timing"}
            self.metrics.append(result)
            directory = self.child / f"models/seed-{seed}"
            self.put(directory / "metrics.json", result)
            self.put(directory / "artifacts/ranker.json", ranker)
            self.put(directory / "artifacts/serving.json", {"model_run_id": model_id, "candidates": 100, "history_stage": "test", "feature_cache": binding["feature_cache"], **features})
            self.put(directory / "process/process.json", process)
        aggregate = {}
        for method in exporter.METHODS:
            aggregate[method] = {}
            for metric in exporter.COUNT_METRICS + exporter.SCORE_METRICS:
                values = [result["test"][method][metric] for result in self.metrics]
                aggregate[method][metric] = {"mean": statistics.mean(values), "std": statistics.pstdev(values), "std_definition": "population ddof=0", "values": values}
        self.put(self.child / "model-run.json", {"provenance": provenance, "input_binding": str(self.child / "input-binding.json"),
                 "model_python": config["model_python"], "status": "completed", "seeds": [42, 43, 44], "cache_only": True,
                 "http_seed_predeclared": 42, "model_python_identity": str(self.child / "model-python-identity.json"),
                 "per_seed_paths": [str(self.child / f"models/seed-{seed}/metrics.json") for seed in exporter.SEEDS], "summary": aggregate})
        self.put(self.child / "api-run.json", {"provenance": provenance, "input_binding": str(self.child / "input-binding.json"),
                 "model_python": config["model_python"], "status": "completed", "predeclared_seed": 42, "excluded_warmups": 50,
                 "repetitions": 1, "concurrency": 8, "raw_http": str(self.child / "api/raw-http.json"), "client_timer": "synthetic client timer", "handler_timer": "synthetic handler timer"})
        samples = [{"request": index, "user_id": 900000000 + index, "client_ms": 1.2345 + (index % 100) / 100,
                    "handler_ms": .12345 + (index % 50) / 100, "status": 200, "private_response": "PRIVATE_RESPONSE_SENTINEL"} for index in range(5000)]
        raw = {**interval, "requests": 5000, "successful_responses": 5000, "errors": 0, "failures": {}, "status_counts": {"200": 5000},
               "concurrency": 8, "warmup_requests": 50, "seed": 0, "k": 10, "users": 4, "products": 100,
               "model_run_id": self.metrics[0]["model_run_id"], "elapsed_seconds": 2.5, "throughput_rps": 2000.0,
               "client_latency_ms": exporter.percentiles([sample["client_ms"] for sample in samples]),
               "handler_latency_ms": exporter.percentiles([sample["handler_ms"] for sample in samples]), "protocol": "synthetic closed-loop JSON contract", "raw_requests": samples}
        self.put(self.child / "api/raw-http.json", raw)
        self.put(self.child / "api-lifecycle.json", {"provenance": provenance, "owned_pid": 12345, "owned_process_terminated": True, "wall_seconds": 3.0})
        self.put(self.child / "api/benchmark-process/process.json", process)

    def run_export(self):
        return exporter.export(self.child, self.output)

    def assert_rejected(self):
        with self.assertRaises((ValueError, KeyError, FileNotFoundError, TypeError)):
            self.run_export()
        self.assertFalse(self.output.exists())

    def test_preserves_all_timings_and_original_aggregates_but_strips_private_fields(self):
        before = {str(path): hfile(path) for tree in (self.child, self.parent) for path in tree.rglob("*.json")}
        modules_before = set(sys.modules)
        manifest = self.run_export()
        summary = self.get(self.output / "summary.json")
        public = self.get(self.output / "http-timings.json")
        raw = self.get(self.child / "api/raw-http.json")
        self.assertEqual(len(public["raw_requests"]), 5000)
        for original, sample in zip(raw["raw_requests"], public["raw_requests"]):
            self.assertEqual(sample, {key: original[key] for key in ("request", "status", "client_ms", "handler_ms")})
        self.assertEqual(summary["http"]["client_latency_ms"], raw["client_latency_ms"])
        self.assertEqual(summary["test_aggregate"], self.get(self.child / "model-run.json")["summary"])
        self.assertEqual(before, {str(path): hfile(path) for tree in (self.child, self.parent) for path in tree.rglob("*.json")})
        self.assertEqual(manifest["extractor_sha256"], hfile(SCRIPT))
        for path in self.output.glob("*.json"):
            content = path.read_text()
            for forbidden in ("user_id", "PRIVATE_", str(self.child), str(self.parent)):
                self.assertFalse(forbidden in content, "private field/path survived whitelist extraction")
            def scalars(value):
                if isinstance(value, dict):
                    return [scalar for child in value.values() for scalar in scalars(child)]
                if isinstance(value, list):
                    return [scalar for child in value for scalar in scalars(child)]
                return [value]
            for private_number in (900000001, 987654321, 999999999):
                self.assertNotIn(private_number, scalars(json.loads(content)))
        newly_imported = set(sys.modules) - modules_before
        self.assertFalse(any(name in newly_imported for name in ("torch", "numpy", "duckdb", "dagster", "faiss", "lightgbm")))

    def test_rounding_tolerance_checks_but_does_not_replace_original_percentile(self):
        self.change("api/raw-http.json", lambda record: record["client_latency_ms"].update(p50=math.nextafter(record["client_latency_ms"]["p50"], math.inf)))
        original = self.get(self.child / "api/raw-http.json")["client_latency_ms"]["p50"]
        self.run_export()
        self.assertEqual(self.get(self.output / "summary.json")["http"]["client_latency_ms"]["p50"], original)

    def test_rejects_wrong_parent_binding_or_event(self):
        for relative, mutate in (("input-verification.json", lambda record: record.update(binding_sha256="0" * 64)),
                                 ("downstream-run.json", lambda record: record["parent"].update(manual_run_id="wrong")),
                                 ("downstream-events.json", lambda record: record["events"].append({"event_type": "ASSET_MATERIALIZATION", "asset_key": ["feature_export"]}))):
            path = self.child / relative
            original = path.read_bytes()
            self.change(relative, mutate)
            self.assert_rejected()
            path.write_bytes(original)

    def test_rejects_incomplete_or_changed_inputs(self):
        self.change("input-verification.json", lambda record: record.update(unchanged=False))
        self.assert_rejected()

    def test_rejects_changed_parent_receipt(self):
        self.put(self.parent / "model-run.json", {"status": "completed"})
        self.assert_rejected()

    def test_rejects_missing_field(self):
        self.change("models/seed-43/metrics.json", lambda record: record["two_tower"].pop("epochs"))
        self.assert_rejected()

    def test_rejects_nonfinite_input_even_in_unpublished_field(self):
        self.change("api/raw-http.json", lambda record: record.update(private_value=float("nan")))
        self.assert_rejected()

    def test_rejects_wrong_seed_or_serving_identity(self):
        self.change("models/seed-44/artifacts/serving.json", lambda record: record.update(model_run_id="0" * 64))
        self.assert_rejected()

    def test_rejects_inaccurate_population_summary(self):
        self.change("model-run.json", lambda record: record["summary"]["two_tower_hnsw"]["ndcg@10"].update(std=.9))
        self.assert_rejected()

    def test_rejects_inaccurate_http_summary(self):
        self.change("api/raw-http.json", lambda record: record["client_latency_ms"].update(p99=10000.0))
        self.assert_rejected()

    def test_rejects_missing_or_reordered_measured_sample(self):
        self.change("api/raw-http.json", lambda record: record["raw_requests"].pop())
        self.assert_rejected()

    def test_rejects_wrong_http_model_or_unterminated_api(self):
        self.change("api-lifecycle.json", lambda record: record.update(owned_process_terminated=False))
        self.assert_rejected()

    def test_create_only_output_and_accepted_input_tree_guard(self):
        self.output.mkdir()
        with self.assertRaises(ValueError):
            self.run_export()
        self.assertEqual(list(self.output.iterdir()), [])
        with self.assertRaises(ValueError):
            exporter.export(self.child, self.child / "must-not-create")
        with self.assertRaises(ValueError):
            exporter.export(self.child, self.parent / "must-not-create")


def hfile(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


if __name__ == "__main__":
    unittest.main()
