"""Synthetic guard fixtures only; no Dagster/model/DB/service imports or runs."""
import importlib.util
from datetime import datetime
import json
import os
from pathlib import Path
import sys
import tempfile
import types
import unittest
from unittest.mock import patch

from m4.accepted_cache import (database_stamp, make_binding, parent_identity, sha256,
                               validate_binding, verify_parent_unchanged)
from m4.dashboard import Reader, build_snapshot, linked_downstream
from m4.downstream_completion import complete, require_materializations, validate_config
from m4.replay import make_manifest


class DownstreamCompletionContracts(unittest.TestCase):
    def setUp(self):
        base = Path(os.environ.get("M4_TEST_TMP", os.environ.get("TMPDIR", Path(__file__).parent / "scratch")))
        base.mkdir(parents=True, exist_ok=True)
        self.temporary = tempfile.TemporaryDirectory(dir=base)
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.repo = self.root / "repo"
        for name in ("warehouse/models/recommendations/a.sql", "recsys/recsys/data.py", "recsys/recsys/features.py"):
            target = self.repo / name
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text("synthetic definition\n")
        self.database = self.root / "accepted.duckdb"
        self.database.write_bytes(b"synthetic identity only; not a database")
        self.cache = self.root / "features"
        self.cache.mkdir()
        self.manifest = {"database": database_stamp(self.database, self.repo),
                         "split_sha256": "synthetic-split", "feature_fingerprint": "synthetic-feature"}
        self.put(self.cache / "manifest.json", self.manifest)
        (self.cache / "array.npy").write_bytes(b"synthetic cache identity only")
        self.receipt = self.root / "feature-export.json"
        self.put(self.receipt, {"only_complete_original_snapshot": True,
                 "feature_manifest_sha256": sha256(self.cache / "manifest.json"),
                 "split_sha256": "synthetic-split", "feature_fingerprint": "synthetic-feature"})
        self.binding = make_binding(self.repo, self.database, self.cache, self.receipt)

    def put(self, path, value):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(value), encoding="utf-8")
        return path

    def parent(self, success=True):
        root = self.root / "parent"
        manifest = make_manifest({0: {"orders": 2, "lines": 3}}, width=10, sources={})
        records = {"config.json": {"training_enabled": False}, "initialization.json": {"scenario": "original"},
                   "arrival_manifest.json": manifest, "provenance.json": {"git_sha": "synthetic-parent-source"},
                   "full-incremental-parity.json": {"passed": True},
                   "spark-original/spark-dbt-parity.json": {"passed": True},
                   "terminal/gate.json": {"passed": True, "original_source_only": True},
                   "terminal/feature-export.json": {}, "model-run.json": {"status": "not run"},
                   "api-run.json": {"status": "not run"},
                   "manual-run.json": {"run_id": "synthetic-data-run", "success": success},
                   "promoted.json": {"visible_prefix": ["000000"], "arrival_manifest_sha256": manifest["manifest_sha256"],
                                     "status": "promoted", "scenario": "original", "overlay_sha256": None}}
        for name, value in records.items():
            self.put(root / name, value)
        return root

    def test_valid_binding_is_read_only_and_preserves_resolved_input_identity(self):
        before = self.binding["cache_snapshot"]
        self.assertEqual(validate_binding(self.binding, self.repo, verify_files=True), self.manifest)
        self.assertEqual(make_binding(self.repo, self.database, self.cache, self.receipt)["cache_snapshot"], before)

    def test_changed_database_stamp_is_rejected(self):
        self.database.write_bytes(b"different synthetic bytes")
        with self.assertRaisesRegex(ValueError, "stamp mismatch"):
            validate_binding(self.binding, self.repo)

    def test_changed_feature_definition_is_rejected(self):
        (self.repo / "recsys/recsys/features.py").write_text("changed definition")
        with self.assertRaisesRegex(ValueError, "stamp mismatch"):
            validate_binding(self.binding, self.repo)

    def test_changed_cache_bytes_are_rejected(self):
        (self.cache / "array.npy").write_bytes(b"changed")
        with self.assertRaisesRegex(ValueError, "bytes changed"):
            validate_binding(self.binding, self.repo, verify_files=True)

    def test_another_database_path_cannot_trigger_fallback_export(self):
        copy = self.root / "copied.duckdb"
        copy.write_bytes(self.database.read_bytes())
        with self.assertRaisesRegex(ValueError, "resolved paths"):
            validate_binding(self.binding, self.repo, database=copy)

    def test_unfinished_parent_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "not completed"):
            parent_identity(self.parent(success=False), self.repo)

    def test_parent_source_receipt_changes_are_rejected(self):
        root = self.parent()
        identity, _ = parent_identity(root, self.repo)
        self.put(root / "provenance.json", {"git_sha": "wrong-source"})
        with self.assertRaisesRegex(ValueError, "receipt changed"):
            verify_parent_unchanged(identity)

    def test_only_training_flag_can_change_in_full_models_config(self):
        parent = {"training_enabled": False, "training_seeds": [42, 43, 44], "http_seed": 42, "training_epochs": 3}
        validate_config(parent, {**parent, "training_enabled": True})
        with self.assertRaises(ValueError):
            validate_config(parent, {**parent, "training_enabled": True, "training_epochs": 1})

    def test_missing_duplicate_or_upstream_materializations_fail(self):
        events = [{"event_type": "ASSET_MATERIALIZATION", "asset_key": [key]}
                  for key in ("recsys_train_eval", "api_benchmark")]
        require_materializations(events)
        for invalid in (events[:1], events + events[:1], events + [{"event_type": "ASSET_MATERIALIZATION", "asset_key": ["arrival_ingest"]}]):
            with self.assertRaises(ValueError):
                require_materializations(invalid)

    def test_cache_miss_fails_before_importing_or_calling_original_export(self):
        script = Path(__file__).resolve().parents[1] / "scripts/run_recsys_cache_only.py"
        spec = importlib.util.spec_from_file_location("m4_cache_wrapper_contract", script)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        binding = self.put(self.root / "binding.json", self.binding)
        (self.cache / "manifest.json").unlink()
        with self.assertRaises(FileNotFoundError), patch.dict(sys.modules, {"recsys": None}):
            module.run_cached(binding_path=binding, database=self.database, cache=self.cache)

    def test_child_with_wrong_parent_cannot_replace_dashboard_not_run(self):
        parent, child = self.root / "dashboard-parent", self.root / "dashboard-child"
        self.put(parent / "manual-run.json", {"run_id": "original-parent", "success": True})
        self.put(parent / "model-run.json", {"status": "not run"})
        self.put(parent / "api-run.json", {"status": "not run"})
        self.put(child / "input-binding.json", {"parent": {"run_root": str(self.root / "wrong-parent")}})
        self.put(child / "downstream-run.json", {"success": True})
        self.put(child / "input-verification.json", {"unchanged": True})
        self.put(child / "downstream-events.json", {"events": []})
        snapshot = build_snapshot(parent, downstream_root=child)
        self.assertEqual(snapshot["recommendations"]["status"], "not run")
        self.assertIsNone(snapshot["downstream_lineage"])
        self.assertTrue(any(issue["source"] == "downstream" for issue in snapshot["issues"]))

    def test_failed_child_records_utc_and_wall_without_running_engines(self):
        parent = self.parent()
        config = {"training_enabled": False, "training_seeds": [42, 43, 44], "http_seed": 42,
                  "runtime_root": str(self.root)}
        self.put(parent / "config.json", config)
        terminal = parent / "terminal"
        database, cache = terminal / "warehouse.duckdb", terminal / "features"
        database.write_bytes(self.database.read_bytes())
        cache.mkdir()
        manifest = {**self.manifest, "database": database_stamp(database, self.repo)}
        self.put(cache / "manifest.json", manifest)
        self.put(terminal / "feature-export.json", {"only_complete_original_snapshot": True,
                 "feature_manifest_sha256": sha256(cache / "manifest.json"),
                 "split_sha256": manifest["split_sha256"], "feature_fingerprint": manifest["feature_fingerprint"]})
        self.put(parent / "manifest-parse/dbt-target/manifest.json", {})
        requested = self.put(self.root / "models-config.json", {**config, "training_enabled": True})
        child = self.root / "child"
        runtime = types.ModuleType("m4.runtime")
        runtime.Runtime = lambda root: types.SimpleNamespace(root=Path(root))
        with patch.dict(sys.modules, {"m4.runtime": runtime, "dagster": None}), \
             patch("m4.downstream_completion.collect", return_value={"git_sha": "synthetic-new-child-source"}):
            with self.assertRaises(RuntimeError):
                complete(self.repo, parent, requested, child)
        result = json.loads((child / "downstream-run.json").read_text())
        self.assertFalse(result["success"])
        self.assertIsNotNone(datetime.fromisoformat(result["started_utc"]).tzinfo)
        self.assertGreaterEqual(datetime.fromisoformat(result["finished_utc"]), datetime.fromisoformat(result["started_utc"]))
        self.assertGreaterEqual(result["wall_seconds"], 0)
        self.assertIn("post-run input verification", result["timer_scope"])


if __name__ == "__main__":
    unittest.main()
