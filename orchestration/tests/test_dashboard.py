"""Synthetic JSON only: tests the view contract, never data-engine/runtime results."""
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

from m4.dashboard import Reader, build_snapshot, render_html, write_dashboard

NOW = "2026-01-01T12:00:00+00:00"


class DashboardContract(unittest.TestCase):
    def setUp(self):
        base = Path(os.environ.get("M4_TEST_TMP", Path(__file__).parent / "scratch"))
        base.mkdir(parents=True, exist_ok=True)
        self.temporary = tempfile.TemporaryDirectory(dir=base)
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name) / "synthetic-run"
        self.root.mkdir()

    def put(self, name, value):
        path = self.root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(value, ensure_ascii=False), encoding="utf-8")
        return path

    def synthetic(self):
        self.put("provenance.json", {"environment": "synthetic test only", "data_kind": "synthetic_test"})
        self.put("manual-run.json", {"run_id": "synthetic-parent", "success": True})
        self.put("arrival_manifest.json", {"batches": [{"key": "000000"}, {"key": "000001"}]})
        self.put("promoted.json", {"attempt_id": "synthetic-attempt", "visible_prefix": ["000000", "000001"], "materialized_at": NOW})
        self.put("full-incremental-parity.json", {"passed": True, "tables": {
            "marts.fct_order_lines": {"incremental": {"rows": 7}}, "marts.fct_orders": {"incremental": {"rows": 3}}}})
        self.put("full-reference/reference.json", {"process": {"wall_seconds": 1.125}})
        self.put("summary.json", {"incremental_etl_and_tests_wall_seconds_total": 3.25,
                  "per_batch": [{"partition": "000000", "etl_and_test_wall_seconds": 1.5},
                                {"partition": "000001", "etl_and_test_wall_seconds": 1.75}]})
        self.put("attempts/synthetic-attempt/output-checks.json", {"checks": [
            {"name": "batch_row_reconciliation", "passed": True},
            {"name": "processing_freshness", "passed": True, "age_seconds": 2.5, "maximum_age_seconds": 60}]})
        self.put("spark-original/spark-run.json", {"master_actual": "synthetic-local", "etl_wall_seconds_excluding_physical_hashes": 2.125,
            "tables": {"fct_order_lines": {"rows": 7, "nonempty_partition_count": 2,
                       "physical": {"parquet_file_count": 2, "parquet_bytes": 321}}}})
        self.put("spark-original/spark-dbt-parity.json", {"passed": True})
        self.put("model-run.json", {"status": "completed", "seeds": [42, 43, 44], "summary": {
            "two_tower_lambdamart": {"ndcg@10": {"mean": 0.12345678901234567, "std": 0.012345678901234567},
                                    "recall@10": {"mean": 0.21, "std": 0.03}}}})
        self.put("api-run.json", {"status": "completed", "raw_http": "api/raw-http.json"})
        self.put("api/raw-http.json", {"requests": 2, "errors": 0, "client_latency_ms": {"p50": 5.125, "p99": 7.875}})

    def test_no_results_has_no_estimated_or_historical_numbers(self):
        snapshot = build_snapshot(self.root, generated_at=NOW)
        self.assertEqual(snapshot["run"]["status"], "not_run")
        self.assertIsNone(snapshot["replay"]["line_rows"])
        self.assertIsNone(snapshot["replay"]["incremental_seconds"])
        self.assertIsNone(snapshot["spark"]["seconds"])
        self.assertEqual(snapshot["recommendations"]["rows"], [])
        page = render_html(snapshot)
        self.assertIn("未运行", page)
        self.assertNotIn("33819106", page)

    def test_real_schema_pointers_preserve_exact_values_and_synthetic_label(self):
        self.synthetic()
        snapshot = build_snapshot(self.root, generated_at=NOW)
        self.assertEqual(snapshot["data_kind"], "synthetic_test")
        self.assertEqual(snapshot["replay"]["line_rows"], 7)
        self.assertEqual(snapshot["replay"]["incremental_seconds"], 3.25)
        self.assertEqual(snapshot["spark"]["tables"][0]["bytes"], 321)
        self.assertEqual(snapshot["api"]["client_p99_ms"], 7.875)
        page = render_html(snapshot)
        self.assertIn("Synthetic test fixture", page)
        self.assertIn(str(0.12345678901234567), page)
        self.assertIn("3.25 s", page)

    def test_failure_boolean_and_failed_check_never_show_pass(self):
        self.synthetic()
        self.put("manual-run.json", {"success": False})
        self.put("full-incremental-parity.json", {"passed": False})
        self.put("attempts/synthetic-attempt/output-checks.json", {"checks": [{"name": "unique_grains", "passed": False}]})
        snapshot = build_snapshot(self.root, generated_at=NOW)
        self.assertEqual(snapshot["run"]["status"], "failed")
        self.assertEqual(snapshot["replay"]["status"], "failed")
        self.assertEqual(snapshot["quality"]["failed_count"], 1)

    def test_malformed_check_member_blocks_green_quality_and_completed_run(self):
        self.synthetic()
        for bad in (None, "malformed synthetic record", 7, [], True):
            with self.subTest(bad=bad):
                self.put("attempts/synthetic-attempt/output-checks.json", {"checks": [
                    {"name": "batch_row_reconciliation", "passed": True}, bad]})
                snapshot = build_snapshot(self.root, generated_at=NOW)
                self.assertEqual(snapshot["quality"]["status"], "unavailable")
                self.assertEqual(snapshot["run"]["status"], "partial")
                self.assertTrue(any(i["source"] == "output-checks.json#/checks" for i in snapshot["issues"]))

    def test_explicit_failed_check_stays_failed_with_malformed_member(self):
        self.synthetic()
        self.put("attempts/synthetic-attempt/output-checks.json", {"checks": [
            {"name": "unique_grains", "passed": False}, "malformed synthetic record"]})
        snapshot = build_snapshot(self.root, generated_at=NOW)
        self.assertEqual(snapshot["quality"]["status"], "failed")
        self.assertEqual(snapshot["quality"]["failed_count"], 1)
        self.assertEqual(snapshot["run"]["status"], "partial")
        self.assertTrue(snapshot["issues"])

    def test_partial_inputs_do_not_become_completed(self):
        self.put("full-reference/reference.json", {"process": {"wall_seconds": 0.5}})
        snapshot = build_snapshot(self.root, generated_at=NOW)
        self.assertEqual(snapshot["run"]["status"], "partial")
        self.assertEqual(snapshot["replay"]["full_seconds"], 0.5)
        self.assertIsNone(snapshot["replay"]["line_rows"])

    def test_invalid_json_and_nonfinite_values_are_visible_not_filled(self):
        self.synthetic()
        (self.root / "full-incremental-parity.json").write_text('{"passed":true,"rows":NaN}', encoding="utf-8")
        snapshot = build_snapshot(self.root, generated_at=NOW)
        self.assertEqual(snapshot["run"]["status"], "partial")
        self.assertIsNone(snapshot["replay"]["line_rows"])
        self.assertTrue(snapshot["issues"])

    def test_negative_boolean_or_string_metrics_are_not_numbers(self):
        for value in (True, -1, "33819106"):
            self.put("full-incremental-parity.json", {"passed": True, "tables": {"marts.fct_order_lines": {"incremental": {"rows": value}}}})
            snapshot = build_snapshot(self.root, generated_at=NOW)
            self.assertIsNone(snapshot["replay"]["line_rows"])
            self.assertTrue(snapshot["issues"])

    def test_bounded_reader_does_not_read_large_files(self):
        (self.root / "summary.json").write_text("x" * 33, encoding="utf-8")
        with patch("m4.dashboard.MAX_FILE_BYTES", 32):
            snapshot = build_snapshot(self.root, generated_at=NOW)
        self.assertTrue(any(s["path"] == "summary.json" and s["status"] == "unavailable" for s in snapshot["sources"]))
        self.assertIsNone(snapshot["replay"]["incremental_seconds"])

    def test_path_escape_is_rejected_without_reading_outside_run(self):
        outside = Path(self.temporary.name) / "outside.json"
        outside.write_text('{"secret":"should not be read"}', encoding="utf-8")
        rd = Reader(self.root)
        self.assertIsNone(rd.json(outside))
        self.assertEqual(rd.total_bytes, 0)
        self.put("promoted.json", {"attempt_id": "../../outside"})
        snapshot = build_snapshot(self.root, generated_at=NOW)
        self.assertTrue(snapshot["issues"])

    def test_alerts_deduplicate_and_html_escapes_untrusted_fields(self):
        path = self.root / "alerts/alerts.jsonl"
        path.parent.mkdir()
        record = {"alert_id": "synthetic-alert", "reason": '<script>alert("bad")</script>', "partition": "000001"}
        path.write_text(json.dumps(record) + "\n" + json.dumps(record) + "\n", encoding="utf-8")
        snapshot = build_snapshot(self.root, generated_at=NOW)
        self.assertEqual(snapshot["alerts"][0]["count"], 1)
        page = render_html(snapshot)
        self.assertNotIn('<script>alert("bad")</script>', page)
        self.assertIn("&lt;script&gt;", page)

    def test_malformed_alert_is_incomplete_not_zero(self):
        path = self.root / "alerts/alerts.jsonl"
        path.parent.mkdir()
        path.write_text("not JSON\n", encoding="utf-8")
        snapshot = build_snapshot(self.root, generated_at=NOW)
        self.assertEqual(snapshot["alerts"][0]["status"], "unavailable")
        self.assertIsNone(snapshot["alerts"][0]["count"])

    def test_model_not_run_cannot_display_leftover_scores(self):
        self.synthetic()
        self.put("model-run.json", {"status": "not run", "summary": {"two_tower_lambdamart": {"ndcg@10": {"mean": 1, "std": 0}}}})
        snapshot = build_snapshot(self.root, generated_at=NOW)
        self.assertEqual(snapshot["recommendations"]["rows"], [])
        self.assertIn("本次尚未运行推荐模型", render_html(snapshot))

    def test_controlled_stale_demo_is_not_the_live_processing_status(self):
        self.synthetic()
        self.put("freshness-demo.json", {"fresh": {"passed": True}, "stale": {"passed": False}, "clock_injected": True})
        snapshot = build_snapshot(self.root, generated_at=NOW)
        self.assertEqual(snapshot["freshness"]["status"], "passed")
        self.assertEqual(snapshot["freshness"]["controlled_demo"]["stale"], "failed")

    def test_manual_generation_is_create_only_and_does_not_modify_sources(self):
        self.synthetic()
        before = {str(p.relative_to(self.root)): p.read_bytes() for p in self.root.rglob("*") if p.is_file()}
        output = Path(self.temporary.name) / "snapshot"
        snapshot = write_dashboard(self.root, output, generated_at=NOW)
        self.assertEqual(snapshot["generated_at_utc"], NOW)
        self.assertTrue((output / "index.html").exists())
        self.assertEqual(before, {str(p.relative_to(self.root)): p.read_bytes() for p in self.root.rglob("*") if p.is_file()})
        with self.assertRaises(FileExistsError):
            write_dashboard(self.root, output, generated_at=NOW)

    def test_no_engines_network_or_polling_are_loaded(self):
        snapshot = build_snapshot(self.root, generated_at=NOW)
        page = render_html(snapshot)
        self.assertNotIn("setInterval", page)
        self.assertNotIn("fetch(", page)
        self.assertNotIn("http://", page)
        self.assertNotIn("https://", page)
        self.assertNotIn('http-equiv="refresh"', page)
        for module in ("m4.runtime", "duckdb", "dagster", "pyspark", "torch"):
            self.assertNotIn(module, sys.modules)

    def test_fallback_reads_embedded_summary_proofs_without_running_anything(self):
        self.put("summary.json", {"backfill": {"corrected_parity_passed": True, "selected": ["000000", "000001"]},
                  "quality_gate": {"run_success": False, "downstream_blocked": True, "promoted_pointer_unchanged": True},
                  "recovery": {"original_input_parity": {"passed": True}},
                  "model": {"status": "not run"}, "api": {"status": "not run"}})
        snapshot = build_snapshot(self.root, generated_at=NOW)
        self.assertEqual(snapshot["backfill"]["blocking_demo"], "passed")
        self.assertEqual(snapshot["backfill"]["recovery"], "passed")
        self.assertEqual(snapshot["run"]["status"], "partial")


if __name__ == "__main__":
    unittest.main()
