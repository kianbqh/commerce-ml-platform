"""Control/data-contract tests only; these do not substitute for engine gates."""
from datetime import date, datetime, timezone, timedelta
from decimal import Decimal
import json
import os
from pathlib import Path
import random
import tempfile
import unittest

from m4.canonical import cell, digest_small, digest_sorted, row_bytes
from m4.gates import (emit_local_alert, processing_freshness, promote, read_json,
                      require_checks, require_terminal, write_json_new)
from m4.replay import (backfill_range, batch_index, correction_overlay, json_hash,
                       make_manifest, prefix, reference_upsert, validate_manifest,
                       verify_changed_partitions)


def manifest():
    return make_manifest({0: {"orders": 2, "lines": 4}, 1: {"orders": 2, "lines": 5},
                          2: {"orders": 2, "lines": 6}}, width=2, sources={"fixture": {"sha256": "test-only"}},
                         delays={"000001": 1})


class ReplayTests(unittest.TestCase):
    def test_batch_boundaries_and_invalid_numbers(self):
        self.assertEqual([batch_index(n, 2) for n in range(1, 7)], [0, 0, 1, 1, 2, 2])
        for n, width in ((0, 2), (1, 0), (1.5, 2), (True, 2)):
            with self.assertRaises(ValueError):
                batch_index(n, width)

    def test_metadata_identity_and_immutable_prefix(self):
        m = manifest()
        self.assertEqual(prefix(m, "000001"), ["000000", "000001"])
        self.assertEqual(m["batches"][1]["late_by_days_simulated"], 1)
        self.assertTrue(m["batches"][2]["received_day_simulated"] > m["batches"][1]["received_day_simulated"])
        m["batches"][0]["lines"] += 1
        with self.assertRaises(ValueError):
            validate_manifest(m)

    def test_unknown_partition_rejected(self):
        with self.assertRaises(ValueError):
            prefix(manifest(), "000003")

    def test_bounded_backfill_must_be_multiple_nonempty_partitions(self):
        self.assertEqual(backfill_range(manifest(), "000000", "000001"), ["000000", "000001"])
        for first, last in (("000000", "000000"), ("000002", "000001"), ("000000", "000003")):
            with self.assertRaises(ValueError):
                backfill_range(manifest(), first, last)

    def test_smallest_existing_correction_deterministic_and_first_order_excluded(self):
        rows = [dict(batch="000000", order_id=7, product_id=4, order_number=2, reordered=1),
                dict(batch="000000", order_id=5, product_id=3, order_number=2, reordered=0),
                dict(batch="000000", order_id=1, product_id=1, order_number=1, reordered=0),
                dict(batch="000001", order_id=8, product_id=1, order_number=3, reordered=0)]
        a = correction_overlay(rows, ["000000", "000001"])
        random.Random(42).shuffle(rows)
        self.assertEqual(a, correction_overlay(rows, ["000000", "000001"]))
        self.assertEqual(a["rows"][0]["order_id"], 5)
        self.assertEqual(a["rows"][0]["new_reordered"], 1)

    def test_no_eligible_existing_line_fails(self):
        with self.assertRaises(ValueError):
            correction_overlay([], ["000000", "000001"])

    def test_idempotent_keyed_replay_and_correction_closure(self):
        facts = [{"order_id": 2, "product_id": 1, "reordered": 0},
                 {"order_id": 2, "product_id": 3, "reordered": 1}]
        replayed = reference_upsert(facts, facts, ["order_id", "product_id"])
        self.assertEqual(replayed, facts)
        changed = reference_upsert(facts, [{"order_id": 2, "product_id": 1, "reordered": 1}], ["order_id", "product_id"])
        self.assertEqual(sum(r["reordered"] for r in facts), 1)
        self.assertEqual(sum(r["reordered"] for r in changed), 2)
        self.assertEqual(len(changed), len(facts))

    def test_duplicate_and_null_keys_fail_before_upsert(self):
        for rows in ([{"id": None}], [{"id": 1}, {"id": 1}]):
            with self.assertRaises(ValueError):
                reference_upsert([], rows, ["id"])

    def test_backfill_changes_exact_selected_partitions(self):
        before = {"a": "old", "b": "old", "c": "same"}
        after = {"a": "new", "b": "new", "c": "same"}
        self.assertEqual(verify_changed_partitions(before, after, ["a", "b"])["unchanged"], ["c"])
        with self.assertRaises(ValueError):
            verify_changed_partitions(before, {**after, "c": "bad"}, ["a", "b"])
        with self.assertRaises(ValueError):
            verify_changed_partitions(before, {"a": "new", "b": "new"}, ["a", "b"])


class CanonicalTests(unittest.TestCase):
    def test_order_independent_typed_digest(self):
        columns = [("id", "BIGINT"), ("text", "VARCHAR")]
        rows = [(3, "黄桃:🍑\n"), (1, None), (2, "N")]
        baseline = digest_small(rows, columns, ["id"])
        for seed in range(10):
            shuffled = list(rows)
            random.Random(seed).shuffle(shuffled)
            self.assertEqual(digest_small(shuffled, columns, ["id"]), baseline)

    def test_length_framing_cannot_alias_delimiter_or_null(self):
        columns = [("a", "VARCHAR"), ("b", "VARCHAR")]
        self.assertNotEqual(row_bytes(["a:b", "c"], columns), row_bytes(["a", "b:c"], columns))
        self.assertNotEqual(cell(None, "VARCHAR"), cell("N", "VARCHAR"))
        self.assertNotEqual(cell("", "VARCHAR"), cell(None, "VARCHAR"))

    def test_binary64_is_exact_without_tolerance(self):
        self.assertNotEqual(cell(0.0, "DOUBLE"), cell(-0.0, "DOUBLE"))
        self.assertNotEqual(cell(0.1, "DOUBLE"), cell(0.10000000000000002, "DOUBLE"))
        for value in (float("nan"), float("inf"), float("-inf")):
            with self.assertRaises(ValueError):
                cell(value, "DOUBLE")

    def test_decimal_fixed_scale_contract(self):
        self.assertEqual(cell(Decimal("1.200"), "DECIMAL(8,2)"), cell(Decimal("1.20"), "DECIMAL(8,2)"))
        with self.assertRaises(ValueError):
            cell(Decimal("1.201"), "DECIMAL(8,2)")

    def test_dates_and_type_checks(self):
        self.assertEqual(cell(date(2026, 1, 1), "DATE"), cell("2026-01-01", "DATE"))
        for value, dtype in ((True, "BIGINT"), (1.1, "BIGINT"), (1, "VARCHAR"), ("x", "UNKNOWN")):
            with self.assertRaises((ValueError, TypeError)):
                cell(value, dtype)

    def test_schema_participates_in_identity(self):
        a = digest_small([(1,)], [("id", "BIGINT")], ["id"])
        b = digest_small([(1,)], [("id", "INTEGER")], ["id"])
        self.assertNotEqual(a["sha256"], b["sha256"])

    def test_duplicate_and_unsorted_streams_rejected(self):
        for rows in ([(1,), (1,)], [(2,), (1,)], [(None,)]):
            with self.assertRaises(ValueError):
                digest_sorted(rows, [("id", "BIGINT")], ["id"])

    def test_changed_value_changes_hash(self):
        columns = [("id", "BIGINT"), ("n", "BIGINT")]
        self.assertNotEqual(digest_small([(1, 2)], columns, ["id"])["sha256"],
                            digest_small([(1, 3)], columns, ["id"])["sha256"])


class GateTests(unittest.TestCase):
    def temporary(self):
        root = Path(os.environ.get("M4_TEST_TMP", Path(__file__).resolve().parent / "scratch"))
        root.mkdir(parents=True, exist_ok=True)
        temporary = tempfile.TemporaryDirectory(dir=root)
        self.addCleanup(temporary.cleanup)
        return Path(temporary.name)

    def test_quality_results_fail_closed(self):
        require_checks([{"name": "a", "passed": True}], {"a"})
        for checks in ([], [{"name": "a", "passed": False}], [{"name": "a", "passed": 1}],
                       [{"name": "a", "passed": True}, {"name": "a", "passed": True}]):
            with self.assertRaises(ValueError):
                require_checks(checks, {"a"})

    def test_fresh_stale_future_and_watermark(self):
        now = datetime(2026, 1, 1, tzinfo=timezone.utc)
        kwargs = {"materialized_at": now, "maximum_age_seconds": 86400, "actual_prefix": ["000000"], "expected_prefix": ["000000"]}
        self.assertTrue(processing_freshness(**kwargs, now=now)["passed"])
        self.assertFalse(processing_freshness(**kwargs, now=now + timedelta(seconds=86401))["passed"])
        self.assertFalse(processing_freshness(**kwargs, now=now - timedelta(seconds=1))["passed"])
        self.assertFalse(processing_freshness(**{**kwargs, "expected_prefix": ["000000", "000001"]}, now=now)["passed"])
        with self.assertRaises(ValueError):
            processing_freshness(**kwargs, now=datetime(2026, 1, 1))

    def test_terminal_gate_rejects_future_label_exposure_and_bad_branches(self):
        m = manifest()
        receipt = {"visible_prefix": [b["key"] for b in m["batches"]], "arrival_manifest_sha256": m["manifest_sha256"],
                   "status": "promoted", "scenario": "original", "overlay_sha256": None}
        require_terminal(m, receipt, parity=True, quality=True, original=True)
        for changed in ({"visible_prefix": ["000000"]}, {"scenario": "corrected"}, {"status": "checked"}, {"overlay_sha256": "overlay"}):
            with self.assertRaises(ValueError):
                require_terminal(m, {**receipt, **changed}, parity=True, quality=True, original=True)
        for parity, quality in ((False, True), (True, False)):
            with self.assertRaises(ValueError):
                require_terminal(m, receipt, parity=parity, quality=quality, original=True)

    def test_promotion_failure_preserves_previous_pointer_and_candidate(self):
        root = self.temporary()
        folder = root / "attempt"
        folder.mkdir()
        candidate = folder / "candidate.duckdb"
        candidate.write_bytes(b"unit fixture only, no engine")
        before = {"checkpoint": "old"}
        write_json_new(root / "promoted.json", before)
        receipt = {"status": "checked", "database": str(candidate.resolve()), "attempt_id": "test"}
        with self.assertRaises(ValueError):
            promote(root, candidate, receipt, expected_pointer=before, checks=[{"name": "quality", "passed": False}], expected_names={"quality"})
        self.assertEqual(read_json(root / "promoted.json"), before)
        self.assertTrue(candidate.exists())
        self.assertFalse((folder / "promoted-receipt.json").exists())

    def test_successful_atomic_pointer_and_stale_writer_rejection(self):
        root = self.temporary()
        for name in ("a", "b"):
            (root / name).mkdir()
            (root / name / "candidate.duckdb").write_bytes(b"not a runtime DB")
        candidate = root / "a/candidate.duckdb"
        checks = [{"name": "q", "passed": True}]
        receipt = {"status": "checked", "database": str(candidate.resolve()), "attempt_id": "a"}
        saved = promote(root, candidate, receipt, expected_pointer=None, checks=checks, expected_names={"q"})
        self.assertEqual(saved, read_json(root / "promoted.json"))
        second = root / "b/candidate.duckdb"
        with self.assertRaises(ValueError):
            promote(root, second, {**receipt, "database": str(second.resolve()), "attempt_id": "b"},
                    expected_pointer=None, checks=checks, expected_names={"q"})

    def test_create_only_artifacts(self):
        root = self.temporary()
        write_json_new(root / "result.json", {"first": True})
        with self.assertRaises(FileExistsError):
            write_json_new(root / "result.json", {"first": False})

    def test_local_alert_is_structured_idempotent_and_no_send(self):
        root = self.temporary()
        failure = {"run_id": "run", "check": "foreign_key", "partition": "000001", "attempt_id": "attempt", "reason": "unknown product"}
        messages = []
        now = datetime(2026, 1, 1, tzinfo=timezone.utc)
        first, created = emit_local_alert(root, failure, cursor="0", now=now, log=messages.append)
        duplicate, created_again = emit_local_alert(root, failure, cursor="1", now=now, log=messages.append)
        self.assertTrue(created)
        self.assertFalse(created_again)
        self.assertEqual(first, duplicate)
        self.assertEqual(len(messages), 1)
        lines = (root / "alerts.jsonl").read_text(encoding="utf-8").splitlines()
        self.assertEqual(len(lines), 1)
        self.assertEqual(json.loads(lines[0])["delivery"], "local JSONL only")


if __name__ == "__main__":
    unittest.main()
