"""Deferred adapter tests. Run only with M4_RUNTIME_TEST_ROOT + manifest prepared.

They intentionally fail/skip distinctly when not configured. Pure preparation
never imports this file and never reports these engine gates as passed.
"""
import os
import hashlib
from pathlib import Path
import unittest
from unittest.mock import patch


@unittest.skipUnless(os.environ.get("M4_RUNTIME_TEST_ROOT"), "not run: explicit isolated runtime not configured")
class DefinitionsContract(unittest.TestCase):
    def test_selected_downstream_production_builder_has_no_side_effects(self):
        from m4.runtime import Runtime
        from m4.downstream_completion import build_downstream_definitions
        rt = Runtime(os.environ["M4_RUNTIME_TEST_ROOT"])
        self.assertEqual(rt.config.get("data_kind"), "synthetic_test")

        def snapshot():
            return {str(path.relative_to(rt.root)): hashlib.sha256(path.read_bytes()).hexdigest()
                    if path.is_file() else "directory" for path in rt.root.rglob("*")}

        before = snapshot()
        with patch("subprocess.run", side_effect=AssertionError("selected definitions executed a command")), \
             patch("duckdb.connect", side_effect=AssertionError("selected definitions opened a database")):
            definitions = build_downstream_definitions(rt, os.environ["M4_DBT_MANIFEST"])
            selected = definitions.get_job_def("recsys_downstream_only")
            self.assertEqual({node.name for node in selected.graph.node_defs},
                             {"recsys_train_eval", "api_benchmark"})
        self.assertEqual(before, snapshot())

    def test_load_has_no_command_or_database_side_effects(self):
        from m4.runtime import Runtime
        from m4.definitions import build_definitions
        from dagster import Definitions, DefaultScheduleStatus, DefaultSensorStatus
        rt = Runtime(os.environ["M4_RUNTIME_TEST_ROOT"])
        before = sorted(str(p.relative_to(rt.root)) for p in rt.root.rglob("*"))
        with patch("subprocess.run", side_effect=AssertionError("definition loading executed a command")), \
             patch("duckdb.connect", side_effect=AssertionError("definition loading opened a database")):
            defs = build_definitions(rt, os.environ["M4_DBT_MANIFEST"])
            Definitions.validate_loadable(defs)
        self.assertEqual(before, sorted(str(p.relative_to(rt.root)) for p in rt.root.rglob("*")))
        self.assertEqual(defs.get_schedule_def("daily_replay_definition_only").default_status, DefaultScheduleStatus.STOPPED)
        self.assertEqual(defs.get_sensor_def("local_quality_alert").default_status, DefaultSensorStatus.STOPPED)


if __name__ == "__main__":
    unittest.main()
