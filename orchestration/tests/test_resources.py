"""Synthetic adapter contracts; no DuckDB installation or engine is used here."""
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch
from m4 import duck

class ResourceContracts(unittest.TestCase):
    def test_run_limits_are_explicit_and_temp_is_resolved(self):
        settings=duck.connection_options({'dbt_threads':2,'dbt_memory_limit':'1GB'},Path('owned-temp'))
        self.assertEqual(settings,{'threads':2,'memory_limit':'1GB','temp_directory':str(Path('owned-temp').resolve())})

    def test_invalid_limits_fail_before_opening_an_engine(self):
        for threads,memory in [(True,'1GB'),(0,'1GB'),(-1,'1GB'),(1.5,'1GB'),(2,'unlimited'),(2,'0GB'),(2,"1GB'; select 1;--")]:
            with self.subTest(threads=threads,memory=memory),patch.dict(sys.modules,{'duckdb':None}):
                with self.assertRaises(ValueError):
                    duck.connect(':memory:',threads=threads,memory_limit=memory)

    def test_every_connection_sets_declared_limits_and_owned_temp(self):
        fake=MagicMock()
        engine=SimpleNamespace(connect=MagicMock(return_value=fake))
        with patch.dict(sys.modules,{'duckdb':engine}):
            result=duck.connect(':memory:',read_only=False,threads=2,memory_limit='1GB',temp_directory=Path('owned-temp'))
        engine.connect.assert_called_once_with(':memory:',read_only=False)
        self.assertIs(result,fake)
        self.assertEqual([c.args[0] for c in fake.execute.call_args_list],["set threads=2","set memory_limit='1GB'","set temp_directory="+duck.quote(Path('owned-temp').resolve())])

    def test_failed_configuration_closes_open_connection(self):
        fake=MagicMock();fake.execute.side_effect=RuntimeError('synthetic setting failure')
        with patch.dict(sys.modules,{'duckdb':SimpleNamespace(connect=lambda *a,**k:fake)}):
            with self.assertRaisesRegex(RuntimeError,'synthetic setting failure'):
                duck.connect(':memory:',threads=2,memory_limit='1GB')
        fake.close.assert_called_once_with()

if __name__=='__main__':unittest.main()
