from pathlib import Path

import pytest

from duckdb_unittest import DuckDBUnittestRunner


@pytest.mark.spark_seed_tables("default.truncate_partitioned_varchar")
def test_truncate_varchar(unittest_binary, unittest_test_config, print_unittest_stdin):
    with DuckDBUnittestRunner(
        unittest_binary,
        test_config=unittest_test_config,
        print_stdin=print_unittest_stdin,
    ) as runner:
        runner.run_sqllogic_file(Path(__file__).with_name("truncate_varchar.test"))
