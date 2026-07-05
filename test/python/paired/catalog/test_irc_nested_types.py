import pytest

from duckdb_unittest import DuckDBUnittestRunner
from paired.local.test_nested_types import SEED_TABLE


@pytest.mark.spark_seed_tables(SEED_TABLE)
def test_irc_nested_types(
    paired_sqllogic_test_path,
    unittest_binary,
    unittest_test_config,
    print_unittest_stdin,
):
    with DuckDBUnittestRunner(
        unittest_binary,
        test_config=unittest_test_config,
        print_stdin=print_unittest_stdin,
    ) as runner:
        runner.run_sqllogic_file(paired_sqllogic_test_path)
