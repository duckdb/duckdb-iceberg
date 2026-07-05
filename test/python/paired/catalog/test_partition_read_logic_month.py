import pytest

from duckdb_unittest import DuckDBUnittestRunner
from spark_seed import RegisteredSeedTable


@pytest.mark.spark_seed_tables(RegisteredSeedTable("default.day_partitioned_table"))
def test_partition_read_logic_month(
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
