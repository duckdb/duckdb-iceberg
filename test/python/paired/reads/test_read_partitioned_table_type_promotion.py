import pytest

from duckdb_unittest import DuckDBUnittestRunner


@pytest.mark.spark_seed_tables("default.type_promotion_partitioned_table")
def test_read_partitioned_table_type_promotion(
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
