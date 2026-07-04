import pytest

from duckdb_unittest import DuckDBUnittestRunner


@pytest.mark.spark_seed_tables("default.spark_written_upper_lower_bounds_single_value")
def test_write_upper_and_lower_bounds_single_values(
    paired_sqllogic_test_path, unittest_binary, unittest_test_config, print_unittest_stdin, catalog_connection
):
    with DuckDBUnittestRunner(
        unittest_binary,
        test_config=unittest_test_config,
        print_stdin=print_unittest_stdin,
    ) as runner:
        runner.run_sqllogic_file(paired_sqllogic_test_path)
