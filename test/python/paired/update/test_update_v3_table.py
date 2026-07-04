import pytest

from duckdb_unittest import DuckDBUnittestRunner


@pytest.mark.requires_capabilities("format_v3")
@pytest.mark.spark_seed_tables("default.simple_v3_table", "default.pyspark_iceberg_table_v1")
def test_update_v3_table(
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
