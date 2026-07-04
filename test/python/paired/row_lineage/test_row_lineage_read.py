import pytest

from duckdb_unittest import DuckDBUnittestRunner


@pytest.mark.requires_spark(">=4.0")
@pytest.mark.requires_capabilities("row_lineage", "format_v3")
@pytest.mark.spark_seed_tables("default.row_lineage_test")
def test_row_lineage_read(
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
