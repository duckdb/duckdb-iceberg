import pytest

from duckdb_unittest import DuckDBUnittestRunner


@pytest.mark.requires_catalog("fixture")
def test_duckdb_catalog_functions_and_iceberg(
    paired_sqllogic_test_path,
    unittest_binary,
    print_unittest_stdin,
):
    with DuckDBUnittestRunner(
        unittest_binary,
        use_test_config=False,
        preamble="",
        print_stdin=print_unittest_stdin,
    ) as runner:
        runner.run_sqllogic_file(paired_sqllogic_test_path)
