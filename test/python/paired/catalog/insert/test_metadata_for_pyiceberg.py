import pytest

from duckdb_unittest import DuckDBUnittestRunner


pyice = pytest.importorskip("pyiceberg")


def test_metadata_for_pyiceberg(
    paired_sqllogic_test_path,
    unittest_binary,
    unittest_test_config,
    print_unittest_stdin,
    rest_catalog,
):
    with DuckDBUnittestRunner(
        unittest_binary,
        test_config=unittest_test_config,
        print_stdin=print_unittest_stdin,
    ) as runner:
        runner.run_sqllogic_file(paired_sqllogic_test_path)

    scan = rest_catalog.load_table("default.test_metadata_for_pyiceberg").scan(
        row_filter=pyice.expressions.EqualTo("a", 350)
    )
    matched_files = [task.file.file_path for task in scan.plan_files()]
    assert len(matched_files) == 1
