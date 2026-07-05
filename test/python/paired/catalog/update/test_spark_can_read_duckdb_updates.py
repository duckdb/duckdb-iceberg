import pytest

from duckdb_unittest import DuckDBUnittestRunner


pa = pytest.importorskip("pyarrow")


def test_spark_can_read_duckdb_updates(
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

    arrow_table: pa.Table = rest_catalog.load_table("default.duckdb_updates_for_other_engines").scan().to_arrow()
    assert arrow_table.to_pylist() == [
        {"a": 1},
        {"a": 3},
        {"a": 5},
        {"a": 7},
        {"a": 9},
        {"a": 51},
        {"a": 53},
        {"a": 55},
        {"a": 57},
        {"a": 59},
        {"a": 100},
        {"a": 100},
        {"a": 100},
        {"a": 100},
        {"a": 100},
        {"a": 100},
        {"a": 100},
        {"a": 100},
        {"a": 100},
        {"a": 100},
    ]
