import pytest

from duckdb_unittest import DuckDBUnittestRunner


pyspark_sql = pytest.importorskip("pyspark.sql")
Row = pyspark_sql.Row


def test_create_table_then_read_with_other_engine(
    paired_sqllogic_test_path,
    catalog_connection,
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

    catalog_connection.restart()
    rows = catalog_connection.con.sql("select * from default.duckdb_written_table order by a").collect()
    assert rows == [Row(a=value) for value in range(10)]
