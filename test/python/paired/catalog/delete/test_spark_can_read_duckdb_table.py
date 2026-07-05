import pytest

from duckdb_unittest import DuckDBUnittestRunner


pyspark_sql = pytest.importorskip("pyspark.sql")
pa = pytest.importorskip("pyarrow")
Row = pyspark_sql.Row


def test_spark_can_read_duckdb_table(
    paired_sqllogic_test_path,
    catalog_connection,
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

    catalog_connection.refresh_table("default.duckdb_deletes_for_other_engines")
    spark_rows = catalog_connection.con.sql(
        "select * from default.duckdb_deletes_for_other_engines order by a"
    ).collect()
    assert spark_rows == [
        Row(a=1),
        Row(a=3),
        Row(a=5),
        Row(a=7),
        Row(a=9),
        Row(a=51),
        Row(a=53),
        Row(a=55),
        Row(a=57),
        Row(a=59),
    ]

    arrow_table: pa.Table = rest_catalog.load_table("default.duckdb_deletes_for_other_engines").scan().to_arrow()
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
    ]
