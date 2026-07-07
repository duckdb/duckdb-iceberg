import pytest

from duckdb_unittest import DuckDBUnittestRunner


pyspark_sql = pytest.importorskip("pyspark.sql")
pa = pytest.importorskip("pyarrow")
Row = pyspark_sql.Row


def test_write_upper_lower_bounds_nested_types(
    paired_sqllogic_test_path,
    catalog_connection,
    unittest_binary,
    unittest_test_config,
    print_unittest_stdin,
    rest_catalog,
):
    with DuckDBUnittestRunner(
        unittest_binary,
        preamble='',
        test_config=unittest_test_config,
        print_stdin=print_unittest_stdin,
    ) as runner:
        runner.run_sqllogic_file(paired_sqllogic_test_path)

    catalog_connection.refresh_table("default.duckdb_nested_types")
    spark_rows = catalog_connection.con.sql("select * from default.duckdb_nested_types").collect()
    assert spark_rows == [
        Row(
            id=1,
            name="Alice",
            address=Row(street="123 Main St", city="Metropolis", zip="12345"),
            phone_numbers=["123-456-7890", "987-654-3210"],
            metadata={"age": "30", "membership": "gold"},
        )
    ]

    arrow_table: pa.Table = rest_catalog.load_table("default.duckdb_nested_types").scan().to_arrow()
    assert arrow_table.to_pylist() == [
        {
            "id": 1,
            "name": "Alice",
            "address": {"street": "123 Main St", "city": "Metropolis", "zip": "12345"},
            "phone_numbers": ["123-456-7890", "987-654-3210"],
            "metadata": [("age", "30"), ("membership", "gold")],
        }
    ]
