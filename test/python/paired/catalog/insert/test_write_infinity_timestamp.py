from math import inf

import pytest

from duckdb_unittest import DuckDBUnittestRunner


pyspark_sql = pytest.importorskip("pyspark.sql")
pa = pytest.importorskip("pyarrow")
Row = pyspark_sql.Row


def test_write_infinity_timestamp(
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

    catalog_connection.refresh_table("default.test_infinities")
    spark_rows = catalog_connection.con.sql("select * from default.test_infinities").collect()
    assert spark_rows == [Row(float_type=inf, double_type=inf), Row(float_type=-inf, double_type=-inf)]

    arrow_table: pa.Table = rest_catalog.load_table("default.test_infinities").scan().to_arrow()
    assert arrow_table.to_pylist() == [
        {"float_type": inf, "double_type": inf},
        {"float_type": -inf, "double_type": -inf},
    ]
