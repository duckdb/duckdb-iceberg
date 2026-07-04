import pytest

from duckdb_unittest import DuckDBUnittestRunner


pyspark_sql = pytest.importorskip("pyspark.sql")
Row = pyspark_sql.Row


@pytest.mark.requires_spark(">=4.0")
@pytest.mark.requires_capabilities("format_v3")
def test_delete_consolidation_transactional(
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
    rows = catalog_connection.con.sql("select * from default.write_v3_update_and_delete order by all").collect()
    assert rows == [
        Row(id=1, data="a"),
        Row(id=2, data="b_u1"),
        Row(id=3, data="c"),
        Row(id=4, data="d_u1"),
        Row(id=5, data="e"),
    ]
