import pytest

from duckdb_unittest import DuckDBUnittestRunner


pyspark_sql = pytest.importorskip("pyspark.sql")
Row = pyspark_sql.Row


@pytest.mark.requires_spark(">=4.0")
@pytest.mark.requires_capabilities("row_lineage", "format_v3")
@pytest.mark.spark_seed_tables("default.row_lineage_test_upgraded_insert")
def test_row_lineage_read_from_upgraded(
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

    catalog_connection.refresh_table("default.row_lineage_test_upgraded_insert")
    rows = catalog_connection.con.sql(
        """
        select id, data
        from default.row_lineage_test_upgraded_insert
        order by id
        """
    ).collect()
    assert rows == [
        Row(id=1, data="replaced"),
        Row(id=2, data="replaced_again"),
        Row(id=4, data="d_u1"),
        Row(id=6, data="replaced_again"),
        Row(id=7, data="g_new"),
    ]
