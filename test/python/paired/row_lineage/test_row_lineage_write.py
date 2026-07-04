import pytest

from duckdb_unittest import DuckDBUnittestRunner


pyspark_sql = pytest.importorskip("pyspark.sql")
Row = pyspark_sql.Row


@pytest.mark.requires_spark(">=4.0")
@pytest.mark.requires_capabilities("row_lineage", "format_v3")
def test_row_lineage_write(
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
    rows = catalog_connection.con.sql(
        """
        select _last_updated_sequence_number, _row_id, * from default.duckdb_row_lineage order by _row_id
        """
    ).collect()
    assert rows == [
        Row(_last_updated_sequence_number=5, _row_id=0, id=1, data="replaced"),
        Row(_last_updated_sequence_number=2, _row_id=1, id=2, data="b_u1"),
        Row(_last_updated_sequence_number=2, _row_id=3, id=4, data="d_u1"),
        Row(_last_updated_sequence_number=5, _row_id=7, id=6, data="replaced"),
        Row(_last_updated_sequence_number=7, _row_id=11, id=7, data="g_new"),
    ]
