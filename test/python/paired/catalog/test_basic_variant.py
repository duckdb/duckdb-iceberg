import pytest

from duckdb_unittest import DuckDBUnittestRunner


pyspark = pytest.importorskip("pyspark")


@pytest.mark.requires_spark(">=4.0")
@pytest.mark.requires_capabilities("format_v3")
@pytest.mark.spark_seed_tables("default.variant_column")
def test_basic_variant(
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

    catalog_connection.refresh_table("default.my_variant_tbl")
    rows = catalog_connection.con.sql("select * from default.my_variant_tbl order by b").collect()

    def assert_variant_equal(actual, value_bytes, metadata_bytes):
        assert bytes(actual.value) == value_bytes
        assert bytes(actual.metadata) == metadata_bytes

    first, second = rows
    assert first.b == 42
    assert_variant_equal(first.a, b"\x11test", b"\x11\x00\x00")

    assert second.b == 43
    assert_variant_equal(
        second.a,
        b"\x02\x02\x00\x01\x00\x05&\x149\x05\x00\x00\x03\x03\x00\x05\x11\x1b\x14\x01\x00\x00\x00-hello world\x02\x01\x02\x00\x05\x14)\x00\x00\x00",
        b"\x11\x03\x00\x01\x02\x03abd",
    )
