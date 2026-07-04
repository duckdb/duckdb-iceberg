import datetime
from decimal import Decimal

import pytest

from duckdb_unittest import DuckDBUnittestRunner
from scripts.data_generators.integration_config import REST_CATALOG_NAMES, resolve_active_catalog


pyspark_sql = pytest.importorskip("pyspark.sql")
pa = pytest.importorskip("pyarrow")
Row = pyspark_sql.Row


def _is_active_catalog(catalog: str) -> bool:
    try:
        return (
            resolve_active_catalog(
                allowed_catalogs=REST_CATALOG_NAMES,
                purpose="catalog-backed test/python runs",
            )
            == catalog
        )
    except RuntimeError:
        return False


@pytest.mark.spark_seed_tables("default.spark_written_upper_lower_bounds")
@pytest.mark.skipif(
    _is_active_catalog("polaris"), reason="Polaris does not currently support this Spark bounds read test"
)
@pytest.mark.skipif(_is_active_catalog("lakekeeper"), reason="Lakekeeper writes bounds differently for some reason")
def test_write_upper_and_lower_bounds(
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

    catalog_connection.restart()
    spark_rows = catalog_connection.con.sql("select * from default.lower_upper_bounds_test").collect()
    assert spark_rows == [
        Row(
            int_type=-2147483648,
            long_type=-9223372036854775808,
            varchar_type="",
            bool_type=False,
            float_type=-3.4028234663852886e38,
            double_type=-1.7976931348623157e308,
            decimal_type_18_3=Decimal("-9999999999999.999"),
            date_type=datetime.date(1, 1, 1),
            timestamp_type=datetime.datetime(1, 1, 1, 0, 0),
            binary_type=bytearray(b""),
        ),
        Row(
            int_type=2147483647,
            long_type=9223372036854775807,
            varchar_type="ZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZ",
            bool_type=True,
            float_type=3.4028234663852886e38,
            double_type=1.7976931348623157e308,
            decimal_type_18_3=Decimal("9999999999999.999"),
            date_type=datetime.date(9999, 12, 31),
            timestamp_type=datetime.datetime(9999, 12, 31, 23, 59, 59, 999999),
            binary_type=bytearray(b"\xff\xff\xff\xff\xff\xff\xff\xff"),
        ),
        Row(
            int_type=None,
            long_type=None,
            varchar_type=None,
            bool_type=None,
            float_type=None,
            double_type=None,
            decimal_type_18_3=None,
            date_type=None,
            timestamp_type=None,
            binary_type=None,
        ),
    ]

    arrow_table: pa.Table = rest_catalog.load_table("default.lower_upper_bounds_test").scan().to_arrow()
    assert arrow_table.to_pylist() == [
        {
            "int_type": -2147483648,
            "long_type": -9223372036854775808,
            "varchar_type": "",
            "bool_type": False,
            "float_type": -3.4028234663852886e38,
            "double_type": -1.7976931348623157e308,
            "decimal_type_18_3": Decimal("-9999999999999.999"),
            "date_type": datetime.date(1, 1, 1),
            "timestamp_type": datetime.datetime(1, 1, 1, 0, 0),
            "binary_type": b"",
        },
        {
            "int_type": 2147483647,
            "long_type": 9223372036854775807,
            "varchar_type": "ZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZ",
            "bool_type": True,
            "float_type": 3.4028234663852886e38,
            "double_type": 1.7976931348623157e308,
            "decimal_type_18_3": Decimal("9999999999999.999"),
            "date_type": datetime.date(9999, 12, 31),
            "timestamp_type": datetime.datetime(9999, 12, 31, 23, 59, 59, 999999),
            "binary_type": b"\xff\xff\xff\xff\xff\xff\xff\xff",
        },
        {
            "int_type": None,
            "long_type": None,
            "varchar_type": None,
            "bool_type": None,
            "float_type": None,
            "double_type": None,
            "decimal_type_18_3": None,
            "date_type": None,
            "timestamp_type": None,
            "binary_type": None,
        },
    ]
