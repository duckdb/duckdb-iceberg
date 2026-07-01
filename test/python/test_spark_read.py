import pytest
import os
import datetime
from decimal import Decimal
from math import inf

from conftest import *

from pprint import pprint

SCRIPT_DIR = os.path.dirname(__file__)

pyspark = pytest.importorskip("pyspark")
pyspark_sql = pytest.importorskip("pyspark.sql")
SparkSession = pyspark_sql.SparkSession
SparkContext = pyspark.SparkContext
Row = pyspark_sql.Row

requires_equality_deletes_available = pytest.mark.skipif(
    (os.getenv("EQUALITY_DELETE_WRITES_ENABLED", None) is None)
    or (os.getenv("EQUALITY_DELETE_WRITES_ENABLED", '0') == '0'),
    reason="Test data wasn't generated, run tests in test/sql/local/irc first (and set 'export EQUALITY_DELETE_WRITES_ENABLED=1')",
)


def is_active_catalog(catalog: str):
    try:
        return (
            resolve_active_catalog(
                allowed_catalogs=REST_CATALOG_NAMES,
                purpose="catalog-backed test/python runs",
            )
            == catalog
        )
    except:
        return False


class TestSparkRead:
    def test_spark_read_duckdb_table(self, spark_con):
        df = spark_con.sql(
            """
            select * from default.duckdb_written_table order by a
            """
        )
        res = df.collect()
        assert res == [
            Row(a=0),
            Row(a=1),
            Row(a=2),
            Row(a=3),
            Row(a=4),
            Row(a=5),
            Row(a=6),
            Row(a=7),
            Row(a=8),
            Row(a=9),
        ]

    def test_spark_read_table_with_deletes(self, spark_con):
        df = spark_con.sql(
            """
            select * from default.duckdb_deletes_for_other_engines order by a
            """
        )
        res = df.collect()
        assert res == [
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

    def test_spark_read_duckdb_compacted_spark_rewrite_mor_residual_deletes(self, spark_con):
        # requires rewrite_data_files_spark_mor_residual_deletes.test to run
        df = spark_con.sql(
            """
            select * from default.spark_rewrite_mor_residual_deletes order by id
            """
        )
        res = df.collect()
        assert res == [
            Row(id=1, category="compact", payload="c1"),
            Row(id=2, category="compact", payload="c2_u"),
            Row(id=4, category="compact", payload="c4"),
            Row(id=5, category="compact", payload="c5"),
            Row(id=6, category="compact", payload="c6"),
            Row(id=100, category="residual", payload="r100"),
            Row(id=101, category="residual", payload="r101_u"),
        ]

    @pytest.mark.skipif(
        is_active_catalog('polaris'), reason="Polaris does not currently support this Spark bounds read test"
    )
    @pytest.mark.skipif(is_active_catalog('lakekeeper'), reason="Lakekeeper writes bounds differently for some reason")
    def test_spark_read_upper_and_lower_bounds(self, spark_con):
        df = spark_con.sql(
            """
            select * from default.lower_upper_bounds_test;
            """
        )
        res = df.collect()
        assert len(res) == 3
        assert res == [
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

    def test_spark_read_infinities(self, spark_con):
        df = spark_con.sql(
            """
            select * from default.test_infinities;
            """
        )
        res = df.collect()
        assert len(res) == 2
        assert res == [
            Row(float_type=inf, double_type=inf),
            Row(float_type=-inf, double_type=-inf),
        ]

    def test_duckdb_written_nested_types(self, spark_con):
        df = spark_con.sql(
            """
            select * from default.duckdb_nested_types;
            """
        )
        res = df.collect()
        assert len(res) == 1
        assert res == [
            Row(
                id=1,
                name="Alice",
                address=Row(street="123 Main St", city="Metropolis", zip="12345"),
                phone_numbers=["123-456-7890", "987-654-3210"],
                metadata={"age": "30", "membership": "gold"},
            ),
        ]

    @pytest.mark.requires_spark(">=4.0")
    @pytest.mark.requires_capabilities("format_v3")
    def test_duckdb_written_deletion_vectors(self, spark_con):
        # requires test_delete_consolidation_transactional.test to run
        res = spark_con.sql(
            """
            select * from default.write_v3_update_and_delete order by all
            """
        ).collect()
        assert (
            str(res)
            == "[Row(id=1, data='a'), Row(id=2, data='b_u1'), Row(id=3, data='c'), Row(id=4, data='d_u1'), Row(id=5, data='e')]"
        )

    @pytest.mark.requires_spark(">=4.0")
    @pytest.mark.requires_capabilities("format_v3")
    def test_spark_read_duckdb_created_variant(self, spark_con):
        VariantVal = pyspark.sql.VariantVal

        def assert_variant_equal(actual, value_bytes, metadata_bytes):
            assert bytes(actual.value) == value_bytes
            assert bytes(actual.metadata) == metadata_bytes

        res = spark_con.sql(
            """
            select * from default.my_variant_tbl order by b
            """
        ).collect()

        row = res[0]
        assert row.b == 42
        assert_variant_equal(
            row.a,
            b"\x11test",
            b"\x11\x00\x00",
        )
        row = res[1]
        assert row.b == 43
        assert_variant_equal(
            row.a,
            b"\x02\x02\x00\x01\x00\x05&\x149\x05\x00\x00\x03\x03\x00\x05\x11\x1b\x14\x01\x00\x00\x00-hello world\x02\x01\x02\x00\x05\x14)\x00\x00\x00",
            b"\x11\x03\x00\x01\x02\x03abd",
        )

    @pytest.mark.requires_spark(">=4.0")
    @pytest.mark.requires_capabilities("row_lineage", "format_v3")
    def test_duckdb_written_row_lineage(self, spark_con):
        df = spark_con.sql(
            """
            select _last_updated_sequence_number, _row_id, * from default.duckdb_row_lineage order by _row_id;
            """
        )
        res = df.collect()
        print(res)
        assert res == [
            Row(_last_updated_sequence_number=5, _row_id=0, id=1, data="replaced"),
            Row(_last_updated_sequence_number=2, _row_id=1, id=2, data="b_u1"),
            Row(_last_updated_sequence_number=2, _row_id=3, id=4, data="d_u1"),
            Row(_last_updated_sequence_number=5, _row_id=7, id=6, data="replaced"),
            Row(_last_updated_sequence_number=7, _row_id=11, id=7, data="g_new"),
        ]

    # Written by Spark, read by Spark
    # See https://github.com/duckdb/duckdb-iceberg/pull/908 on why spark behavior is unclear
    @pytest.mark.skip(reason="Failures due to unclear Spark behavior regarding row ids")
    @pytest.mark.requires_spark(">=4.0")
    def test_spark_read_row_lineage_from_upgraded(self, spark_con):
        df = spark_con.sql(
            """
            select _last_updated_sequence_number, _row_id, * from default.row_lineage_test_upgraded_insert order by id;
            """
        )
        res = df.collect()
        assert res == [
            Row(_last_updated_sequence_number=5, _row_id=3, id=1, data="replaced"),
            Row(_last_updated_sequence_number=8, _row_id=0, id=2, data="replaced_again"),
            Row(_last_updated_sequence_number=2, _row_id=6, id=4, data="d_u1"),
            Row(_last_updated_sequence_number=8, _row_id=1, id=6, data="replaced_again"),
            Row(_last_updated_sequence_number=7, _row_id=2, id=7, data="g_new"),
        ]

    # Written by DuckDB (after upgrading with Spark), read by Spark
    @pytest.mark.requires_spark(">=4.0")
    @pytest.mark.skip(reason="Failures due to unclear Spark behavior regarding row ids")
    def test_spark_read_row_lineage_from_upgraded_by_duckdb(self, spark_con):
        df = spark_con.sql(
            """
            select _last_updated_sequence_number, _row_id, * from default.row_lineage_test_upgraded order by id;
            """
        )
        res = df.collect()
        assert res == [
            Row(_last_updated_sequence_number=8, _row_id=3, id=2, data="replaced_again"),
            Row(_last_updated_sequence_number=7, _row_id=0, id=7, data="g_new"),
        ]


@requires_equality_deletes_available
class TestSparkReadEqualityDeletes:
    @pytest.mark.skip(
        reason="Spark errors when reading tables with a column that has an equality delete applied and the column has been dropped"
    )
    def test_spark_read_equality_deletes_with_dropped_column(self, spark_con):
        df = spark_con.sql(
            """
            select * from default.equality_delete_table_1 order by all
            """
        )
        res = df.collect()
        assert res == [
            Row(a=0, c=0),
            Row(a=2, c=2),
            Row(a=3, c=3),
            Row(a=4, c=4),
            Row(a=5, c=5),
            Row(a=7, c=7),
            Row(a=8, c=8),
            Row(a=9, c=9),
            Row(a=10, c=0),
            Row(a=12, c=2),
            Row(a=13, c=3),
            Row(a=14, c=4),
            Row(a=15, c=5),
            Row(a=17, c=7),
            Row(a=18, c=8),
            Row(a=19, c=9),
            Row(a=20, c=0),
            Row(a=100, c=100),
            Row(a=101, c=101),
            Row(a=102, c=102),
            Row(a=103, c=103),
            Row(a=104, c=104),
            Row(a=105, c=105),
        ]

    def test_spark_read_equality_deletes(self, spark_con):
        df = spark_con.sql(
            """
            select * from default.equality_delete_table_test_multiple_equality_deletes order by all
            """
        )
        res = df.collect()
        assert res == [
            Row(a=0, b=0, c=0),
            Row(a=1, b=1, c=1),
            Row(a=2, b=2, c=2),
            Row(a=3, b=3, c=3),
            Row(a=4, b=4, c=4),
            Row(a=5, b=0, c=5),
            Row(a=7, b=2, c=7),
            Row(a=8, b=3, c=8),
            Row(a=9, b=4, c=9),
            Row(a=10, b=0, c=0),
            Row(a=11, b=1, c=1),
            Row(a=12, b=2, c=2),
            Row(a=13, b=3, c=3),
            Row(a=14, b=4, c=4),
            Row(a=15, b=0, c=5),
            Row(a=17, b=2, c=7),
            Row(a=18, b=3, c=8),
            Row(a=19, b=4, c=9),
            Row(a=20, b=0, c=0),
        ]
