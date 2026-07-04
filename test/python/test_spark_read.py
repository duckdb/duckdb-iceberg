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
