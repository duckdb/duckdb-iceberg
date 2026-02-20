import pytest
import os
import datetime
from decimal import Decimal
from math import inf
from dataclasses import dataclass
from packaging.version import Version
from packaging.specifiers import SpecifierSet

from conftest import *

from pprint import pprint

SCRIPT_DIR = os.path.dirname(__file__)

pyspark_sql = pytest.importorskip("pyspark.sql")
SparkSession = pyspark_sql.SparkSession
SparkContext = pyspark.SparkContext
Row = pyspark_sql.Row

PYSPARK_VERSION = Version(pyspark.__version__)


@dataclass
class IcebergRuntimeConfig:
    spark_version: Version
    scala_binary_version: str
    iceberg_library_version: str


# uses {spark}_{scala}-{iceberg}
def generate_jar_location(config: IcebergRuntimeConfig) -> str:
    return f"iceberg-spark-runtime-{config.spark_version}_{config.scala_binary_version}-{config.iceberg_library_version}.jar"


# uses {spark}_{scala}:{iceberg}
def generate_package(config: IcebergRuntimeConfig) -> str:
    return f'org.apache.iceberg:iceberg-spark-runtime-{config.spark_version}_{config.scala_binary_version}:{config.iceberg_library_version}'


# List of runtimes you want to test
ICEBERG_RUNTIMES = [
    IcebergRuntimeConfig(
        spark_version=Version("3.5"),
        scala_binary_version="2.12",
        iceberg_library_version="1.4.1",
    ),
    IcebergRuntimeConfig(
        spark_version=Version("3.5"),
        scala_binary_version="2.12",
        iceberg_library_version="1.9.0",
    ),
    IcebergRuntimeConfig(
        spark_version=Version("3.5"),
        scala_binary_version="2.13",
        iceberg_library_version="1.9.1",
    ),
    IcebergRuntimeConfig(
        spark_version=Version("4.0"),
        scala_binary_version="2.13",
        iceberg_library_version="1.10.0",
    ),
]


@pytest.fixture(params=ICEBERG_RUNTIMES, scope="session")
def spark_con(request):
    runtime_config = request.param
    if runtime_config.spark_version.major != PYSPARK_VERSION.major:
        pytest.skip(
            f"Skipping Iceberg runtime "
            f"(Spark {runtime_config.spark_version}, Scala {runtime_config.scala_binary_version}, "
            f"Iceberg {runtime_config.iceberg_library_version}) "
            f"because current PySpark version is {PYSPARK_VERSION}"
        )

    runtime_jar = generate_jar_location(runtime_config)
    runtime_pkg = generate_package(runtime_config)
    runtime_path = os.path.abspath(os.path.join(SCRIPT_DIR, '..', '..', 'scripts', 'data_generators', runtime_jar))

    os.environ["PYSPARK_SUBMIT_ARGS"] = (
        f"--packages {runtime_pkg},org.apache.iceberg:iceberg-aws-bundle:{runtime_config.iceberg_library_version} pyspark-shell"
    )
    os.environ["AWS_REGION"] = "us-east-1"
    os.environ["AWS_ACCESS_KEY_ID"] = "admin"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "password"

    spark = (
        SparkSession.builder.appName(f"DuckDB REST Integration tes")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.demo", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.demo.type", "rest")
        .config("spark.sql.catalog.demo.uri", "http://127.0.0.1:8181")
        .config("spark.sql.catalog.demo.warehouse", "s3://warehouse/wh/")
        .config("spark.sql.catalog.demo.s3.endpoint", "http://127.0.0.1:9000")
        .config("spark.sql.catalog.demo.s3.path-style-access", "true")
        .config("spark.driver.memory", "10g")
        .config('spark.jars', runtime_path)
        .config("spark.sql.catalogImplementation", "in-memory")
        .config("spark.sql.catalog.demo.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .getOrCreate()
    )
    spark.sql("USE demo")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS default")
    spark.sql("USE NAMESPACE default")
    return spark


requires_iceberg_server = pytest.mark.skipif(
    os.getenv("ICEBERG_SERVER_AVAILABLE", None) is None,
    reason="Test data wasn't generated, run tests in test/sql/local/irc first (and set 'export ICEBERG_SERVER_AVAILABLE=1')",
)


@requires_iceberg_server
class TestSparkRead:
    def test_spark_read_insert_test(self, spark_con):
        df = spark_con.sql(
            """
            select * from default.insert_test order by col1, col2, col3
        """
        )
        res = df.collect()
        assert res == [
            Row(col1=datetime.date(2010, 6, 11), col2=42, col3='test'),
            Row(col1=datetime.date(2020, 8, 12), col2=45345, col3='inserted by con1'),
            Row(col1=datetime.date(2020, 8, 13), col2=1, col3='insert 1'),
            Row(col1=datetime.date(2020, 8, 14), col2=2, col3='insert 2'),
            Row(col1=datetime.date(2020, 8, 15), col2=3, col3='insert 3'),
            Row(col1=datetime.date(2020, 8, 16), col2=4, col3='insert 4'),
        ]

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
                varchar_type='',
                bool_type=False,
                float_type=-3.4028234663852886e38,
                double_type=-1.7976931348623157e308,
                decimal_type_18_3=Decimal('-9999999999999.999'),
                date_type=datetime.date(1, 1, 1),
                timestamp_type=datetime.datetime(1, 1, 1, 0, 0),
                binary_type=bytearray(b''),
            ),
            Row(
                int_type=2147483647,
                long_type=9223372036854775807,
                varchar_type='ZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZ',
                bool_type=True,
                float_type=3.4028234663852886e38,
                double_type=1.7976931348623157e308,
                decimal_type_18_3=Decimal('9999999999999.999'),
                date_type=datetime.date(9999, 12, 31),
                timestamp_type=datetime.datetime(9999, 12, 31, 23, 59, 59, 999999),
                binary_type=bytearray(b'\xff\xff\xff\xff\xff\xff\xff\xff'),
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
                name='Alice',
                address=Row(street='123 Main St', city='Metropolis', zip='12345'),
                phone_numbers=['123-456-7890', '987-654-3210'],
                metadata={'age': '30', 'membership': 'gold'},
            ),
        ]

    def test_duckdb_written_deletion_vectors(self, spark_con):
        res = spark_con.sql(
            """
            select * from default.write_v3_update_and_delete order by all
            """
        ).collect()

        assert str(res) == "[Row(id=1, data='a')]"

    def test_duckdb_written_row_lineage(self, spark_con):
        df = spark_con.sql(
            """
            select _last_updated_sequence_number, _row_id, * from default.duckdb_row_lineage order by _row_id;
            """
        )
        res = df.collect()
        print(res)
        assert res == [
            Row(_last_updated_sequence_number=5, _row_id=0, id=1, data='replaced'),
            Row(_last_updated_sequence_number=2, _row_id=1, id=2, data='b_u1'),
            Row(_last_updated_sequence_number=2, _row_id=3, id=4, data='d_u1'),
            Row(_last_updated_sequence_number=5, _row_id=7, id=6, data='replaced'),
            Row(_last_updated_sequence_number=7, _row_id=11, id=7, data='g_new'),
        ]
