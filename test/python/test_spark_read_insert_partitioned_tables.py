import pytest
import os
import time
import datetime
from decimal import Decimal
from conftest import *

from pprint import pprint

# Pin the process timezone to UTC so that TIMESTAMP values round-trip
# correctly on machines whose system timezone is not UTC.
os.environ["TZ"] = "UTC"
time.tzset()

SCRIPT_DIR = os.path.dirname(__file__)

pyspark_sql = pytest.importorskip("pyspark.sql")
SparkSession = pyspark_sql.SparkSession
SparkContext = pyspark.SparkContext
Row = pyspark_sql.Row

from dataclasses import dataclass
from packaging.version import Version
from packaging.specifiers import SpecifierSet


@dataclass
class IcebergRuntimeConfig:
    spark_version: Version
    scala_binary_version: str
    iceberg_library_version: str
    supports_v3: bool = True


def generate_jar_location(config: IcebergRuntimeConfig) -> str:
    return f"iceberg-spark-runtime-{config.spark_version}_{config.scala_binary_version}-{config.iceberg_library_version}.jar"


def generate_package(config: IcebergRuntimeConfig) -> str:
    return f"org.apache.iceberg:iceberg-spark-runtime-{config.spark_version}_{config.scala_binary_version}:{config.iceberg_library_version}"


ICEBERG_RUNTIMES = [
    IcebergRuntimeConfig(
        spark_version=Version("3.5"),
        scala_binary_version="2.12",
        iceberg_library_version="1.4.1",
        supports_v3=False,
    ),
    IcebergRuntimeConfig(
        spark_version=Version("3.5"),
        scala_binary_version="2.12",
        iceberg_library_version="1.9.0",
        supports_v3=False,
    ),
    IcebergRuntimeConfig(
        spark_version=Version("3.5"),
        scala_binary_version="2.13",
        iceberg_library_version="1.9.1",
        supports_v3=False,
    ),
    IcebergRuntimeConfig(
        spark_version=Version("4.0"),
        scala_binary_version="2.13",
        iceberg_library_version="1.10.0",
    ),
]


def _get_spark(spark_con):
    spark, _ = spark_con
    return spark


@pytest.fixture(params=ICEBERG_RUNTIMES, scope="session")
def spark_con(request):
    runtime_config = request.param
    if runtime_config.spark_version.major != PYSPARK_VERSION.major:
        pytest.skip(
            f"Skipping Iceberg runtime "
            f"Iceberg {runtime_config.iceberg_library_version}) "
            f"because current PySpark version is {PYSPARK_VERSION}"
        )

    runtime_jar = generate_jar_location(runtime_config)
    runtime_pkg = generate_package(runtime_config)
    runtime_path = os.path.abspath(os.path.join(SCRIPT_DIR, "..", "..", "scripts", "data_generators", runtime_jar))

    os.environ["PYSPARK_SUBMIT_ARGS"] = (
        f"--packages {runtime_pkg},org.apache.iceberg:iceberg-aws-bundle:{runtime_config.iceberg_library_version} pyspark-shell"
    )
    os.environ["AWS_REGION"] = "us-east-1"
    os.environ["AWS_ACCESS_KEY_ID"] = "admin"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "password"

    active = SparkSession.getActiveSession()
    if active is not None:
        active.stop()

    spark = (
        SparkSession.builder.appName("DuckDB Insert Partitioned Tables Read Test")
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
        .config("spark.jars", runtime_path)
        .config("spark.sql.catalogImplementation", "in-memory")
        .config("spark.sql.catalog.demo.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sql("USE demo")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS default")
    spark.sql("USE NAMESPACE default")
    yield spark, runtime_config
    spark.stop()


requires_iceberg_server = pytest.mark.skipif(
    os.getenv("ICEBERG_SERVER_AVAILABLE", None) is None,
    reason="Test data wasn't generated, run tests in test/sql/local/irc first (and set 'export ICEBERG_SERVER_AVAILABLE=1')",
)

# ---------------------------------------------------------------------------
# Expected full datasets — Spark wrote ids 1-11, DuckDB wrote ids 101-111
# with the same values. Each table should contain exactly 22 rows.
# ---------------------------------------------------------------------------

TRUNCATE_BIGINT_ROWS = (
    [Row(id=i, value=v) for i, v in zip(range(1, 11), [1, 11, 21, 31, 41, 51, 61, 71, 81, 91])]
    + [Row(id=11, value=None)]
    + [Row(id=i + 100, value=v) for i, v in zip(range(1, 11), [1, 11, 21, 31, 41, 51, 61, 71, 81, 91])]
    + [Row(id=111, value=None)]
)

_ANIMALS = ["aardvark", "bison", "camel", "dingo", "eagle", "falcon", "gecko", "hippo", "ibis", "jaguar"]

TRUNCATE_VARCHAR_ROWS = (
    [Row(id=i, value=v) for i, v in zip(range(1, 11), _ANIMALS)]
    + [Row(id=11, value=None)]
    + [Row(id=i + 100, value=v) for i, v in zip(range(1, 11), _ANIMALS)]
    + [Row(id=111, value=None)]
)

_TRUNCATE_BINARY_VALUES = [
    bytearray(b"\x01\x02\x03"),
    bytearray(b"\x02\x03\x04"),
    bytearray(b"\x03\x04\x05"),
    bytearray(b"\x04\x05\x06"),
    bytearray(b"\x05\x06\x07"),
    bytearray(b"\x06\x07\x08"),
    bytearray(b"\x07\x08\x09"),
    bytearray(b"\x08\x09\x0a"),
    bytearray(b"\x09\x00\x0a"),
    bytearray(b"\x0a\x00\x0b"),
]

TRUNCATE_BINARY_ROWS = (
    [Row(id=i, value=v) for i, v in zip(range(1, 11), _TRUNCATE_BINARY_VALUES)]
    + [Row(id=11, value=None)]
    + [Row(id=i + 100, value=v) for i, v in zip(range(1, 11), _TRUNCATE_BINARY_VALUES)]
    + [Row(id=111, value=None)]
)

BUCKET_INT_ROWS = (
    [Row(id=i, value=i) for i in range(1, 11)]
    + [Row(id=11, value=None)]
    + [Row(id=i + 100, value=i) for i in range(1, 11)]
    + [Row(id=111, value=None)]
)

BUCKET_BIGINT_ROWS = (
    [Row(id=i, value=i) for i in range(1, 11)]
    + [Row(id=11, value=None)]
    + [Row(id=i + 100, value=i) for i in range(1, 11)]
    + [Row(id=111, value=None)]
)

BUCKET_VARCHAR_ROWS = (
    [Row(id=i, value=v) for i, v in zip(range(1, 11), _ANIMALS)]
    + [Row(id=11, value=None)]
    + [Row(id=i + 100, value=v) for i, v in zip(range(1, 11), _ANIMALS)]
    + [Row(id=111, value=None)]
)

_BUCKET_DATES = [datetime.date(2020, m, 1) for m in range(1, 11)]

BUCKET_DATE_ROWS = (
    [Row(id=i, value=v) for i, v in zip(range(1, 11), _BUCKET_DATES)]
    + [Row(id=11, value=None)]
    + [Row(id=i + 100, value=v) for i, v in zip(range(1, 11), _BUCKET_DATES)]
    + [Row(id=111, value=None)]
)

_BUCKET_TIMESTAMPS = [datetime.datetime(2023, m, 1, 0, 0, 0) for m in range(1, 11)]

BUCKET_TIMESTAMP_ROWS = (
    [Row(id=i, value=v) for i, v in zip(range(1, 11), _BUCKET_TIMESTAMPS)]
    + [Row(id=11, value=None)]
    + [Row(id=i + 100, value=v) for i, v in zip(range(1, 11), _BUCKET_TIMESTAMPS)]
    + [Row(id=111, value=None)]
)

_BUCKET_BLOB_VALUES = [
    bytearray(b"\x01\x02\x03\x04\x05"),
    bytearray(b"\x06\x07\x08\x09\x10"),
    bytearray(b"\x11\x12\x13\x14\x15"),
    bytearray(b"\x16\x17\x18\x19\x20"),
    bytearray(b"\x21\x22\x23\x24\x25"),
    bytearray(b"\x26\x27\x28\x29\x30"),
    bytearray(b"\x31\x32\x33\x34\x35"),
    bytearray(b"\x36\x37\x38\x39\x40"),
    bytearray(b"\x41\x42\x43\x44\x45"),
    bytearray(b"\x46\x47\x48\x49\x50"),
]

BUCKET_BLOB_ROWS = (
    [Row(id=i, value=v) for i, v in zip(range(1, 11), _BUCKET_BLOB_VALUES)]
    + [Row(id=11, value=None)]
    + [Row(id=i + 100, value=v) for i, v in zip(range(1, 11), _BUCKET_BLOB_VALUES)]
    + [Row(id=111, value=None)]
)

_BUCKET_DECIMAL_AMOUNTS = [Decimal(f"{v}.00") for v in [10, 20, 30, 40, 50, 60, 70, 80, 90, 100]]

BUCKET_DECIMAL_ROWS = (
    [Row(id=i, amount=v) for i, v in zip(range(1, 11), _BUCKET_DECIMAL_AMOUNTS)]
    + [Row(id=11, amount=None)]
    + [Row(id=i + 100, amount=v) for i, v in zip(range(1, 11), _BUCKET_DECIMAL_AMOUNTS)]
    + [Row(id=111, amount=None)]
)

_TRUNCATE_DECIMAL_AMOUNTS = [Decimal(f"{v}.00") for v in range(1, 11)]

TRUNCATE_DECIMAL_ROWS = (
    [Row(id=i, amount=v) for i, v in zip(range(1, 11), _TRUNCATE_DECIMAL_AMOUNTS)]
    + [Row(id=11, amount=None)]
    + [Row(id=i + 100, amount=v) for i, v in zip(range(1, 11), _TRUNCATE_DECIMAL_AMOUNTS)]
    + [Row(id=111, amount=None)]
)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@requires_iceberg_server
class TestSparkReadInsertPartitionedTables:
    """
    Each table was written by two engines:
      - Spark (ids 1-11, via data generators)
      - DuckDB (ids 101-111, via sqllogictest INSERT statements)

    Tests verify that Spark can read all 22 rows back correctly, and that
    filtering on the partitioned column returns exactly the two matching rows
    (one from each engine), proving both engines assigned the same partition.
    """

    # --------------------------------------------------- TRUNCATE / BIGINT
    def test_truncate_bigint_total_rows(self, spark_con):
        spark = _get_spark(spark_con)
        res = spark.sql("SELECT * FROM default.truncate_partitioned_bigint_for_insert ORDER BY id").collect()
        assert res == TRUNCATE_BIGINT_ROWS

    def test_truncate_bigint_filter(self, spark_con):
        spark = _get_spark(spark_con)
        res = spark.sql(
            "SELECT * FROM default.truncate_partitioned_bigint_for_insert " "WHERE value = 1 ORDER BY id"
        ).collect()
        assert res == [Row(id=1, value=1), Row(id=101, value=1)]

    # --------------------------------------------------- TRUNCATE / VARCHAR
    def test_truncate_varchar_total_rows(self, spark_con):
        spark = _get_spark(spark_con)
        res = spark.sql("SELECT * FROM default.truncate_partitioned_varchar_for_insert ORDER BY id").collect()
        assert res == TRUNCATE_VARCHAR_ROWS

    def test_truncate_varchar_filter(self, spark_con):
        spark = _get_spark(spark_con)
        res = spark.sql(
            "SELECT * FROM default.truncate_partitioned_varchar_for_insert " "WHERE value = 'aardvark' ORDER BY id"
        ).collect()
        assert res == [Row(id=1, value="aardvark"), Row(id=101, value="aardvark")]

    # --------------------------------------------------- TRUNCATE / BINARY
    def test_truncate_binary_total_rows(self, spark_con):
        spark = _get_spark(spark_con)
        res = spark.sql("SELECT * FROM default.truncate_partitioned_binary_for_insert ORDER BY id").collect()
        assert res == TRUNCATE_BINARY_ROWS

    def test_truncate_binary_filter(self, spark_con):
        spark = _get_spark(spark_con)
        res = spark.sql(
            "SELECT * FROM default.truncate_partitioned_binary_for_insert " "WHERE value = X'010203' ORDER BY id"
        ).collect()
        v = bytearray(b"\x01\x02\x03")
        assert res == [Row(id=1, value=v), Row(id=101, value=v)]

    # ----------------------------------------------------- BUCKET / INTEGER
    def test_bucket_int_total_rows(self, spark_con):
        spark = _get_spark(spark_con)
        res = spark.sql("SELECT * FROM default.bucket_partitioned_int_for_insert ORDER BY id").collect()
        assert res == BUCKET_INT_ROWS

    def test_bucket_int_filter(self, spark_con):
        spark = _get_spark(spark_con)
        res = spark.sql(
            "SELECT * FROM default.bucket_partitioned_int_for_insert " "WHERE value = 1 ORDER BY id"
        ).collect()
        assert res == [Row(id=1, value=1), Row(id=101, value=1)]

    # ------------------------------------------------------ BUCKET / BIGINT
    def test_bucket_bigint_total_rows(self, spark_con):
        spark = _get_spark(spark_con)
        res = spark.sql("SELECT * FROM default.bucket_partitioned_bigint_for_insert ORDER BY id").collect()
        assert res == BUCKET_BIGINT_ROWS

    def test_bucket_bigint_filter(self, spark_con):
        spark = _get_spark(spark_con)
        res = spark.sql(
            "SELECT * FROM default.bucket_partitioned_bigint_for_insert " "WHERE value = 1 ORDER BY id"
        ).collect()
        assert res == [Row(id=1, value=1), Row(id=101, value=1)]

    # ----------------------------------------------------- BUCKET / VARCHAR
    def test_bucket_varchar_total_rows(self, spark_con):
        spark = _get_spark(spark_con)
        res = spark.sql("SELECT * FROM default.bucket_partitioned_varchar_for_insert ORDER BY id").collect()
        assert res == BUCKET_VARCHAR_ROWS

    def test_bucket_varchar_filter(self, spark_con):
        spark = _get_spark(spark_con)
        res = spark.sql(
            "SELECT * FROM default.bucket_partitioned_varchar_for_insert " "WHERE value = 'aardvark' ORDER BY id"
        ).collect()
        assert res == [Row(id=1, value="aardvark"), Row(id=101, value="aardvark")]

    # ------------------------------------------------------- BUCKET / DATE
    def test_bucket_date_total_rows(self, spark_con):
        spark = _get_spark(spark_con)
        res = spark.sql("SELECT * FROM default.bucket_partitioned_date_for_insert ORDER BY id").collect()
        assert res == BUCKET_DATE_ROWS

    def test_bucket_date_filter(self, spark_con):
        spark = _get_spark(spark_con)
        res = spark.sql(
            "SELECT * FROM default.bucket_partitioned_date_for_insert " "WHERE value = DATE '2020-01-01' ORDER BY id"
        ).collect()
        d = datetime.date(2020, 1, 1)
        assert res == [Row(id=1, value=d), Row(id=101, value=d)]

    # ------------------------------------------------------- BUCKET / TIME
    # Spark does not support the Iceberg TIME type (Iceberg v3 feature).
    # These tests are disabled until Spark adds TIME support.
    #
    # def test_bucket_time_total_rows(self, spark_con): ...
    # def test_bucket_time_filter(self, spark_con): ...

    # -------------------------------------------------- BUCKET / TIMESTAMP
    def test_bucket_timestamp_total_rows(self, spark_con):
        spark = _get_spark(spark_con)
        res = spark.sql("SELECT * FROM default.bucket_partitioned_timestamp_for_insert ORDER BY id").collect()
        assert res == BUCKET_TIMESTAMP_ROWS

    def test_bucket_timestamp_filter(self, spark_con):
        spark = _get_spark(spark_con)
        res = spark.sql(
            "SELECT * FROM default.bucket_partitioned_timestamp_for_insert "
            "WHERE value = TIMESTAMP '2023-01-01 00:00:00' ORDER BY id"
        ).collect()
        ts = datetime.datetime(2023, 1, 1, 0, 0, 0)
        assert res == [Row(id=1, value=ts), Row(id=101, value=ts)]

    # ------------------------------------------------------- BUCKET / BLOB
    # def test_bucket_blob_total_rows(self, spark_con):
    #     spark = _get_spark(spark_con)
    #     res = spark.sql(
    #         "SELECT * FROM default.bucket_partitioned_blob_for_insert ORDER BY id"
    #     ).collect()
    #     assert res == BUCKET_BLOB_ROWS
    #
    # def test_bucket_blob_filter(self, spark_con):
    #     spark = _get_spark(spark_con)
    #     res = spark.sql(
    #         "SELECT * FROM default.bucket_partitioned_blob_for_insert "
    #         "WHERE value = X'0102030405' ORDER BY id"
    #     ).collect()
    #     v = bytearray(b"\x01\x02\x03\x04\x05")
    #     assert res == [Row(id=1, value=v), Row(id=101, value=v)]

    # ------------------------------------------------------ BUCKET / DECIMAL
    def test_bucket_decimal_total_rows(self, spark_con):
        spark = _get_spark(spark_con)
        res = spark.sql("SELECT * FROM default.bucket_partitioned_decimal_for_insert ORDER BY id").collect()
        assert res == BUCKET_DECIMAL_ROWS

    def test_bucket_decimal_filter(self, spark_con):
        spark = _get_spark(spark_con)
        res = spark.sql(
            "SELECT * FROM default.bucket_partitioned_decimal_for_insert WHERE amount = 10.00 ORDER BY id"
        ).collect()
        v = Decimal("10.00")
        assert res == [Row(id=1, amount=v), Row(id=101, amount=v)]

    # ----------------------------------------------------- TRUNCATE / DECIMAL
    def test_truncate_decimal_total_rows(self, spark_con):
        spark = _get_spark(spark_con)
        res = spark.sql("SELECT * FROM default.truncate_partitioned_decimal_for_insert ORDER BY id").collect()
        assert res == TRUNCATE_DECIMAL_ROWS

    def test_truncate_decimal_filter(self, spark_con):
        spark = _get_spark(spark_con)
        res = spark.sql(
            "SELECT * FROM default.truncate_partitioned_decimal_for_insert WHERE amount = 1.00 ORDER BY id"
        ).collect()
        v = Decimal("1.00")
        assert res == [Row(id=1, amount=v), Row(id=101, amount=v)]
