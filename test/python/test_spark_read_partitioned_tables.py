import pytest
import os
import time
import datetime
from decimal import Decimal
from conftest import *

from pprint import pprint

# PySpark's collect() converts epoch-microsecond timestamps to Python datetimes
# using datetime.fromtimestamp(), which respects the Python process's local
# timezone — NOT the Spark session timezone or the JVM timezone.  Pin the
# process timezone to UTC here, at import time, before any Spark session is
# created, so that TIMESTAMPTZ values round-trip correctly on machines whose
# system timezone is not UTC.
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


def _get_spark(spark_con, table_name=None):
    """Extract the Spark session, skipping v3 tables for runtimes that don't support them."""
    spark, runtime = spark_con
    if table_name and "format_version_3" in table_name and not runtime.supports_v3:
        pytest.skip(
            f"Iceberg {runtime.iceberg_library_version} (Spark {runtime.spark_version} "
            f"Scala {runtime.scala_binary_version}) does not support format version 3"
        )
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

    # SparkSession.getOrCreate() returns the cached active session, so we must
    # explicitly stop any existing session before creating one with new config.
    active = SparkSession.getActiveSession()
    if active is not None:
        active.stop()

    spark = (
        SparkSession.builder.appName(f"DuckDB Partitioned Tables Read Test")
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
    os.getenv("FIXTURE_SERVER_AVAILABLE", None) is None,
    reason="Test data wasn't generated, run tests in test/sql/local/irc first (and set 'export FIXTURE_SERVER_AVAILABLE=1')",
)

# ---------------------------------------------------------------------------
# Expected rows — defined once, shared across same-type tables regardless of
# which partition transform was applied (the stored data is identical).
# ---------------------------------------------------------------------------

INT_ROWS = [Row(id=1, val=10), Row(id=2, val=20), Row(id=3, val=10), Row(id=4, val=30)]

BIGINT_ROWS = [
    Row(id=1, val=1000000000),
    Row(id=2, val=2000000000),
    Row(id=3, val=1000000000),
    Row(id=4, val=3000000000),
]

VARCHAR_ROWS = [
    Row(id=1, val="apple"),
    Row(id=2, val="banana"),
    Row(id=3, val="apple"),
    Row(id=4, val="cherry"),
]

# Iceberg DECIMAL(10,2) → Spark DecimalType → Python Decimal
DECIMAL_ROWS = [
    Row(id=1, val=Decimal("1.50")),
    Row(id=2, val=Decimal("2.75")),
    Row(id=3, val=Decimal("1.50")),
    Row(id=4, val=Decimal("3.00")),
]

FLOAT_ROWS = [
    Row(id=1, val=1.0),
    Row(id=2, val=2.0),
    Row(id=3, val=1.0),
    Row(id=4, val=3.0),
]

DOUBLE_ROWS = [
    Row(id=1, val=1.0),
    Row(id=2, val=2.0),
    Row(id=3, val=1.0),
    Row(id=4, val=3.0),
]

# Iceberg UUID → Spark StringType (lowercase hyphenated string)
UUID_ROWS = [
    Row(id=1, val="a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"),
    Row(id=2, val="b0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"),
    Row(id=3, val="a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"),
    Row(id=4, val="c0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"),
]

# Iceberg TIME → Spark LongType (microseconds since midnight)
TIME_ROWS = [
    Row(id=1, val=28_800_000_000),  # 08:00:00
    Row(id=2, val=45_000_000_000),  # 12:30:00
    Row(id=3, val=28_800_000_000),  # 08:00:00
    Row(id=4, val=67_500_000_000),  # 18:45:00
]

DATE_ROWS = [
    Row(id=1, val=datetime.date(2020, 1, 15)),
    Row(id=2, val=datetime.date(2021, 6, 20)),
    Row(id=3, val=datetime.date(2022, 3, 10)),
    Row(id=4, val=datetime.date(2020, 7, 4)),
]

# Standard timestamp rows (ids 1,2,3,4) used by all timestamp tables except
# the v2 identity table which was inserted with a duplicate id=1.
TIMESTAMP_ROWS = [
    Row(id=1, val=datetime.datetime(2020, 1, 15, 8, 30, 0)),
    Row(id=2, val=datetime.datetime(2021, 6, 20, 14, 45, 0)),
    Row(id=3, val=datetime.datetime(2022, 3, 10, 22, 15, 0)),
    Row(id=4, val=datetime.datetime(2020, 1, 15, 10, 0, 0)),
]

# The v2 identity timestamp table was inserted with (1,…),(1,…),(3,…),(4,…)
TIMESTAMP_V2_IDENTITY_ROWS = [
    Row(id=1, val=datetime.datetime(2020, 1, 15, 8, 30, 0)),
    Row(id=1, val=datetime.datetime(2021, 6, 20, 14, 45, 0)),
    Row(id=3, val=datetime.datetime(2022, 3, 10, 22, 15, 0)),
    Row(id=4, val=datetime.datetime(2020, 1, 15, 10, 0, 0)),
]

# TIMESTAMPTZ: stored in UTC; Spark session timezone pinned to UTC above,
# so values are returned as naive datetimes matching the UTC wall-clock time.
TIMESTAMPTZ_ROWS = [
    Row(id=1, val=datetime.datetime(2020, 1, 15, 8, 30, 0)),
    Row(id=2, val=datetime.datetime(2021, 6, 20, 14, 45, 0)),
    Row(id=3, val=datetime.datetime(2022, 3, 10, 22, 15, 0)),
    Row(id=4, val=datetime.datetime(2020, 1, 15, 10, 0, 0)),
]

# TIMESTAMP_NS: Spark truncates nanoseconds to microseconds; values are
# whole seconds so there is no precision loss.
TIMESTAMPNS_ROWS = TIMESTAMP_ROWS


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@requires_iceberg_server
class TestSparkReadPartitionedTables:
    # ------------------------------------------------------------------ INT
    @pytest.mark.parametrize(
        "table_name",
        [
            "test_table_partitioned_by_int_format_version_2",
            "test_table_partitioned_by_int_format_version_3",
        ],
    )
    def test_int_partitioned(self, spark_con, table_name):
        spark = _get_spark(spark_con, table_name)
        res = spark.sql(f"SELECT * FROM default.{table_name} ORDER BY id, val").collect()
        assert res == INT_ROWS

    # --------------------------------------------------------------- BIGINT
    @pytest.mark.parametrize(
        "table_name",
        [
            "test_table_partitioned_by_bigint_format_version_2",
            "test_table_partitioned_by_bigint_format_version_3",
        ],
    )
    def test_bigint_partitioned(self, spark_con, table_name):
        spark = _get_spark(spark_con, table_name)
        res = spark.sql(f"SELECT * FROM default.{table_name} ORDER BY id, val").collect()
        assert res == BIGINT_ROWS

    # -------------------------------------------------------------- VARCHAR
    @pytest.mark.parametrize(
        "table_name",
        [
            "test_table_partitioned_by_varchar_format_version_2",
            "test_table_partitioned_by_varchar_format_version_3",
        ],
    )
    def test_varchar_partitioned(self, spark_con, table_name):
        spark = _get_spark(spark_con, table_name)
        res = spark.sql(f"SELECT * FROM default.{table_name} ORDER BY id, val").collect()
        assert res == VARCHAR_ROWS

    # -------------------------------------------------------------- DECIMAL
    @pytest.mark.parametrize(
        "table_name",
        [
            "test_table_partitioned_by_decimal_format_version_2",
            "test_table_partitioned_by_decimal_format_version_3",
        ],
    )
    def test_decimal_partitioned(self, spark_con, table_name):
        spark = _get_spark(spark_con, table_name)
        res = spark.sql(f"SELECT * FROM default.{table_name} ORDER BY id, val").collect()
        assert res == DECIMAL_ROWS

    # -------------------------------- DECIMAL / BUCKET (DuckDB-created table)
    def test_bucket_decimal_duckdb_created(self, spark_con):
        spark = _get_spark(spark_con)
        res = spark.sql("SELECT * FROM default.test_bucket_decimal ORDER BY id").collect()
        assert res == TEST_BUCKET_DECIMAL_ROWS

    # ------------------------------- DECIMAL / TRUNCATE (DuckDB-created table)
    def test_truncate_decimal_duckdb_created(self, spark_con):
        spark = _get_spark(spark_con)
        res = spark.sql("SELECT * FROM default.test_truncate_decimal ORDER BY id").collect()
        assert res == TEST_TRUNCATE_DECIMAL_ROWS

    # ---------------------------------------------------------------- FLOAT
    @pytest.mark.parametrize(
        "table_name",
        [
            "test_table_partitioned_by_float_format_version_2",
            "test_table_partitioned_by_float_format_version_3",
        ],
    )
    def test_float_partitioned(self, spark_con, table_name):
        spark = _get_spark(spark_con, table_name)
        res = spark.sql(f"SELECT * FROM default.{table_name} ORDER BY id, val").collect()
        assert res == FLOAT_ROWS

    # --------------------------------------------------------------- DOUBLE
    @pytest.mark.parametrize(
        "table_name",
        [
            "test_table_partitioned_by_double_format_version_2",
            "test_table_partitioned_by_double_format_version_3",
        ],
    )
    def test_double_partitioned(self, spark_con, table_name):
        spark = _get_spark(spark_con, table_name)
        res = spark.sql(f"SELECT * FROM default.{table_name} ORDER BY id, val").collect()
        assert res == DOUBLE_ROWS

    # ----------------------------------------------------------------- UUID
    # Spark doesn't really support proper UUID types. They store UUID as varchar
    # and expect it to be stored differently
    #    @pytest.mark.parametrize(
    #        "table_name",
    #        [
    #            "test_table_partitioned_by_uuid_format_version_2",
    #            "test_table_partitioned_by_uuid_format_version_3",
    #        ],
    #    )
    #    def test_uuid_partitioned(self, spark_con, table_name):
    #        spark = _get_spark(spark_con, table_name)
    #        res = spark.sql(f"SELECT * FROM default.{table_name} ORDER BY id, val").collect()
    #        assert res == UUID_ROWS

    # ----------------------------------------------------------------- TIME
    # Spark does not support time fields
    # @pytest.mark.parametrize(
    # "table_name",
    # [
    #    "test_table_partitioned_by_time_format_version_2",
    #            "test_table_partitioned_by_time_format_version_3",
    # ],
    # )
    # def test_time_partitioned(self, spark_con, table_name):
    # spark = _get_spark(spark_con, table_name)
    # res = spark.sql(
    #    f"SELECT * FROM default.{table_name} ORDER BY id, val"
    # ).collect()
    # assert res == TIME_ROWS

    # ----------------------------------------------------------------- DATE
    @pytest.mark.parametrize(
        "table_name",
        [
            "test_table_partitioned_by_date_format_version_2",
            "test_table_partitioned_by_date_format_version_3",
            "test_table_partitioned_by_date_year_format_version_2",
            "test_table_partitioned_by_date_year_format_version_3",
            "test_table_partitioned_by_date_month_format_version_2",
            "test_table_partitioned_by_date_month_format_version_3",
            "test_table_partitioned_by_date_day_format_version_2",
            "test_table_partitioned_by_date_day_format_version_3",
        ],
    )
    def test_date_partitioned(self, spark_con, table_name):
        spark = _get_spark(spark_con, table_name)
        res = spark.sql(f"SELECT * FROM default.{table_name} ORDER BY id, val").collect()
        assert res == DATE_ROWS

    # ------------------------------------------------------------ TIMESTAMP
    # The v2 identity table was inserted with duplicate id=1 (linter-applied).
    def test_timestamp_identity_v2(self, spark_con):
        spark, _ = spark_con
        res = spark.sql(
            "SELECT * FROM default.test_table_partitioned_by_timestamp_format_version_2 ORDER BY id, val"
        ).collect()
        assert res == TIMESTAMP_V2_IDENTITY_ROWS

    @pytest.mark.parametrize(
        "table_name",
        [
            "test_table_partitioned_by_timestamp_format_version_3",
            "test_table_partitioned_by_timestamp_year_format_version_2",
            "test_table_partitioned_by_timestamp_year_format_version_3",
            "test_table_partitioned_by_timestamp_month_format_version_2",
            "test_table_partitioned_by_timestamp_month_format_version_3",
            "test_table_partitioned_by_timestamp_day_format_version_2",
            "test_table_partitioned_by_timestamp_day_format_version_3",
            "test_table_partitioned_by_timestamp_hour_format_version_2",
            "test_table_partitioned_by_timestamp_hour_format_version_3",
        ],
    )
    def test_timestamp_partitioned(self, spark_con, table_name):
        spark = _get_spark(spark_con, table_name)
        res = spark.sql(f"SELECT * FROM default.{table_name} ORDER BY id, val").collect()
        assert res == TIMESTAMP_ROWS

    # --------------------------------------------------------- TIMESTAMPTZ
    @pytest.mark.parametrize(
        "table_name",
        [
            "test_table_partitioned_by_timestamptz_format_version_2",
            "test_table_partitioned_by_timestamptz_format_version_3",
            "test_table_partitioned_by_timestamptz_year_format_version_2",
            "test_table_partitioned_by_timestamptz_year_format_version_3",
            "test_table_partitioned_by_timestamptz_month_format_version_2",
            "test_table_partitioned_by_timestamptz_month_format_version_3",
            "test_table_partitioned_by_timestamptz_day_format_version_2",
            "test_table_partitioned_by_timestamptz_day_format_version_3",
            "test_table_partitioned_by_timestamptz_hour_format_version_2",
            "test_table_partitioned_by_timestamptz_hour_format_version_3",
        ],
    )
    def test_timestamptz_partitioned(self, spark_con, table_name):
        spark = _get_spark(spark_con, table_name)
        res = spark.sql(f"SELECT * FROM default.{table_name} ORDER BY id, val").collect()
        assert res == TIMESTAMPTZ_ROWS

    # --------------------------------------------------------- TIMESTAMP_NS
    # Spark does not support timestamp ns
    # @pytest.mark.parametrize("table_name", [
    # "test_table_partitioned_by_timestampns_format_version_2",
    # "test_table_partitioned_by_timestampns_format_version_3",
    # "test_table_partitioned_by_timestampns_year_format_version_2",
    # "test_table_partitioned_by_timestampns_year_format_version_3",
    # "test_table_partitioned_by_timestampns_month_format_version_2",
    # "test_table_partitioned_by_timestampns_month_format_version_3",
    # "test_table_partitioned_by_timestampns_day_format_version_2",
    # "test_table_partitioned_by_timestampns_day_format_version_3",
    # "test_table_partitioned_by_timestampns_hour_format_version_2",
    # "test_table_partitioned_by_timestampns_hour_format_version_3",
    # ])
    # def test_timestampns_partitioned(self, spark_con, table_name):
    # res = spark_con.sql(f"SELECT * FROM default.{table_name} ORDER BY id, val").collect()
    # assert res == TIMESTAMPNS_ROWS


# ---------------------------------------------------------------------------
# Expected full datasets for cross-engine (Spark-wrote, DuckDB-appended) tables.
# Spark inserted ids 1-11; DuckDB inserted ids 101-111 with the same values.
# Each table contains exactly 22 rows.
# ---------------------------------------------------------------------------

_ANIMALS = ["aardvark", "bison", "camel", "dingo", "eagle", "falcon", "gecko", "hippo", "ibis", "jaguar"]

_BUCKET_DATES = [datetime.date(2020, m, 1) for m in range(1, 11)]

_BUCKET_TIMESTAMPS = [datetime.datetime(2023, m, 1, 0, 0, 0) for m in range(1, 11)]

_TRUNCATE_INT_VALUES = [1, 11, 21, 31, 41, 51, 61, 71, 81, 91]

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

_BUCKET_DECIMAL_AMOUNTS = [Decimal(f"{v}.00") for v in [10, 20, 30, 40, 50, 60, 70, 80, 90, 100]]
_TRUNCATE_DECIMAL_AMOUNTS = [Decimal(f"{v}.00") for v in range(1, 11)]

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


def _cross_engine_rows(values, null_id=11):
    """Build 22-row expected list: Spark rows (ids 1-10 + null) then DuckDB rows (ids 101-110 + null)."""
    rows = [Row(id=i, value=v) for i, v in zip(range(1, 11), values)]
    rows.append(Row(id=null_id, value=None))
    rows += [Row(id=i + 100, value=v) for i, v in zip(range(1, 11), values)]
    rows.append(Row(id=null_id + 100, value=None))
    return rows


BUCKET_INT_FOR_INSERT_ROWS = _cross_engine_rows(list(range(1, 11)))
BUCKET_BIGINT_FOR_INSERT_ROWS = _cross_engine_rows(list(range(1, 11)))
BUCKET_VARCHAR_FOR_INSERT_ROWS = _cross_engine_rows(_ANIMALS)
BUCKET_DATE_FOR_INSERT_ROWS = _cross_engine_rows(_BUCKET_DATES)
BUCKET_TIMESTAMP_FOR_INSERT_ROWS = _cross_engine_rows(_BUCKET_TIMESTAMPS)
# BUCKET_BLOB_FOR_INSERT_ROWS — omitted: Spark binary handling is unreliable across versions
BUCKET_DECIMAL_FOR_INSERT_ROWS = _cross_engine_rows(_BUCKET_DECIMAL_AMOUNTS)

TRUNCATE_INT_FOR_INSERT_ROWS = _cross_engine_rows(_TRUNCATE_INT_VALUES)
TRUNCATE_BIGINT_FOR_INSERT_ROWS = _cross_engine_rows(_TRUNCATE_INT_VALUES)
TRUNCATE_VARCHAR_FOR_INSERT_ROWS = _cross_engine_rows(_ANIMALS)
TRUNCATE_BINARY_FOR_INSERT_ROWS = _cross_engine_rows(_TRUNCATE_BINARY_VALUES)
TRUNCATE_DECIMAL_FOR_INSERT_ROWS = _cross_engine_rows(_TRUNCATE_DECIMAL_AMOUNTS)

# Rows written by DuckDB alone into bucket/truncate decimal tables (3 columns: id, amount, label)
_BUCKET_DECIMAL_LABELS = [
    "ten",
    "twenty",
    "thirty",
    "forty",
    "fifty",
    "sixty",
    "seventy",
    "eighty",
    "ninety",
    "hundred",
]
TEST_BUCKET_DECIMAL_ROWS = [
    Row(id=i, amount=a, label=l) for i, a, l in zip(range(1, 11), _BUCKET_DECIMAL_AMOUNTS, _BUCKET_DECIMAL_LABELS)
] + [Row(id=11, amount=None, label="null_row")]
_TRUNCATE_DECIMAL_LABELS = ["one", "two", "three", "four", "five", "six", "seven", "eight", "nine", "ten"]
TEST_TRUNCATE_DECIMAL_ROWS = [
    Row(id=i, amount=a, label=l) for i, a, l in zip(range(1, 11), _TRUNCATE_DECIMAL_AMOUNTS, _TRUNCATE_DECIMAL_LABELS)
] + [Row(id=11, amount=None, label="null_row")]


@requires_iceberg_server
class TestSparkReadBucketTruncateForInsert:
    """
    Cross-engine round-trip tests for bucket- and truncate-partitioned tables.

    Each table was populated by two engines:
      - Spark (ids 1-11, via data generators)
      - DuckDB (ids 101-111, via sqllogictest INSERT statements in
        other_engines/partitions/bucket and other_engines/partitions/truncate)

    Tests verify that Spark reads back all 22 rows and that filtering on the
    partitioned column returns exactly the two matching rows (one per engine),
    proving both engines computed the same partition value.
    """

    # --------------------------------------------------- BUCKET / INTEGER
    def test_bucket_int_total_rows(self, spark_con):
        spark = _get_spark(spark_con)
        res = spark.sql("SELECT * FROM default.bucket_partitioned_int_for_insert ORDER BY id").collect()
        assert res == BUCKET_INT_FOR_INSERT_ROWS

    def test_bucket_int_filter(self, spark_con):
        spark = _get_spark(spark_con)
        res = spark.sql("SELECT * FROM default.bucket_partitioned_int_for_insert WHERE value = 1 ORDER BY id").collect()
        assert res == [Row(id=1, value=1), Row(id=101, value=1)]

    # --------------------------------------------------- BUCKET / BIGINT
    def test_bucket_bigint_total_rows(self, spark_con):
        spark = _get_spark(spark_con)
        res = spark.sql("SELECT * FROM default.bucket_partitioned_bigint_for_insert ORDER BY id").collect()
        assert res == BUCKET_BIGINT_FOR_INSERT_ROWS

    def test_bucket_bigint_filter(self, spark_con):
        spark = _get_spark(spark_con)
        res = spark.sql(
            "SELECT * FROM default.bucket_partitioned_bigint_for_insert WHERE value = 1 ORDER BY id"
        ).collect()
        assert res == [Row(id=1, value=1), Row(id=101, value=1)]

    # --------------------------------------------------- BUCKET / VARCHAR
    def test_bucket_varchar_total_rows(self, spark_con):
        spark = _get_spark(spark_con)
        res = spark.sql("SELECT * FROM default.bucket_partitioned_varchar_for_insert ORDER BY id").collect()
        assert res == BUCKET_VARCHAR_FOR_INSERT_ROWS

    def test_bucket_varchar_filter(self, spark_con):
        spark = _get_spark(spark_con)
        res = spark.sql(
            "SELECT * FROM default.bucket_partitioned_varchar_for_insert WHERE value = 'aardvark' ORDER BY id"
        ).collect()
        assert res == [Row(id=1, value="aardvark"), Row(id=101, value="aardvark")]

    # ----------------------------------------------------- BUCKET / DATE
    def test_bucket_date_total_rows(self, spark_con):
        spark = _get_spark(spark_con)
        res = spark.sql("SELECT * FROM default.bucket_partitioned_date_for_insert ORDER BY id").collect()
        assert res == BUCKET_DATE_FOR_INSERT_ROWS

    def test_bucket_date_filter(self, spark_con):
        spark = _get_spark(spark_con)
        res = spark.sql(
            "SELECT * FROM default.bucket_partitioned_date_for_insert WHERE value = DATE '2020-01-01' ORDER BY id"
        ).collect()
        d = datetime.date(2020, 1, 1)
        assert res == [Row(id=1, value=d), Row(id=101, value=d)]

    # -------------------------------------------------- BUCKET / TIMESTAMP
    def test_bucket_timestamp_total_rows(self, spark_con):
        spark = _get_spark(spark_con)
        res = spark.sql("SELECT * FROM default.bucket_partitioned_timestamp_for_insert ORDER BY id").collect()
        assert res == BUCKET_TIMESTAMP_FOR_INSERT_ROWS

    def test_bucket_timestamp_filter(self, spark_con):
        spark = _get_spark(spark_con)
        res = spark.sql(
            "SELECT * FROM default.bucket_partitioned_timestamp_for_insert "
            "WHERE value = TIMESTAMP '2023-01-01 00:00:00' ORDER BY id"
        ).collect()
        ts = datetime.datetime(2023, 1, 1, 0, 0, 0)
        assert res == [Row(id=1, value=ts), Row(id=101, value=ts)]

    # ------------------------------------------------------- BUCKET / BLOB
    # Spark binary round-trip is unreliable across runtime versions; skipped.
    # def test_bucket_blob_total_rows(self, spark_con): ...
    # def test_bucket_blob_filter(self, spark_con): ...

    # ------------------------------------------------- TRUNCATE / INTEGER
    def test_truncate_int_total_rows(self, spark_con):
        spark = _get_spark(spark_con)
        res = spark.sql("SELECT * FROM default.truncate_partitioned_int_for_insert ORDER BY id").collect()
        assert res == TRUNCATE_INT_FOR_INSERT_ROWS

    def test_truncate_int_filter(self, spark_con):
        spark = _get_spark(spark_con)
        res = spark.sql(
            "SELECT * FROM default.truncate_partitioned_int_for_insert WHERE value = 1 ORDER BY id"
        ).collect()
        assert res == [Row(id=1, value=1), Row(id=101, value=1)]

    # -------------------------------------------------- TRUNCATE / BIGINT
    def test_truncate_bigint_total_rows(self, spark_con):
        spark = _get_spark(spark_con)
        res = spark.sql("SELECT * FROM default.truncate_partitioned_bigint_for_insert ORDER BY id").collect()
        assert res == TRUNCATE_BIGINT_FOR_INSERT_ROWS

    def test_truncate_bigint_filter(self, spark_con):
        spark = _get_spark(spark_con)
        res = spark.sql(
            "SELECT * FROM default.truncate_partitioned_bigint_for_insert WHERE value = 1 ORDER BY id"
        ).collect()
        assert res == [Row(id=1, value=1), Row(id=101, value=1)]

    # ------------------------------------------------- TRUNCATE / VARCHAR
    def test_truncate_varchar_total_rows(self, spark_con):
        spark = _get_spark(spark_con)
        res = spark.sql("SELECT * FROM default.truncate_partitioned_varchar_for_insert ORDER BY id").collect()
        assert res == TRUNCATE_VARCHAR_FOR_INSERT_ROWS

    def test_truncate_varchar_filter(self, spark_con):
        spark = _get_spark(spark_con)
        res = spark.sql(
            "SELECT * FROM default.truncate_partitioned_varchar_for_insert WHERE value = 'aardvark' ORDER BY id"
        ).collect()
        assert res == [Row(id=1, value="aardvark"), Row(id=101, value="aardvark")]

    # -------------------------------------------------- TRUNCATE / BINARY
    def test_truncate_binary_total_rows(self, spark_con):
        spark = _get_spark(spark_con)
        res = spark.sql("SELECT * FROM default.truncate_partitioned_binary_for_insert ORDER BY id").collect()
        assert res == TRUNCATE_BINARY_FOR_INSERT_ROWS

    def test_truncate_binary_filter(self, spark_con):
        spark = _get_spark(spark_con)
        res = spark.sql(
            "SELECT * FROM default.truncate_partitioned_binary_for_insert WHERE value = X'010203' ORDER BY id"
        ).collect()
        v = bytearray(b"\x01\x02\x03")
        assert res == [Row(id=1, value=v), Row(id=101, value=v)]

    # -------------------------------------------------- BUCKET / DECIMAL
    def test_bucket_decimal_total_rows(self, spark_con):
        spark = _get_spark(spark_con)
        res = spark.sql("SELECT * FROM default.bucket_partitioned_decimal_for_insert ORDER BY id").collect()
        assert res == BUCKET_DECIMAL_FOR_INSERT_ROWS

    def test_bucket_decimal_filter(self, spark_con):
        spark = _get_spark(spark_con)
        res = spark.sql(
            "SELECT * FROM default.bucket_partitioned_decimal_for_insert WHERE amount = 10.00 ORDER BY id"
        ).collect()
        v = Decimal("10.00")
        assert res == [Row(id=1, amount=v), Row(id=101, amount=v)]

    # ------------------------------------------------- TRUNCATE / DECIMAL
    def test_truncate_decimal_total_rows(self, spark_con):
        spark = _get_spark(spark_con)
        res = spark.sql("SELECT * FROM default.truncate_partitioned_decimal_for_insert ORDER BY id").collect()
        assert res == TRUNCATE_DECIMAL_FOR_INSERT_ROWS

    def test_truncate_decimal_filter(self, spark_con):
        spark = _get_spark(spark_con)
        res = spark.sql(
            "SELECT * FROM default.truncate_partitioned_decimal_for_insert WHERE amount = 1.00 ORDER BY id"
        ).collect()
        v = Decimal("1.00")
        assert res == [Row(id=1, amount=v), Row(id=101, amount=v)]
