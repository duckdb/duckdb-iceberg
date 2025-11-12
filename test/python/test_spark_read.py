import pytest
import os
import datetime

from pprint import pprint

SCRIPT_DIR = os.path.dirname(__file__)


pyspark = pytest.importorskip("pyspark")
pyspark_sql = pytest.importorskip("pyspark.sql")
SparkSession = pyspark_sql.SparkSession
SparkContext = pyspark.SparkContext
Row = pyspark_sql.Row


# List of runtimes you want to test
ICEBERG_RUNTIMES = [
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.1",
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.0",
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.13:1.9.1",
]


@pytest.fixture(params=ICEBERG_RUNTIMES, scope="session")
def spark_con(request):
    runtime_pkg = request.param
    runtime_pkg_jar = (runtime_pkg[len("org.apache.iceberg:") :] + ".jar").replace(":", "-")
    runtime_path = os.path.abspath(os.path.join(SCRIPT_DIR, '..', '..', 'scripts', 'data_generators', runtime_pkg_jar))

    os.environ["PYSPARK_SUBMIT_ARGS"] = (
        f"--packages {runtime_pkg},org.apache.iceberg:iceberg-aws-bundle:1.9.0 pyspark-shell"
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


@pytest.mark.skipif(
    os.getenv('ICEBERG_SERVER_AVAILABLE', None) == None, reason="Test data wasn't generated, run 'make data' first"
)
class TestSparkRead:
    def test_spark_read(self, spark_con):
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


@pytest.mark.skipif(
    os.getenv('ICEBERG_SERVER_AVAILABLE', None) == None,
    reason="Test data wasn't generated, run tests in test/sql/local/irc first",
)
class TestSparkReadDuckDBTable:
    def test_spark_read(self, spark_con):
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


@pytest.mark.skipif(
    os.getenv('ICEBERG_SERVER_AVAILABLE', None) == None,
    reason="Test data wasn't generated, run tests in test/sql/local/irc first",
)
class TestSparkReadDuckDBTableWithDeletes:
    def test_spark_read(self, spark_con):
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


@pytest.mark.skipif(
    os.getenv('ICEBERG_SERVER_AVAILABLE', None) == None,
    reason="Test data wasn't generated, run tests in test/sql/local/irc first",
)
class TestSparkReadDuckDBTableWithDeletes:
    def test_spark_read(self, spark_con):
        df = spark_con.sql(
            """
            select * from default.duckdb_updates_for_other_engines order by a
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
            Row(a=100),
            Row(a=100),
            Row(a=100),
            Row(a=100),
            Row(a=100),
            Row(a=100),
            Row(a=100),
            Row(a=100),
            Row(a=100),
            Row(a=100),
        ]
