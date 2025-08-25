import pytest
import os
import datetime

from pprint import pprint

SCRIPT_DIR = os.path.dirname(__file__)
SPARK_RUNTIME_PATH = os.path.join(
    SCRIPT_DIR, '..', '..', 'scripts', 'data_generators', 'iceberg-spark-runtime-3.5_2.12-1.9.0.jar'
)

pyspark = pytest.importorskip("pyspark")
pyspark_sql = pytest.importorskip("pyspark.sql")
SparkSession = pyspark_sql.SparkSession
SparkContext = pyspark.SparkContext
Row = pyspark_sql.Row


@pytest.fixture()
def spark_con():
    os.environ["PYSPARK_SUBMIT_ARGS"] = (
        "--packages org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.9.0,org.apache.iceberg:iceberg-aws-bundle:1.9.0 pyspark-shell"
    )
    os.environ["AWS_REGION"] = "us-east-1"
    os.environ["AWS_ACCESS_KEY_ID"] = "admin"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "password"

    spark = (
        SparkSession.builder.appName("DuckDB REST Integration test")
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
        .config('spark.driver.memory', '10g')
        .config("spark.sql.catalogImplementation", "in-memory")
        .config("spark.sql.catalog.demo.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config('spark.jars', SPARK_RUNTIME_PATH)
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
