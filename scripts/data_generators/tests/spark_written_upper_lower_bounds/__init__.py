from scripts.data_generators.tests.base import IcebergTest
import pathlib
from pyspark.sql import SparkSession
import pyspark
import pyspark.sql
from pyspark import SparkContext
import os

SPARK_RUNTIME_PATH = os.path.join(os.path.dirname(__file__), '..', '..', 'iceberg-spark-runtime-3.5_2.12-1.9.0.jar')

@IcebergTest.register()
class Test(IcebergTest):
    def __init__(self):
        path = pathlib.PurePath(__file__)
        super().__init__(path.parent.name)

    def setup(self, con):
        os.environ["PYSPARK_SUBMIT_ARGS"] = (
            "--packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.0,org.apache.iceberg:iceberg-aws-bundle:1.9.0 pyspark-shell"
        )
        os.environ["AWS_REGION"] = "us-east-1"
        os.environ["AWS_ACCESS_KEY_ID"] = "admin"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "password"
        if SparkContext._active_spark_context is not None:
            SparkContext._active_spark_context.stop()

        spark = (
            SparkSession.builder.appName("DuckDB REST Integration test")
            .master("local[1]")
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            )
            .config("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
            .config("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
            .config("spark.sql.catalog.demo", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.demo.type", "rest")
            .config("spark.sql.catalog.demo.uri", "http://127.0.0.1:8181")
            .config("spark.sql.catalog.demo.warehouse", "s3://warehouse/wh/")
            .config("spark.sql.catalog.demo.s3.endpoint", "http://127.0.0.1:9000")
            .config("spark.sql.catalog.demo.s3.path-style-access", "true")
            .config('spark.driver.memory', '10g')
            .config('spark.sql.session.timeZone', 'UTC')
            .config("spark.sql.catalogImplementation", "in-memory")
            .config("spark.sql.catalog.demo.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
            .config('spark.jars', SPARK_RUNTIME_PATH)
            .getOrCreate()
        )
        spark.sql("USE demo")
        spark.sql("CREATE NAMESPACE IF NOT EXISTS default")
        con.con = spark

