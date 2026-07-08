import os

from pyspark import SparkContext
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession

from ..base import IcebergConnection
from scripts.data_generators.integration_config import get_spark_runtime


CONNECTION_KEY = "polaris"


@IcebergConnection.register(CONNECTION_KEY)
class IcebergSparkLocal(IcebergConnection):
    def __init__(self, runtime=None):
        super().__init__(CONNECTION_KEY, "iceberg_catalog")
        self.runtime = get_spark_runtime(runtime)
        self.con = self.get_connection()

    def get_connection(self):
        os.environ["PYSPARK_SUBMIT_ARGS"] = (
            f"--packages {self.runtime.runtime_package},{self.runtime.aws_bundle_package} pyspark-shell"
        )

        client_id = "root"
        client_secret = "s3cr3t"
        os.environ["AWS_REGION"] = "us-west-2"
        os.environ["AWS_ACCESS_KEY_ID"] = "minio_root"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "m1n1opwd"
        if SparkContext._active_spark_context is not None:
            SparkContext._active_spark_context.stop()

        config = SparkConf()
        config.set("spark.jars.packages", f"{self.runtime.runtime_package},{self.runtime.aws_bundle_package}")
        config.set("spark.sql.iceberg.vectorization.enabled", "false")
        config.set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        config.set("spark.sql.catalog.iceberg_catalog.type", "rest")
        config.set("spark.driver.memory", "10g")
        config.set("spark.sql.catalog.iceberg_catalog.rest.auth.type", "oauth2")
        config.set("spark.sql.catalog.iceberg_catalog", "org.apache.iceberg.spark.SparkCatalog")
        config.set("spark.sql.catalog.iceberg_catalog.uri", "http://localhost:8181/api/catalog")
        config.set(
            "spark.sql.catalog.iceberg_catalog.oauth2-server-uri",
            "http://localhost:8181/api/catalog/v1/oauth/tokens",
        )
        config.set("spark.sql.catalog.iceberg_catalog.token-refresh-enabled", "true")
        config.set("spark.sql.catalog.iceberg_catalog.credential", f"{client_id}:{client_secret}")
        config.set("spark.sql.catalog.iceberg_catalog.warehouse", "quickstart_catalog")
        config.set("spark.sql.catalog.iceberg_catalog.scope", "PRINCIPAL_ROLE:ALL")
        config.set("spark.sql.catalog.iceberg_catalog.header.X-Iceberg-Access-Delegation", "vended-credentials")
        config.set("spark.sql.catalog.iceberg_catalog.io-impl", "org.apache.iceberg.io.ResolvingFileIO")
        config.set("spark.sql.catalog.iceberg_catalog.s3.region", "us-west-2")
        config.set("spark.history.fs.logDirectory", "/home/iceberg/spark-events")
        config.set("spark.jars", self.runtime.jar_path.as_posix())

        spark = SparkSession.builder.config(conf=config).getOrCreate()
        spark.sql("USE iceberg_catalog")
        spark.sql("CREATE NAMESPACE IF NOT EXISTS default")
        spark.sql("USE NAMESPACE default")
        return spark
