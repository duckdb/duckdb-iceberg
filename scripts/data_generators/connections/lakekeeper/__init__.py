from pyspark import SparkContext
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession

from ..base import IcebergConnection
from scripts.data_generators.integration_config import get_spark_runtime


CONNECTION_KEY = "lakekeeper"

CATALOG_URL = "http://localhost:8181/catalog"
KEYCLOAK_TOKEN_URL = "http://localhost:30080/realms/iceberg/protocol/openid-connect/token"
WAREHOUSE = "demo"
CLIENT_ID = "spark"
CLIENT_SECRET = "2OR3eRvYfSZzzZ16MlPd95jhLnOaLM52"


@IcebergConnection.register(CONNECTION_KEY)
class IcebergSparkLocal(IcebergConnection):
    def __init__(self, runtime=None):
        super().__init__(CONNECTION_KEY, "iceberg_catalog")
        self.runtime = get_spark_runtime(runtime)
        self.con = self.get_connection()

    def get_connection(self):
        conf = {
            "spark.jars.packages": f"{self.runtime.runtime_package},{self.runtime.aws_bundle_package}",
            "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            "spark.sql.catalog.iceberg_catalog": "org.apache.iceberg.spark.SparkCatalog",
            "spark.sql.catalog.iceberg_catalog.type": "rest",
            "spark.sql.catalog.iceberg_catalog.uri": CATALOG_URL,
            "spark.sql.catalog.iceberg_catalog.credential": f"{CLIENT_ID}:{CLIENT_SECRET}",
            "spark.sql.catalog.iceberg_catalog.warehouse": WAREHOUSE,
            "spark.sql.catalog.iceberg_catalog.scope": "lakekeeper",
            "spark.sql.catalog.iceberg_catalog.oauth2-server-uri": KEYCLOAK_TOKEN_URL,
            "spark.jars": self.runtime.jar_path.as_posix(),
        }
        if SparkContext._active_spark_context is not None:
            SparkContext._active_spark_context.stop()

        spark_config = SparkConf().setMaster("local").setAppName("Iceberg-REST")
        for key, value in conf.items():
            spark_config = spark_config.set(key, value)

        spark = SparkSession.builder.config(conf=spark_config).getOrCreate()
        spark.sql("USE iceberg_catalog")
        spark.sql("CREATE NAMESPACE IF NOT EXISTS default")
        spark.sql("USE NAMESPACE default")
        return spark
