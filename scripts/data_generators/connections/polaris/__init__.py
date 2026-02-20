from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark import SparkContext
import pyspark
import pyspark.sql

from ..base import IcebergConnection
from ..spark_settings import iceberg_runtime_configuration

import sys
import os

CONNECTION_KEY = 'polaris'

RUNTIME_CONFIG = iceberg_runtime_configuration()
SPARK_VERSION = RUNTIME_CONFIG['spark_version']
SCALA_BINARY_VERSION = RUNTIME_CONFIG['scala_binary_version']
ICEBERG_LIBRARY_VERSION = RUNTIME_CONFIG['iceberg_library_version']

ICEBERG_SPARK_RUNTIME = f'iceberg-spark-runtime-{SPARK_VERSION}_{SCALA_BINARY_VERSION}-{ICEBERG_LIBRARY_VERSION}'

SPARK_RUNTIME_PATH = os.path.join(os.path.dirname(__file__), '..', '..', f'{ICEBERG_SPARK_RUNTIME}.jar')

@IcebergConnection.register(CONNECTION_KEY)
class IcebergSparkLocal(IcebergConnection):
    def __init__(self):
        super().__init__(CONNECTION_KEY, 'quickstart_catalog')
        self.con = self.get_connection()

    def get_connection(self):
        os.environ["PYSPARK_SUBMIT_ARGS"] = (
            f"--packages org.apache.iceberg:iceberg-spark-runtime-{SPARK_VERSION}_{SCALA_BINARY_VERSION}:{ICEBERG_LIBRARY_VERSION},org.apache.iceberg:iceberg-aws-bundle:{ICEBERG_LIBRARY_VERSION} pyspark-shell"
        )

        client_id = os.getenv('POLARIS_CLIENT_ID', '')
        client_secret = os.getenv('POLARIS_CLIENT_SECRET', '')
        os.environ["AWS_REGION"] = "us-east-1"
        os.environ["AWS_ACCESS_KEY_ID"] = "admin"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "password"

        if client_id == '' or client_secret == '':
            print("could not find client id or client secret to connect to polaris, aborting")
            return

        config = SparkConf()
        config.set(
            "spark.jars.packages",
            f"org.apache.iceberg:iceberg-spark-runtime-{SPARK_VERSION}_{SCALA_BINARY_VERSION}:{ICEBERG_LIBRARY_VERSION},org.apache.hadoop:hadoop-aws:3.4.0,software.amazon.awssdk:bundle:2.23.19,software.amazon.awssdk:url-connection-client:2.23.19",
        )
        config.set('spark.sql.iceberg.vectorization.enabled', 'false')
        # Configure the 'polaris' catalog as an Iceberg rest catalog
        config.set("spark.sql.catalog.quickstart_catalog.type", "rest")
        config.set('spark.driver.memory', '10g')
        config.set("spark.sql.catalog.quickstart_catalog.rest.auth.type", "oauth2")
        config.set("spark.sql.catalog.quickstart_catalog", "org.apache.iceberg.spark.SparkCatalog")
        # Specify the rest catalog endpoint
        config.set("spark.sql.catalog.quickstart_catalog.uri", "http://localhost:8181/api/catalog")
        config.set("spark.sql.catalog.quickstart_catalog.oauth2-server-uri", "http://localhost:8181/api/catalog/v1/oauth/tokens")
        # Enable token refresh
        config.set("spark.sql.catalog.quickstart_catalog.token-refresh-enabled", "true")
        # specify the client_id:client_secret pair
        config.set("spark.sql.catalog.quickstart_catalog.credential", f"{client_id}:{client_secret}")
        # Set the warehouse to the name of the catalog we created
        config.set("spark.sql.catalog.quickstart_catalog.warehouse", "quickstart_catalog")
        # Scope set to PRINCIPAL_ROLE:ALL
        config.set("spark.sql.catalog.quickstart_catalog.scope", 'PRINCIPAL_ROLE:ALL')
        # Enable access credential delegation
        config.set("spark.sql.catalog.quickstart_catalog.header.X-Iceberg-Access-Delegation", 'vended-credentials')
        config.set("spark.sql.catalog.quickstart_catalog.io-impl", "org.apache.iceberg.io.ResolvingFileIO")
        config.set("spark.sql.catalog.quickstart_catalog.s3.region", "us-west-2")
        config.set("spark.history.fs.logDirectory", "/home/iceberg/spark-events")
        config.set("spark.jars", SPARK_RUNTIME_PATH)

        spark = SparkSession.builder.config(conf=config).getOrCreate()
        spark.sql("USE quickstart_catalog")
        spark.sql("CREATE NAMESPACE IF NOT EXISTS default")
        spark.sql("USE NAMESPACE default")
        return spark
