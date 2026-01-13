from pyspark.sql import SparkSession
import pyspark
import pyspark.sql
from pyspark import SparkContext

from ..base import IcebergConnection

import sys
import os

CONNECTION_KEY = 'local'

SCRIPT_DIR = os.path.dirname(__file__)
DATA_GENERATION_DIR = os.path.join(SCRIPT_DIR, '..', '..', '..', '..', 'data', 'generated', 'iceberg', 'spark-local')
SPARK_RUNTIME_PATH = os.path.join(SCRIPT_DIR, '..', '..', 'iceberg-spark-runtime-3.5_2.12-1.9.0.jar')


@IcebergConnection.register(CONNECTION_KEY)
class IcebergSparkLocal(IcebergConnection):
    def __init__(self):
        super().__init__('spark-local', 'iceberg_catalog')
        self.con = self.get_connection()

    def get_connection(self):
        conf = pyspark.SparkConf()
        conf.setMaster('local[*]')
        conf.set('spark.sql.catalog.iceberg_catalog', 'org.apache.iceberg.spark.SparkCatalog')
        conf.set('spark.sql.catalog.iceberg_catalog.type', 'hadoop')
        conf.set('spark.sql.catalog.iceberg_catalog.warehouse', DATA_GENERATION_DIR)
        conf.set('spark.sql.parquet.outputTimestampType', 'TIMESTAMP_MICROS')
        conf.set('spark.driver.memory', '10g')
        conf.set('spark.jars', SPARK_RUNTIME_PATH)
        conf.set('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions')
        conf.set('spark.sql.session.timeZone', 'UTC')
        spark = pyspark.sql.SparkSession.builder.config(conf=conf).getOrCreate()
        sc = spark.sparkContext
        sc.setLogLevel("ERROR")
        spark.sql("USE iceberg_catalog")
        spark.sql("CREATE NAMESPACE IF NOT EXISTS default")
        spark.sql("USE NAMESPACE default")
        return spark
