from pyspark.sql import SparkSession
import pyspark
import pyspark.sql
from pyspark import SparkContext

import sys
import os
import glob

DATA_GENERATION_DIR = 'data/persistent/query_optimizer'
SCRIPT_DIR = os.path.dirname(__file__)
SPARK_RUNTIME_PATH = os.path.join(SCRIPT_DIR, '..', 'data_generators', 'iceberg-spark-runtime-3.5_2.12-1.9.0.jar')

conf = pyspark.SparkConf()
conf.setMaster('local[1]')
conf.set('spark.sql.catalog.iceberg_catalog', 'org.apache.iceberg.spark.SparkCatalog')
conf.set('spark.sql.catalog.iceberg_catalog.type', 'hadoop')
conf.set('spark.sql.catalog.iceberg_catalog.warehouse', DATA_GENERATION_DIR)
conf.set('spark.driver.memory', '5g')
conf.set('spark.jars', SPARK_RUNTIME_PATH)
conf.set('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions')
spark = pyspark.sql.SparkSession.builder.config(conf=conf).getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")
spark.sql("USE iceberg_catalog")

# Generate tpch 0.01

import pathlib
import tempfile
import duckdb

spark.sql(
    """
CREATE OR REPLACE TABLE count_star_optimizer (
    id INT,
    name STRING
)
USING iceberg
TBLPROPERTIES (
    'format-version' = '2',
    'write.update.mode' = 'merge-on-read',
    'write.data.partition-columns' = false,
    'write.parquet.write-partition-values' = false
);

"""
)

spark.sql(
    """
    INSERT INTO count_star_optimizer VALUES  (1,'aaa'), (2,'bbb');
    """
)


spark.sql(
    """
CREATE OR REPLACE TABLE count_star_optimizer_with_deletes (
    id INT,
    name STRING
)
USING iceberg
TBLPROPERTIES (
    'format-version' = '2',
    'write.delete.mode' = 'merge-on-read',
    'write.merge.mode' = 'merge-on-read',
    'write.update.mode' = 'merge-on-read',
    'write.data.partition-columns' = false,
    'write.parquet.write-partition-values' = false
);

"""
)

spark.sql(
    """
INSERT INTO count_star_optimizer_with_deletes SELECT * FROM count_star_optimizer;
"""
)

spark.sql(
    """
    DELETE FROM count_star_optimizer_with_deletes where id = 1;
    """
)


